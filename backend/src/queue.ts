import { Pool } from 'pg';
import { Config } from './config';

export interface ClaimedEvent {
  eventId: string;
  type: string;
  payload: any;
  attempt: number;
}

/**
 * PostgreSQL-backed queue. All timing uses the database clock (now()) so skew
 * between containers cannot affect lease expiry.
 *
 * Invariants:
 *  - An event is claimed by at most one owner at a time: the claim is a single
 *    UPDATE over a `FOR UPDATE SKIP LOCKED` candidate.
 *  - Every state transition out of 'processing' is fenced on
 *    (lease_owner, attempts). A worker whose lease was reaped can no longer
 *    complete/fail the event or write to processed_orders.
 *  - The business write (processed_orders) and the 'succeeded' transition happen
 *    in ONE transaction, so there is no window where the work is done but the event
 *    isn't marked. UNIQUE(event_id) backs this up.
 */
export class EventQueue {
  constructor(
    private readonly pool: Pool,
    private readonly cfg: Pick<
      Config,
      'maxAttempts' | 'backoffBaseSeconds' | 'backoffCapSeconds' | 'leaseSeconds'
    >,
  ) {}

  /** Persist an incoming event. Returns false if eventId already existed (first one wins). */
  async ingest(eventId: string, type: string, payload: unknown): Promise<boolean> {
    const r = await this.pool.query(
      `INSERT INTO events (event_id, type, payload) VALUES ($1, $2, $3)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [eventId, type, JSON.stringify(payload)],
    );
    return r.rowCount === 1;
  }

  /** Claim one runnable event (new attempt row written in the same statement). */
  async claim(owner: string, workerId: string): Promise<ClaimedEvent | null> {
    const r = await this.pool.query(
      `WITH candidate AS (
         SELECT event_id FROM events
         WHERE status = 'pending' AND next_run_at <= now()
         ORDER BY next_run_at, received_at
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       ), claimed AS (
         UPDATE events e SET
           status = 'processing',
           attempts = e.attempts + 1,
           lease_owner = $1,
           lease_expires_at = now() + make_interval(secs => $2),
           updated_at = now()
         FROM candidate c WHERE e.event_id = c.event_id
         RETURNING e.event_id, e.type, e.payload, e.attempts
       ), hist AS (
         INSERT INTO processing_attempts (event_id, attempt_number, worker_id)
         SELECT event_id, attempts, $3 FROM claimed
       )
       SELECT event_id, type, payload, attempts FROM claimed`,
      [owner, this.cfg.leaseSeconds, workerId],
    );
    if (r.rowCount === 0) return null;
    const row = r.rows[0];
    return { eventId: row.event_id, type: row.type, payload: row.payload, attempt: row.attempts };
  }

  /** Extend the lease. false => we no longer own the event and must stop. */
  async heartbeat(eventId: string, owner: string, attempt: number): Promise<boolean> {
    const r = await this.pool.query(
      `UPDATE events SET lease_expires_at = now() + make_interval(secs => $4)
       WHERE event_id = $1 AND status = 'processing' AND lease_owner = $2 AND attempts = $3`,
      [eventId, owner, attempt, this.cfg.leaseSeconds],
    );
    return r.rowCount === 1;
  }

  /** Business action + completion, atomically and fenced. Returns false if the lease was lost. */
  async complete(eventId: string, owner: string, attempt: number): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const fence = await client.query(
        `SELECT payload FROM events
         WHERE event_id = $1 AND status = 'processing' AND lease_owner = $2 AND attempts = $3
         FOR UPDATE`,
        [eventId, owner, attempt],
      );
      if (fence.rowCount === 0) {
        await client.query('ROLLBACK');
        return false;
      }
      const orderId = fence.rows[0].payload?.data?.orderId;
      await client.query(
        `INSERT INTO processed_orders (order_id, event_id) VALUES ($1, $2)
         ON CONFLICT (event_id) DO NOTHING`,
        [orderId === undefined || orderId === null ? null : String(orderId), eventId],
      );
      await client.query(
        `UPDATE events SET status = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
                last_error = NULL, updated_at = now()
         WHERE event_id = $1`,
        [eventId],
      );
      await client.query(
        `UPDATE processing_attempts SET result = 'succeeded', finished_at = now()
         WHERE event_id = $1 AND attempt_number = $2 AND result = 'running'`,
        [eventId, attempt],
      );
      await client.query('COMMIT');
      return true;
    } catch (e) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw e;
    } finally {
      client.release();
    }
  }

  /**
   * Record a failed attempt: back to 'pending' with exponential backoff + jitter, or
   * 'failed' (permanent) once the retry budget is used. Returns the new status, or null
   * if the lease was lost.
   */
  async fail(
    eventId: string,
    owner: string,
    attempt: number,
    error: string,
  ): Promise<'pending' | 'failed' | null> {
    const r = await this.pool.query(
      `WITH upd AS (
         UPDATE events SET
           status = CASE WHEN attempts - attempts_base >= $4 THEN 'failed' ELSE 'pending' END,
           next_run_at = now() + ${backoffSql(6, 7)},
           last_error = $5, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
         WHERE event_id = $1 AND status = 'processing' AND lease_owner = $2 AND attempts = $3
         RETURNING event_id, attempts, status
       ), hist AS (
         UPDATE processing_attempts a SET result = 'failed', finished_at = now(), error = $5
         FROM upd WHERE a.event_id = upd.event_id AND a.attempt_number = upd.attempts
       )
       SELECT status FROM upd`,
      [
        eventId,
        owner,
        attempt,
        this.cfg.maxAttempts,
        error,
        this.cfg.backoffBaseSeconds,
        this.cfg.backoffCapSeconds,
      ],
    );
    return r.rowCount === 0 ? null : r.rows[0].status;
  }

  /**
   * Crash recovery: events whose lease expired are released. The dead attempt is
   * closed as 'lease_expired' and counts against the retry budget, so an event that
   * keeps killing workers cannot loop forever.
   */
  async reapExpired(): Promise<number> {
    const r = await this.pool.query(
      `WITH expired AS (
         SELECT event_id FROM events
         WHERE status = 'processing' AND lease_expires_at < now()
         ORDER BY lease_expires_at
         FOR UPDATE SKIP LOCKED
         LIMIT 100
       ), upd AS (
         UPDATE events e SET
           status = CASE WHEN e.attempts - e.attempts_base >= $1 THEN 'failed' ELSE 'pending' END,
           next_run_at = now() + ${backoffSql(2, 3)},
           last_error = 'worker lost: lease expired during attempt ' || e.attempts,
           lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
         FROM expired x WHERE e.event_id = x.event_id
         RETURNING e.event_id, e.attempts
       ), hist AS (
         UPDATE processing_attempts a SET result = 'lease_expired', finished_at = now(),
                error = 'lease expired (worker presumed crashed or stalled)'
         FROM upd WHERE a.event_id = upd.event_id AND a.attempt_number = upd.attempts
                    AND a.result = 'running'
       )
       SELECT count(*)::int AS n FROM upd`,
      [this.cfg.maxAttempts, this.cfg.backoffBaseSeconds, this.cfg.backoffCapSeconds],
    );
    return r.rows[0].n;
  }

  /** Manual retry of a permanently failed event: fresh budget, attempt numbers keep growing. */
  async manualRetry(eventId: string): Promise<'retried' | 'not_found' | 'not_failed'> {
    const r = await this.pool.query(
      `UPDATE events SET status = 'pending', attempts_base = attempts, next_run_at = now(),
              updated_at = now()
       WHERE event_id = $1 AND status = 'failed'
       RETURNING event_id`,
      [eventId],
    );
    if (r.rowCount === 1) return 'retried';
    const e = await this.pool.query('SELECT 1 FROM events WHERE event_id = $1', [eventId]);
    return e.rowCount === 0 ? 'not_found' : 'not_failed';
  }
}

/**
 * Backoff = min(cap, base * 2^(attemptsInBudget-1)) * (0.8..1.2 jitter), in seconds.
 * `base`/`cap` are the positional parameter numbers holding the config values.
 * Expects `attempts` and `attempts_base` in scope (the UPDATE target).
 */
const backoffSql = (base: number, cap: number) => `make_interval(secs =>
  least($${cap}::float8, $${base}::float8 * power(2, greatest(attempts - attempts_base - 1, 0)))
  * (0.8 + random() * 0.4))`;
