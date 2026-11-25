import { Pool } from 'pg';

/**
 * Schema is created idempotently on startup by every process, serialised with an
 * advisory lock so concurrent boots (api + workers) don't race on CREATE TABLE.
 */
export const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS events (
  event_id         text PRIMARY KEY,
  type             text NOT NULL,
  payload          jsonb NOT NULL,
  status           text NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','processing','succeeded','failed')),
  -- total attempts ever started; monotonically increasing, never reset
  attempts         int NOT NULL DEFAULT 0,
  -- value of attempts when the current retry budget began (moved by manual retry)
  attempts_base    int NOT NULL DEFAULT 0,
  next_run_at      timestamptz NOT NULL DEFAULT now(),
  lease_owner      text,
  lease_expires_at timestamptz,
  last_error       text,
  received_at      timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_pending_idx ON events (next_run_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS events_lease_idx   ON events (lease_expires_at) WHERE status = 'processing';

-- Record of real work done. UNIQUE(event_id) is the last line of defence for exactly-once.
CREATE TABLE IF NOT EXISTS processed_orders (
  order_id     text,
  event_id     text NOT NULL UNIQUE REFERENCES events(event_id),
  processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processing_attempts (
  id             bigserial PRIMARY KEY,
  event_id       text NOT NULL REFERENCES events(event_id),
  attempt_number int NOT NULL,
  worker_id      text NOT NULL,
  started_at     timestamptz NOT NULL DEFAULT now(),
  finished_at    timestamptz,
  result         text NOT NULL DEFAULT 'running'
                 CHECK (result IN ('running','succeeded','failed','lease_expired')),
  error          text,
  UNIQUE (event_id, attempt_number)
);
`;

export async function migrate(pool: Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('SELECT pg_advisory_lock(727274)');
    await client.query(SCHEMA_SQL);
    await client.query('SELECT pg_advisory_unlock(727274)');
  } finally {
    client.release();
  }
}
