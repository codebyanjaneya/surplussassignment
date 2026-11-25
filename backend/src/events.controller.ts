import { Controller, Get, HttpException, Param, Post, Query } from '@nestjs/common';
import { Pool } from 'pg';
import { Inject } from '@nestjs/common';
import { EventQueue } from './queue';

const EVENT_SELECT = `
  SELECT e.event_id AS "eventId", e.type, e.payload, e.status, e.attempts,
         e.attempts - e.attempts_base AS "attemptsInBudget",
         e.next_run_at AS "nextRunAt", e.lease_owner AS "leaseOwner",
         e.lease_expires_at AS "leaseExpiresAt", e.last_error AS "lastError",
         e.received_at AS "receivedAt", e.updated_at AS "updatedAt",
         COALESCE((
           SELECT json_agg(json_build_object(
                    'attemptNumber', a.attempt_number, 'workerId', a.worker_id,
                    'startedAt', a.started_at, 'finishedAt', a.finished_at,
                    'result', a.result, 'error', a.error) ORDER BY a.attempt_number)
           FROM processing_attempts a WHERE a.event_id = e.event_id
         ), '[]'::json) AS history
  FROM events e`;

@Controller()
export class EventsController {
  constructor(
    private readonly queue: EventQueue,
    @Inject('POOL') private readonly pool: Pool,
  ) {}

  @Get('events')
  async list(@Query('limit') limit?: string) {
    const n = Math.min(Math.max(parseInt(limit ?? '100', 10) || 100, 1), 1000);
    const r = await this.pool.query(`${EVENT_SELECT} ORDER BY e.received_at DESC, e.event_id LIMIT $1`, [n]);
    return r.rows;
  }

  @Get('events/:id')
  async get(@Param('id') id: string) {
    const r = await this.pool.query(`${EVENT_SELECT} WHERE e.event_id = $1`, [id]);
    if (r.rowCount === 0) throw new HttpException('event not found', 404);
    return r.rows[0];
  }

  @Post('events/:id/retry')
  async retry(@Param('id') id: string) {
    const res = await this.queue.manualRetry(id);
    if (res === 'not_found') throw new HttpException('event not found', 404);
    if (res === 'not_failed') throw new HttpException('only permanently failed events can be retried', 409);
    return { eventId: id, status: 'pending' };
  }

  @Get('processed-orders')
  async processedOrders() {
    const r = await this.pool.query(
      `SELECT order_id AS "orderId", event_id AS "eventId", processed_at AS "processedAt"
       FROM processed_orders ORDER BY processed_at DESC LIMIT 1000`,
    );
    return r.rows;
  }

  @Get('stats')
  async stats() {
    const r = await this.pool.query(
      `SELECT status, count(*)::int AS n FROM events GROUP BY status`,
    );
    const out: Record<string, number> = { pending: 0, processing: 0, succeeded: 0, failed: 0 };
    for (const row of r.rows) out[row.status] = row.n;
    const po = await this.pool.query('SELECT count(*)::int AS n FROM processed_orders');
    return { ...out, processedOrders: po.rows[0].n };
  }
}
