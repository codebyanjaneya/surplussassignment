import { Pool } from 'pg';
import { Config, loadConfig } from '../src/config';
import { createPool } from '../src/db';
import { EventQueue } from '../src/queue';
import { Worker } from '../src/worker/worker';

const TEST_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/webhooks_test';

export async function testPool(): Promise<Pool> {
  // create the test database on demand
  const u = new URL(TEST_URL);
  const dbName = u.pathname.slice(1);
  u.pathname = '/postgres';
  const admin = new Pool({ connectionString: u.toString(), max: 1 });
  const exists = await admin.query('SELECT 1 FROM pg_database WHERE datname = $1', [dbName]);
  if (exists.rowCount === 0) await admin.query(`CREATE DATABASE "${dbName}"`);
  await admin.end();
  const pool = await createPool({ databaseUrl: TEST_URL }, 30);
  return pool;
}

export async function reset(pool: Pool) {
  await pool.query('TRUNCATE processing_attempts, processed_orders, events CASCADE');
}

export const fastCfg = (o: Partial<Config> = {}): Config =>
  loadConfig({
    databaseUrl: TEST_URL,
    maxAttempts: 3,
    backoffBaseSeconds: 0.05,
    backoffCapSeconds: 0.2,
    leaseSeconds: 2,
    heartbeatSeconds: 0.5,
    pollMs: 20,
    reapIntervalMs: 100,
    workerConcurrency: 4,
    ...o,
  });

export const quiet = () => undefined;

export function makeWorker(pool: Pool, id: string, o: Partial<Config> = {}) {
  const cfg = fastCfg({ workerId: id, ...o });
  return { worker: new Worker(new EventQueue(pool, cfg), cfg, quiet), cfg };
}

export async function waitFor<T>(fn: () => Promise<T | false | null | undefined>, ms = 15000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > end) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
}

export const ev = (id: string, simulate?: string, orderId = `ORD-${id}`) => ({
  eventId: id,
  type: 'order.created',
  data: { orderId, customerId: 'CUS-1', ...(simulate ? { simulate } : {}) },
});
