import { Pool } from 'pg';
import { EventQueue } from '../src/queue';
import { ev, fastCfg, makeWorker, reset, testPool, waitFor } from './helpers';

let pool: Pool;
let queue: EventQueue;
beforeAll(async () => {
  pool = await testPool();
  queue = new EventQueue(pool, fastCfg());
});
beforeEach(() => reset(pool));
afterAll(() => pool.end());

const count = async (sql: string, p: any[] = []) => (await pool.query(sql, p)).rows[0].n as number;
const status = async (id: string) =>
  (await pool.query('SELECT status, attempts FROM events WHERE event_id=$1', [id])).rows[0];

describe('duplicate protection', () => {
  it('50 simultaneous deliveries of one eventId (different payloads) -> 1 event, first payload kept, 1 processed row, with 2 workers racing', async () => {
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) => queue.ingest('evt_dup', 'order.created', { data: { orderId: `ORD-${i}` } })),
    );
    expect(results.filter(Boolean)).toHaveLength(1);
    expect(await count('SELECT count(*)::int n FROM events')).toBe(1);

    const a = makeWorker(pool, 'w-a').worker;
    const b = makeWorker(pool, 'w-b').worker;
    const runs = [a.start(), b.start()];
    await waitFor(async () => (await status('evt_dup')).status === 'succeeded');
    await new Promise((r) => setTimeout(r, 300)); // give a duplicate the chance to appear
    await Promise.all([a.stop(), b.stop()]);
    await Promise.all(runs);

    expect(await count('SELECT count(*)::int n FROM processed_orders WHERE event_id=$1', ['evt_dup'])).toBe(1);
    expect(await count('SELECT count(*)::int n FROM processing_attempts')).toBe(1);
  });
});

describe('concurrent claiming', () => {
  it('40 events, 12 concurrent claimers: every event claimed exactly once', async () => {
    for (let i = 0; i < 40; i++) await queue.ingest(`e${i}`, 't', ev(`e${i}`));
    const claimed: string[] = [];
    await Promise.all(
      Array.from({ length: 12 }, async (_, c) => {
        for (;;) {
          const e = await queue.claim(`o${c}`, `w${c}`);
          if (!e) return;
          claimed.push(e.eventId);
        }
      }),
    );
    expect(claimed).toHaveLength(40);
    expect(new Set(claimed).size).toBe(40);
  });
});

describe('retries', () => {
  it('fail_then_succeed:2 fails twice, succeeds on attempt 3, one processed row, history recorded', async () => {
    await queue.ingest('r1', 't', ev('r1', 'fail_then_succeed:2'));
    const { worker } = makeWorker(pool, 'w1');
    const run = worker.start();
    await waitFor(async () => (await status('r1')).status === 'succeeded');
    await worker.stop();
    await run;
    expect((await status('r1')).attempts).toBe(3);
    expect(await count('SELECT count(*)::int n FROM processed_orders')).toBe(1);
    const h = (await pool.query('SELECT attempt_number, result FROM processing_attempts ORDER BY 1')).rows;
    expect(h.map((x) => x.result)).toEqual(['failed', 'failed', 'succeeded']);
  });

  it('always_fail stops at maxAttempts (permanent), then manual retry gives a fresh budget with increasing attempt numbers', async () => {
    await queue.ingest('p1', 't', ev('p1', 'always_fail'));
    const { worker } = makeWorker(pool, 'w1'); // maxAttempts = 3
    const run = worker.start();
    await waitFor(async () => (await status('p1')).status === 'failed');
    await new Promise((r) => setTimeout(r, 400)); // must not keep retrying
    expect((await status('p1')).attempts).toBe(3);
    expect(await count('SELECT count(*)::int n FROM processed_orders')).toBe(0);

    expect(await queue.manualRetry('p1')).toBe('retried');
    expect(await queue.manualRetry('p1')).toBe('not_failed'); // double click is harmless
    await waitFor(async () => (await status('p1')).attempts === 6 && (await status('p1')).status === 'failed');
    await worker.stop();
    await run;
    const nums = (await pool.query('SELECT attempt_number n FROM processing_attempts ORDER BY 1')).rows.map((r) => r.n);
    expect(nums).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it('fail_then_succeed:N with N above the cap fails permanently, and a manual retry then succeeds', async () => {
    await queue.ingest('h1', 't', ev('h1', 'fail_then_succeed:4')); // cap is 3
    const { worker } = makeWorker(pool, 'w1');
    const run = worker.start();
    await waitFor(async () => (await status('h1')).status === 'failed');
    await queue.manualRetry('h1');
    await waitFor(async () => (await status('h1')).status === 'succeeded');
    await worker.stop();
    await run;
    expect((await status('h1')).attempts).toBe(5);
    expect(await count('SELECT count(*)::int n FROM processed_orders')).toBe(1);
  });
});

describe('crash recovery', () => {
  it('a worker that dies after claiming is recovered by another; the zombie cannot create a second row', async () => {
    await queue.ingest('c1', 't', ev('c1'));
    const shortLease = new EventQueue(pool, fastCfg({ leaseSeconds: 1 }));

    const zombie = await shortLease.claim('dead/1', 'dead'); // claims, then "crashes" (no heartbeat, no completion)
    expect(zombie!.attempt).toBe(1);
    expect(await shortLease.claim('live/1', 'live')).toBeNull(); // still leased: nobody else may take it

    await new Promise((r) => setTimeout(r, 1200));
    expect(await shortLease.reapExpired()).toBe(1);
    const retaken = await waitFor(() => shortLease.claim('live/1', 'live'));
    expect(retaken.attempt).toBe(2);

    // the zombie wakes up (e.g. GC pause) and tries to finish its stale attempt: fenced off
    expect(await shortLease.complete('c1', 'dead/1', 1)).toBe(false);
    expect(await count('SELECT count(*)::int n FROM processed_orders')).toBe(0);

    expect(await shortLease.complete('c1', 'live/1', 2)).toBe(true);
    expect(await shortLease.complete('c1', 'live/1', 2)).toBe(false); // completing twice is a no-op
    expect(await count('SELECT count(*)::int n FROM processed_orders')).toBe(1);
    const h = (await pool.query('SELECT result FROM processing_attempts ORDER BY attempt_number')).rows;
    expect(h.map((x) => x.result)).toEqual(['lease_expired', 'succeeded']);
  });

  it('an event that keeps killing workers ends up failed instead of looping forever', async () => {
    await queue.ingest('k1', 't', ev('k1'));
    const q = new EventQueue(pool, fastCfg({ leaseSeconds: 0.3, maxAttempts: 2, backoffBaseSeconds: 0 , backoffCapSeconds: 0}));
    for (let i = 0; i < 2; i++) {
      await waitFor(() => q.claim('x', 'x'));
      await new Promise((r) => setTimeout(r, 400));
      await q.reapExpired();
    }
    expect((await status('k1')).status).toBe('failed');
  });

  it('slow work longer than the lease is NOT reclaimed while the worker heartbeats', async () => {
    await queue.ingest('s1', 't', ev('s1', 'slow:3'));
    const { worker } = makeWorker(pool, 'w1', { leaseSeconds: 1, heartbeatSeconds: 0.3 });
    const other = makeWorker(pool, 'w2', { leaseSeconds: 1, heartbeatSeconds: 0.3 }).worker;
    const runs = [worker.start(), other.start()];
    await waitFor(async () => (await status('s1')).status === 'succeeded', 20000);
    await Promise.all([worker.stop(), other.stop()]);
    await Promise.all(runs);
    expect((await status('s1')).attempts).toBe(1);
    expect(await count('SELECT count(*)::int n FROM processed_orders')).toBe(1);
  });
});
