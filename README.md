# Reliable Webhook Processor

NestJS API + PostgreSQL-backed workers + Next.js operations page. TypeScript throughout.

```bash
docker compose up --build        # db, backend (:3001), worker-1, worker-2, frontend (:3000)
./scripts/demo.sh all            # or: duplicate | temp-failure | perm-failure | parallel | crash | burst
docker compose run --rm backend npm test    # integration tests (own database: webhooks_test)
```

Ops page: http://localhost:3000. API: `POST /webhooks`, `GET /events`, `GET /events/:id`,
`POST /events/:id/retry`, `GET /processed-orders`, `GET /stats`.
The demo needs `curl`, `python3` and `docker` on the host (`crash` runs `docker kill` on a worker).
Single event by hand:
```bash
curl -i -X POST localhost:3001/webhooks -H 'content-type: application/json' \
  -d '{"eventId":"evt_1","type":"order.created","data":{"orderId":"ORD-1","simulate":"fail_then_succeed:2"}}'
```

## Architecture

```
POST /webhooks ─► INSERT events ... ON CONFLICT DO NOTHING ─► 202 (new or duplicate)
                         │
   worker-1 (4 slots) ◄──┤  claim = UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)
   worker-2 (4 slots) ◄──┘  state: pending → processing → succeeded | pending (retry) | failed
```

* **No Redis.** Postgres is already the source of truth, and the claim, the attempt history and the
  business write can share one transaction. A separate broker would make "marked done" and "work
  done" two systems that can disagree.
* **Claim.** One statement picks a due `pending` event with `FOR UPDATE SKIP LOCKED`, sets
  `status='processing'`, increments `attempts`, takes a **lease** (`lease_owner`, `lease_expires_at`)
  and inserts the `processing_attempts` row. Workers never block each other.
* **Heartbeat.** While processing, a worker extends its lease every `HEARTBEAT_SECONDS`. If the
  heartbeat finds the lease gone, it abandons the work.
* **Completion.** One transaction: lock the event row with a fence
  (`lease_owner = me AND attempts = my attempt AND status='processing'`), insert into
  `processed_orders`, mark `succeeded`, close the attempt. No fence match → rollback, nothing written.
* **Failure.** Fenced update to `pending` with `next_run_at = now() + min(cap, base·2^(n-1))` ±20% jitter,
  or to `failed` once `attempts - attempts_base >= MAX_ATTEMPTS`.
* **Crash recovery.** Every worker runs a reaper: `processing` events with an expired lease are
  released (attempt closed as `lease_expired`, counted against the budget, backoff applied) and
  picked up by any worker. All time comparisons use the database clock.
* **Manual retry.** `failed → pending`, `attempts_base = attempts`: fresh budget, attempt numbers keep growing.

Data: `events` (state + lease), `processed_orders(order_id, event_id UNIQUE, processed_at)`,
`processing_attempts` (event, attempt number, worker, start/finish, result, error).
The schema is created idempotently at startup under an advisory lock (no migration tool; see limitations).

## Libraries
NestJS (required) with `class-validator` for DTO validation; `pg` with hand-written SQL, because the
correctness here *is* the SQL (`SKIP LOCKED`, fenced updates, CTEs) and an ORM would hide it;
Jest + ts-jest for tests; Next.js (required) with a small route handler proxying `/api/*` to the backend
at runtime, so the browser only talks to one origin.

## Correctness guarantees
1. **Exactly one `processed_orders` row per event.** Three layers: `eventId` is the PK of `events`
   (duplicates and simultaneous submissions collapse on `ON CONFLICT`); the claim is exclusive; the
   row is written only inside the fenced completion transaction, and `UNIQUE(event_id)` backstops it.
2. **A crash after the business action but before "complete" cannot leave a gap**, because the action
   and the completion are the same transaction: both commit or neither does. A crash before commit
   leaves no row, and the retry writes it.
3. **A zombie cannot corrupt state.** A worker that stalls past its lease and wakes up later fails the
   fence (its `attempts` number is stale) and its completion/failure is a no-op. Covered by a test.
4. **No event stays in `processing` forever**: lease expiry + reaper. An event that keeps killing workers
   still consumes attempts and ends `failed` rather than looping forever.
5. **Slow work longer than the lease is safe** while the worker is alive (heartbeat).
   `fail_then_succeed:N` with N above the cap ends `failed`; a manual retry then succeeds. Tested.

## Known limitations
* The simulated downstream call (`slow`, `fail_then_succeed`) runs *before* the DB transaction. A real
  external side effect (email, payment) is at-least-once: if a worker dies after calling it but before
  committing, the retry calls it again. It would need an idempotency key on the downstream call.
  The `processed_orders` guarantee is exact; side effects outside Postgres cannot be.
* If a worker is alive but cannot reach Postgres for longer than `LEASE_SECONDS`, another worker will
  reclaim the event while the first is still executing the simulated work. The first one's commit is then
  rejected by the fence, so there is no duplicate row, but the non-DB part of the work runs twice.
* Crash detection latency is up to `LEASE_SECONDS` (15s) + reaper interval (2s) + backoff. A killed worker's
  `slow:60` event restarts from zero; there is no checkpointing.
* Lease-expired attempts count against the retry budget, so a flaky host can burn attempts on an
  innocent event.
* Workers poll (500ms) instead of using `LISTEN/NOTIFY`: up to 500ms of pickup latency, and idle workers
  cost one query per slot per poll.
* Ordering is approximate (`next_run_at`), not guaranteed, and nothing orders events per customer/order.
* Malformed `simulate` values (e.g. `"banana"`) are treated as `ok`.
* A duplicate `eventId` with a different payload is silently dropped (as specified), without logging the difference.
* The schema is `CREATE TABLE IF NOT EXISTS`: fine for this scope, but there is no versioned migration path.
* No auth on the API or the retry endpoint. The ops page polls every 1.5s (no websockets) and shows the
  latest 200 events.
* **Verification gap:** I ran the tests and every demo scenario (including `kill -9` of a worker process and
  the 500-event burst) against a local Postgres with the API and two worker processes started by hand.
  I could not run `docker compose up` itself (no Docker daemon available when I built this), so the
  compose file and Dockerfiles are untested, and `demo.sh crash` (which uses `docker kill`) has only been
  exercised with `kill -9` equivalents.

## Decisions where the brief was open
* `MAX_ATTEMPTS` = total attempts per budget (5), not "retries after the first".
* Attempt numbers are global per event; `fail_then_succeed:N` fails attempts `1..N` by that global number,
  so it keeps its meaning after a manual retry.
* `data.orderId` missing → the event still processes and `order_id` is NULL (the brief gives no schema rule).
* 400 only for a body without a string `eventId`/`type` or with a non-object `data`.
* 2 workers × 4 concurrent slots; each slot is an independent claim loop with its own lease owner.

## What I'd do next
1. A real `docker compose` CI run including a `docker kill` test, since that's the scenario that matters most.
2. An idempotency key passed to the downstream call, so external side effects are safe, not just the DB row.
3. Versioned migrations and `LISTEN/NOTIFY` wake-ups.
4. Metrics/alerts (failed count, oldest pending age, lease expiries) and an event retention policy.
5. Auth on the API, a payload-differs warning for duplicates, pagination on the ops page.
