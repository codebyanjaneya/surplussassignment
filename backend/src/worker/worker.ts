import { Config } from '../config';
import { EventQueue } from '../queue';
import { LeaseLostError, parseSimulate, runSimulation } from '../simulate';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * One worker process = `workerConcurrency` independent claim loops plus a reaper loop.
 * Coordination between processes happens only through the database.
 */
export class Worker {
  private stopping = false;
  private inflight = 0;

  constructor(
    private readonly queue: EventQueue,
    private readonly cfg: Pick<
      Config,
      'workerId' | 'workerConcurrency' | 'pollMs' | 'reapIntervalMs' | 'heartbeatSeconds'
    >,
    private readonly log: (m: string) => void = (m) => console.log(m),
  ) {}

  start(): Promise<void[]> {
    const loops: Promise<void>[] = [this.reaperLoop()];
    for (let slot = 1; slot <= this.cfg.workerConcurrency; slot++) loops.push(this.claimLoop(slot));
    return Promise.all(loops);
  }

  async stop(graceMs = 8000): Promise<void> {
    this.stopping = true;
    const deadline = Date.now() + graceMs;
    while (this.inflight > 0 && Date.now() < deadline) await sleep(100);
  }

  private async reaperLoop() {
    while (!this.stopping) {
      try {
        const n = await this.queue.reapExpired();
        if (n > 0) this.log(`[${this.cfg.workerId}] reaped ${n} expired lease(s)`);
      } catch (e: any) {
        this.log(`[${this.cfg.workerId}] reaper error: ${e.message}`);
      }
      await sleep(this.cfg.reapIntervalMs);
    }
  }

  private async claimLoop(slot: number) {
    const owner = `${this.cfg.workerId}/${slot}`;
    while (!this.stopping) {
      let ev;
      try {
        ev = await this.queue.claim(owner, this.cfg.workerId);
      } catch (e: any) {
        this.log(`[${owner}] claim error: ${e.message}`);
        await sleep(this.cfg.pollMs * 2);
        continue;
      }
      if (!ev) {
        await sleep(this.cfg.pollMs);
        continue;
      }
      this.inflight++;
      try {
        await this.process(owner, ev.eventId, ev.attempt, ev.payload);
      } catch (e: any) {
        this.log(`[${owner}] unexpected error on ${ev.eventId}: ${e.message}`);
      } finally {
        this.inflight--;
      }
    }
  }

  private async process(owner: string, eventId: string, attempt: number, payload: any) {
    this.log(`[${owner}] start ${eventId} attempt ${attempt}`);
    const abort = new AbortController();
    // Heartbeat keeps the lease alive for long-running work (e.g. slow:N > lease).
    const hb = setInterval(async () => {
      try {
        if (!(await this.queue.heartbeat(eventId, owner, attempt))) abort.abort();
      } catch (e: any) {
        this.log(`[${owner}] heartbeat error (will retry): ${e.message}`);
      }
    }, this.cfg.heartbeatSeconds * 1000);

    try {
      await runSimulation(parseSimulate(payload?.data?.simulate), attempt, abort.signal);
    } catch (e: any) {
      clearInterval(hb);
      if (e instanceof LeaseLostError) {
        this.log(`[${owner}] lost lease on ${eventId}, abandoning`);
        return;
      }
      const status = await this.queue.fail(eventId, owner, attempt, e.message);
      this.log(`[${owner}] ${eventId} attempt ${attempt} failed (${e.message}) -> ${status ?? 'lease lost'}`);
      return;
    }
    clearInterval(hb);
    if (abort.signal.aborted) return;
    const ok = await this.queue.complete(eventId, owner, attempt);
    this.log(`[${owner}] ${eventId} attempt ${attempt} ${ok ? 'succeeded' : 'finished but lease was lost (discarded)'}`);
  }
}
