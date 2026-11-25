export type Simulation =
  | { kind: 'ok' }
  | { kind: 'fail_then_succeed'; n: number }
  | { kind: 'always_fail' }
  | { kind: 'slow'; seconds: number };

/**
 * Parses data.simulate. Anything absent or unrecognised behaves like "ok"
 * (documented in the README) so a malformed value can never wedge an event.
 */
export function parseSimulate(raw: unknown): Simulation {
  if (typeof raw !== 'string') return { kind: 'ok' };
  const s = raw.trim();
  if (s === 'always_fail') return { kind: 'always_fail' };
  let m = /^fail_then_succeed:(\d+)$/.exec(s);
  if (m) return { kind: 'fail_then_succeed', n: Number(m[1]) };
  m = /^slow:(\d+(?:\.\d+)?)$/.exec(s);
  if (m) return { kind: 'slow', seconds: Number(m[1]) };
  return { kind: 'ok' };
}

export class LeaseLostError extends Error {
  constructor() {
    super('lease lost');
  }
}

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    if (signal?.aborted) return reject(new LeaseLostError());
    const t = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      reject(new LeaseLostError());
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });

/**
 * Stand-in for the unreliable downstream call. `attemptNumber` is the global,
 * never-reset attempt number, so fail_then_succeed:N means "attempts 1..N fail".
 */
export async function runSimulation(
  sim: Simulation,
  attemptNumber: number,
  signal?: AbortSignal,
): Promise<void> {
  switch (sim.kind) {
    case 'ok':
      return;
    case 'always_fail':
      throw new Error('simulated permanent failure (always_fail)');
    case 'fail_then_succeed':
      if (attemptNumber <= sim.n) {
        throw new Error(`simulated transient failure (attempt ${attemptNumber} of ${sim.n} failing)`);
      }
      return;
    case 'slow':
      await sleep(sim.seconds * 1000, signal);
      return;
  }
}
