const num = (name: string, def: number): number => {
  const v = Number(process.env[name]);
  return Number.isFinite(v) && process.env[name] !== '' && process.env[name] !== undefined ? v : def;
};

export interface Config {
  databaseUrl: string;
  port: number;
  /** Max attempts per retry budget (a manual retry starts a fresh budget). */
  maxAttempts: number;
  backoffBaseSeconds: number;
  backoffCapSeconds: number;
  /** How long a claim is valid without a heartbeat. */
  leaseSeconds: number;
  heartbeatSeconds: number;
  pollMs: number;
  reapIntervalMs: number;
  workerConcurrency: number;
  workerId: string;
}

export function loadConfig(overrides: Partial<Config> = {}): Config {
  return {
    databaseUrl:
      process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/webhooks',
    port: num('PORT', 3001),
    maxAttempts: num('MAX_ATTEMPTS', 5),
    backoffBaseSeconds: num('BACKOFF_BASE_SECONDS', 2),
    backoffCapSeconds: num('BACKOFF_CAP_SECONDS', 60),
    leaseSeconds: num('LEASE_SECONDS', 15),
    heartbeatSeconds: num('HEARTBEAT_SECONDS', 5),
    pollMs: num('POLL_MS', 500),
    reapIntervalMs: num('REAP_INTERVAL_MS', 2000),
    workerConcurrency: num('WORKER_CONCURRENCY', 4),
    workerId: process.env.WORKER_ID ?? `worker-${process.pid}`,
    ...overrides,
  };
}
