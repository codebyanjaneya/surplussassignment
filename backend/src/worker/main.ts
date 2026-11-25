import { loadConfig } from '../config';
import { createPool } from '../db';
import { EventQueue } from '../queue';
import { Worker } from './worker';

async function main() {
  const cfg = loadConfig();
  const pool = await createPool(cfg, cfg.workerConcurrency + 3);
  const worker = new Worker(new EventQueue(pool, cfg), cfg);
  console.log(
    `[${cfg.workerId}] up: concurrency=${cfg.workerConcurrency} lease=${cfg.leaseSeconds}s maxAttempts=${cfg.maxAttempts}`,
  );
  const shutdown = async () => {
    await worker.stop();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  await worker.start();
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});
