import { Pool } from 'pg';
import { Config } from './config';
import { migrate } from './schema';

export async function createPool(cfg: Pick<Config, 'databaseUrl'>, max = 10): Promise<Pool> {
  const pool = new Pool({ connectionString: cfg.databaseUrl, max });
  pool.on('error', (e) => console.error('[pg] idle client error', e.message));
  // Postgres may still be starting when containers come up together.
  for (let i = 0; ; i++) {
    try {
      await migrate(pool);
      return pool;
    } catch (e: any) {
      if (i >= 30) throw e;
      console.log(`[db] waiting for postgres (${e.code ?? e.message})`);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}
