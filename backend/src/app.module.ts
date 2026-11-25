import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { WebhooksController } from './webhooks.controller';
import { EventsController } from './events.controller';
import { loadConfig } from './config';
import { createPool } from './db';
import { EventQueue } from './queue';

@Module({
  controllers: [WebhooksController, EventsController],
  providers: [
    { provide: 'CONFIG', useFactory: () => loadConfig() },
    {
      provide: 'POOL',
      inject: ['CONFIG'],
      useFactory: (cfg: ReturnType<typeof loadConfig>) => createPool(cfg, 10),
    },
    {
      provide: EventQueue,
      inject: ['POOL', 'CONFIG'],
      useFactory: (pool: any, cfg: any) => new EventQueue(pool, cfg),
    },
  ],
})
export class AppModule {}
