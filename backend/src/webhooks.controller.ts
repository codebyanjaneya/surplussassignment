import { Body, Controller, HttpCode, Post } from '@nestjs/common';
import { IsNotEmpty, IsObject, IsOptional, IsString, MaxLength } from 'class-validator';
import { EventQueue } from './queue';

export class WebhookDto {
  @IsString() @IsNotEmpty() @MaxLength(255) eventId!: string;
  @IsString() @IsNotEmpty() @MaxLength(255) type!: string;
  @IsOptional() @IsObject() data?: Record<string, unknown>;
}

@Controller('webhooks')
export class WebhooksController {
  constructor(private readonly queue: EventQueue) {}

  /**
   * The event is durably inserted before we respond. 202 for new AND duplicate
   * deliveries; for a duplicate eventId the first payload is kept, later ones ignored.
   */
  @Post()
  @HttpCode(202)
  async receive(@Body() body: WebhookDto) {
    const stored = await this.queue.ingest(body.eventId, body.type, {
      eventId: body.eventId,
      type: body.type,
      data: body.data ?? {},
    });
    return { eventId: body.eventId, status: 'accepted', duplicate: !stored };
  }
}
