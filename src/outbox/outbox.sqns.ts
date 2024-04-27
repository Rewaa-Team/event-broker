import {
  IEmitOptions,
  IBatchEmitOptions,
  IFailedEmitBatchMessage,
} from "src/types";
import {
  IOutbox,
  OutboxConfig,
  OutboxEvent,
  OutboxEventFailureResponse,
  OutboxEventPayload,
  OutboxEventStatus,
} from "./types";
import { ulid } from "ulid";

export class Outbox implements IOutbox {
  constructor(private readonly config: OutboxConfig) {}
  async updateEvents(events: OutboxEvent[]): Promise<void> {
    return await this.config.save({
      events,
      config: {}
    });
  }
  handleEvent(event: OutboxEvent, errorReason?: unknown): OutboxEvent {
    if (!this.config) {
      throw new Error("Outbox config is not configured");
    }
    const clonedEvent = structuredClone(event);
    if (errorReason) {
      clonedEvent.status = OutboxEventStatus.Error;
      clonedEvent.error = errorReason;
    } else {
      clonedEvent.status = OutboxEventStatus.Processed;
    }

    return clonedEvent;
  }

  handleBatchEvent(
    event: OutboxEvent,
    response: IFailedEmitBatchMessage[],
    errorReason?: unknown
  ): OutboxEvent {
    const clonedEvent = structuredClone(event);
    if (errorReason) {
      clonedEvent.status = OutboxEventStatus.Error;
      clonedEvent.error = errorReason;
    } else {
      if (response.length === 0) {
        clonedEvent.status = OutboxEventStatus.Processed;
      } else if (response.length === clonedEvent.payload.length) {
        clonedEvent.status = OutboxEventStatus.Failed;
      } else {
        clonedEvent.status = OutboxEventStatus.PartiallyProcessed;
      }
      clonedEvent.failureResponse = response.map(
        (failedMessage): OutboxEventFailureResponse => ({
          id: failedMessage.id,
          code: failedMessage.code,
          message: failedMessage.message,
        })
      );
    }
    return clonedEvent;
  }

  async createEvent(
    eventName: string,
    options: IEmitOptions | IBatchEmitOptions,
    payload?: any,
    isBatch = false
  ): Promise<OutboxEvent> {
    const { outboxData: _outboxData, ...emitOptions } = options;
    const outboxEvent: OutboxEvent = {
      topicName: eventName,
      id: ulid(),
      payload,
      options: emitOptions,
      isBatch,
      failureResponse: [],
      error: null,
      status: OutboxEventStatus.Pending,
    };
    await this.config.save({
      events: [outboxEvent],
      config: options.outboxData!,
    });
    return outboxEvent;
  }

  async getOutboxEvents(ids: string[]): Promise<OutboxEvent[]> {
    return await this.config.getOutboxEvents(ids);
  }
}
