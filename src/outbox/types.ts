import {
  ConsumeOptions,
  IBatchEmitOptions,
  IEmitOptions,
  IFailedEmitBatchMessage,
} from "../types";

export interface OutboxConfig {
  /**
   * saving (insert or update) the events to the consumer service's outbox table
   */
  save: (outboxData: OutboxData<any>) => Promise<void>;

  /**
   * gets events usings ids from outbox table on consumer service
   * @param ids ulids of the events to get
   */
  getOutboxEvents: (ids: string[]) => Promise<OutboxEvent[]>;

  /**
   * An optional name of the queue that processes the outbox events
   */
  consumerName?: string;

  /**
   * The delay before the outbox events are sent to outbox queue
   *
   * unit: s
   * default: 5s
   */
  delay?: number;

  /**
   * Optional consume options for the outbox queue
   */
  consumeOptions?: ConsumeOptions;
}

export interface OutboxData<T extends any> {
  events: OutboxEvent[];
  config: T;
}

export enum OutboxEventStatus {
  Error = "Error", // All events did not emit
  Pending = "Pending", // All events yet to emit
  Processed = "Processed", // All events emitted
  Failed = "Failed", // All events in a batch fail
  PartiallyProcessed = "PartiallyProcessed", // Some events in a batch fail
}

export interface OutboxEvent {
  /**
   * ulid
   */
  id: string;
  topicName: string;
  payload?: any;
  options: Omit<IEmitOptions, "outboxData">;
  isBatch: boolean;
  error?: unknown;
  failureResponse?: OutboxEventFailureResponse[];
  status: OutboxEventStatus;
}

export interface OutboxEventFailureResponse {
  id?: string;
  code?: string;
  message: any;
}

export interface OutboxEventPayload {
  /**
   * ulid array
   */
  ids: string[];
  isFifo: boolean;
  isBatch: boolean;
}

export interface IOutbox {
  updateEvents: (events: OutboxEvent[]) => Promise<void>;
  handleEvent: (event: OutboxEvent, errorReason?: unknown) => OutboxEvent;
  handleBatchEvent: (
    event: OutboxEvent,
    response: IFailedEmitBatchMessage[],
    errorReason?: unknown
  ) => OutboxEvent;
  createEvent: (
    eventName: string,
    options: IEmitOptions | IBatchEmitOptions,
    payload?: any,
    isBatch?: boolean
  ) => Promise<OutboxEvent>;
  /**
   * gets events usings ids from outbox table on consumer service
   * @param ids ulids of the events
   */
  getOutboxEvents: (ids: string[]) => Promise<OutboxEvent[]>;
}
