import EventEmitter from "events";
import { Consumer } from "sqs-consumer";

export interface ISQSMessage {
  data: any;
  eventName: string;
  retryCount: number;
  messageGroupId?: string;
}

export interface ISQSConsumerMessage {
  Body: string;
}

export interface ISQSMessageOptions {
  delay: number;
}

export interface ISQSQueueCreateOptions {
  delay: string;
  messageRetentionPeriod: string;
}

export interface IEmitOptions {
  partitionKey?: string;
  delay?: number;
  retryCount?: number;
  useLocalEmitter?: boolean;
}

export interface IFailedEventMessage {
  topic: string;
  event: any;
  error?: any;
}

export interface Queue {
  isFifo: boolean;
  isConsuming: boolean;
  consumer?: Consumer;
  url?: string;
  visibilityTimeout?: number;
  batchSize?: number;
}

export interface Topic {
  name: string;
  isFifo: boolean;
  isConsuming: boolean;
  servicePrefix?: string;
  visibilityTimeout?: number;
  batchSize?: number;
}

export interface IEventTopicMap {
  [eventName: string]: Topic;
}

export enum EmitterType {
  SQS = "SQS",
}

export interface IEmitterOptions {
  isConsumer?: boolean;
  eventTopicMap: IEventTopicMap;
  useExternalBroker?: boolean;
  emitterType: EmitterType;
  maxProcessingTime?: number;
  environment: string;
  localEmitter: EventEmitter;
  eventOnFailure?: string;
  region?: string;
  maxRetries?: number;
}

export type EventListener<T> = (...args: T[]) => Promise<void>;

export interface IEmitter {
  initialize(options: IEmitterOptions): Promise<void>;
  initializeConsumer(): Promise<void>;
  emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean>;
  on<T>(eventName: string, listener: EventListener<T>, useLocal?: boolean): void;
  removeAllListener(): void;
  removeListener(eventName: string, listener: EventListener<any>): void;
}
