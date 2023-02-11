import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
import { Message, SQSClientConfig } from "@aws-sdk/client-sqs";

export enum ExchangeType {
  Direct = "Direct",
  Fanout = "Fanout",
}

export interface ISQSMessage {
  data: any;
  eventName: string;
  messageGroupId?: string;
}

export interface ISQSMessageOptions {
  delay: number;
}

export interface ISQSQueueCreateOptions {
  delay: string;
  messageRetentionPeriod: string;
}

export interface IEmitOptions {
  /**
   * use with FIFO Topic/Queue to ensure the ordering of events
   * For SQS client, refer to https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html
   */
  partitionKey?: string;
  /**
   * Delay message for a specific time before which they can be
   * processed. Specify in seconds.
   * Max value is 900
   */
  delay?: number;
  /**
   * Set to true if using local emitter to emit on the
   * local node emitter
   */
  useLocalEmitter?: boolean;
}

export interface IFailedEventMessage {
  topic?: string;
  topicReference?: string;
  event: any;
  error?: any;
}

export interface Queue {
  isFifo: boolean;
  isConsuming: boolean;
  consumer?: Consumer;
  url?: string;
  arn?: string;
  isDLQ?: boolean;
  visibilityTimeout?: number;
  batchSize?: number;
}

export interface Topic {
  name: string;
  /**
   * Set to true if topic is FIFO, default is false
   */
  isFifo: boolean;
  /**
   * Set to true if you are consuming this topic
   */
  isConsuming: boolean;
  /**
   * Prefix to differentiate topics from other
   * services using the same topic name
   */
  servicePrefix: string;
  /**
   * The time for which message won't be available to other
   * consumers when it is received by a consumer
   * Unit: s
   * Default: 360s
   */
  visibilityTimeout?: number;
  /**
   * Default: 10 for consumption
   */
  batchSize?: number;
  /**
   * Maximum number the broker will attempt to retry the message
   * before which it is added to the related DLQ if deadLetterQueueEnabled
   * is true in emitter options
   * Default: 3
   */
  maxRetryCount?: number;

  /**
   * Topic level DLQ specification
   * By default, the value will be whatever is in IEmitterOptions
   */
  deadLetterQueueEnabled?: boolean;
  /**
   * The type of exchange for this topic
   * Direct exchange type uses SQS
   * Fanout exchange type uses SNS + SQS
   */
  exchangeType: ExchangeType;
}

export interface IEventTopicMap {
  /**
   * Map events to topics.
   * Multiple events maybe mapped to the same topic
   */
  [eventName: string]: Topic;
}

export enum EmitterType {
  SQS = "SQS",
  SNS_SQS = "SNS_SQS",
}

export interface IEmitterOptions {
  /**
   * Set to true if you are consuming any topic
   */
  isConsumer?: boolean;
  /**
   * Map of events to topics
   */
  eventTopicMap: IEventTopicMap;
  /**
   * Set to true if using external broker as client like SQS
   */
  useExternalBroker?: boolean;
  emitterType: EmitterType;
  /**
   * Optional, to log slow messages
   * Unit: ms
   * Default: 60000ms
   */
  maxProcessingTime?: number;
  /**
   * local, dev, stag, prod etc
   */
  environment: string;
  /**
   * The local NodeJS Emitter used for logging failed events
   */
  localEmitter: EventEmitter;
  /**
   * An optional event on which failed events will be emitted.
   * These include failures when sending and consuming messages.
   */
  eventOnFailure?: string;
  /**
   * Maximum number of times the broker will retry the message
   * in case of failure in consumption after which it will be
   * moved to a DLQ if deadLetterQueueEnabled is true
   * Default: 3
   */
  maxRetries?: number;
  /**
   * Optional SQS Client config used by message producer
   */
  sqsConfig?: SQSClientConfig;
  /**
   * Optional SNS Client config used by message producer
   */
  snsConfig?: SQSClientConfig;
  /**
   * Set to true if you want to use DLQs
   * Every topic will have a DLQ created against it that
   * will be used when maxRetryCount is exceeded for a topic
   */
  deadLetterQueueEnabled?: boolean;
  /**
   * Used as prefix for the any temporary files created
   * and as prefix to any queues created in Fanout mode
   */
  serviceName: string;
  /**
   * Use this to force load topics from external clients
   */
  refreshTopicsCache?: boolean;
}

export type EventListener<T> = (...args: T[]) => Promise<void>;

export type ClientMessage = {
  [ExchangeType.Direct]: Message;
  [ExchangeType.Fanout]: Message;
};

export interface IEmitter {
  initialize(options: IEmitterOptions): Promise<void>;
  emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean>;
  on<T>(
    eventName: string,
    listener: EventListener<T>,
    useLocal?: boolean
  ): void;
  removeAllListener(): void;
  removeListener(eventName: string, listener: EventListener<any>): void;
  /**
   * Use this method to when you need to consume messages by yourself
   * but use the routing logic defined in the broker.
   * @param exchangeType The type of exchange
   * @param message The message received from queue/topic
   * Can be of type corresponding to ClientMessage
   * @param topicUrl Optional queue/topic reference for logging purposes
   */
  processMessage<T extends ExchangeType>(
    exchangeType: T,
    message: ClientMessage[T],
    topicUrl?: string | undefined
  ): Promise<void>;
  /**
   *
   * @param topic The topic object
   * @returns Reference to the topic. In case of SQS, this is
   * the queue arn.
   */
  getTopicReference(topic: Topic): string;
  /**
   * Start consuming messages for consuming topics
   */
  startConsumers(): Promise<void>;
}

export interface ISNSMessage {
  data: any;
  eventName: string;
  messageGroupId?: string;
}

export type SQSMessageHandler = (message: Message) => Promise<void>;