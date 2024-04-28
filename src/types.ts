import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
import {
  MessageAttributeValue,
  SQSClientConfig,
  Message,
  SendMessageRequest,
  SendMessageBatchRequest,
} from "@aws-sdk/client-sqs";
import {
  PublishBatchInput,
  PublishInput,
  SNSClientConfig,
} from "@aws-sdk/client-sns";
import { LambdaClientConfig } from "@aws-sdk/client-lambda";
import { OutboxConfig } from "./outbox/types";

export interface Logger {
  error(error: any): void;
  warn(message: any): void;
  debug(message: any): void;
  info(message: any): void;
}

export interface IMessage<T> {
  data: T;
  eventName: string;
  messageGroupId?: string;
  messageAttributes?: { [key: string]: MessageAttributeValue };
  deduplicationId?: string;
  id?: string;
  delay?: number;
}

export type ISQSMessage = IMessage<any>;

export interface ISQSMessageOptions {
  delay: number;
}

type Binary = Buffer | Uint8Array | Blob | string;

export interface IMessageAttributes {
  DataType: "String" | "Number" | "Binary" | "String.Array";
  StringValue?: string;
  BinaryValue?: Uint8Array;
  StringListValues?: string[];
  BinaryListValues?: Uint8Array[];
}

export type ConsumerOptions = Omit<
  Topic,
  "separateConsumerGroup" | "exchangeType" | "consumerGroup"
>;

export interface IEmitOptions {
  /**
   * Set to true when emitting to fifo topic
   *
   * Default is false
   */
  isFifo: boolean;
  /**
   * Use with FIFO Topic/Queue to ensure the ordering of events
   *
   * Refer to https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html
   */
  partitionKey?: string;
  /**
   * For Queue type, message is sent directly to queue.
   * This means that the quotas for SQS apply here for throttling
   *
   * For Fanout, message is sent from Topic to queues.
   * This means that the quotas for SNS apply here for throttling
   *
   * Default is Fanout
   */
  exchangeType: ExchangeType;
  /**
   * Use to identify separate queue
   */
  consumerGroup?: string;
  /**
   * Delay receiving the message on consumer
   *
   * Unit: s
   */
  delay?: number;

  /**
   * Message attributes to be sent along with the message
   */
  MessageAttributes?: { [key: string]: MessageAttributeValue };
  /**
   * Set to a unique id if you want to avoid duplications in
   * a FIFO queue. The same deduplicationId sent within a 5
   * minite interval will be discarded.
   */
  deduplicationId?: string;
  /**
   * Anything that is required by the save function to save the outbox events to the consumer service outbox table
   * e.g transaction, entity manager etc
   */
  outboxData?: Record<string, any>;
}

export type IBatchEmitOptions = Pick<
  IEmitOptions,
  "isFifo" | "exchangeType" | "consumerGroup" | "outboxData"
>;

export type IBatchMessage = Omit<
  IEmitOptions,
  "isFifo" | "exchangeType" | "consumerGroup"
> & {
  data: any;
  /**
   * A batch-level unique id. Used for reporting the result
   * of the batch api
   */
  id: string;
};

export interface IFailedEmitBatchMessage {
  /**
   * The batch-level unique id of the failed message
   */
  id?: string;
  /**
   * An error code representing why the message failed
   */
  code?: string;
  /**
   * An optional message explaining the failure
   */
  message?: string;
  /**
   * A boolean indicating wether the message failed due to sender
   */
  wasSenderFault?: boolean;
}

export interface IFailedConsumerMessages {
  batchItemFailures: {
    itemIdentifier: string;
  }[];
}

export interface IFailedEventMessage {
  topic?: string;
  topicReference?: string;
  event: any;
  error?: any;
  failureType: FailedEventCategory;
  executionContext?: ProcessMessageContext;
}

export interface Queue {
  name: string;
  isFifo: boolean;
  consumer?: Consumer;
  url?: string;
  arn?: string;
  isDLQ?: boolean;
  visibilityTimeout?: number;
  batchSize?: number;
  listenerIsLambda?: boolean;
  topic: Topic;
  allTopics: Topic[];
}

export interface Topic {
  name: string;
  /**
   * Set to true if topic is FIFO, default is false
   */
  isFifo?: boolean;
  /**
   * The time for which message won't be available to other
   * consumers when it is received by a consumer
   *
   * Unit: s
   *
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
   *
   * Default: 3
   */
  maxRetryCount?: number;

  /**
   * Topic level DLQ specification
   *
   * Set to true to create a corresponding DLQ
   */
  deadLetterQueueEnabled?: boolean;
  /**
   * An optional consumer group name
   *
   * Set if you want to use a separate consumer group
   *
   * Used to identify the queue from which the consumer
   * will consume the messages from this topic. When not provided,
   * the broker will subscribe the default queue to this topic. If
   * the default queue is not specified, an error will be thrown.
   * @deprecated Use consumerGroup instead
   */
  separateConsumerGroup?: string;

  /**
   * An optional consumer group specification
   * Use this when consumer configuration is different from the topic
   *
   * When specified, the broker will consume the messages from this
   * otherwise the broker will subscribe the default queue to this topic
   */
  consumerGroup?: ConsumerOptions;

  /**
   * An optional Lambda function specification
   *
   * When specified, the broker will create an event source
   * mapping for the lambda and consumer
   */
  lambdaHandler?: ILambdaHandler;
  /**
   * For Queue type, message is sent directly to queue.
   * This means that the quotas for SQS apply here for throttling
   *
   * For Fanout, message is sent from Topic to queues.
   * This means that the quotas for SNS apply here for throttling
   *
   * Queue exchange type is always consumed via a separate consumer group (queue)
   */
  exchangeType: ExchangeType;
  /**
   * An optional filter policy
   *
   * When specified, the broker will create an event source
   * mapping for the lambda and consumer
   */
  filterPolicy?: { [key: string]: string[] };
  /**
   * Set to true to enable high throughput on FIFO queues
   */
  enableHighThroughput?: boolean;
  /**
   * Retention time for messages in the queue of this topic
   *
   * Valid Values: An integer from 60 seconds (1 minute) to 1,209,600 seconds (14 days)
   *
   * Default is 1 Day
   *
   * Unit: s
   */
  retentionPeriod?: number;
  /**
   * Enable content based deduplication. Enabling it means that for messages that
   * are sent without an explicit DeduplicationId AWS will generate it based on the
   * message body using SHA 256 and use it for deduplication. Enabling this is required
   * for interacting with some AWS services like dropping messages from the event bridge
   * scheduler into the sqs queue
   *
   * Is only effective for Fifo queues
   *
   * Default value is false (off)
   */
  contentBasedDeduplication?: boolean;
}

export interface Hooks {
  /**
   *
   * @param topicName name of the topic on which beforeEmit was
   * executed
   * @param data the data with which emit was called
   * @returns the data with any changes to be done before it's emitted
   */
  beforeEmit?<T>(topicName: string, data: T): Promise<T>;
  /**
   *
   * @param topicName name of the topic on which afterEmit was
   * executed
   * @param data the data with which emit was called
   */
  afterEmit?<T>(topicName: string, data: T): Promise<void>;
  /**
   *
   * @param topicName name of the topic on which beforeConsume was
   * executed
   * @param data the data with which the consumer function will be called
   * @returns the data with any changes to be done before calling the consumer
   * function
   */
  beforeConsume?<T>(topicName: string, data: T): Promise<T>;
  /**
   *
   * @param topicName name of the topic on which afterConsume will be
   * executed
   * @param data the data with which the consumer function was called
   */
  afterConsume?<T>(topicName: string, data: T): Promise<void>;
}

export interface ILambdaHandler {
  /**
   * The complete function name constructed as
   * serviceName-environment-functionName
   */
  functionName: string;
  maximumConcurrency?: number;
}

export type ConsumeOptions = Omit<Topic, "name" | "lambdaHandler"> & {
  useLocal?: boolean;
};

export interface IEmitterOptions {
  /**
   * Set to true if using external broker as client
   */
  useExternalBroker?: boolean;
  /**
   * local, dev, stag, prod etc
   */
  environment: string;
  /**
   * The local NodeJS Emitter used for logging failed events
   */
  localEmitter: EventEmitter;
  /**
   * An optional event on which failed events will be emitted
   *
   * These include failures when sending and consuming messages
   */
  eventOnFailure?: string;
  /**
   * Maximum number of times the broker will retry the message
   * in case of failure in consumption after which it will be
   * moved to a DLQ if deadLetterQueueEnabled is true
   *
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
  snsConfig?: SNSClientConfig;
  /**
   * Optional Lambda Client config used by message producer
   */
  lambdaConfig?: LambdaClientConfig;
  /**
   * Optional default queues options when consuming on a default queue
   *
   * When using default queues, Topics for which a separateConsumerGroup
   * is not specified are consumed from the default queues.
   */
  defaultQueueOptions?: {
    fifo: DefaultQueueOptions;
    standard: DefaultQueueOptions;
  };
  /**
   * Optional AWS Config used by the emitter when useExternalBroker is true
   */
  awsConfig?: {
    region: string;
    accountId: string;
  };
  /**
   * Set to true to enable logging
   */
  log?: boolean;
  /**
   * Set to true to enable local aws
   */
  isLocal?: boolean;
  /**
   * @description Provide this to override the default logger and control
   * log levels and mapping
   */
  logger?: Logger;
  /**
   * Optional global hooks that run on every topic
   * make sure to catch any errors since the broker will throw
   * if any of the hooks throws
   */
  hooks?: Hooks;
  /**
   * Optional global outbox options
   * If provided, the broker will save the events to the outbox table
   */
  outboxConfig?: OutboxConfig;
  /**
   * The name of your service
   */
  serviceName: string;
}

export type DefaultQueueOptions = Omit<
  Topic,
  "separateConsumerGroup" | "isFifo" | "exchangeType"
>;

export interface MessageMetaData {
  executionContext: ProcessMessageContext;
  messageId?: string;
  messageAttributes?: { [key: string]: MessageAttributeValue };
}

export type EventListener<T> = (
  args: T,
  metadata?: MessageMetaData
) => Promise<void>;

export interface IEmitter {
  /**
   * Call for creation of topics and queues
   * @param topics An optional array of topics.
   * Only required if Emitter.on is not used
   */
  bootstrap(topics?: Topic[]): Promise<void>;
  emit(eventName: string, options?: IEmitOptions, payload?: any): Promise<void>;
  /**
   * @param eventName Name of the topic/event to emit in batch
   * @param messages A list of max 10 messages to send as a batch
   * @param options Optional batch emit options
   */
  emitBatch(
    eventName: string,
    messages: IBatchMessage[],
    options?: IBatchEmitOptions
  ): Promise<IFailedEmitBatchMessage[]>;
  on<T>(
    eventName: string,
    listener: EventListener<T>,
    options?: ConsumeOptions
  ): void;
  removeAllListener(): void;
  removeListener(
    eventName: string,
    listener: EventListener<any>,
    consumeOptions?: ConsumeOptions
  ): void;
  /**
   * Use this method to when you need to consume messages by yourself
   * but use the routing logic defined in the broker.
   * This function throws if the consumer function fails
   * @param message The message received from topic
   * @param options ProcessMessageOptions
   */
  processMessage(
    message: Message,
    options?: ProcessMessageOptions
  ): Promise<void>;
  /**
   * This function does not throw when the consumer function fails.
   * Instead, it returns a list of failed messages as IFailedConsumerMessages
   * @param messages A list of messages received from topic
   * @param options ProcessMessageOptions
   * @returns An object containing a list of the messages that failed.
   * This object is compatible with the return type required by lambda event
   * source mapping and thus can be returned from the lambda directly
   */
  processMessages(
    messages: Message[],
    options?: ProcessMessageOptions
  ): Promise<IFailedConsumerMessages>;
  /**
   * @param topic A Topic object
   *
   * To get the correct arn, the following properties should be provided
   * if applicable
   *
   * name, isFifo
   * @returns ARN of Topic that the broker generates internally
   */
  getTopicReference(topic: Topic): string;
  /**
   * @param topic A Topic object
   *
   * To get the correct name, the following properties should be provided
   * if applicable
   *
   * name, isFifo
   * @returns Name of Topic that the broker generates internally
   */
  getInternalTopicName(topic: Topic): string;
  /**
   * @returns An array of all the queues being consumed
   * by the broker
   */
  getQueues(): Queue[];
  /**
   * @param topic A Topic object
   *
   * To get the correct queue, the following properties should be provided
   * if applicable
   *
   * name, isFifo, separateConsumerGroup, exchangeType
   * @returns The subscribed queue arn for the topic
   */
  getQueueReference(topic: Topic): string;
  /**
   * @param topic A Topic object
   *
   * To get the correct queue, the following properties should be provided
   * if applicable
   *
   * name, isFifo, separateConsumerGroup, exchangeType
   * @returns The subscribed queue internal name for the topic
   */
  getInternalQueueName(topic: Topic): string;
  /**
   * Start consuming the topics
   */
  startConsumers(): Promise<void>;
  /**
   * @return Returns an exact copy of payload handed to aws client for sending.
   */
  getEmitPayload(
    eventName: string,
    options?: IEmitOptions,
    payload?: any
  ): EmitPayload;
  /**
   * @return Returns an exact copy of batch payload handed to aws client for sending.
   */
  getBatchEmitPayload(
    eventName: string,
    messages: IBatchMessage[],
    options?: IBatchEmitOptions
  ): EmitBatchPayload;
  /**
   *
   * @param receivedMessage The message received from the consumer
   * @returns The expected parsed data in the message as provided by the producer
   */
  parseDataFromMessage<T>(receivedMessage: Message): IMessage<T>;
}

export type ISNSMessage = IMessage<any>;

export interface ISNSReceiveMessage {
  Message: string;
  MessageId: string;
  Signature: string;
  SignatureVersion: string;
  SigningCertURL: string;
  Timestamp: string;
  TopicArn: string;
  Type: string;
  UnsubscribeURL: string;
  MessageAttributes: { [key: string]: MessageAttributeValue };
}

export type QueueEmitPayload = SendMessageRequest;

export type FanoutEmitPayload = PublishInput;

export type EmitPayload = QueueEmitPayload | FanoutEmitPayload;

export type EmitBatchPayload = SendMessageBatchRequest | PublishBatchInput;

export interface ICreateQueueLambdaEventSourceInput {
  functionName: string;
  queueARN: string;
  maximumConcurrency?: number;
  batchSize?: number;
}

export enum ExchangeType {
  Queue = "Queue",
  Fanout = "Fanout",
}

export enum FailedEventCategory {
  MessageProducingFailed = "MessageProducingFailed",
  QueueError = "QueueError",
  QueueProcessingError = "QueueProcessingError",
  QueueStopped = "QueueStopped",
  QueueTimedOut = "QueueTimedOut",
  IncomingMessageFailedToParse = "IncomingMessageFailedToParse",
  NoListenerFound = "NoListenerFound",
  MessageProcessingFailed = "MessageProcessingFailed",
}

export interface MessageDeleteOptions {
  /**
   * The unique ReceiptHandle of the message to delete
   */
  receiptHandle: string;
  /**
   * The url of the queue from which the message is received
   */
  queueUrl: string;
}

export interface ProcessMessageOptions {
  /**
   * Set to true if you want to delete the message after processing
   */
  shouldDeleteMessage?: boolean;
  /**
   * The queue ARN from which the message is received.
   * In case of Lambda, the received messages have the eventSourceARN,
   * so this property is optional. In all other cases, this must be
   * provided
   */
  queueReference?: string;
}

export interface ProcessMessageContext {
  /**
   * @description Uniquely generated for each attempt to process a pulled message
   * and run it through all the listeners
   */
  executionTraceId: string;
  /**
   * @description Unique identifier for this message. Stays consistent across
   * multiple attempts to process it
   */
  messageId?: string;
  /**
   * @description Similar to @see executionTraceId but provided by aws
   */
  receiptHandler?: string;
}
