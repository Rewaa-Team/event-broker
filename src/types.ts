import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
import { SQS, SNS, Lambda } from "aws-sdk";

export interface ISQSMessage {
  data: any;
  eventName: string;
  messageGroupId?: string;
}

export interface ISQSMessageOptions {
  delay: number;
}

type Binary = Buffer | Uint8Array | Blob | string;

export interface IMessageAttributes {
	DataType: 'String' | 'Number' | 'Binary' | 'String.Array';
	StringValue?: string;
	BinaryValue?: Binary;
	StringListValues?: string[];
	BinaryListValues?: Binary[];
}

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
  MessageAttributes?: { [key: string]: IMessageAttributes };
}

export interface IFailedEventMessage {
  topic?: string;
  topicReference?: string;
  event: any;
  error?: any;
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
	 * By default, the value will be whatever is in IEmitterOptions
	 */
	deadLetterQueueEnabled?: boolean;
	/**
	 * An optional consumer group name
	 *
	 * Set if you want to use a separate consumer group
	 *
	 * When specified, messages emitted to this Topic will be received by
	 * this specific consumer group only
	 */
	separateConsumerGroup?: string;
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
}

export interface ILambdaHandler {
  functionName: string;
  maximumConcurrency?: number;
  /**
   * When set to true, the function name is constructed internally as
   * consumerGroup-environment-functionName
   */
  useServerlessLambdaName?: boolean;
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
	 * Optional, to log slow messages
	 *
	 * Unit: ms
	 *
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
	sqsConfig?: SQS.ClientConfiguration;
	/**
	 * Optional SNS Client config used by message producer
	 */
	snsConfig?: SNS.ClientConfiguration;
	/**
	 * Optional Lambda Client config used by message producer
	 */
	lambdaConfig?: Lambda.ClientConfiguration;
	/**
	 * Set to true if you want to use DLQs
	 *
	 * Every topic will have a DLQ created against it that
	 * will be used when maxRetryCount is exceeded for a topic
	 */
	deadLetterQueueEnabled?: boolean;
	/**
	 * Used as prefix for lambda handlers
	 */
	consumerGroup: string;
	/**
	 * Use this to force load topics from external clients
	 */
	refreshTopicsCache?: boolean;
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
}

export type DefaultQueueOptions = Omit<
  Topic,
  "separateConsumerGroup" | "isFifo" | "exchangeType"
>;

export type EventListener<T> = (...args: T[]) => Promise<void>;

export interface IEmitter {
  /**
   * Call for creation of topics and queues
   * @param topics An optional array of topics.
   * Only required if Emitter.on is not used
   */
  bootstrap(topics?: Topic[]): Promise<void>;
  emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean>;
  on<T>(
    eventName: string,
    listener: EventListener<T>,
    options?: ConsumeOptions
  ): void;
  removeAllListener(): void;
  removeListener(eventName: string, listener: EventListener<any>): void;
  /**
   * Use this method to when you need to consume messages by yourself
   * but use the routing logic defined in the broker.
   * @param message The message received from topic
   * @param topicReference Optional topic reference for logging purposes
   */
  processMessage(
    message: SQS.Message,
    topicReference?: string | undefined
  ): Promise<void>;
  /**
   * @param topicName Actual topic name
   * @param isFifo Set to true if Topic is FIFO
   * @returns ARN of Topic that the broker generates internally
   */
  getTopicReference(topicName: string, isFifo?: boolean): string;
  /**
   *
   * @param topicName Actual topic name
   * @param isFifo Set to true if Topic is FIFO
   * @returns Name of Topic that the broker generates internally
   */
  getInternalTopicName(topicName: string, isFifo?: boolean): string;
  /**
   * @returns An array of all the queues being consumed
   * by the broker
   */
  getConsumingQueues(): Queue[];
  /**
   * Start consuming the topics
   */
  startConsumers(): Promise<void>;
}

export interface ISNSMessage {
	data: any;
	eventName: string;
	messageGroupId?: string;
	messageAttr?: { [key: string]: IMessageAttributes };
}

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
}

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
