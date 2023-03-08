import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
import {
  CUSTOM_HANDLER_NAME,
  DEFAULT_BATCH_SIZE,
  DEFAULT_MAX_PROCESSING_TIME,
  DEFAULT_MESSAGE_DELAY,
  DEFAULT_VISIBILITY_TIMEOUT,
  DLQ_PREFIX,
  SOURCE_QUEUE_PREFIX,
  TOPIC_SUBSCRIBE_CHUNK_SIZE,
} from "../constants";
import { v4 } from "uuid";
import {
  IEmitterOptions,
  IEmitter,
  ISQSMessage,
  Queue,
  Topic,
  IFailedEventMessage,
  IEmitOptions,
  EventListener,
  ConsumeOptions,
  ISNSReceiveMessage,
  ExchangeType,
} from "../types";
import { Logger } from "../utils/utils";
import { Message } from "@aws-sdk/client-sqs";
import { SNS, SQS } from "aws-sdk";
import { Lambda } from "aws-sdk";
import { SNSProducer } from "../producers/producer.sns";
import { SQSProducer } from "../producers/producer.sqs";
import { LambdaClient } from "../utils/lambda.client";

export class SqnsEmitter implements IEmitter {
  private snsProducer!: SNSProducer;
  private sqsProducer!: SQSProducer;
  private lambdaClient!: LambdaClient;
  private localEmitter!: EventEmitter;
  private options!: IEmitterOptions;
  private topicListeners: Map<string, EventListener<any>[]> = new Map();
  private topics: Map<string, Topic & { isDefaultQueue?: boolean }> = new Map();
  private queues: Map<string, Queue> = new Map();
  private consumersStarted: boolean = false;

  constructor(options: IEmitterOptions) {
    this.options = options;
    if (!this.options.awsConfig) {
      throw new Error(
        `awsConfig is required in options when using external broker.`
      );
    }
    this.localEmitter = options.localEmitter;
    this.snsProducer = new SNSProducer(
      this.options.snsConfig || { ...this.options.awsConfig }
    );
    this.sqsProducer = new SQSProducer(
      this.options.sqsConfig || { ...this.options.awsConfig }
    );
    this.lambdaClient = new LambdaClient(
      this.options.lambdaConfig || { ...this.options.awsConfig }
    );
    this.addDefaultQueues();
  }

  async bootstrap(topics?: Topic[]) {
    if (topics?.length) {
      topics.forEach((topic) => {
        this.on(topic.name, async () => {}, topic);
      });
    }
    await this.createTopics();
    await this.createQueues();
    await this.subscribeToTopics();
    await this.createEventSourceMappings();
  }

  private async createEventSourceMappings() {
    const promises: Promise<Lambda.EventSourceMappingConfiguration | void>[] =
      [];
    const uniqueQueueMap: Map<string, boolean> = new Map();
    this.topics.forEach((topic) => {
      const queueName = this.getQueueName(topic);
      if (topic.lambdaHandler && !uniqueQueueMap.has(queueName)) {
        uniqueQueueMap.set(queueName, true);
        const lambdaName = topic.lambdaHandler.useServerlessLambdaName
          ? `${this.options.consumerGroup}-${this.options.environment}-${topic.lambdaHandler.functionName}`
          : topic.lambdaHandler.functionName;
        promises.push(
          this.lambdaClient.createQueueMappingForLambda({
            functionName: lambdaName,
            queueARN: this.getQueueArn(queueName),
            batchSize: topic.batchSize || DEFAULT_BATCH_SIZE,
            maximumConcurrency: topic.lambdaHandler.maximumConcurrency,
          })
        );
      }
    });
    await Promise.all(promises);
    Logger.info(`Event source mappings created`);
  }

  private addDefaultQueues() {
    if (!this.options.defaultQueueOptions) {
      Logger.info(`No default queues specified.`);
      return;
    }
    this.topics.set(this.options.defaultQueueOptions.fifo.name, {
      ...this.options.defaultQueueOptions.fifo,
      isDefaultQueue: true,
      exchangeType: ExchangeType.Fanout,
    });
    this.topics.set(this.options.defaultQueueOptions.standard.name, {
      ...this.options.defaultQueueOptions.standard,
      isDefaultQueue: true,
      exchangeType: ExchangeType.Fanout,
    });
  }

  private async createTopic(topic: Topic) {
    let topicAttributes: Record<string, string> = {};
    await this.snsProducer.createTopic(
      this.getTopicName(topic),
      topicAttributes
    );
  }

  private async createTopics() {
    const topicCreationPromises: Promise<void>[] = [];
    this.topics.forEach((topic) => {
      if (topic.exchangeType === ExchangeType.Queue) {
        return;
      }
      topicCreationPromises.push(this.createTopic(topic));
    });
    await Promise.all(topicCreationPromises);
    Logger.info(`Topics created`);
  }

  private async createQueue(topic: Topic) {
    if (
      this.options.deadLetterQueueEnabled &&
      topic.deadLetterQueueEnabled !== false
    ) {
      const queueName = this.getQueueName(topic, true);
      await this.sqsProducer.createQueueFromTopic({
        queueName,
        topic,
        isDLQ: true,
        queueArn: this.getQueueArn(this.getQueueName(topic)),
        dlqArn: this.getQueueArn(this.getQueueName(topic, true)),
      });
    }
    const queueName = this.getQueueName(topic);
    await this.sqsProducer.createQueueFromTopic({
      queueName,
      topic,
      isDLQ: false,
      queueArn: this.getQueueArn(this.getQueueName(topic)),
      dlqArn: this.getQueueArn(this.getQueueName(topic, true)),
    });
  }

  private async createQueues() {
    const uniqueQueueMap: Map<string, boolean> = new Map();
    const queueCreationPromises: Promise<void>[] = [];
    this.topics.forEach((topic) => {
      const queueName = this.getQueueName(topic);
      if (uniqueQueueMap.has(queueName)) {
        return;
      }
      uniqueQueueMap.set(queueName, true);
      queueCreationPromises.push(this.createQueue(topic));
    });
    await Promise.all(queueCreationPromises);
    Logger.info(`Queues created`);
  }

  private getTopicArn(topicName: string): string {
    return `arn:aws:sns:${this.options.awsConfig?.region}:${this.options.awsConfig?.accountId}:${topicName}`;
  }

  private getQueueArn(queueName: string): string {
    return `arn:aws:sqs:${this.options.awsConfig?.region}:${this.options.awsConfig?.accountId}:${queueName}`;
  }

  private getQueueUrl(queueName: string): string {
    if (this.options.isLocal) {
			return `${this.options.sqsConfig?.endpoint}${this.options.awsConfig?.accountId}/${queueName}`;
		}
    return `https://sqs.${this.options.awsConfig?.region}.amazonaws.com/${this.options.awsConfig?.accountId}/${queueName}`;
  }

  private async subscribeToTopics() {
    let subscriptionPromises: Promise<SNS.SubscribeResponse>[] = [];
    const topics = Array.from(this.topics, ([_, value]) => {
      return value;
    });
    for (let i = 0; i < topics.length; i += TOPIC_SUBSCRIBE_CHUNK_SIZE) {
      const chunk = topics.slice(i, i + TOPIC_SUBSCRIBE_CHUNK_SIZE);
      for (const topic of chunk) {
        if (topic.exchangeType === ExchangeType.Queue) {
          continue;
        }
        const queueArn = this.getQueueArn(this.getQueueName(topic));
        const topicArn = this.getTopicArn(this.getTopicName(topic));
        if (topic.isDefaultQueue) {
          continue;
        }
        if (!queueArn || !topicArn) {
          Logger.warn(
            `Skipping subscription for topic: ${topic.name}. Topic ARN: ${topicArn} Queue ARN: ${queueArn}`
          );
          continue;
        }
        subscriptionPromises.push(
          this.snsProducer.subscribeToTopic(topicArn, queueArn, topic.filterPolicy)
        );
      }
      await Promise.all(subscriptionPromises);
      subscriptionPromises = [];
    }
    Logger.info(`Topic subscription complete`);
  }

  private getQueueName = (topic: Topic, isDLQ: boolean = false): string => {
    let qName: string = "";
    if (topic.separateConsumerGroup) {
      qName = topic.separateConsumerGroup;
    } else {
      if (topic.isFifo) {
        qName = this.options.defaultQueueOptions?.fifo.name || "";
      } else {
        qName = this.options.defaultQueueOptions?.standard.name || "";
      }
    }
    if (topic.exchangeType === ExchangeType.Queue) {
      qName = topic.name;
    }
    qName = qName.replace(".fifo", "");
    return `${this.options.environment}_${
      isDLQ ? DLQ_PREFIX : SOURCE_QUEUE_PREFIX
    }_${qName}${topic.isFifo ? ".fifo" : ""}`;
  };

  private getTopicName = (topic: Topic): string => {
    const topicName = topic.name.replace(".fifo", "");
    return `${this.options.environment}_${topicName}${
      topic.isFifo ? ".fifo" : ""
    }`;
  };

  private logFailedEvent = (data: IFailedEventMessage) => {
    if (!this.options.eventOnFailure) {
      return;
    }
    this.localEmitter.emit(this.options.eventOnFailure, data);
  };

  async emitToTopic(
    topic: Topic,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean> {
    const topicArn = this.getTopicArn(this.getTopicName(topic));
    await this.snsProducer.send(topicArn, {
			messageGroupId: options?.partitionKey || topic.name,
			eventName: topic.name,
			messageAttr: options?.MessageAttributes,
			data: args,
		});
    return true;
  }

  async emitToQueue(
    topic: Topic,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean> {
    const queueUrl = this.getQueueUrl(this.getQueueName(topic));
    await this.sqsProducer.send(
      queueUrl,
      {
        messageGroupId: options?.partitionKey || topic.name,
        eventName: topic.name,
        data: args,
      },
      {
        delay: options?.delay || DEFAULT_MESSAGE_DELAY,
      }
    );
    return true;
  }

  async emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean> {
    try {
      const topic: Topic = {
        name: eventName,
        isFifo: !!options?.isFifo,
        exchangeType: options?.exchangeType || ExchangeType.Fanout,
        separateConsumerGroup: options?.consumerGroup
      };
      if (topic.exchangeType === ExchangeType.Queue) {
        return await this.emitToQueue(topic, options, ...args);
      }
      return await this.emitToTopic(topic, options, ...args);
    } catch (error) {
      Logger.error(
        `Message producing failed: ${eventName} ${JSON.stringify(error)}`
      );
      this.logFailedEvent({
        topic: eventName,
        event: args,
        error: error,
      });
      return false;
    }
  }

  async startConsumers() {
    if (this.consumersStarted) {
      return;
    }
    this.queues.forEach((queue) => {
      if (!queue || queue.isDLQ || queue.listenerIsLambda) {
        return;
      }
      this.startConsumer(queue);
    });
    this.consumersStarted = true;
    Logger.info(`Consumers started`);
  }

  private startConsumer(queue: Queue) {
    if (!queue.url) {
      return;
    }
    queue.consumer = Consumer.create({
      region: this.options.awsConfig?.region,
      queueUrl: queue.url,
      handleMessage: async (message) => {
        await this.handleMessageReceipt(message as Message, queue.url!);
      },
      batchSize: queue.batchSize || DEFAULT_BATCH_SIZE,
      visibilityTimeout: queue.visibilityTimeout || DEFAULT_VISIBILITY_TIMEOUT,
    });

    queue.consumer.on("error", (error, message) => {
      Logger.error(`Queue error ${JSON.stringify(error)}`);
      this.logFailedEvent({
        topic: "",
        event: message,
        error,
      });
    });

    queue.consumer.on("processing_error", (error, message) => {
      Logger.error(`Queue Processing error ${JSON.stringify(error)}`);
      this.logFailedEvent({
        topic: "",
        event: message,
        error,
      });
    });

    queue.consumer.on("stopped", () => {
      Logger.error("Queue stopped");
      this.logFailedEvent({
        topic: "",
        event: "Queue stopped",
      });
    });

    queue.consumer.on("timeout_error", () => {
      Logger.error("Queue timed out");
      this.logFailedEvent({
        topic: "",
        event: "Queue timed out",
      });
    });

    queue.consumer.on("empty", () => {
      if (!queue.consumer?.isRunning) {
        Logger.info(`Queue not running`);
      }
    });
    queue.consumer.start();
  }

  private handleMessageReceipt = async (message: Message, queueUrl: string) => {
    const key = v4();
    Logger.info(
      `Message started ${queueUrl}_${key}_${new Date()}_${message?.Body?.toString()}`
    );
    const messageConsumptionStartTime = new Date();
    await this.onMessageReceived(message, queueUrl);
    const messageConsumptionEndTime = new Date();
    const difference =
      messageConsumptionEndTime.getTime() -
      messageConsumptionStartTime.getTime();
    if (
      difference >
      (this.options.maxProcessingTime || DEFAULT_MAX_PROCESSING_TIME)
    ) {
      Logger.warn(`Slow message ${queueUrl}_${key}_${new Date()}`);
    }
    Logger.info(`Message ended ${queueUrl}_${key}_${new Date()}`);
  };

  removeListener(eventName: string, listener: EventListener<any>) {
    this.topicListeners.delete(eventName);
  }

  removeAllListener() {
    this.topicListeners.clear();
  }

  on(
    eventName: string,
    listener: EventListener<any>,
    options?: ConsumeOptions
  ) {
    let listeners = this.topicListeners.get(eventName) || [];
    listeners.push(listener);
    this.topicListeners.set(eventName, listeners);
    const topic: Topic = {
      ...options,
      name: eventName,
      exchangeType: options?.exchangeType || ExchangeType.Fanout,
    };
    this.topics.set(eventName, topic);
    const queueName = this.getQueueName(topic);
    if (!this.queues.has(queueName)) {
      if (!topic.separateConsumerGroup && topic.exchangeType === ExchangeType.Fanout && !this.options.defaultQueueOptions) {
        throw new Error(
          `${topic.name} - separateConsumerGroup is required when defaultQueueOptions are not specified.
          Or the Exchange Type should be Queue`
        );
      }
      const queue: Queue = {
        name:
          topic.exchangeType === ExchangeType.Queue
            ? topic.name
            : topic.separateConsumerGroup ||
              (topic.isFifo
                ? this.options.defaultQueueOptions?.fifo.name
                : this.options.defaultQueueOptions?.standard.name) ||
              "",
        isFifo: !!topic.isFifo,
        batchSize: topic.batchSize || DEFAULT_BATCH_SIZE,
        visibilityTimeout:
          topic.visibilityTimeout || DEFAULT_VISIBILITY_TIMEOUT,
        url: this.getQueueUrl(queueName),
        arn: this.getQueueArn(this.getQueueName(topic)),
        isDLQ: false,
        listenerIsLambda: !!topic.lambdaHandler,
      };
      this.queues.set(queueName, queue);
    }
  }

  private async onMessageReceived(receivedMessage: Message, queueUrl: string) {
    let snsMessage: ISNSReceiveMessage;
    let message: ISQSMessage;
    try {
      snsMessage = JSON.parse(receivedMessage.Body!.toString());
      message = snsMessage as any;
      if (snsMessage.TopicArn) {
        message = JSON.parse(snsMessage.Message);
      }
    } catch (error) {
      Logger.error("Failed to parse message");
      this.logFailedEvent({
        topicReference: queueUrl,
        event: receivedMessage.Body,
        error: `Failed to parse message`,
      });
      throw new Error(`Failed to parse message`);
    }
    const listeners = this.topicListeners.get(message.eventName);
    if (!listeners) {
      Logger.error(`No listener found. Message: ${JSON.stringify(message)}`);
      this.logFailedEvent({
        topic: message.eventName,
        event: message,
        error: `No listener found`,
      });
      throw new Error(`No listener found`);
    }

    try {
      for (const listener of listeners) {
        await listener(...message.data);
      }
    } catch (error) {
      this.logFailedEvent({
        topic: message.eventName,
        event: message,
        error: error,
      });
      throw error;
    }
  }

  async processMessage(
    message: SQS.Message,
    topicReference?: string | undefined
  ): Promise<void> {
    return await this.handleMessageReceipt(
      message as Message,
      topicReference || CUSTOM_HANDLER_NAME
    );
  }

  getTopicReference(topicName: string, isFifo?: boolean): string {
    const topic: Topic = {
      name: topicName,
      isFifo: isFifo,
      exchangeType: ExchangeType.Fanout,
    };
    return this.getTopicArn(this.getTopicName(topic)) || "";
  }

  getInternalTopicName(topicName: string, isFifo?: boolean): string {
    const topic: Topic = {
      name: topicName,
      isFifo: isFifo,
      exchangeType: ExchangeType.Fanout,
    };
    return this.getTopicName(topic) || "";
  }

  getConsumingQueues(): Queue[] {
    const queues: Queue[] = [];
    this.queues.forEach((queue) => {
      queues.push(queue);
    });
    return queues;
  }
}
