import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
import {
  CUSTOM_HANDLER_NAME,
  DEFAULT_BATCH_SIZE,
  DEFAULT_MAX_PROCESSING_TIME,
  DEFAULT_MAX_RETRIES,
  DEFAULT_QUEUE_NAME_FIFO,
  DEFAULT_QUEUE_NAME_STANDARD,
  DEFAULT_VISIBILITY_TIMEOUT,
  DLQ_PREFIX,
  SOURCE_QUEUE_PREFIX,
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
} from "../types";
import { Logger } from "../utils";
import { Message } from "@aws-sdk/client-sqs";
import { SubscribeCommandOutput } from "@aws-sdk/client-sns";
import { SNSProducer } from "../producers/producer.sns";
import { SQSProducer } from "../producers/producer.sqs";

export class SqnsEmitter implements IEmitter {
  private snsProducer!: SNSProducer;
  private sqsProducer!: SQSProducer;
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
    this.snsProducer = new SNSProducer(this.options.snsConfig || {});
    this.sqsProducer = new SQSProducer(this.options.sqsConfig || {});
    if (this.options.isConsumer) {
      this.addDefaultQueues();
    }
  }

  async bootstrap(): Promise<void> {
    await this.createTopics();
    if (this.options.isConsumer) {
      this.addDefaultQueues();
    }
    if (this.options.deadLetterQueueEnabled) {
      // Create DLQs first so that the TargetARN can be used in source queue
      await this.createQueues(true);
    }
    await this.createQueues();
    await this.subscribeToTopics();
  }

  private addDefaultQueues() {
    const defaultFifoQueueOptions = {
      ...this.options.defaultQueueOptions?.fifo,
      name: DEFAULT_QUEUE_NAME_FIFO,
      isFifo: true,
      isDefaultQueue: true,
    };
    defaultFifoQueueOptions.batchSize =
      defaultFifoQueueOptions.batchSize || DEFAULT_BATCH_SIZE;
    defaultFifoQueueOptions.visibilityTimeout =
      defaultFifoQueueOptions.visibilityTimeout || DEFAULT_VISIBILITY_TIMEOUT;
    defaultFifoQueueOptions.maxRetryCount =
      defaultFifoQueueOptions.maxRetryCount || DEFAULT_MAX_RETRIES;
    const defaultStandardQueueOptions = {
      ...this.options.defaultQueueOptions?.standard,
      name: DEFAULT_QUEUE_NAME_STANDARD,
      isFifo: false,
      isDefaultQueue: true,
    };
    defaultStandardQueueOptions.batchSize =
      defaultStandardQueueOptions.batchSize || DEFAULT_BATCH_SIZE;
    defaultStandardQueueOptions.visibilityTimeout =
      defaultStandardQueueOptions.visibilityTimeout ||
      DEFAULT_VISIBILITY_TIMEOUT;
    defaultStandardQueueOptions.maxRetryCount =
      defaultStandardQueueOptions.maxRetryCount || DEFAULT_MAX_RETRIES;
    this.topics.set(DEFAULT_QUEUE_NAME_FIFO, defaultFifoQueueOptions);
    this.topics.set(DEFAULT_QUEUE_NAME_STANDARD, defaultStandardQueueOptions);
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
      topicCreationPromises.push(this.createTopic(topic));
    });
    await Promise.all(topicCreationPromises);
    Logger.info(`Topics created`);
  }

  private async createQueue(
    queueName: string,
    topic: Topic,
    isDLQ: boolean = false
  ) {
    await this.sqsProducer.createQueueFromTopic({
      queueName,
      topic,
      isDLQ,
      globalDLQEnabled: !!this.options.deadLetterQueueEnabled,
      queueArn: this.getQueueArn(this.getQueueName(topic)),
      dlqArn: this.getQueueArn(this.getQueueName(topic, true)),
    });
  }

  private async createQueues(dlqs: boolean = false) {
    const uniqueQueueMap: Map<string, boolean> = new Map();
    const queueCreationPromises: Promise<void>[] = [];
    this.topics.forEach((topic) => {
      const queueName = this.getQueueName(topic);
      if (uniqueQueueMap.has(queueName)) {
        return;
      }
      uniqueQueueMap.set(queueName, true);
      if (dlqs && topic.deadLetterQueueEnabled !== false) {
        queueCreationPromises.push(
          this.createQueue(this.getQueueName(topic, true), topic, true)
        );
      } else if (!dlqs) {
        queueCreationPromises.push(this.createQueue(queueName, topic));
      }
    });
    await Promise.all(queueCreationPromises);
    Logger.info(`DLQs created`);
  }

  private getTopicArn(topicName: string): string {
    return `arn:aws:sns:${this.options.awsConfig?.region}:${this.options.awsConfig?.accountId}:${topicName}`;
  }

  private getQueueArn(queueName: string): string {
    return `arn:aws:sqs:${this.options.awsConfig?.region}:${this.options.awsConfig?.accountId}:${queueName}`;
  }

  private getQueueUrl(queueName: string): string {
    return `https://sqs.${this.options.awsConfig?.region}.amazonaws.com/${this.options.awsConfig?.accountId}/${queueName}`;
  }

  private async subscribeToTopics() {
    const subscriptionPromises: Promise<SubscribeCommandOutput>[] = [];
    this.topics.forEach((topic) => {
      const queueArn = this.getQueueArn(this.getQueueName(topic));
      const topicArn = this.getTopicArn(this.getTopicName(topic));
      if (topic.isDefaultQueue) {
        return;
      }
      if (!queueArn || !topicArn) {
        Logger.warn(
          `Skipping subscription for topic: ${topic.name}. Topic ARN: ${topicArn} Queue ARN: ${queueArn}`
        );
        return;
      }
      subscriptionPromises.push(
        this.snsProducer.subscribeToTopic(topicArn, queueArn)
      );
    });
    await Promise.all(subscriptionPromises);
  }

  private getQueueName = (topic: Topic, isDLQ: boolean = false): string => {
    const qName = topic.separate
      ? topic.name.replace(".fifo", "")
      : topic.isFifo
      ? DEFAULT_QUEUE_NAME_FIFO
      : DEFAULT_QUEUE_NAME_STANDARD;
    return `${this.options.environment}_${this.options.consumerGroup}_${
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

  async emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean> {
    try {
      const topic: Topic = {
        name: eventName,
        isFifo: !!options?.isFifo
      }
      const topicArn = this.getTopicArn(this.getTopicName(topic));
      await this.snsProducer.send(topicArn, {
        messageGroupId: `${options?.partitionKey || ''}_${topic.name}`,
        eventName: topic.name,
        data: args,
      });
      return true;
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
    if (this.consumersStarted || !this.options.isConsumer) {
      return;
    }
    this.queues.forEach((queue) => {
      if (!queue || queue.isDLQ) {
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
      sqs: this.sqsProducer.client,
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
    if (this.options.schemaValidator) {
      try {
        await this.options.schemaValidator.validate(message);
      } catch (error) {
        Logger.error(
          `Schema validation failed for message: ${JSON.stringify(message)}`
        );
        throw error;
      }
    }
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
    const topic = {
      ...options,
      name: eventName,
    };
    this.topics.set(eventName, topic);
    const queueName = this.getQueueName(topic);
    if (!this.queues.has(queueName)) {
      const queue: Queue = {
        isFifo: !!topic.isFifo,
        batchSize: topic.batchSize,
        visibilityTimeout: topic.visibilityTimeout,
        url: this.getQueueUrl(queueName),
        isDLQ: false,
      };
      this.queues.set(queueName, queue);
    }
  }

  private async onMessageReceived(receivedMessage: Message, queueUrl: string) {
    let snsMessage: ISNSReceiveMessage;
    let message: ISQSMessage;
    try {
      snsMessage = JSON.parse(receivedMessage.Body!.toString());
      message = JSON.parse(snsMessage.Message);
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
    message: Message,
    topicUrl?: string | undefined
  ): Promise<void> {
    return await this.handleMessageReceipt(
      message as Message,
      topicUrl || CUSTOM_HANDLER_NAME
    );
  }

  getProducerReference(topicName: string, isFifo?: boolean): string {
    const topic: Topic = {
      name: topicName,
      isFifo: isFifo
    }
    return this.getTopicArn(this.getTopicName(topic)) || "";
  }

  getConsumerReference(topicName: string, separate?: boolean, isFifo?: boolean): string {
    const topic: Topic = {
      name: topicName,
      isFifo: isFifo,
      separate: separate
    }
    return this.getQueueArn(this.getQueueName(topic)) || "";
  }
}
