import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
import {
  DEFAULT_BATCH_SIZE,
  DEFAULT_DLQ_MESSAGE_RETENTION_PERIOD,
  DEFAULT_MAX_PROCESSING_TIME,
  DEFAULT_MAX_RETRIES,
  DEFAULT_MESSAGE_DELAY,
  DEFAULT_MESSAGE_RETENTION_PERIOD,
  DEFAULT_VISIBILITY_TIMEOUT,
  DLQ_PREFIX,
  SOURCE_QUEUE_PREFIX,
} from "../constants";
import { SQSProducer } from "../producers/producer.sqs";
import { v4 } from "uuid";
import {
  IEmitterOptions,
  IEmitter,
  IEventTopicMap,
  ISQSMessage,
  Queue,
  Topic,
  IFailedEventMessage,
  IEmitOptions,
  EventListener,
  ISQSConsumerMessage,
} from "../types";
import { logger } from "../utils";

export class SqsEmitter implements IEmitter {
  private producer!: SQSProducer;
  private localEmitter!: EventEmitter;
  private options!: IEmitterOptions;
  private topicListeners: Map<string, EventListener<any>[]> = new Map();
  private queueMap: IEventTopicMap = {};
  private queues: Map<string, Queue | undefined> = new Map();
  private consumersStarted: boolean = false;

  async initialize(options: IEmitterOptions): Promise<void> {
    this.options = options;
    this.localEmitter = options.localEmitter;
    this.queueMap = this.options.eventTopicMap;
    this.producer = new SQSProducer(this.options.sqsConfig || {});
    if (this.options.deadLetterQueueEnabled) {
      // Create DLQs first so that the TargetARN can be used in source queue
      await this.createQueues(true);
      logger(`DLQs created`);
    }
    await this.createQueues();
    logger(`Source queues created`);
    await this.initializeConsumer();
  }

  async createQueue(queueName: string, topic: Topic, isDLQ: boolean = false) {
    const queueAttributes: Record<string, string> = {
      DelaySeconds: `${DEFAULT_MESSAGE_DELAY}`,
      MessageRetentionPeriod: `${
        isDLQ
          ? DEFAULT_DLQ_MESSAGE_RETENTION_PERIOD
          : DEFAULT_MESSAGE_RETENTION_PERIOD
      }`,
    };
    topic.batchSize;
    if (!isDLQ && this.options.deadLetterQueueEnabled) {
      queueAttributes.RedrivePolicy = `{\"deadLetterTargetArn\":\"${
        this.queues.get(this.getQueueName(topic, true))?.arn
      }\",\"maxReceiveCount\":\"${
        topic.maxRetryCount || DEFAULT_MAX_RETRIES
      }\"}`;
    }
    const queueUrl = await this.producer.createQueue(
      queueName,
      queueAttributes
    );
    const queue: Queue = {
      isFifo: topic.isFifo,
      isConsuming: topic.isConsuming,
      batchSize: topic.batchSize,
      visibilityTimeout: topic.visibilityTimeout,
      url: queueUrl,
      isDLQ,
    };
    if (isDLQ) {
      let dlQueueAttributes: Record<string, string> = {};
      let attributes = await this.producer.getQueueAttributes(queueUrl!, [
        "QueueArn",
      ]);
      if (attributes) {
        dlQueueAttributes = attributes;
      }
      //Not consuming DLQs
      queue.isConsuming = false;
      queue.arn = dlQueueAttributes?.QueueArn;
    }
    this.queues.set(queueName, queue);
  }

  async createQueues(dlqs: boolean = false) {
    const uniqueQueueMap: Map<string, boolean> = new Map();
    const queueCreationPromises: Promise<void>[] = [];
    for (const topicKey in this.queueMap) {
      const topic = this.queueMap[topicKey];
      const queueName = this.getQueueName(topic);
      if (uniqueQueueMap.has(queueName)) {
        continue;
      }
      uniqueQueueMap.set(queueName, true);
      if (dlqs) {
        queueCreationPromises.push(
          this.createQueue(this.getQueueName(topic, true), topic, true)
        );
      } else {
        queueCreationPromises.push(this.createQueue(queueName, topic));
      }
    }
    await Promise.all(queueCreationPromises);
  }

  getQueueName = (topic: Topic, isDLQ: boolean = false): string => {
    const qName = topic.name.replace(".fifo", "");
    return `${this.options.environment}_${topic.servicePrefix || ""}_${
      isDLQ ? DLQ_PREFIX : SOURCE_QUEUE_PREFIX
    }_${qName}${topic.isFifo ? ".fifo" : ""}`;
  };

  getQueueUrlForEvent = (eventName: string): string | undefined => {
    return this.queues.get(this.getQueueName(this.queueMap[eventName]))?.url;
  };

  logFailedEvent = (data: IFailedEventMessage) => {
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
      const queueUrl = this.getQueueUrlForEvent(eventName);
      if (!queueUrl) {
        throw new Error(`Queue URL not found: ${eventName}`);
      }
      await this.producer.send(
        queueUrl,
        {
          messageGroupId: options?.partitionKey || eventName,
          eventName,
          data: args,
        },
        {
          delay: options?.delay || DEFAULT_MESSAGE_DELAY,
        }
      );
      return true;
    } catch (error) {
      logger(`Message producing failed: ${eventName} ${JSON.stringify(error)}`);
      this.logFailedEvent({
        topic: eventName,
        event: args,
        error: error,
      });
      return false;
    }
  }

  private async initializeConsumer() {
    if(this.consumersStarted || !this.options.isConsumer) {
      return;
    }
    this.queues.forEach((queue) => {
      if (!queue) {
        return;
      }
      if (queue.isConsuming) {
        this.attachConsumer(queue);
      }
    });
    this.consumersStarted = true;
    logger(`Consumers started`);
  }

  private attachConsumer(queue: Queue) {
    if (!queue.url) {
      return;
    }
    queue.consumer = Consumer.create({
      sqs: this.producer.client,
      queueUrl: queue.url,
      handleMessage: async (message) => {
        await this.handleMessageReceipt(
          message as ISQSConsumerMessage,
          queue.url!
        );
      },
      batchSize: queue.batchSize || DEFAULT_BATCH_SIZE,
      visibilityTimeout: queue.visibilityTimeout || DEFAULT_VISIBILITY_TIMEOUT,
    });

    queue.consumer.on("error", (error, message) => {
      logger(`Queue error ${JSON.stringify(error)}`);
      this.logFailedEvent({
        topic: "",
        event: message,
        error,
      });
    });

    queue.consumer.on("processing_error", (error, message) => {
      logger(`Queue Processing error ${JSON.stringify(error)}`);
      this.logFailedEvent({
        topic: "",
        event: message,
        error,
      });
    });

    queue.consumer.on("stopped", () => {
      logger("Queue stopped");
      this.logFailedEvent({
        topic: "",
        event: "Queue stopped",
      });
    });

    queue.consumer.on("timeout_error", () => {
      logger("Queue timed out");
      this.logFailedEvent({
        topic: "",
        event: "Queue timed out",
      });
    });

    queue.consumer.on("empty", () => {
      logger("Queue empty");
      if (!queue.consumer?.isRunning) {
        logger(`Queue not running`);
      }
    });
    queue.consumer.start();
  }

  handleMessageReceipt = async (
    message: ISQSConsumerMessage,
    queueUrl: string
  ) => {
    const key = v4();
    logger(
      `Message started ${queueUrl}_${key}_${new Date()}_${message.Body.toString()}`
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
      logger(`Slow message ${queueUrl}_${key}_${new Date()}`);
    }
    logger(`Message ended ${queueUrl}_${key}_${new Date()}`);
  };

  removeListener(eventName: string, listener: EventListener<any>) {
    this.topicListeners.delete(eventName);
  }

  removeAllListener() {
    this.topicListeners.clear();
  }

  on(eventName: string, listener: EventListener<any>) {
    let listeners = this.topicListeners.get(eventName) || [];
    listeners.push(listener);
    this.topicListeners.set(eventName, listeners);
  }

  private async onMessageReceived(receivedMessage: any, queueUrl: string) {
    let message: ISQSMessage;
    try {
      message = JSON.parse(receivedMessage.Body.toString());
    } catch (error) {
      logger("Failed to parse message");
      this.logFailedEvent({
        queueUrl: queueUrl,
        event: receivedMessage.Body,
        error: `Failed to parse message`,
      });
      throw new Error(`Failed to parse message`);
    }
    const listeners = this.topicListeners.get(message.eventName);
    if (!listeners) {
      logger(`No listener found. Message: ${JSON.stringify(message)}`);
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
}
