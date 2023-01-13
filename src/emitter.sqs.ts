import {
  CreateQueueRequest,
  SendMessageCommandOutput,
  SendMessageRequest,
  SQS,
} from "@aws-sdk/client-sqs";
import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
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
} from "./types";
import { logger } from "./utils";

class SQSProducer {
  private sqs: SQS;
  constructor() {
    this.sqs = new SQS({});
  }

  send = async (
    queueUrl: string,
    message: ISQSMessage
  ): Promise<SendMessageCommandOutput> => {
    const params: SendMessageRequest = {
      MessageBody: JSON.stringify(message),
      QueueUrl: queueUrl,
      DelaySeconds: 0,
      MessageAttributes: {
        ContentType: {
          DataType: "String",
          StringValue: "JSON",
        },
      },
    };

    if (this.isFifoQueue(queueUrl)) {
      params.MessageDeduplicationId = v4();
      params.MessageGroupId = message.messageGroupId;
    }

    return await this.sqs.sendMessage(params);
  };

  createQueue = async (queueName: string): Promise<string | undefined> => {
    const params: CreateQueueRequest = {
      QueueName: queueName,
      Attributes: {
        DelaySeconds: "0",
        MessageRetentionPeriod: "86400",
      },
    };

    if (this.isFifoQueue(queueName)) {
      params.Attributes = {
        FifoQueue: "true",
      };
    }

    try {
      const { QueueUrl } = await this.sqs.createQueue(params);
      return QueueUrl;
    } catch (error) {
      logger(`Queue creation failed: ${queueName}`);
      throw error;
    }
  };

  isFifoQueue = (queueUrl: string) => queueUrl.includes(".fifo");
}

export class SqsEmitter implements IEmitter {
  private producer!: SQSProducer;
  private localEmitter!: EventEmitter;
  private options!: IEmitterOptions;
  private topicListeners: Map<string, EventListener[]> = new Map();
  private maxRetries = 0;
  private queueMap: IEventTopicMap = {};
  private queues: Map<string, Queue | undefined> = new Map();

  async initialize(options: IEmitterOptions): Promise<void> {
    this.options = options;
    this.localEmitter = options.localEmitter;
    this.producer = new SQSProducer();
    await this.createQueues();
  }

  async createQueue(queueName: string, topic: Topic) {
    const queueUrl = await this.producer.createQueue(queueName);
    this.queues.set(queueName, {
      isFifo: topic.isFifo,
      isConsuming: topic.isConsuming,
      batchSize: topic.batchSize,
      visibilityTimeout: topic.visibilityTimeout,
      url: queueUrl,
    });
  }

  async createQueues() {
    const uniqueQueueMap: Map<string, boolean> = new Map();
    const queueCreationPromises: Promise<void>[] = [];
    for (const topicKey in this.queueMap) {
      const topic = this.queueMap[topicKey];
      const queueName = this.getQueueName(topic);
      if (uniqueQueueMap.has(queueName)) {
        continue;
      }
      uniqueQueueMap.set(queueName, true);
      queueCreationPromises.push(this.createQueue(queueName, topic));
    }
    await Promise.all(queueCreationPromises);
  }

  getQueueName = (topic: Topic): string => {
    const qName = topic.name.replace(".fifo", "");
    return `${this.options.environment}_${topic.servicePrefix || ":"}_${qName}${
      topic.isFifo ? ".fifo" : ""
    }`;
  };

  getQueueUrlForEvent = (eventName: string): string | undefined => {
    return this.queues.get(this.queueMap[eventName].name)?.url;
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
      await this.producer.send(queueUrl, {
        messageGroupId: options?.partitionKey || eventName,
        eventName,
        data: args,
        retryCount: 0,
      });
      return true;
    } catch (error) {
      console.error(
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

  initializeConsumer() {
    for (const queue in this.queues) {
      const q = this.queues.get(queue);
      if (!q) {
        continue;
      }
      if (q.isConsuming) {
        this.attachConsumer(q);
      }
    }
  }

  private attachConsumer(queue: Queue) {
    if (!queue.url) {
      return;
    }
    queue.consumer = Consumer.create({
      queueUrl: queue.url,
      handleMessage: async (message: any) => {
        const key = v4();
        logger(
          `Message started ${
            queue.url
          }_${key}_${new Date()}_${message.Body.toString()}`
        );
        const messageConsumptionStartTime = new Date();
        await this.onMessageReceived(message);
        const messageConsumptionEndTime = new Date();
        const difference =
          messageConsumptionEndTime.getTime() -
          messageConsumptionStartTime.getTime();
        if (difference > this.options.maxProcessingTime) {
          logger(`Slow message ${queue.url}_${key}_${new Date()}`);
        }
        logger(`Message ended ${queue.url}_${key}_${new Date()}`);
      },
      batchSize: queue.batchSize ?? 10,
      visibilityTimeout: queue.visibilityTimeout || 360,
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

  removeListener(eventName: string, listener: EventListener) {
    this.topicListeners.delete(eventName);
  }

  removeAllListener() {
    this.topicListeners.clear();
  }

  async on(eventName: string, listener: EventListener) {
    let listeners = this.topicListeners.get(eventName);
    if (!listeners) listeners = [];
    listeners.push(listener);
    this.topicListeners.set(eventName, listeners);
  }

  private async onMessageReceived(receivedMessage: any) {
    let message: ISQSMessage;
    try {
      message = JSON.parse(receivedMessage.Body.toString());
    } catch (error) {
      logger("Failed to parse message");
      return;
    }
    const listeners = this.topicListeners.get(message.eventName);
    if (!listeners) {
      logger(`No listener found. Message: ${JSON.stringify(message)}`);
      return;
    }

    try {
      for (const listener of listeners) {
        await listener(message.data);
      }
    } catch (error) {
      if (message.retryCount < this.maxRetries) {
        message.retryCount++;
        try {
          await this.producer.send(
            this.getQueueUrlForEvent(message.eventName)!,
            message
          );
        } catch (error) {
          this.logFailedEvent({
            topic: message.eventName,
            event: message,
            error: error,
          });
        }
      } else {
        this.logFailedEvent({
          topic: message.eventName,
          event: message,
          error: error,
        });
      }
    }
  }
}
