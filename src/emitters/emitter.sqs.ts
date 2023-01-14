import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
import {
  DEFAULT_BATCH_SIZE,
  DEFAULT_MAX_PROCESSING_TIME,
  DEFAULT_MESSAGE_DELAY,
  DEFAULT_RETRY_COUNT,
  DEFAULT_VISIBILITY_TIMEOUT,
} from "src/constants";
import { SQSProducer } from "src/producers/producer.sqs";
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
      await this.producer.send(
        queueUrl,
        {
          messageGroupId: options?.partitionKey || eventName,
          eventName,
          data: args,
          retryCount: options?.retryCount || DEFAULT_RETRY_COUNT,
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
    await this.onMessageReceived(message);
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

  removeListener(eventName: string, listener: EventListener) {
    this.topicListeners.delete(eventName);
  }

  removeAllListener() {
    this.topicListeners.clear();
  }

  on(eventName: string, listener: EventListener) {
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
            message,
            {
              delay: DEFAULT_MESSAGE_DELAY,
            }
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
