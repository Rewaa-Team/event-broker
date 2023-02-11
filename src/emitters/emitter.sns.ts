import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
import {
    CUSTOM_HANDLER_NAME,
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_PROCESSING_TIME,
    DEFAULT_VISIBILITY_TIMEOUT,
    DLQ_PREFIX,
    SOURCE_QUEUE_PREFIX,
} from "../constants";
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
    ClientMessage,
    ExchangeType,
} from "../types";
import { loadDataFromFile, logger, mapReplacer, mapReviver, writeDataToFile } from "../utils";
import { Message } from "@aws-sdk/client-sqs";
import { SubscribeCommandOutput } from "@aws-sdk/client-sns";
import { SNSProducer } from "src/producers/producer.sns";
import { SQSProducer } from "src/producers/producer.sqs";

export class SnsEmitter implements IEmitter {
    private producer!: SNSProducer;
    private consumer!: SQSProducer;
    private localEmitter!: EventEmitter;
    private options!: IEmitterOptions;
    private topicListeners: Map<string, EventListener<any>[]> = new Map();
    private topicMap: IEventTopicMap = {};
    private topics: Map<string, Topic & { arn?: string }> = new Map();
    private queues: Map<string, Queue & { topicArn?: string }> = new Map();
    private consumersStarted: boolean = false;

    async initialize(options: IEmitterOptions): Promise<void> {
        this.options = options;
        this.localEmitter = options.localEmitter;
        this.topicMap = this.options.eventTopicMap;
        this.producer = new SNSProducer(this.options.snsConfig || {});
        this.consumer = new SQSProducer(this.options.sqsConfig || {});
        await this.loadTopics();
        await this.loadQueues();
        await this.subscribeToTopics();
    }

    private async loadTopics() {
        let existingTopics = loadDataFromFile(this.getTopicsFileName());
        if (existingTopics && existingTopics.length && !this.options.refreshTopicsCache) {
            existingTopics = new Map(JSON.parse(existingTopics, (key, value) => mapReviver(key, value)));
            this.topics = existingTopics;
            logger(`Topics loaded from saved file`);
            return;
        }
        await this.createTopics();
        logger(`Topics created`);
        writeDataToFile(this.getTopicsFileName(), JSON.stringify(this.topics, (key, value) => mapReplacer(key, value)));
    }

    private async createTopic(
        topicName: string,
        topic: Topic
    ) {
        let topicAttributes: Record<string, string> = {};
        const topicArn = await this.producer.createTopic(
            topicName,
            topicAttributes
        );
        this.topics.set(topicName, { ...topic, arn: topicArn });
    }

    private async createTopics() {
        const uniqueTopicMap: Map<string, boolean> = new Map();
        const topicCreationPromises: Promise<void>[] = [];
        for (const topicKey in this.topicMap) {
            const topic = this.topicMap[topicKey];
            const topicName = this.getTopicName(topic);
            if (uniqueTopicMap.has(topicName)) {
                continue;
            }
            uniqueTopicMap.set(topicName, true);
            topicCreationPromises.push(this.createTopic(topicName, topic));
        }
        await Promise.all(topicCreationPromises);
    }

    private async loadQueues() {
        let existingQueues = loadDataFromFile(this.getQueuesFileName());
        if (existingQueues && existingQueues.length && !this.options.refreshTopicsCache) {
            existingQueues = new Map(JSON.parse(existingQueues, (key, value) => mapReviver(key, value)));
            this.queues = existingQueues;
            logger(`Topics loaded from saved file`);
            return;
        }
        if (this.options.deadLetterQueueEnabled) {
            // Create DLQs first so that the TargetARN can be used in source queue
            await this.createQueues(true);
            logger(`DLQs created`);
        }
        await this.createQueues();
        logger(`Source queues created`);
        writeDataToFile(this.getQueuesFileName(), JSON.stringify(this.queues, (key, value) => mapReplacer(key, value)));
    }

    private getQueuesFileName = (): string => {
        return `${this.options.serviceName}_event_broker_queues_fanout.json`;
    }

    private async createQueue(
        queueName: string,
        topic: Topic,
        isDLQ: boolean = false
    ) {
        const queue = await this.consumer.createQueueFromTopic({
            queueName,
            topic,
            isDLQ,
            globalDLQEnabled: !!this.options.deadLetterQueueEnabled,
            dlqArn: this.queues.get(this.getQueueName(topic, true))?.arn
        })
        this.queues.set(queueName, { ...queue, topicArn: this.getTopicReference(topic) });
    }

    private async createQueues(dlqs: boolean = false) {
        const uniqueQueueMap: Map<string, boolean> = new Map();
        const queueCreationPromises: Promise<void>[] = [];
        for (const topicKey in this.topicMap) {
            const topic = this.topicMap[topicKey];
            if (!topic.isConsuming) {
                continue;
            }
            const queueName = this.getQueueName(topic);
            if (uniqueQueueMap.has(queueName)) {
                continue;
            }
            uniqueQueueMap.set(queueName, true);
            if (dlqs && topic.deadLetterQueueEnabled !== false) {
                queueCreationPromises.push(
                    this.createQueue(this.getQueueName(topic, true), topic, true)
                );
            } else if (!dlqs) {
                queueCreationPromises.push(this.createQueue(queueName, topic));
            }
        }
        await Promise.all(queueCreationPromises);
    }

    private async subscribeToTopics() {
        const subscriptionPromises: Promise<SubscribeCommandOutput>[] = [];
        this.queues.forEach((queue) => {
            if (!queue || !queue.topicArn || !queue.arn) {
                logger(`Skipping subscription for ${queue?.topicArn} ${queue.arn}`);
                return;
            }
            subscriptionPromises.push(this.producer.subscribeToTopic(queue.topicArn, queue.arn));
        });
        await Promise.all(subscriptionPromises);
    }

    private getQueueName = (topic: Topic, isDLQ: boolean = false): string => {
        const qName = topic.name.replace(".fifo", "");
        return `${this.options.environment}_${topic.servicePrefix}_${isDLQ ? DLQ_PREFIX : SOURCE_QUEUE_PREFIX
            }_${qName}${topic.isFifo ? ".fifo" : ""}`;
    };

    private getTopicName = (topic: Topic): string => {
        const topicName = topic.name.replace(".fifo", "");
        return `${this.options.environment}_${topic.servicePrefix}_${topicName}${topic.isFifo ? ".fifo" : ""}`;
    };

    private getTopicsFileName = (): string => {
        return `${this.options.serviceName}_event_broker_topics_fanout.json`;
    }

    private getTopicArnForEvent = (eventName: string): string | undefined => {
        return this.topics.get(this.getTopicName(this.topicMap[eventName]))?.arn;
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
            const topicArn = this.getTopicArnForEvent(eventName);
            if (!topicArn) {
                throw new Error(`Topic ARN not found: ${eventName}`);
            }
            await this.producer.send(
                topicArn,
                {
                    messageGroupId: options?.partitionKey || eventName,
                    eventName,
                    data: args,
                }
            );
            return true;
        } catch (error) {
            logger(`SNS Message producing failed: ${eventName} ${JSON.stringify(error)}`);
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
        this.topics.forEach((queue) => {
            if (!queue) {
                return;
            }
            this.attachConsumer(queue);
        });
        this.consumersStarted = true;
        logger(`Consumers started`);
    }

    private attachConsumer(queue: Queue) {
        if (!queue.url) {
            return;
        }
        queue.consumer = Consumer.create({
            sqs: this.consumer.client,
            queueUrl: queue.url,
            handleMessage: async (message) => {
                await this.handleMessageReceipt(message as Message, queue.url!);
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

    private handleMessageReceipt = async (message: Message, queueUrl: string) => {
        const key = v4();
        logger(
            `SQS Message started ${queueUrl}_${key}_${new Date()}_${message?.Body?.toString()}`
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
            logger(`SQS Slow message ${queueUrl}_${key}_${new Date()}`);
        }
        logger(`SQS Message ended ${queueUrl}_${key}_${new Date()}`);
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
                topicReference: queueUrl,
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

    async processMessage<T extends ExchangeType>(
        exchangeType: T,
        message: ClientMessage[T],
        topicUrl?: string | undefined
    ): Promise<void> {
        return await this.handleMessageReceipt(
            message as Message,
            topicUrl || CUSTOM_HANDLER_NAME
        );
    }

    getTopicReference(topic: Topic): string {
        return (
            this.topics.get(
                this.getTopicName(
                    topic
                )
            )?.arn || ""
        );
    }
}
