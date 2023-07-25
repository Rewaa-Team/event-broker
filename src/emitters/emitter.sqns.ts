import { AsyncLocalStorage } from 'async_hooks';
import EventEmitter from "events";
import { Consumer } from "sqs-consumer";
import {
  CUSTOM_HANDLER_NAME,
  DEFAULT_BATCH_SIZE,
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
  ProcessMessageOptions,
  MessageDeleteOptions,
  IBatchMessage,
  IBatchEmitOptions,
  IFailedEmitBatchMessage,
  IFailedConsumerMessages,
  FailedEventCategory,
  ProcessMessageContext,
  Logger,
} from "../types";
import { SubscribeResponse } from '@aws-sdk/client-sns';
import { Message } from '@aws-sdk/client-sqs';
import { EventSourceMappingConfiguration } from "@aws-sdk/client-lambda";
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

  constructor(
    private readonly logger: Logger,
    options: IEmitterOptions
  ) {
    this.options = options;
    this.logger = logger;
    if (!this.options.awsConfig) {
      throw new Error(
        `awsConfig is required in options when using external broker.`
      );
    }
    this.localEmitter = options.localEmitter;
    this.snsProducer = new SNSProducer(
      this.logger,
      this.options.snsConfig || { ...this.options.awsConfig }
    );
    this.sqsProducer = new SQSProducer(
      this.logger,
      this.options.sqsConfig || { ...this.options.awsConfig }
    );
    this.lambdaClient = new LambdaClient(
      this.logger,
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
    const promises: Promise<EventSourceMappingConfiguration | void>[] =
      [];
    const uniqueQueueMap: Map<string, boolean> = new Map();
    this.topics.forEach((topic) => {
      const queueName = this.getQueueName(topic);
      if (topic.lambdaHandler && !uniqueQueueMap.has(queueName)) {
        uniqueQueueMap.set(queueName, true);
        const lambdaName = topic.lambdaHandler.functionName;
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
    if (promises.length) {
      this.logger.info(`Event source mappings created`);
    } else {
      this.logger.info(`No event source mappings created`);
    }
  }

  private addDefaultQueues() {
    if (!this.options.defaultQueueOptions) {
      this.logger.info(`No default queues specified.`);
      return;
    }
    this.topics.set(this.options.defaultQueueOptions.fifo.name, {
      ...this.options.defaultQueueOptions.fifo,
      isDefaultQueue: true,
      exchangeType: ExchangeType.Queue,
    });
    this.topics.set(this.options.defaultQueueOptions.standard.name, {
      ...this.options.defaultQueueOptions.standard,
      isDefaultQueue: true,
      exchangeType: ExchangeType.Queue,
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
    this.logger.info(`Topics created`);
  }

  private async createQueue(topic: Topic) {
    if (topic.deadLetterQueueEnabled) {
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
    const queueCreationPromises: Promise<void>[] = [];
    const queues = Array.from(this.queues, ([_, value]) => {
      return value;
    });
    queues.forEach((queue) => {
      const topic = queue.topic;
      queueCreationPromises.push(this.createQueue(topic));
    });
    const responses = await Promise.allSettled(queueCreationPromises);
    responses.forEach((response, index) => {
      if (response.status === "rejected") {
        // Checking this for localstack since it throws when queue already exists
        if (response.reason.code !== "QueueAlreadyExists") {
          throw new Error(
            `Queue creation failed: ${queues[index].name} - ${response.reason}`
          );
        }
      }
    });
    this.logger.info(`Queues created`);
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
    let subscriptionPromises: Promise<SubscribeResponse>[] = [];
    const queues = Array.from(this.queues, ([_, value]) => {
      return value;
    });
    for (const queue of queues) {
      const queueTopics = queue.allTopics;
      for (let i = 0; i < queueTopics.length; i += TOPIC_SUBSCRIBE_CHUNK_SIZE) {
        const chunk = queueTopics.slice(i, i + TOPIC_SUBSCRIBE_CHUNK_SIZE);
        for (const topic of chunk) {
          if (topic.exchangeType === ExchangeType.Queue) {
            continue;
          }
          const queueArn = this.getQueueArn(this.getQueueName(topic));
          const topicArn = this.getTopicArn(this.getTopicName(topic));
          subscriptionPromises.push(
            this.snsProducer.subscribeToTopic(
              topicArn,
              queueArn,
              topic.filterPolicy
            )
          );
        }
        await Promise.all(subscriptionPromises);
        subscriptionPromises = [];
      }
    }
    this.logger.info(`Topic subscription complete`);
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
    if (
      topic.exchangeType === ExchangeType.Queue &&
      !topic.separateConsumerGroup
    ) {
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
      messageAttributes: options?.MessageAttributes,
      deduplicationId: options?.deduplicationId,
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
        messageAttributes: options?.MessageAttributes,
        deduplicationId: options?.deduplicationId,
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
        separateConsumerGroup: options?.consumerGroup,
      };
      if (topic.exchangeType === ExchangeType.Queue) {
        return await this.emitToQueue(topic, options, ...args);
      }
      return await this.emitToTopic(topic, options, ...args);
    } catch (error) {
      this.logger.error(
        `Message producing failed: ${eventName} ${JSON.stringify(error)}`
      );
      this.logFailedEvent({
        failureType: FailedEventCategory.MessageProducingFailed,
        topic: eventName,
        event: args,
        error: error,
      });
      return false;
    }
  }

  async emitBatchToTopic(
    topic: Topic,
    messages: IBatchMessage[]
  ): Promise<IFailedEmitBatchMessage[]> {
    const topicArn = this.getTopicArn(this.getTopicName(topic));
    const result = await this.snsProducer.sendBatch(
      topicArn,
      messages.map((message) => {
        return {
          data: [message.data],
          deduplicationId: message.deduplicationId,
          eventName: topic.name,
          messageAttributes: message.MessageAttributes,
          messageGroupId: message.partitionKey || topic.name,
          id: message.id,
        };
      })
    );
    return (
      result.Failed?.map((failed) => ({
        id: failed.Id,
        code: failed.Code,
        message: failed.Message,
        wasSenderFault: failed.SenderFault,
      })) || []
    );
  }

  async emitBatchToQueue(
    topic: Topic,
    messages: IBatchMessage[]
  ): Promise<IFailedEmitBatchMessage[]> {
    const queueUrl = this.getQueueUrl(this.getQueueName(topic));
    const result = await this.sqsProducer.sendBatch(
      queueUrl,
      messages.map((message) => {
        return {
          data: [message.data],
          deduplicationId: message.deduplicationId,
          eventName: topic.name,
          messageAttributes: message.MessageAttributes,
          messageGroupId: message.partitionKey || topic.name,
          delay: message.delay || DEFAULT_MESSAGE_DELAY,
          id: message.id,
        };
      })
    );
    return (result.Failed?.map((failed) => ({
      id: failed.Id,
      code: failed.Code,
      message: failed.Message,
      wasSenderFault: failed.SenderFault,
    })) || []
    );
  }

  async emitBatch(
    eventName: string,
    messages: IBatchMessage[],
    options?: IBatchEmitOptions
  ): Promise<IFailedEmitBatchMessage[]> {
    try {
      const topic: Topic = {
        name: eventName,
        isFifo: !!options?.isFifo,
        exchangeType: options?.exchangeType || ExchangeType.Fanout,
        separateConsumerGroup: options?.consumerGroup,
      };
      if (topic.exchangeType === ExchangeType.Queue) {
        return await this.emitBatchToQueue(topic, messages);
      }
      return await this.emitBatchToTopic(topic, messages);
    } catch (error) {
      this.logger.error(
        `Batch Message producing failed: ${eventName} ${JSON.stringify(error)}`
      );
      throw error;
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
    this.logger.info(`Consumers started`);
  }

  private startConsumer(queue: Queue) {
    if (!queue.url) {
      return;
    }
    queue.consumer = Consumer.create({
      /**
       * Handling delete message explcitly because sqs-consumer
       * does not delete the successful ones if one of the message
       * in the batch throws
       */
      shouldDeleteMessages: false,
      sqs: this.sqsProducer.client,
      region: this.options.awsConfig?.region,
      queueUrl: queue.url,
      messageAttributeNames: ["All"],
      handleMessageBatch: async (messages) => {
        await this.processMessages(messages as Message[], {
          shouldDeleteMessage: true,
          queueReference: queue.url!,
        });
      },
      batchSize: queue.batchSize || DEFAULT_BATCH_SIZE,
      visibilityTimeout: queue.visibilityTimeout || DEFAULT_VISIBILITY_TIMEOUT,
    });

    queue.consumer.on("error", (error, message) => {
      this.logger.error(`Queue error ${JSON.stringify(error)}`);
      this.logFailedEvent({
        failureType: FailedEventCategory.QueueError,
        topic: "",
        event: message,
        error,
      });
    });

    queue.consumer.on("processing_error", (error, message) => {
      this.logger.error(`Queue Processing error ${JSON.stringify(error)}`);
      this.logFailedEvent({
        failureType: FailedEventCategory.QueueProcessingError,
        topic: "",
        event: message,
        error,
      });
    });

    queue.consumer.on("stopped", () => {
      this.logger.error("Queue stopped");
      this.logFailedEvent({
        failureType: FailedEventCategory.QueueStopped,
        topic: "",
        event: "Queue stopped",
      });
    });

    queue.consumer.on("timeout_error", () => {
      this.logger.error("Queue timed out");
      this.logFailedEvent({
        failureType: FailedEventCategory.QueueTimedOut,
        topic: "",
        event: "Queue timed out",
      });
    });

    queue.consumer.on("empty", () => {
      if (!queue.consumer?.isRunning) {
        this.logger.info(`Queue not running`);
      }
    });
    queue.consumer.start();
  }

  private runMessageHandling = async (
    message: Message,
    queueUrl: string,
    executionContext: ProcessMessageContext,
    deleteOptions?: MessageDeleteOptions,
  ) => {
    this.logger.info(
      `Message started ${queueUrl}_${executionContext.executionTraceId}_${new Date()}_${message?.Body?.toString()}`
    );
    await this.onMessageReceived(message, queueUrl, executionContext);
    if (deleteOptions) {
      await this.sqsProducer.deleteMessage(
        deleteOptions.queueUrl,
        deleteOptions.receiptHandle
      );
    }
    this.logger.info(`Message ended ${queueUrl}_${executionContext.executionTraceId}_${new Date()}`);
  }

  private handleMessageReceipt = async (
    message: Message,
    queueUrl: string,
    deleteOptions?: MessageDeleteOptions
  ) => {
    const executionContext: ProcessMessageContext = {
      executionTraceId: v4(),
    };
    const localStorageParams = this.logger.getStore?.(executionContext);
    if (localStorageParams) {
      await localStorageParams.storage.run(
        localStorageParams.store,
        async () => await this.runMessageHandling(message, queueUrl, executionContext, deleteOptions)
      )
    } else {
      await this.runMessageHandling(message, queueUrl, executionContext, deleteOptions);
    }
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
      if (
        !topic.separateConsumerGroup &&
        topic.exchangeType === ExchangeType.Fanout &&
        !this.options.defaultQueueOptions
      ) {
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
        topic,
        allTopics: [topic],
      };
      this.queues.set(queueName, queue);
    } else {
      this.queues.get(queueName)?.allTopics.push(topic);
    }
  }

  private async onMessageReceived(
    receivedMessage: Message,
    queueUrl: string,
    executionContext: ProcessMessageContext,
  ) {
    let snsMessage: ISNSReceiveMessage;
    let message: ISQSMessage;
    try {
      snsMessage = JSON.parse(receivedMessage.Body!.toString());
      message = snsMessage as any;
      if (snsMessage.TopicArn) {
        message = JSON.parse(snsMessage.Message);
      }
    } catch (error) {
      this.logger.error(`Failed to parse message. Trace Id: ${executionContext.executionTraceId}`);
      this.logFailedEvent({
        failureType: FailedEventCategory.IncomingMessageFailedToParse,
        topicReference: queueUrl,
        event: receivedMessage.Body,
        error: `Failed to parse message`,
        executionContext,
      });
      throw new Error(`Failed to parse message`);
    }
    const listeners = this.topicListeners.get(message.eventName);
    if (!listeners) {
      this.logger.error(`No listener found. Trace Id: ${executionContext.executionTraceId}. Message: ${JSON.stringify(message)}`);
      this.logFailedEvent({
        failureType: FailedEventCategory.NoListenerFound,
        topic: message.eventName,
        event: message,
        error: `No listener found`,
        executionContext,
      });
      throw new Error(`No listener found`);
    }

    try {
      for (const listener of listeners) {
        await listener(...message.data);
      }
    } catch (error: any) {
      this.logFailedEvent({
        failureType: FailedEventCategory.MessageProcessingFailed,
        topic: message.eventName,
        event: message,
        error: error,
        executionContext,
      });
      // Doing this because i don't want to mess with stack trace of rethrowing error
      error['executionTraceId'] = executionContext.executionTraceId;
      throw error;
    }
  }

  private async deleteMessages(
    queueUrl: string,
    messages: Message[],
    results: PromiseSettledResult<any>[]
  ): Promise<void> {
    const receiptsToDelete: string[] = [];
    results.forEach((result, index) => {
      if (result.status === "fulfilled") {
        receiptsToDelete.push(messages[index].ReceiptHandle!);
      }
    });
    if(receiptsToDelete.length) {
      await this.sqsProducer.deleteMessages(queueUrl, receiptsToDelete);
    }
  }

  private async processFifoQueueMessages(
    queueUrl: string,
    messages: Message[],
    options?: ProcessMessageOptions
  ): Promise<IFailedConsumerMessages> {
    let i = 0;
    try {
      for (i = 0; i < messages.length; i++) {
        await this.processMessage(messages[i], options);
      }
      return {
        batchItemFailures: [],
      };
    } catch (error: any) {
      this.logger.error(
        `Fifo queue message failed :: ${queueUrl} Execution Trace ID ${error['executionTraceId'] ?? ''} :: ${JSON.stringify(
          messages[i]
        )}`
      );
      return {
        batchItemFailures: messages.slice(i, undefined).map((message) => {
          return {
            itemIdentifier: this.getMessageIdFromMessage(message),
          };
        }),
      };
    }
  }

  private async processStandardQueueMessages(
    queueUrl: string,
    messages: Message[],
    options?: ProcessMessageOptions
  ): Promise<IFailedConsumerMessages> {
    const results = await Promise.allSettled(
      messages.map((message) => this.processMessage(message))
    );
    // Will this not delete the message even when it failed, don't know for sure - please check
    if (options?.shouldDeleteMessage) {
      await this.deleteMessages(queueUrl, messages, results);
    }
    const failedMessages: Message[] = [];
    results.forEach((result, index) => {
      if (result.status === "rejected") {
        failedMessages.push(messages[index]);
      }
    });
    return {
      batchItemFailures: failedMessages.map((message) => {
        return {
          itemIdentifier: this.getMessageIdFromMessage(message),
        };
      }),
    };
  }

  async processMessages(
    messages: Message[],
    options?: ProcessMessageOptions
  ): Promise<IFailedConsumerMessages> {
    const queueUrl =
      options?.queueReference || this.getQueueUrlFromMessage(messages[0]);
    const isFifoQueue = this.sqsProducer.isFifoQueue(queueUrl);
    if (isFifoQueue) {
      return await this.processFifoQueueMessages(queueUrl, messages, options);
    }
    return await this.processStandardQueueMessages(queueUrl, messages, options);
  }

  async processMessage(
    message: Message,
    options?: ProcessMessageOptions
  ): Promise<void> {
    /**
     * The lambda interface provides keys with camel case
     * but the SQS.Message type has Pascal case
     */
    if (!message.Body) {
      message.Body = (message as any).body;
    }
    if (!message.ReceiptHandle) {
      message.ReceiptHandle = (message as any).receiptHandle;
    }
    if (!message.MessageAttributes) {
      message.MessageAttributes = (message as any).messageAttributes;
    }
    let deleteOptions: MessageDeleteOptions | undefined;
    if (options?.shouldDeleteMessage) {
      const queueUrl =
        options.queueReference || this.getQueueUrlFromMessage(message);
      deleteOptions = {
        queueUrl,
        receiptHandle: message.ReceiptHandle!,
      };
    }
    return await this.handleMessageReceipt(
      message,
      message.Attributes?.QueueUrl || CUSTOM_HANDLER_NAME,
      deleteOptions
    );
  }

  getTopicReference(topic: Topic): string {
    return this.getTopicArn(this.getTopicName(topic)) || "";
  }

  getInternalTopicName(topic: Topic): string {
    return this.getTopicName(topic) || "";
  }

  getQueues(): Queue[] {
    const queues: Queue[] = [];
    this.queues.forEach((queue) => {
      queues.push(queue);
    });
    return queues;
  }

  getQueueReference(topic: Topic): string {
    return this.getQueueArn(this.getQueueName(topic));
  }

  getInternalQueueName(topic: Topic): string {
    return this.getQueueName(topic);
  }

  private getQueueUrlFromMessage(message: Message): string {
    const receivedMessage = message as any;
    let queueUrl =
      receivedMessage.MessageAttributes?.QueueUrl.StringValue ||
      receivedMessage.MessageAttributes?.QueueUrl.stringValue;
    //Message was received in a lambda
    queueUrl =
      queueUrl || this.getQueueUrlFromARN(receivedMessage.eventSourceARN);
    if (!queueUrl) {
      throw new Error(`QueueUrl or eventSourceARN not found in the message`);
    }
    return queueUrl;
  }

  private getQueueUrlFromARN(arn?: string): string | undefined {
    if (!arn) return;
    const parts = arn.split(":");

    const service = parts[2];
    const region = parts[3];
    const accountId = parts[4];
    const queueName = parts[5];

    if (this.options.isLocal) {
      return `${this.options.sqsConfig?.endpoint}${this.options.awsConfig?.accountId}/${queueName}`;
    }
    return `https://${service}.${region}.amazonaws.com/${accountId}/${queueName}`;
  }

  private getMessageIdFromMessage(message: Message): string {
    const messageId = message.MessageId || (message as any).messageId;
    return messageId;
  }
}
