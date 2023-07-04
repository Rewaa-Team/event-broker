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
  IMessage,
} from "../types";
import { Logger } from "../utils/utils";
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
			Logger.info(`Event source mappings created`);
		} else {
			Logger.info(`No event source mappings created`);
		}
	}

	private addDefaultQueues() {
		if (!this.options.defaultQueueOptions) {
			Logger.info(`No default queues specified.`);
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
		Logger.info(`Topics created`);
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
		await this.snsProducer.send(
			topicArn,
			this.getTransformedMessage(topic, args, options)
		);
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
			this.getTransformedMessage(topic, args, options),
			{
				delay: options?.delay || DEFAULT_MESSAGE_DELAY,
			}
		);
		return true;
	}

	getTransformedMessage(
		topic: Topic,
    args: any[],
		options?: Pick<IEmitOptions, "partitionKey" | "MessageAttributes" | "deduplicationId">,
	): IMessage {
		const message: IMessage = {
			messageGroupId: options?.partitionKey || topic.name,
			eventName: topic.name,
			data: args,
			messageAttributes: options?.MessageAttributes,
			deduplicationId: options?.deduplicationId,
		};
		return message;
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
		return result.Failed.map((failed) => ({
			id: failed.Id,
			code: failed.Code,
			message: failed.Message,
			wasSenderFault: failed.SenderFault,
		}));
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
			Logger.error(
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
		Logger.info(`Consumers started`);
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
			region: this.options.awsConfig?.region,
			queueUrl: queue.url,
      messageAttributeNames: ["All"],
			handleMessageBatch: async (messages) => {
				await this.processMessages(messages as SQS.Message[], {
					shouldDeleteMessage: true,
					queueReference: queue.url!,
				});
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

	private handleMessageReceipt = async (
		message: SQS.Message,
		queueUrl: string,
		deleteOptions?: MessageDeleteOptions
	) => {
		const key = v4();
		Logger.info(
			`Message started ${queueUrl}_${key}_${new Date()}_${message?.Body?.toString()}`
		);
		await this.onMessageReceived(message, queueUrl);
		if (deleteOptions) {
			await this.sqsProducer.deleteMessage(
				deleteOptions.queueUrl,
				deleteOptions.receiptHandle
			);
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
		receivedMessage: SQS.Message,
		queueUrl: string
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

	private async deleteMessages(
		queueUrl: string,
		messages: SQS.Message[],
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
		messages: SQS.Message[],
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
		} catch (error) {
			Logger.error(
				`Fifo queue message failed :: ${queueUrl} :: ${JSON.stringify(
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
		messages: SQS.Message[],
		options?: ProcessMessageOptions
	): Promise<IFailedConsumerMessages> {
		const results = await Promise.allSettled(
			messages.map((message) => this.processMessage(message))
		);
		if (options?.shouldDeleteMessage) {
			await this.deleteMessages(queueUrl, messages, results);
		}
		const failedMessages: SQS.Message[] = [];
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
		messages: SQS.Message[],
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
		message: SQS.Message,
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

	private getQueueUrlFromMessage(message: SQS.Message): string {
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

	private getMessageIdFromMessage(message: SQS.Message): string {
		const messageId = message.MessageId || (message as any).messageId;
		return messageId;
	}
}
