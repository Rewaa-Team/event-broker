import {
  CreateQueueRequest,
  SendMessageCommandOutput,
  SendMessageRequest,
  SQS,
  DeleteQueueCommandInput,
  SQSClientConfig,
  GetQueueAttributesRequest,
} from "@aws-sdk/client-sqs";
import {
  ISQSMessage,
  ISQSMessageOptions,
  Queue,
  Topic,
} from "../types";
import { logger } from "../utils";
import { v4 } from "uuid";
import { DEFAULT_MESSAGE_DELAY, DEFAULT_DLQ_MESSAGE_RETENTION_PERIOD, DEFAULT_MESSAGE_RETENTION_PERIOD, DEFAULT_MAX_RETRIES } from "src/constants";

export class SQSProducer {
  private readonly sqs: SQS;
  constructor(config: SQSClientConfig) {
    this.sqs = new SQS(config);
  }

  get client(): SQS {
    return this.sqs;
  }

  send = async (
    queueUrl: string,
    message: ISQSMessage,
    messageOptions: ISQSMessageOptions
  ): Promise<SendMessageCommandOutput> => {
    const params: SendMessageRequest = {
      MessageBody: JSON.stringify(message),
      QueueUrl: queueUrl,
      DelaySeconds: messageOptions.delay,
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

  createQueue = async (
    queueName: string,
    attributes: Record<string, string>
  ): Promise<string | undefined> => {
    const params: CreateQueueRequest = {
      QueueName: queueName,
      Attributes: attributes,
    };

    if (this.isFifoQueue(queueName) && params.Attributes) {
      params.Attributes.FifoQueue = "true";
    }

    try {
      const { QueueUrl } = await this.sqs.createQueue(params);
      return QueueUrl;
    } catch (error) {
      logger(`Queue creation failed: ${queueName}`);
      throw error;
    }
  };

  async createQueueFromTopic(params: {
    queueName: string,
    topic: Topic,
    isDLQ: boolean,
    globalDLQEnabled: boolean,
    dlqArn?: string
  }
  ) {
    const { queueName, topic, isDLQ, globalDLQEnabled, dlqArn } = params;
    let queueAttributes: Record<string, string> = {
      DelaySeconds: `${DEFAULT_MESSAGE_DELAY}`,
      MessageRetentionPeriod: `${isDLQ
        ? DEFAULT_DLQ_MESSAGE_RETENTION_PERIOD
        : DEFAULT_MESSAGE_RETENTION_PERIOD
        }`,
    };
    if (
      !isDLQ &&
      globalDLQEnabled &&
      topic.deadLetterQueueEnabled !== false
    ) {
      queueAttributes.RedrivePolicy = `{\"deadLetterTargetArn\":\"${dlqArn
        }\",\"maxReceiveCount\":\"${topic.maxRetryCount || DEFAULT_MAX_RETRIES
        }\"}`;
    }
    const queueUrl = await this.createQueue(
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
      //Not consuming DLQs
      queue.isConsuming = false;
    }
    let attributes = await this.getQueueAttributes(queueUrl!, [
      "QueueArn",
    ]);
    if (attributes) {
      queueAttributes = { ...queueAttributes, ...attributes };
    }
    queue.arn = queueAttributes?.QueueArn;
    return queue;
  }

  getQueueAttributes = async (
    queueUrl: string,
    attributes: string[]
  ): Promise<Record<string, string> | undefined> => {
    const params: GetQueueAttributesRequest = {
      QueueUrl: queueUrl,
      AttributeNames: attributes,
    };
    try {
      const { Attributes } = await this.sqs.getQueueAttributes(params);
      return Attributes;
    } catch (error) {
      logger(`Failed to fetch queue attributes: ${queueUrl}`);
      throw error;
    }
  };

  deleteQueue = async (queueUrl: string): Promise<boolean> => {
    const params: DeleteQueueCommandInput = {
      QueueUrl: queueUrl,
    };
    try {
      await this.sqs.deleteQueue(params);
      return true;
    } catch (error) {
      logger(`Queue deletion failed: ${queueUrl}`);
      throw error;
    }
  };

  isFifoQueue = (queueUrl: string) => queueUrl.includes(".fifo");
}
