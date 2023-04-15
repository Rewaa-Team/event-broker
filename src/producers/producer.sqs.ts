import {
  SQS,
  SQSClientConfig,
  SendMessageResult,
  SendMessageRequest,
  CreateQueueRequest,
  GetQueueAttributesRequest,
  DeleteQueueRequest,
  DeleteMessageRequest,
  GetQueueUrlRequest,
  SetQueueAttributesRequest,
} from "@aws-sdk/client-sqs";
import { ISQSMessage, ISQSMessageOptions, Topic } from "../types";
import { Logger } from "../utils/utils";
import {
  DEFAULT_MESSAGE_DELAY,
  DEFAULT_DLQ_MESSAGE_RETENTION_PERIOD,
  DEFAULT_MESSAGE_RETENTION_PERIOD,
  DEFAULT_MAX_RETRIES,
  DEFAULT_VISIBILITY_TIMEOUT,
} from "../constants";
import { v4 } from "uuid";

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
  ): Promise<SendMessageResult> => {
    const params: SendMessageRequest = {
      MessageBody: JSON.stringify(message),
      QueueUrl: queueUrl,
      DelaySeconds: messageOptions.delay,
      MessageAttributes: {
        ...(message.messageAttributes || {}),
        ContentType: {
          DataType: "String",
          StringValue: "JSON",
        },
        QueueUrl: {
          DataType: "String",
          StringValue: queueUrl,
        },
      },
    };

    if (this.isFifoQueue(queueUrl)) {
      params.MessageDeduplicationId = message.deduplicationId || v4();
      params.MessageGroupId = message.messageGroupId;
    }

    return await this.sqs.sendMessage(params);
  };

  createQueue = async (
    queueName: string,
    attributes: Record<string, string>
  ): Promise<string | undefined> => {
    attributes = attributes || {};
    if (this.isFifoQueue(queueName)) {
      attributes.FifoQueue = "true";
    }
    const params: CreateQueueRequest = {
      QueueName: queueName,
      Attributes: attributes,
    };

    try {
      const { QueueUrl } = await this.sqs.createQueue(params);
      return QueueUrl;
    } catch (error) {
      Logger.error(`Queue creation failed: ${queueName}`);
      throw error;
    }
  };

  async createQueueFromTopic(params: {
    queueName: string;
    topic: Topic;
    isDLQ: boolean;
    queueArn: string;
    dlqArn?: string;
  }) {
    const { queueName, topic, isDLQ, queueArn, dlqArn } = params;
    let queueAttributes: Record<string, string> = {
      VisibilityTimeout: `${
        topic.visibilityTimeout || DEFAULT_VISIBILITY_TIMEOUT
      }`,
      DelaySeconds: `${DEFAULT_MESSAGE_DELAY}`,
      MessageRetentionPeriod: `${
        topic?.retentionPeriod ||
        (isDLQ
          ? DEFAULT_DLQ_MESSAGE_RETENTION_PERIOD
          : DEFAULT_MESSAGE_RETENTION_PERIOD)
      }`,
      Policy: JSON.stringify({
        Statement: [
          {
            Effect: "Allow",
            Principal: {
              Service: "sns.amazonaws.com",
            },
            Resource: queueArn,
            Action: "sqs:SendMessage",
          },
        ],
      }),
    };
    if (!isDLQ && topic.deadLetterQueueEnabled) {
      queueAttributes.RedrivePolicy = `{\"deadLetterTargetArn\":\"${dlqArn}\",\"maxReceiveCount\":\"${
        topic.maxRetryCount || DEFAULT_MAX_RETRIES
      }\"}`;
    } else {
      queueAttributes.RedrivePolicy = "";
    }
    if (this.isFifoQueue(queueName)) {
      if (topic.enableHighThroughput) {
        queueAttributes.DeduplicationScope = "messageGroup";
        queueAttributes.FifoThroughputLimit = "perMessageGroupId";
      } else {
        queueAttributes.DeduplicationScope = "queue";
        queueAttributes.FifoThroughputLimit = "perQueue";
      }
    }
    const queueUrl = await this.getQueueUrl(queueName);
    if (queueUrl) {
      await this.setQueueAttributes(queueUrl, queueAttributes);
      return;
    }
    await this.createQueue(queueName, queueAttributes);
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
      Logger.error(`Failed to fetch queue attributes: ${queueUrl}`);
      throw error;
    }
  };

  deleteQueue = async (queueUrl: string): Promise<boolean> => {
    const params: DeleteQueueRequest = {
      QueueUrl: queueUrl,
    };
    try {
      await this.sqs.deleteQueue(params);
      return true;
    } catch (error) {
      Logger.error(`Queue deletion failed: ${queueUrl}`);
      throw error;
    }
  };

  deleteMessage = async (
    queueUrl: string,
    receiptHandle: string
  ): Promise<boolean> => {
    const params: DeleteMessageRequest = {
      QueueUrl: queueUrl,
      ReceiptHandle: receiptHandle,
    };
    try {
      await this.sqs.deleteMessage(params);
      return true;
    } catch (error) {
      Logger.error(`Message deletion failed: ${queueUrl}`);
      throw error;
    }
  };

  getQueueUrl = async (queueName: string): Promise<string | undefined> => {
    const params: GetQueueUrlRequest = {
      QueueName: queueName,
    };
    try {
      const result = await this.sqs.getQueueUrl(params);
      return result.QueueUrl;
    } catch (error) {
      Logger.error(`Queue not found, creating new: ${queueName} \n ${error}`);
      return undefined;
    }
  };

  setQueueAttributes = async (
    queueUrl: string,
    attributes: Record<string, string>
  ) => {
    const params: SetQueueAttributesRequest = {
      QueueUrl: queueUrl,
      Attributes: attributes,
    };
    try {
      await this.sqs.setQueueAttributes(params);
    } catch (error) {
      Logger.error(`setQueueAttributes failed for queueUrl: ${queueUrl}`);
      throw error;
    }
  };

  isFifoQueue = (queueUrl: string) => queueUrl.includes(".fifo");
}
