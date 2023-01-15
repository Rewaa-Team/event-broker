import {
  CreateQueueRequest,
  SendMessageCommandOutput,
  SendMessageRequest,
  SQS,
  DeleteQueueCommandInput,
  SQSClientConfig,
} from "@aws-sdk/client-sqs";
import { ISQSMessage, ISQSMessageOptions, ISQSQueueCreateOptions } from "../types";
import { logger } from "../utils";
import { v4 } from "uuid";

export class SQSProducer {
  private sqs: SQS;
  constructor(config: SQSClientConfig) {
    this.sqs = new SQS(config);
  }

  get client():SQS {
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

  createQueue = async (queueName: string, options: ISQSQueueCreateOptions): Promise<string | undefined> => {
    const params: CreateQueueRequest = {
      QueueName: queueName,
      Attributes: {
        DelaySeconds: options.delay,
        MessageRetentionPeriod: options.messageRetentionPeriod,
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
