import {
  SNS,
  SNSClientConfig,
  PublishResponse,
  PublishInput,
  CreateTopicInput,
  SubscribeInput,
  SubscribeResponse,
  PublishBatchResponse,
  PublishBatchInput
} from "@aws-sdk/client-sns";
import { ISNSMessage, Logger } from "../types";
import { v4 } from "uuid";

export class SNSProducer {
  private readonly sns: SNS;
  constructor(
    private readonly logger: Logger,
    config: SNSClientConfig
  ) {
    this.sns = new SNS(config);
  }

  get client(): SNS {
    return this.sns;
  }

  send = async (
    topicArn: string,
    message: ISNSMessage
  ): Promise<PublishResponse> => {
    return await this.sns.publish(this.getPublishInput(topicArn, message));
  };

  getPublishInput(topicArn: string, message: ISNSMessage) {
    const params: PublishInput = {
      Message: JSON.stringify(message),
      TargetArn: topicArn,
    };

    if (this.isFifoTopic(topicArn)) {
      params.MessageDeduplicationId = message.deduplicationId || v4();
      params.MessageGroupId = message.messageGroupId;
    }

    params.MessageAttributes = message.messageAttributes;
    return params;
  }

  sendBatch = async (
    topicArn: string,
    messages: ISNSMessage[]
  ): Promise<PublishBatchResponse> => {
    return await this.sns.publishBatch(this.getBatchPublishInputs(topicArn, messages));
  };

  getBatchPublishInputs(
    topicArn: string,
    messages: ISNSMessage[]
  ): PublishBatchInput {
    const isFifo = this.isFifoTopic(topicArn);
    const params: PublishBatchInput = {
      TopicArn: topicArn,
      PublishBatchRequestEntries: messages.map((message) => {
        return {
          Id: message.id!,
          Message: JSON.stringify(message),
          MessageAttributes: message.messageAttributes,
          ...(isFifo && {
            MessageDeduplicationId: message.deduplicationId || v4(),
            MessageGroupId: message.messageGroupId,
          }),
        };
      }),
    };
    return params;
  }

  createTopic = async (
    topicName: string,
    attributes: Record<string, string>
  ): Promise<string | undefined> => {
    const params: CreateTopicInput = {
      Name: topicName,
      Attributes: attributes,
    };

    if (this.isFifoTopic(topicName) && params.Attributes) {
      params.Attributes.FifoTopic = "true";
    }

    try {
      const { TopicArn } = await this.sns.createTopic(params);
      return TopicArn;
    } catch (error) {
      this.logger.error(`Topic creation failed: ${topicName}`);
      throw error;
    }
  };

  subscribeToTopic = async (
    topicArn: string,
    queueArn: string,
    filterPolicy?: any
  ): Promise<SubscribeResponse> => {
    const params: SubscribeInput = {
      TopicArn: topicArn,
      Protocol: "sqs",
      Endpoint: queueArn,
    };

    if (filterPolicy) {
      params.Attributes = {
        FilterPolicy: JSON.stringify(filterPolicy),
      };
    }

    try {
      return await this.sns.subscribe(params);
    } catch (error) {
      this.logger.error(`Topic subscription failed: ${queueArn} to ${topicArn}`);
      throw error;
    }
  };

  isFifoTopic = (topicArn: string) => topicArn.includes(".fifo");
}
