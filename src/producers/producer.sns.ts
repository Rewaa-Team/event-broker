import {
  SNS,
  SNSClientConfig,
  PublishResponse,
  PublishInput,
  CreateTopicInput,
  SubscribeInput,
  SubscribeResponse,
} from "@aws-sdk/client-sns";
import { ISNSMessage } from "../types";
import { Logger } from "../utils/utils";
import { v4 } from "uuid";

export class SNSProducer {
  private readonly sns: SNS;
  constructor(config: SNSClientConfig) {
    this.sns = new SNS(config);
  }

  get client(): SNS {
    return this.sns;
  }

  send = async (
    topicArn: string,
    iMessage: ISNSMessage
  ): Promise<PublishResponse> => {
    const { messageAttr, messageGroupId, ...message } = iMessage;
    const params: PublishInput = {
      Message: JSON.stringify(message),
      TargetArn: topicArn,
    };

    if (this.isFifoTopic(topicArn)) {
      params.MessageDeduplicationId = message.deduplicationId || v4();
      params.MessageGroupId = messageGroupId;
    }

    if (messageAttr) params.MessageAttributes = messageAttr as any;

    return await this.sns.publish(params);
  };

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
      Logger.error(`Topic creation failed: ${topicName}`);
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
      Logger.error(`Topic subscription failed: ${queueArn} to ${topicArn}`);
      throw error;
    }
  };

  isFifoTopic = (topicArn: string) => topicArn.includes(".fifo");
}
