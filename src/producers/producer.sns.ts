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
import { PAYLOAD_STRUCTURE_VERSION_V2 } from "../constants";

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

    params.MessageAttributes = this.getMessageAttributesForPublish(message.messageAttributes);
    return params;
  }

  private getMessageAttributesForPublish(
    messageAttributes: ISNSMessage['messageAttributes']
  ): ISNSMessage['messageAttributes'] {
    return {
      ...messageAttributes,
      PayloadVersion: {
        DataType: "String",
        StringValue: PAYLOAD_STRUCTURE_VERSION_V2,
      },
    };
  }

  sendBatch = async (
    topicArn: string,
    messages: ISNSMessage[]
  ): Promise<PublishBatchResponse> => {
    return await this.sns.publishBatch(this.getBatchPublishInput(topicArn, messages));
  };

  getBatchPublishInput(
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
          MessageAttributes: this.getMessageAttributesForPublish(message.messageAttributes),
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
    filterPolicy?: object,
    deliverRawMessage?: boolean,
  ): Promise<SubscribeResponse> => {
    const params: SubscribeInput = {
      TopicArn: topicArn,
      Protocol: "sqs",
      Endpoint: queueArn,
      Attributes: {}
    };

    if (params.Attributes) {
      if(filterPolicy) {
        params.Attributes.FilterPolicy = JSON.stringify(filterPolicy);
      }
      if(deliverRawMessage) {
        params.Attributes.RawMessageDelivery = 'true';
      } else {
        params.Attributes.RawMessageDelivery = 'false';
      }
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
