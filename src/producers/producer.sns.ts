import { SNS } from "aws-sdk";
import {
    ISNSMessage,
} from "../types";
import { Logger } from "../utils/utils";
import { v4 } from "uuid";

export class SNSProducer {
    private readonly sns: SNS;
    constructor(config: SNS.ClientConfiguration) {
        this.sns = new SNS(config);
    }

    get client(): SNS {
        return this.sns;
    }

    send = async (
        topicArn: string,
        message: ISNSMessage
    ): Promise<SNS.PublishResponse> => {
        const params: SNS.PublishInput = {
            Message: JSON.stringify(message),
            TargetArn: topicArn
        };

        if (this.isFifoTopic(topicArn)) {
            params.MessageDeduplicationId = v4();
            params.MessageGroupId = message.messageGroupId;
        }

        return await this.sns.publish(params).promise();
    };

    createTopic = async (
        topicName: string,
        attributes: Record<string, string>
    ): Promise<string | undefined> => {
        const params: SNS.CreateTopicInput = {
            Name: topicName,
            Attributes: attributes,
        };

        if (this.isFifoTopic(topicName) && params.Attributes) {
            params.Attributes.FifoTopic = "true";
        }

        try {
            const { TopicArn } = await this.sns.createTopic(params).promise();
            return TopicArn;
        } catch (error) {
            Logger.error(`Topic creation failed: ${topicName}`);
            throw error;
        }
    };

    subscribeToTopic = async (topicArn: string, queueArn: string): Promise<SNS.SubscribeResponse> => {
        const params: SNS.SubscribeInput = {
            TopicArn: topicArn,
            Protocol: 'sqs',
            Endpoint: queueArn
        }
        try {
            return await this.sns.subscribe(params).promise();
        } catch (error) {
            Logger.error(`Topic subscription failed: ${queueArn} to ${topicArn}`);
            throw error;
        }
    }

    isFifoTopic = (topicArn: string) => topicArn.includes(".fifo");
}
