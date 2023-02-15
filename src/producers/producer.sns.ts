import {
    SNS,
    SNSClientConfig,
    PublishCommandInput,
    PublishCommandOutput,
    CreateTopicCommandInput,
    SubscribeCommandInput,
    SubscribeCommandOutput
} from "@aws-sdk/client-sns";
import {
    ISNSMessage,
} from "../types";
import { Logger } from "../utils";
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
        message: ISNSMessage
    ): Promise<PublishCommandOutput> => {
        const params: PublishCommandInput = {
            Message: JSON.stringify(message),
            TargetArn: topicArn
        };

        if (this.isFifoTopic(topicArn)) {
            params.MessageDeduplicationId = v4();
            params.MessageGroupId = message.messageGroupId;
        }

        return await this.sns.publish(params);
    };

    createTopic = async (
        topicName: string,
        attributes: Record<string, string>
    ): Promise<string | undefined> => {
        const params: CreateTopicCommandInput = {
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

    subscribeToTopic = async (topicArn: string, queueArn: string): Promise<SubscribeCommandOutput> => {
        const params: SubscribeCommandInput = {
            TopicArn: topicArn,
            Protocol: 'sqs',
            Endpoint: queueArn
        }
        try {
            return await this.sns.subscribe(params);
        } catch (error) {
            Logger.error(`Topic subscription failed: ${queueArn} to ${topicArn}`);
            throw error;
        }
    }

    isFifoTopic = (topicArn: string) => topicArn.includes(".fifo");
}
