import {
  Kinesis,
  KinesisClientConfig,
  PutRecordCommandInput,
  PutRecordCommandOutput,
  CreateStreamCommandInput,
  DescribeStreamCommandInput,
  StreamDescription,
  StreamMode,
  ListStreamsCommandInput,
  StreamSummary,
  StreamDescriptionSummary,
  DescribeStreamSummaryCommandInput
} from "@aws-sdk/client-kinesis";
import { IKinesisMessage } from "../types";
import { logger } from "../utils";

export class KinesisProducer {
  private readonly kinesis: Kinesis;
  constructor(config: KinesisClientConfig) {
    this.kinesis = new Kinesis(config);
  }

  get client(): Kinesis {
    return this.kinesis;
  }

  send = async (
    streamName: string,
    message: IKinesisMessage
  ): Promise<PutRecordCommandOutput> => {
    const params: PutRecordCommandInput = {
      Data: Uint8Array.from(Buffer.from(JSON.stringify(message), 'utf8')),
      PartitionKey: message.partitionKey,
      StreamName: streamName,
    };
    return await this.kinesis.putRecord(params);
  };

  createStream = async (
    streamName: string,
    shardCount: number = 1
  ): Promise<StreamDescription | undefined> => {
    const params: CreateStreamCommandInput = {
      StreamName: streamName,
      ShardCount: shardCount,
      StreamModeDetails: {
        StreamMode: StreamMode.PROVISIONED
      }
    };
    try {
      await this.kinesis.createStream(params);
      const describeParams: DescribeStreamCommandInput = {
        StreamName: streamName,
      };
      const desctiption = await this.kinesis.describeStream(describeParams);
      return desctiption.StreamDescription;
    } catch (error) {
      logger(`Stream creation failed: ${streamName}`);
      throw error;
    }
  };

  getExistingStreams = async(): Promise<StreamSummary[]> => {
    const params: ListStreamsCommandInput = {};
    const streams: StreamSummary[] = [];
    let hasMoreStreams = true;
    while(hasMoreStreams) {
        const output = await this.kinesis.listStreams(params);
        streams.push(...(output.StreamSummaries) || []);
        hasMoreStreams = !!output.HasMoreStreams;
        params.NextToken = output.NextToken;
    }
    return streams;
  }

  getDescriptionSummaryForStream = async(streamName: string): Promise<StreamDescriptionSummary | undefined> => {
    const params: DescribeStreamSummaryCommandInput = {
      StreamName: streamName
    };
    const output = await this.kinesis.describeStreamSummary(params);
    return output.StreamDescriptionSummary;
  }
}
