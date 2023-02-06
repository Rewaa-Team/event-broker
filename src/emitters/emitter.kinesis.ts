import EventEmitter from "events";
import { readFileSync, writeFileSync } from "fs";
import { KinesisProducer } from "../producers/producer.kinesis";
import {
  ClientMessage,
  EventListener,
  ExchangeType,
  IEmitOptions,
  IEmitter,
  IEmitterOptions,
  IEventTopicMap,
  IFailedEventMessage,
  IKinesisMessage,
  Topic,
} from "../types";
import { logger, mapReplacer, mapReviver } from "../utils";
import { StreamDescriptionSummary, _Record } from "@aws-sdk/client-kinesis";
import { CUSTOM_HANDLER_NAME, DEFAULT_MAX_PROCESSING_TIME } from "../constants";
import { v4 } from "uuid";

export class KinesisEmitter implements IEmitter {
  private producer!: KinesisProducer;
  private localEmitter!: EventEmitter;
  private options!: IEmitterOptions;
  private topicListeners: Map<string, EventListener<any>[]> = new Map();
  private topicMap: IEventTopicMap = {};
  private streams: Map<string, StreamDescriptionSummary> = new Map();

  async initialize(options: IEmitterOptions): Promise<void> {
    this.options = options;
    this.localEmitter = options.localEmitter;
    this.topicMap = this.options.eventTopicMap;
    this.producer = new KinesisProducer(this.options.kinesisConfig || {});
    await this.loadStreams();
  }

  private async loadStreams(): Promise<void> {
    try {
      let existingStreams: any = readFileSync(this.getStreamsFileName(), {
        encoding: "utf-8",
      });
      if (
        existingStreams &&
        existingStreams.length &&
        !this.options.refreshTopicsCache
      ) {
        existingStreams = new Map(
          JSON.parse(existingStreams, (key, value) => mapReviver(key, value))
        );
        this.streams = existingStreams;
        logger(`Streams loaded from saved file`);
        return;
      }
    } catch (error: any) {
      if (error.code !== "ENOENT") {
        throw error;
      }
    }
    await this.createStreamMapping();
    logger(`Streams loaded`);
    this.writeQueuesFile();
  }

  private writeQueuesFile() {
    writeFileSync(
      this.getStreamsFileName(),
      JSON.stringify(this.streams, (key, value) => mapReplacer(key, value))
    );
  }

  private getStreamsFileName = (): string => {
    return `${this.options.serviceName || ""}_event_broker_streams.json`;
  };

  private async createStreamMapping() {
    const uniqueStreams: Map<string, boolean> = new Map();
    const streamDescriptionPromises: Promise<
      StreamDescriptionSummary | undefined
    >[] = [];
    for (const topicKey in this.topicMap) {
      const streamName = this.topicMap[topicKey].name;
      if (uniqueStreams.has(streamName)) {
        continue;
      }
      streamDescriptionPromises.push(
        this.producer.getDescriptionSummaryForStream(streamName)
      );
      uniqueStreams.set(streamName, true);
    }
    const streamDescriptions = await Promise.all(streamDescriptionPromises);
    for (const description of streamDescriptions) {
      if (!description?.StreamName) {
        continue;
      }
      this.streams.set(description?.StreamName, description);
    }
  }

  private getStreamForEvent = (eventName: string): string | undefined => {
    return this.topicMap[eventName].name;
  };

  private logFailedEvent = (data: IFailedEventMessage) => {
    if (!this.options.eventOnFailure) {
      return;
    }
    this.localEmitter.emit(this.options.eventOnFailure, data);
  };

  async emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean> {
    try {
      const streamName = this.getStreamForEvent(eventName);
      if (!streamName) {
        throw new Error(`Stream Name not found: ${eventName}`);
      }
      await this.producer.send(streamName, {
        partitionKey: options?.partitionKey || eventName,
        eventName,
        data: args,
      });
      return true;
    } catch (error) {
      logger(`Message producing failed: ${eventName} ${JSON.stringify(error)}`);
      this.logFailedEvent({
        topic: eventName,
        event: args,
        error: error,
      });
      return false;
    }
  }

  on(eventName: string, listener: EventListener<any>) {
    let listeners = this.topicListeners.get(eventName) || [];
    listeners.push(listener);
    this.topicListeners.set(eventName, listeners);
  }

  removeAllListener() {
    this.topicListeners.clear();
  }

  removeListener(eventName: string, listener: EventListener<any>) {
    this.topicListeners.delete(eventName);
  }

  private handleMessageReceipt = async (message: _Record, streamARN: string) => {
    const key = v4();
    let data: any = message.Data;
    if(data) {
      data = Buffer.from(data).toString('utf8');
    }
    logger(
      `Kinesis Message started ${streamARN}_${key}_${new Date()}_${data}`
    );
    const covertedMessage: any = {...message};
    covertedMessage.data = data;
    const messageConsumptionStartTime = new Date();
    await this.onMessageReceived(covertedMessage, streamARN);
    const messageConsumptionEndTime = new Date();
    const difference =
      messageConsumptionEndTime.getTime() -
      messageConsumptionStartTime.getTime();
    if (
      difference >
      (this.options.maxProcessingTime || DEFAULT_MAX_PROCESSING_TIME)
    ) {
      logger(`Kinesis Slow message ${streamARN}_${key}_${new Date()}`);
    }
    logger(`Kinesis Message ended ${streamARN}_${key}_${new Date()}`);
  };

  private async onMessageReceived(receivedMessage: any, streamARN: string) {
    let message: IKinesisMessage;
    try {
      message = JSON.parse(receivedMessage.body);
    } catch (error) {
      logger("Kinesis Failed to parse message");
      this.logFailedEvent({
        topicReference: streamARN,
        event: receivedMessage.Body,
        error: `Kinesis Failed to parse message`,
      });
      throw new Error(`Kinesis Failed to parse message`);
    }
    const listeners = this.topicListeners.get(message.eventName);
    if (!listeners) {
      logger(`No listener found. Message: ${JSON.stringify(message)}`);
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

  async processMessage<T extends ExchangeType>(
    exchangeType: T,
    message: ClientMessage[T],
    topicUrl?: string | undefined
  ): Promise<void> {
    return await this.handleMessageReceipt(
      message as _Record,
      topicUrl || CUSTOM_HANDLER_NAME
    );
  }

  getTopicReference(topic: Topic): string {
    return this.streams.get(topic.name)?.StreamARN || "";
  }
}
