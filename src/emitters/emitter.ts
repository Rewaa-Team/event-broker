import { Message } from "@aws-sdk/client-sqs";
import {
  ConsumeOptions,
  EmitBatchPayload,
  EmitPayload,
  EventListener,
  IBatchEmitOptions,
  IBatchMessage,
  IEmitOptions,
  IEmitter,
  IEmitterOptions,
  IFailedConsumerMessages,
  IFailedEmitBatchMessage,
  IMessage,
  ProcessMessageOptions,
  Queue,
  Topic,
} from "../types";
import { SqnsEmitter } from "./emitter.sqns";
import { Logger as ILogger } from "../types";
import { Logger } from "../utils/utils";
import { EventEmitter2 } from "eventemitter2";
export class Emitter implements IEmitter {
  private localEmitter: EventEmitter2 = new EventEmitter2();
  private emitter!: IEmitter;
  private options!: IEmitterOptions;
  private logger: ILogger;

  constructor(options: IEmitterOptions) {
    this.options = options;
    this.logger = options.logger ?? new Logger(!!this.options.log);
    if (this.options.useExternalBroker) {
      this.emitter = new SqnsEmitter(this.logger, this.options);
    }
  }

  async bootstrap(topics?: Topic[]) {
    if (this.options.useExternalBroker) {
      await this.emitter.bootstrap(topics);
    }
  }

    private async emitAsync(
    eventName: string,
    options?: IEmitOptions,
    payload?: any
  ): Promise<void> {
    this.logger.info(this.localEmitter.eventNames());
    try {
      await this.localEmitter.emitAsync(eventName, payload);
    } catch (error) {
      this.logger.error({
        msg: `Error emitting event ${eventName} with Emitter2`,
        eventName,
        options,
        payload,
        error,
      });

      if (!this.options.mockEmitter?.catchErrors) {
        throw error;
      }
    }
  }

   private async emitAsyncBatch(
    eventName: string,
    options?: IEmitOptions,
    payloads?: any[],
  ): Promise<void> {
    this.logger.info(this.localEmitter.eventNames());
    try {
      const emitPromises = payloads?.map((payload) =>
        this.localEmitter.emitAsync(eventName, payload)
      ) || [];
      await Promise.all(emitPromises);
    } catch (error) {
      this.logger.error({
        msg: `Error emitting event ${eventName} with Emitter2`,
        eventName,
        options,
        payloads,
        error,
      });

      if (!this.options.mockEmitter?.catchErrors) {
        throw error;
      }
    }
  }

  async emit(
    eventName: string,
    options?: IEmitOptions,
    payload?: any
  ): Promise<void> {
    if (this.options.useExternalBroker) {
      return await this.emitter.emit(eventName, options, payload);
    } else if (this.options.mockEmitter) {
      await this.emitAsync(eventName, options, payload);
    }
  }

  async emitBatch(
    eventName: string,
    messages: IBatchMessage[],
    options?: IBatchEmitOptions
  ): Promise<IFailedEmitBatchMessage[]> {
    if (this.options.useExternalBroker) {
      return await this.emitter.emitBatch(eventName, messages, options);
    } else if (this.options.mockEmitter) {
      await this.emitAsyncBatch(eventName, options, messages);
    }
    return [];
  }
  emitLocal(eventName: string, ...args: any[]) {
    return this.localEmitter.emit(eventName, ...args);
  }
  on(
    eventName: string,
    listener: EventListener<any>,
    options?: ConsumeOptions
  ) {
    if (this.options.useExternalBroker && !options?.useLocal) {
      this.emitter.on(eventName, listener, options);
      return;
    }
    this.localEmitter.on(eventName, listener);
  }
  removeListener(eventName: string, listener: EventListener<any>) {
    if (this.options.useExternalBroker) {
      this.emitter.removeListener(eventName, listener);
      return;
    }
    this.localEmitter.removeListener(eventName, listener);
  }
  removeAllListener() {
    if (this.options.useExternalBroker) {
      this.emitter.removeAllListener();
      return;
    }
    this.localEmitter.removeAllListeners();
  }
  async processMessage(
    message: Message,
    options?: ProcessMessageOptions
  ): Promise<void> {
    return await this.emitter.processMessage(message, options);
  }
  async processMessages(
    messages: Message[],
    options?: ProcessMessageOptions
  ): Promise<IFailedConsumerMessages> {
    return await this.emitter.processMessages(messages, options);
  }
  async startConsumers(): Promise<void> {
    await this.emitter.startConsumers();
  }
  getTopicReference(topic: Topic): string {
    return this.emitter.getTopicReference(topic);
  }
  getInternalTopicName(topic: Topic): string {
    return this.emitter.getInternalTopicName(topic);
  }
  getQueues(): Queue[] {
    return this.emitter.getQueues();
  }
  getQueueReference(topic: Topic): string {
    return this.emitter.getQueueReference(topic);
  }
  getInternalQueueName(topic: Topic): string {
    return this.emitter.getInternalQueueName(topic);
  }
  getEmitPayload(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): EmitPayload {
    return this.emitter.getEmitPayload(eventName, options, ...args);
  }
  getBatchEmitPayload(
    eventName: string,
    messages: IBatchMessage[],
    options?: IBatchEmitOptions
  ): EmitBatchPayload {
    return this.emitter.getBatchEmitPayload(eventName, messages, options);
  }
  parseDataFromMessage<T>(receivedMessage: Message): IMessage<T> {
    return this.emitter.parseDataFromMessage(receivedMessage);
  }
}
