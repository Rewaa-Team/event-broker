import { SQS } from "aws-sdk";
import { EventEmitter } from "events";
import {
  ConsumeOptions,
  EventListener,
  ExchangeType,
  IEmitOptions,
  IEmitter,
  IEmitterOptions,
  Queue,
  Topic,
} from "../types";
import { Logger } from "../utils/utils";
import { SqnsEmitter } from "./emitter.sqns";

export class Emitter implements IEmitter {
  private localEmitter: EventEmitter = new EventEmitter();
  private emitter!: IEmitter;
  private options!: IEmitterOptions;

  constructor(options: IEmitterOptions) {
    this.options = options;
    this.options.localEmitter = this.localEmitter;
    Logger.logsEnabled = !!this.options.log;
    if (this.options.useExternalBroker) {
      this.emitter = new SqnsEmitter(this.options);
    }
  }

  async bootstrap(topics?: Topic[]) {
    if (this.options.useExternalBroker) {
      await this.emitter.bootstrap(topics);
    }
  }
  async emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean> {
    if (this.options.useExternalBroker) {
      return !!(await this.emitter.emit(eventName, options, ...args));
    }
    return false;
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
    message: SQS.Message,
    topicReference?: string | undefined
  ): Promise<void> {
    return await this.emitter.processMessage(message, topicReference);
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
}
