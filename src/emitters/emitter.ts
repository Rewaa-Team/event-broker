import { Message } from "@aws-sdk/client-sqs";
import { EventEmitter } from "events";
import {
  ConsumeOptions,
  EventListener,
  IEmitOptions,
  IEmitter,
  IEmitterOptions,
} from "../types";
import { Logger } from "../utils";
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

  initialize() {
    if (this.options.useExternalBroker) {
      this.emitter.initialize();
    }
  }
  async bootstrap() {
    if (this.options.useExternalBroker) {
      await this.emitter.bootstrap();
    }
  }
  async emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean> {
    if (this.options.useExternalBroker && !options?.useLocalEmitter) {
      return !!(await this.emitter.emit(eventName, options, ...args));
    }
    return this.localEmitter.emit(eventName, ...args);
  }
  on(eventName: string, options: ConsumeOptions, listener: EventListener<any>) {
    if (this.options.useExternalBroker && !options.useLocal) {
      this.emitter.on(eventName, options, listener);
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
    topicUrl?: string | undefined
  ): Promise<void> {
    return await this.emitter.processMessage(message, topicUrl);
  }
  async startConsumers(): Promise<void> {
    await this.emitter.startConsumers();
  }
  getProducerReference(topicName: string): string {
    return this.emitter.getProducerReference(topicName) || "";
  }
  getConsumerReference(topicName: string): string {
    return this.emitter.getConsumerReference(topicName) || "";
  }
}
