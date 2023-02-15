import { EventEmitter } from "events";
import {
  ClientMessage,
  ConsumeOptions,
  EventListener,
  ExchangeType,
  IEmitOptions,
  IEmitter,
  IEmitterOptions,
  Topic,
} from "../types";
import { SqnsEmitter } from "./emitter.sqns";

export class Emitter implements IEmitter {
  private localEmitter: EventEmitter = new EventEmitter();
  private emitter!: IEmitter;
  private options!: IEmitterOptions;

  constructor(options: IEmitterOptions) {
    this.options = options;
    this.options.localEmitter = this.localEmitter;
    if (this.options.useExternalBroker) {
      this.emitter = new SqnsEmitter(this.options);
    }
  }

  async initialize() {
    await this.emitter.initialize();
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
  on(eventName: string, listener: EventListener<any>, options: ConsumeOptions) {
    if (this.options.useExternalBroker && !options.useLocal) {
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
  async processMessage<T extends ExchangeType>(
    exchangeType: T,
    message: ClientMessage[T],
    topicUrl?: string | undefined
  ): Promise<void> {
    return await this.emitter.processMessage(exchangeType, message, topicUrl);
  }
  getProducerReference(topic: Topic): string {
    return this.emitter.getProducerReference(topic) || '';
  }
  getConsumerReference(topic: Topic): string {
    return this.emitter.getConsumerReference(topic) || '';
  }
}