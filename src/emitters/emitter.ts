import { EventEmitter } from "events";
import { SqsEmitter } from "./emitter.sqs";
import {
  ClientMessage,
  EventListener,
  ExchangeType,
  IEmitOptions,
  IEmitter,
  IEmitterOptions,
  IEventTopicMap,
  Topic,
} from "../types";
import { SnsEmitter } from "./emitter.sns";

export class Emitter implements IEmitter {
  private localEmitter: EventEmitter = new EventEmitter();
  private emitterMap: Map<ExchangeType, IEmitter | undefined> = new Map();
  private options: IEmitterOptions;

  constructor(options: IEmitterOptions) {
    this.options = options;
  }
  async initialize(options: IEmitterOptions) {
    this.options = options;
    this.options.localEmitter = this.localEmitter;
    if (this.options.useExternalBroker) {
      const directTopics = this.getTopicsOfType(ExchangeType.Direct);
      const fanoutTopics = this.getTopicsOfType(ExchangeType.Fanout);
      if (Object.keys(directTopics).length) {
        const emitter = new SqsEmitter();
        await emitter.initialize({
          ...this.options,
          eventTopicMap: directTopics,
        });
        this.emitterMap.set(ExchangeType.Direct, emitter);
      }
      if (Object.keys(fanoutTopics).length) {
        const emitter = new SnsEmitter();
        await emitter.initialize({
          ...this.options,
          eventTopicMap: fanoutTopics,
        });
        this.emitterMap.set(ExchangeType.Fanout, emitter);
      }
    }
  }
  async emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean> {
    if (this.options.useExternalBroker && !options?.useLocalEmitter) {
      const emitter = this.emitterMap.get(this.options.eventTopicMap[eventName].exchangeType);
      return !!(await emitter?.emit(eventName, options, ...args));
    }
    return this.localEmitter.emit(eventName, ...args);
  }
  on(eventName: string, listener: EventListener<any>, useLocal?: boolean) {
    if (this.options.useExternalBroker && !useLocal) {
      const emitter = this.emitterMap.get(this.options.eventTopicMap[eventName].exchangeType);
      emitter?.on(eventName, listener);
      return;
    }
    this.localEmitter.on(eventName, listener);
  }
  removeListener(eventName: string, listener: EventListener<any>) {
    if (this.options.useExternalBroker) {
      const emitter = this.emitterMap.get(this.options.eventTopicMap[eventName].exchangeType);
      emitter?.removeListener(eventName, listener);
      return;
    }
    this.localEmitter.removeListener(eventName, listener);
  }
  removeAllListener() {
    if (this.options.useExternalBroker) {
      for (const key in this.emitterMap) {
        this.emitterMap.get(key as ExchangeType)?.removeAllListener();
      }
      return;
    }
    this.localEmitter.removeAllListeners();
  }
  async processMessage<T extends ExchangeType>(
    exchangeType: T,
    message: ClientMessage[T],
    topicUrl?: string | undefined
  ): Promise<void> {
    const emitter = this.emitterMap.get(exchangeType);
    return await emitter?.processMessage(exchangeType, message, topicUrl);
  }
  getTopicReference(topic: Topic): string {
    const emitter = this.emitterMap.get(topic.exchangeType);
    return emitter?.getTopicReference(topic) || '';
  }
  async startConsumers(): Promise<void> {
    if (this.options.useExternalBroker && this.options.isConsumer) {
      for (const key in this.emitterMap) {
        await this.emitterMap.get(key as ExchangeType)?.startConsumers();
      }
    }
  }
  private getTopicsOfType(type: ExchangeType): IEventTopicMap {
    const topics: IEventTopicMap = {};
    for (const key in this.options.eventTopicMap) {
      if (this.options.eventTopicMap[key].exchangeType === type) {
        topics[key] = this.options.eventTopicMap[key];
      }
    }
    return topics;
  }
}
