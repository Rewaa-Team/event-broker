import { EventEmitter } from "events";
import { SqsEmitter } from "./emitter.sqs";
import {
  EmitterType,
  EventListener,
  IEmitOptions,
  IEmitter,
  IEmitterOptions,
} from "../types";

export class Emitter implements IEmitter {
  private localEmitter: EventEmitter = new EventEmitter();
  private emitter: IEmitter | undefined;
  private options: IEmitterOptions;

  constructor(options: IEmitterOptions) {
    this.options = options;
  }
  async initialize(options: IEmitterOptions) {
    this.options = options;
    this.options.localEmitter = this.localEmitter;
    if (
      this.options.useExternalBroker &&
      this.options.emitterType === EmitterType.SQS
    ) {
      this.emitter = new SqsEmitter();
      await this.emitter.initialize(this.options);
    }
  }
  async initializeConsumer(): Promise<void> {
    if (
      this.options.isConsumer &&
      this.options.emitterType === EmitterType.SQS
    ) {
      await this.emitter?.initializeConsumer();
    }
  }
  async emit(
    eventName: string,
    options?: IEmitOptions,
    ...args: any[]
  ): Promise<boolean> {
    if (this.options.useExternalBroker && !options?.useLocalEmitter) {
      return !!(await this.emitter?.emit(eventName, options, ...args));
    }
    return this.localEmitter.emit(eventName, ...args);
  }
  on(eventName: string, listener: EventListener<any>, useLocal?: boolean) {
    if (
      this.options.useExternalBroker &&
      this.options.isConsumer &&
      !useLocal
    ) {
      this.emitter?.on(eventName, listener);
      return;
    }
    this.localEmitter.on(eventName, listener);
  }
  removeListener(eventName: string, listener: EventListener<any>) {
    if (this.options.useExternalBroker) {
      this.emitter?.removeListener(eventName, listener);
      return;
    }
    this.localEmitter.removeListener(eventName, listener);
  }
  removeAllListener() {
    if (this.options.useExternalBroker) {
      this.emitter?.removeAllListener();
      return;
    }
    this.localEmitter.removeAllListeners();
  }
}
