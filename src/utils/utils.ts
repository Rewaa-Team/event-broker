import { Logger as ILogger } from "../types";

export class Logger implements ILogger {
  constructor(private readonly logsEnabled: boolean) {}

  public error(error: any) {
    if(!this.logsEnabled) return;
    console.error(`EventBrokerLog ::: ${error}`);
  }

  public warn(message: any) {
    if(!this.logsEnabled) return;
    console.warn(`EventBrokerLog ::: ${message}`);
  }

  public debug(message: any) {
    if(!this.logsEnabled) return;
    console.debug(`EventBrokerLog ::: ${message}`);
  }

  public info(message: any) {
    if(!this.logsEnabled) return;
    console.info(`EventBrokerLog ::: ${message}`);
  }
}

export const delay = (ms: number): Promise<void> => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};
