import { inspect } from "node:util";
import { Logger as ILogger, LogI } from "../types";

export class Logger implements ILogger {
  constructor(
    private readonly logsEnabled: boolean,
    private readonly environment: string
  ) {}

  private liftErrorProperties(error: any) {
    return {
      name: error.name,
      code: error.code,
      message: error.message,
      stack: error.stack,
      ...error,
    };
  }

  private log(level: number, message: LogI) {
    if (this.logsEnabled) {
      let log: Record<string, unknown> = {
        level,
        time: new Date().valueOf(),
        environment: this.environment,
        context: "EventBrokerLog",
      };

      if (typeof message === "string") {
        log = { ...log, message };
      } else {
        log = { ...log, ...message };
      }

      if (log.error) {
        log.error = this.liftErrorProperties(log.error);
      }

      console.log(inspect(log, { depth: null }));
    }
  }

  public error(error: LogI) {
    this.log(50, error);
  }

  public warn(message: LogI) {
    this.log(40, message);
  }

  public debug(message: LogI) {
    this.log(20, message);
  }

  public info(message: LogI) {
    this.log(30, message);
  }
}
