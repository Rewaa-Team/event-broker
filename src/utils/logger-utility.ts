import { Logger as ILogger, LogI } from "../types";

export class LoggerUtility {
  constructor(
    private readonly loggerClient: ILogger,
    private readonly logsEnabled?: boolean
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

  private processLog(log: LogI) {
    if (typeof log != "string" && log.error) {
      log.error = this.liftErrorProperties(log.error);
    }
    return log;
  }

  private log(message: LogI, level: keyof ILogger) {
    if (this.logsEnabled) {
      this.loggerClient[level](this.processLog(message));
    }
  }

  public error(message: LogI) {
    this.log(message, "error");
  }

  public warn(message: LogI) {
    this.log(message, "warn");
  }

  public debug(message: LogI) {
    this.log(message, "debug");
  }

  public info(message: LogI) {
    this.log(message, "info");
  }
}
