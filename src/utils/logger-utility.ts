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

  public error(message: LogI) {
    if (this.logsEnabled) {
      this.loggerClient.error(this.processLog(message));
    }
  }

  public warn(message: LogI) {
    if (this.logsEnabled) {
      this.loggerClient.warn(this.processLog(message));
    }
  }

  public debug(message: LogI) {
    if (this.logsEnabled) {
      this.loggerClient.debug(this.processLog(message));
    }
  }

  public info(message: LogI) {
    if (this.logsEnabled) {
      this.loggerClient.info(this.processLog(message));
    }
  }
}
