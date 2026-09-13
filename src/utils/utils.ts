import { Logger as ILogger, LogLevel } from "../types";

const LOG_LEVEL_SEVERITY: Record<LogLevel, number> = {
  [LogLevel.Debug]: 0,
  [LogLevel.Info]: 1,
  [LogLevel.Warn]: 2,
  [LogLevel.Error]: 3,
  [LogLevel.Silent]: 4,
};

const consoleLogger: ILogger = {
  error: (error: any) => console.error(`EventBrokerLog ::: ${error}`),
  warn: (message: any) => console.warn(`EventBrokerLog ::: ${message}`),
  debug: (message: any) => console.debug(`EventBrokerLog ::: ${message}`),
  info: (message: any) => console.info(`EventBrokerLog ::: ${message}`),
};

/**
 * Forwards logs at or above the given level to the underlying logger
 * (console by default) and drops the rest
 */
export class Logger implements ILogger {
  constructor(
    private readonly level: LogLevel,
    private readonly logger: ILogger = consoleLogger
  ) {
    if (!Object.values(LogLevel).includes(level)) {
      throw new Error(
        `Invalid logLevel "${level}". Expected one of: ${Object.values(
          LogLevel
        ).join(", ")}`
      );
    }
  }

  private isEnabled(level: LogLevel): boolean {
    return LOG_LEVEL_SEVERITY[level] >= LOG_LEVEL_SEVERITY[this.level];
  }

  public error(error: any) {
    if (!this.isEnabled(LogLevel.Error)) return;
    this.logger.error(error);
  }

  public warn(message: any) {
    if (!this.isEnabled(LogLevel.Warn)) return;
    this.logger.warn(message);
  }

  public debug(message: any) {
    if (!this.isEnabled(LogLevel.Debug)) return;
    this.logger.debug(message);
  }

  public info(message: any) {
    if (!this.isEnabled(LogLevel.Info)) return;
    this.logger.info(message);
  }
}

export const delay = (ms: number): Promise<void> => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};
