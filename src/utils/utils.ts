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

export const retryWithExponentialBackoff = async <T>(
  operation: () => Promise<T>,
  maxAttempts: number,
  initialBackoffMs: number,
  shouldRetry?: (error: any) => boolean
): Promise<T> => {
  let lastError: any;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      const isThrottled =
        error?.Code === 'RequestThrottled' ||
        error?.name === 'RequestThrottled' ||
        error?.__type?.includes('RequestThrottled');
      const shouldRetryError = shouldRetry ? shouldRetry(error) : isThrottled;
      if (!shouldRetryError || attempt === maxAttempts) {
        throw error;
      }
      const backoffMs = initialBackoffMs * Math.pow(2, attempt - 1);
      const jitter = Math.random() * 1000;
      await delay(backoffMs + jitter);
    }
  }
  throw lastError;
};
