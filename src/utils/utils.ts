export class Logger {
  static logsEnabled = false;
  static error(error: any) {
    if(!this.logsEnabled) return;
    console.error(`EventBrokerLog ::: ${error}`);
  }
  static warn(message: any) {
    if(!this.logsEnabled) return;
    console.warn(`EventBrokerLog ::: ${message}`);
  }
  static debug(message: any) {
    if(!this.logsEnabled) return;
    console.debug(`EventBrokerLog ::: ${message}`);
  }
  static info(message: any) {
    if(!this.logsEnabled) return;
    console.info(`EventBrokerLog ::: ${message}`);
  }
}
