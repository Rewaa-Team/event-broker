export class Logger {
  static error(error: any) {
    console.error(`EventBrokerLog ::: ${error}`);
  }
  static warn(message: any) {
    console.warn(`EventBrokerLog ::: ${message}`);
  }
  static debug(message: any) {
    console.debug(`EventBrokerLog ::: ${message}`);
  }
  static info(message: any) {
    console.info(`EventBrokerLog ::: ${message}`);
  }
}
