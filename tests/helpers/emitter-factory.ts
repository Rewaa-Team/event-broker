import EventEmitter from "events";
import { Emitter } from "../../src/emitters/emitter";
import { IEmitterOptions } from "../../src/types";

const LOCALSTACK_ENDPOINT = "http://localhost:4566/";
const REGION = "us-east-1";
const ACCOUNT_ID = "000000000000";

export function createTestEmitter(
  serviceName: string,
  overrides?: Partial<IEmitterOptions>
): Emitter {
  const options: IEmitterOptions = {
    environment: "test",
    serviceName,
    localEmitter: new EventEmitter(),
    useExternalBroker: true,
    awsConfig: {
      region: REGION,
      accountId: ACCOUNT_ID,
    },
    sqsConfig: {
      endpoint: LOCALSTACK_ENDPOINT,
      region: REGION,
      credentials: {
        accessKeyId: "test",
        secretAccessKey: "test",
      },
    },
    snsConfig: {
      endpoint: LOCALSTACK_ENDPOINT,
      region: REGION,
      credentials: {
        accessKeyId: "test",
        secretAccessKey: "test",
      },
    },
    lambdaConfig: {
      endpoint: LOCALSTACK_ENDPOINT,
      region: REGION,
      credentials: {
        accessKeyId: "test",
        secretAccessKey: "test",
      },
    },
    dynamoConfig: {
      endpoint: LOCALSTACK_ENDPOINT,
      region: REGION,
      credentials: {
        accessKeyId: "test",
        secretAccessKey: "test",
      },
    },
    isLocal: true,
    log: true,
    useIdempotency: false,
    ...overrides,
  };

  return new Emitter(options);
}
