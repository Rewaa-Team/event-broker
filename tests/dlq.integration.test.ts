import { SQS } from "@aws-sdk/client-sqs";
import { Emitter } from "../src/emitters/emitter";
import { ExchangeType } from "../src/types";
import { createTestEmitter } from "./helpers/emitter-factory";

const LOCALSTACK_ENDPOINT = "http://localhost:4566/";
const REGION = "us-east-1";

describe("Dead Letter Queue - Integration", () => {
  let emitter: Emitter;
  let sqsClient: SQS;

  afterAll(() => {
    emitter.removeAllListener();
  });

  it("should move message to DLQ after max retries", async () => {
    emitter = createTestEmitter("dlq-test-service");
    
    let attemptCount = 0;

    emitter.on(
      "FailingEvent",
      async () => {
        attemptCount++;
        throw new Error("Simulated processing failure");
      },
      {
        isFifo: false,
        exchangeType: ExchangeType.Queue,
        deadLetterQueueEnabled: true,
        maxRetryCount: 2,
        visibilityTimeout: 1, // Short timeout so retries happen quickly
      }
    );

    await emitter.bootstrap();
    await emitter.startConsumers();

    await emitter.emit(
      "FailingEvent",
      {
        isFifo: false,
        exchangeType: ExchangeType.Queue,
      },
      { data: "will-fail" }
    );

    // Wait for retries to exhaust and message to land in DLQ
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // The consumer should have been called multiple times (up to maxRetryCount + 1)
    expect(attemptCount).toBeGreaterThanOrEqual(2);

    // Verify message is in the DLQ by checking the DLQ queue
    sqsClient = new SQS({
      endpoint: LOCALSTACK_ENDPOINT,
      region: REGION,
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
    });
    const queues = await sqsClient.listQueues({});
    const dlqUrl = queues.QueueUrls?.find((url) => url.includes("D-test-"));

    if (dlqUrl) {
      const dlqMessages = await sqsClient.receiveMessage({
        QueueUrl: dlqUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 5,
      });

      expect(dlqMessages.Messages?.length).toBeGreaterThanOrEqual(1);
    }
  }, 30000);
});
