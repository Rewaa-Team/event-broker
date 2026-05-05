import { Emitter } from "../src/emitters/emitter";
import { ExchangeType } from "../src/types";
import { createTestEmitter } from "./helpers/emitter-factory";
import { waitForMessages } from "./helpers/wait-for-messages";

describe("Fanout Exchange Type - Integration", () => {
  let emitter: Emitter;

  afterAll(() => {
    emitter.removeAllListener();
  });

  it("should emit and consume a message via Fanout exchange with multiple consumer groups", async () => {
    emitter = createTestEmitter("fanout-test-service");
    const receivedGroup1: any[] = [];
    const receivedGroup2: any[] = [];

    // Consumer group 1
    emitter.on(
      "UserNotification",
      async (data) => {
        receivedGroup1.push(data);
      },
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
        deadLetterQueueEnabled: true,
        separateConsumerGroup: "email_service",
      }
    );

    // Consumer group 2
    emitter.on(
      "UserNotification",
      async (data) => {
        receivedGroup2.push(data);
      },
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
        deadLetterQueueEnabled: true,
        separateConsumerGroup: "push_service",
      }
    );

    await emitter.bootstrap();
    await emitter.startConsumers();

    const payload = {
      message: "Hello!",
      userId: "user-456",
    };

    await emitter.emit(
      "UserNotification",
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
      },
      payload
    );

    const [group1Messages, group2Messages] = await Promise.all([
      waitForMessages(receivedGroup1, 1),
      waitForMessages(receivedGroup2, 1),
    ]);

    // Both consumer groups should receive the message
    expect(group1Messages).toHaveLength(1);
    expect(group1Messages[0]).toMatchObject(payload);

    expect(group2Messages).toHaveLength(1);
    expect(group2Messages[0]).toMatchObject(payload);
  });
});
