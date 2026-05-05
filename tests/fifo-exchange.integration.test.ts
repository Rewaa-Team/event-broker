import { Emitter } from "../src/emitters/emitter";
import { ExchangeType } from "../src/types";
import { createTestEmitter } from "./helpers/emitter-factory";
import { waitForMessages } from "./helpers/wait-for-messages";

describe("FIFO Queue - Integration", () => {
  describe("message ordering", () => {
    let emitter: Emitter;

    afterAll(() => {
      emitter.removeAllListener();
    });

    it("should maintain message ordering in FIFO queue", async () => {
      emitter = createTestEmitter("fifo-order-service");
      const received: any[] = [];

      emitter.on(
        "OrderedEvents",
        async (data) => {
          received.push(data);
        },
        {
          isFifo: true,
          exchangeType: ExchangeType.Queue,
          deadLetterQueueEnabled: false,
          batchSize: 1,
        }
      );

      await emitter.bootstrap();
      await emitter.startConsumers();

      // Emit messages sequentially to the same message group
      for (let i = 1; i <= 5; i++) {
        await emitter.emit(
          "OrderedEvents",
          {
            isFifo: true,
            exchangeType: ExchangeType.Queue,
            partitionKey: "same-group",
          },
          { sequence: i, data: `message-${i}` }
        );
      }

      const messages = await waitForMessages(received, 5, 20000);

      expect(messages).toHaveLength(5);
      // Verify ordering within the same message group
      for (let i = 0; i < messages.length; i++) {
        expect(messages[i].sequence).toBe(i + 1);
      }
    });
  });

  describe("deduplication", () => {
    let emitter: Emitter;

    afterAll(() => {
      emitter.removeAllListener();
    });

    it("should deduplicate messages in FIFO queue", async () => {
      emitter = createTestEmitter("fifo-dedup-service");
      const received: any[] = [];

      emitter.on(
        "DeduplicatedEvents",
        async (data) => {
          received.push(data);
        },
        {
          isFifo: true,
          exchangeType: ExchangeType.Queue,
          deadLetterQueueEnabled: false,
        }
      );

      await emitter.bootstrap();
      await emitter.startConsumers();

      const deduplicationId = "unique-dedup-id-123";

      // Emit the same message twice with the same deduplication ID
      await emitter.emit(
        "DeduplicatedEvents",
        {
          isFifo: true,
          exchangeType: ExchangeType.Queue,
          partitionKey: "dedup-group",
          deduplicationId,
        },
        { value: "first" }
      );

      await emitter.emit(
        "DeduplicatedEvents",
        {
          isFifo: true,
          exchangeType: ExchangeType.Queue,
          partitionKey: "dedup-group",
          deduplicationId,
        },
        { value: "duplicate" }
      );

      // Wait a bit, only one message should arrive
      const messages = await waitForMessages(received, 2, 10000);

      // SQS FIFO deduplication should prevent the second message
      expect(messages).toHaveLength(1);
      expect(messages[0].value).toBe("first");
    });
  });
});
