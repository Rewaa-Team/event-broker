import { Emitter } from "../src/emitters/emitter";
import { ExchangeType } from "../src/types";
import { createTestEmitter } from "./helpers/emitter-factory";
import { waitForMessages } from "./helpers/wait-for-messages";

describe("Queue Exchange Type - Integration", () => {
  describe("single message", () => {
    let emitter: Emitter;

    afterAll(() => {
      emitter.removeAllListener();
    });

    it("should emit and consume a single message via Queue exchange", async () => {
      emitter = createTestEmitter("queue-single-svc");
      const received: any[] = [];

      emitter.on(
        "OrderCreated",
        async (data) => {
          received.push(data);
        },
        {
          isFifo: false,
          exchangeType: ExchangeType.Queue,
          deadLetterQueueEnabled: true,
        }
      );

      await emitter.bootstrap();
      await emitter.startConsumers();

      const payload = { orderId: "order-123", amount: 99.99 };

      await emitter.emit(
        "OrderCreated",
        {
          isFifo: false,
          exchangeType: ExchangeType.Queue,
        },
        payload
      );

      const messages = await waitForMessages(received, 1);

      expect(messages).toHaveLength(1);
      expect(messages[0]).toMatchObject(payload);
    });
  });

  describe("batch messages", () => {
    let emitter: Emitter;

    afterAll(() => {
      emitter.removeAllListener();
    });

    it("should emit and consume batch messages via Queue exchange", async () => {
      emitter = createTestEmitter("queue-batch-svc");
      const received: any[] = [];

      emitter.on(
        "ItemsProcessed",
        async (data) => {
          received.push(data);
        },
        {
          isFifo: false,
          exchangeType: ExchangeType.Queue,
          deadLetterQueueEnabled: false,
        }
      );

      await emitter.bootstrap();
      await emitter.startConsumers();

      const messages = [
        { id: "1", data: { itemId: "item-1" } },
        { id: "2", data: { itemId: "item-2" } },
        { id: "3", data: { itemId: "item-3" } },
      ];

      const failures = await emitter.emitBatch("ItemsProcessed", messages, {
        isFifo: false,
        exchangeType: ExchangeType.Queue,
      });

      expect(failures).toHaveLength(0);

      const result = await waitForMessages(received, 3);

      expect(result).toHaveLength(3);
      expect(result.map((r: any) => r.itemId).sort()).toEqual([
        "item-1",
        "item-2",
        "item-3",
      ]);
    });
  });
});
