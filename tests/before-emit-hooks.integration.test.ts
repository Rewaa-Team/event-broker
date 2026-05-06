import { Emitter } from "../src/emitters/emitter";
import {
  BeforeEmitResult,
  ExchangeType,
  IBatchMessage,
  MessageMetaData,
} from "../src/types";
import { createTestEmitter } from "./helpers/emitter-factory";
import { waitForMessages } from "./helpers/wait-for-messages";

describe("beforeEmit Hook - Dynamic Message Attributes", () => {
  let emitter: Emitter;

  afterAll(() => {
    emitter.removeAllListener();
  });

  it("should inject dynamic MessageAttributes via beforeEmit based on payload", async () => {
    emitter = createTestEmitter("before-emit-svc", {
      hooks: {
        async beforeEmit(topicName, data: any, options) {
          return {
            data,
            options: {
              ...options,
              MessageAttributes: {
                ...options?.MessageAttributes,
                tenantId: {
                  DataType: "String",
                  StringValue: data.tenantId,
                },
              },
            },
          } as BeforeEmitResult;
        },
      },
    });

    const received: { data: any; metadata: MessageMetaData | undefined }[] = [];

    emitter.on(
      "TenantOrder",
      async (data, metadata) => {
        received.push({ data, metadata });
      },
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
        separateConsumerGroup: "before-emit-consumer",
        deadLetterQueueEnabled: false,
      }
    );

    await emitter.bootstrap();
    await emitter.startConsumers();

    // Emit for tenant-1
    await emitter.emit(
      "TenantOrder",
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
      },
      { tenantId: "tenant-1", orderId: "order-100" }
    );

    // Emit for tenant-2
    await emitter.emit(
      "TenantOrder",
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
      },
      { tenantId: "tenant-2", orderId: "order-200" }
    );

    const messages = await waitForMessages(received, 2);

    expect(messages).toHaveLength(2);

    const tenant1Msg = messages.find((m) => m.data.tenantId === "tenant-1");
    const tenant2Msg = messages.find((m) => m.data.tenantId === "tenant-2");

    expect(tenant1Msg).toBeDefined();
    expect(tenant1Msg!.metadata?.messageAttributes?.tenantId?.StringValue).toBe(
      "tenant-1"
    );

    expect(tenant2Msg).toBeDefined();
    expect(tenant2Msg!.metadata?.messageAttributes?.tenantId?.StringValue).toBe(
      "tenant-2"
    );
  });
});

describe("beforeBatchEmit Hook - Dynamic Message Attributes", () => {
  let emitter: Emitter;

  afterAll(() => {
    emitter.removeAllListener();
  });

  it("should inject dynamic MessageAttributes via beforeBatchEmit per message", async () => {
    emitter = createTestEmitter("before-batch-emit-svc", {
      hooks: {
        async beforeBatchEmit(topicName, messages: IBatchMessage[]) {
          return messages.map((msg) => ({
            ...msg,
            MessageAttributes: {
              ...msg.MessageAttributes,
              tenantId: {
                DataType: "String",
                StringValue: msg.data.tenantId,
              },
            },
          }));
        },
      },
    });

    const received: { data: any; metadata: MessageMetaData | undefined }[] = [];

    emitter.on(
      "BatchTenantOrder",
      async (data, metadata) => {
        received.push({ data, metadata });
      },
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
        separateConsumerGroup: "before-batch-emit-consumer",
        deadLetterQueueEnabled: false,
      }
    );

    await emitter.bootstrap();
    await emitter.startConsumers();

    const batchMessages: IBatchMessage[] = [
      { id: "1", data: { tenantId: "tenant-A", orderId: "order-1" } },
      { id: "2", data: { tenantId: "tenant-B", orderId: "order-2" } },
      { id: "3", data: { tenantId: "tenant-C", orderId: "order-3" } },
    ];

    const failures = await emitter.emitBatch("BatchTenantOrder", batchMessages, {
      isFifo: false,
      exchangeType: ExchangeType.Fanout,
    });

    expect(failures).toHaveLength(0);

    const messages = await waitForMessages(received, 3);

    expect(messages).toHaveLength(3);

    const tenantA = messages.find((m) => m.data.tenantId === "tenant-A");
    const tenantB = messages.find((m) => m.data.tenantId === "tenant-B");
    const tenantC = messages.find((m) => m.data.tenantId === "tenant-C");

    expect(tenantA).toBeDefined();
    expect(tenantA!.metadata?.messageAttributes?.tenantId?.StringValue).toBe(
      "tenant-A"
    );

    expect(tenantB).toBeDefined();
    expect(tenantB!.metadata?.messageAttributes?.tenantId?.StringValue).toBe(
      "tenant-B"
    );

    expect(tenantC).toBeDefined();
    expect(tenantC!.metadata?.messageAttributes?.tenantId?.StringValue).toBe(
      "tenant-C"
    );
  });
});

describe("skipBeforeEmitHook - Single Emit", () => {
  let emitter: Emitter;

  afterAll(() => {
    emitter.removeAllListener();
  });

  it("should NOT inject MessageAttributes when skipBeforeEmitHook is true", async () => {
    const hookCalls: string[] = [];

    emitter = createTestEmitter("skip-hook-svc", {
      hooks: {
        async beforeEmit(topicName, data: any, options) {
          hookCalls.push(topicName);
          return {
            data,
            options: {
              ...options,
              MessageAttributes: {
                ...options?.MessageAttributes,
                tenantId: {
                  DataType: "String",
                  StringValue: data.tenantId,
                },
              },
            },
          } as BeforeEmitResult;
        },
      },
    });

    const received: { data: any; metadata: MessageMetaData | undefined }[] = [];

    emitter.on(
      "SkipHookOrder",
      async (data, metadata) => {
        received.push({ data, metadata });
      },
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
        separateConsumerGroup: "skip-hook-consumer",
        deadLetterQueueEnabled: false,
      }
    );

    await emitter.bootstrap();
    await emitter.startConsumers();

    // Emit WITH hook (no skip)
    await emitter.emit(
      "SkipHookOrder",
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
      },
      { tenantId: "tenant-hooked", orderId: "order-1" }
    );

    // Emit WITHOUT hook (skip)
    await emitter.emit(
      "SkipHookOrder",
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
        skipBeforeEmitHook: true,
      },
      { tenantId: "tenant-skipped", orderId: "order-2" }
    );

    const messages = await waitForMessages(received, 2);

    expect(messages).toHaveLength(2);

    // Hook should have been called only once
    expect(hookCalls).toHaveLength(1);

    const hookedMsg = messages.find((m) => m.data.tenantId === "tenant-hooked");
    const skippedMsg = messages.find((m) => m.data.tenantId === "tenant-skipped");

    // Hooked message should have tenantId in attributes
    expect(hookedMsg).toBeDefined();
    expect(hookedMsg!.metadata?.messageAttributes?.tenantId?.StringValue).toBe(
      "tenant-hooked"
    );

    // Skipped message should NOT have tenantId in attributes
    expect(skippedMsg).toBeDefined();
    expect(skippedMsg!.metadata?.messageAttributes?.tenantId).toBeUndefined();
  });
});

describe("skipBeforeEmitHook - Batch Emit", () => {
  let emitter: Emitter;

  afterAll(() => {
    emitter.removeAllListener();
  });

  it("should NOT call beforeBatchEmit when skipBeforeEmitHook is true", async () => {
    const hookCalls: string[] = [];

    emitter = createTestEmitter("skip-batch-hook-svc", {
      hooks: {
        async beforeBatchEmit(topicName, messages: IBatchMessage[]) {
          hookCalls.push(topicName);
          return messages.map((msg) => ({
            ...msg,
            MessageAttributes: {
              ...msg.MessageAttributes,
              tenantId: {
                DataType: "String",
                StringValue: msg.data.tenantId,
              },
            },
          }));
        },
      },
    });

    const received: { data: any; metadata: MessageMetaData | undefined }[] = [];

    emitter.on(
      "SkipBatchHookOrder",
      async (data, metadata) => {
        received.push({ data, metadata });
      },
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
        separateConsumerGroup: "skip-batch-hook-consumer",
        deadLetterQueueEnabled: false,
      }
    );

    await emitter.bootstrap();
    await emitter.startConsumers();

    // Batch emit WITH skip flag
    const batchMessages: IBatchMessage[] = [
      { id: "1", data: { tenantId: "tenant-X", orderId: "order-10" } },
      { id: "2", data: { tenantId: "tenant-Y", orderId: "order-20" } },
    ];

    const failures = await emitter.emitBatch("SkipBatchHookOrder", batchMessages, {
      isFifo: false,
      exchangeType: ExchangeType.Fanout,
      skipBeforeEmitHook: true,
    });

    expect(failures).toHaveLength(0);

    const messages = await waitForMessages(received, 2);

    expect(messages).toHaveLength(2);

    // Hook should NOT have been called
    expect(hookCalls).toHaveLength(0);

    // Messages should NOT have tenantId in attributes
    const tenantX = messages.find((m) => m.data.tenantId === "tenant-X");
    const tenantY = messages.find((m) => m.data.tenantId === "tenant-Y");

    expect(tenantX).toBeDefined();
    expect(tenantX!.metadata?.messageAttributes?.tenantId).toBeUndefined();

    expect(tenantY).toBeDefined();
    expect(tenantY!.metadata?.messageAttributes?.tenantId).toBeUndefined();
  });
});
