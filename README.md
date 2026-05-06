# event-broker

A broker for producing and consuming messages across multiple micro-services.

This package abstracts the functionality of an event broker, keeping the underlying client hidden. The underlying client is SNS+SQS, so this package requires AWS and is limited by the quotas for SNS and SQS respectively.

## Features

- Multiple exchange types: **Fanout** and **Queue**
- Dead Letter Queues
- Batch emit/consume
- SNS Filter Policies for message routing
- Hooks (`beforeEmit`, `beforeBatchEmit`, `afterEmit`, `beforeConsume`, `afterConsume`)
- Dynamic Message Attributes injection via hooks
- Consumer Idempotency (DynamoDB-backed)
- Outbox pattern support
- Message compression (Brotli)
- Updates for topic properties (retention, throughput, etc.)
- Serverless support (Lambda event source mappings)
- LocalStack support
- Mock emitter for local testing

## Supported Exchange Types

- **Queue**: 1-to-1 mapping between producer and consumer. Only one consumer receives each message.
- **Fanout**: 1-to-many mapping. Messages are delivered to all subscribed consumer groups. Supports SNS filter policies for selective delivery.

## Installation

```bash
npm install @rewaa/event-broker
```

Peer dependencies:
```bash
npm install @aws-sdk/client-sqs @aws-sdk/client-sns @aws-sdk/client-lambda @aws-sdk/client-dynamodb @smithy/node-http-handler
```

## Usage

### Initializing the Broker

```ts
import { Emitter, IEmitterOptions } from "@rewaa/event-broker";
import { EventEmitter } from "events";

const emitter = new Emitter({
  environment: "prod",
  serviceName: "order-service",
  localEmitter: new EventEmitter(),
  useExternalBroker: true,
  awsConfig: {
    accountId: "123456789012",
    region: "us-east-1",
  },
  log: true,
});
```

#### LocalStack / Offline Configuration

```ts
const emitter = new Emitter({
  environment: "local",
  serviceName: "order-service",
  localEmitter: new EventEmitter(),
  useExternalBroker: true,
  isLocal: true,
  awsConfig: {
    accountId: "000000000000",
    region: "us-east-1",
  },
  sqsConfig: {
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  },
  snsConfig: {
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  },
  lambdaConfig: {
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  },
  dynamoConfig: {
    endpoint: "http://localhost:4566",
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  },
  log: true,
});
```

#### Mock Emitter (No AWS)

For unit testing or local development without AWS:

```ts
const emitter = new Emitter({
  environment: "test",
  serviceName: "order-service",
  localEmitter: new EventEmitter(),
  useExternalBroker: false,
  mockEmitter: { throwErrors: true },
});
```

---

### Consuming Messages

#### Queue Exchange (1-to-1)

```ts
import { ExchangeType } from "@rewaa/event-broker";

emitter.on<{ orderId: string }>(
  "OrderCreated",
  async (data, metadata) => {
    console.log(data.orderId);
    console.log(metadata?.messageAttributes);
  },
  {
    isFifo: false,
    exchangeType: ExchangeType.Queue,
    deadLetterQueueEnabled: true,
  }
);
```

#### Fanout Exchange (1-to-many)

```ts
// Consumer group 1 — email service
emitter.on(
  "UserNotification",
  async (data) => {
    sendEmail(data);
  },
  {
    isFifo: false,
    exchangeType: ExchangeType.Fanout,
    separateConsumerGroup: "email_service",
  }
);

// Consumer group 2 — push service
emitter.on(
  "UserNotification",
  async (data) => {
    sendPush(data);
  },
  {
    isFifo: false,
    exchangeType: ExchangeType.Fanout,
    separateConsumerGroup: "push_service",
  }
);
```

#### With Consumer Group Options

```ts
emitter.on(
  "OrderCreated",
  async (data) => { /* ... */ },
  {
    isFifo: false,
    exchangeType: ExchangeType.Fanout,
    consumerGroup: {
      name: "inventory-service",
      batchSize: 5,
      visibilityTimeout: 120,
      isFifo: false,
      workers: 2,
    },
  }
);
```

#### With Filter Policy

Only receive messages whose `MessageAttributes` match the filter:

```ts
emitter.on(
  "OrderEvents",
  async (data) => {
    // Only receives messages where eventType is "order_created"
  },
  {
    isFifo: false,
    exchangeType: ExchangeType.Fanout,
    separateConsumerGroup: "order-creation-handler",
    filterPolicy: {
      eventType: ["order_created"],
    },
  }
);
```

---

### Emitting Messages

#### Single Emit — Queue

```ts
await emitter.emit(
  "OrderCreated",
  {
    isFifo: false,
    exchangeType: ExchangeType.Queue,
  },
  { orderId: "order-123", amount: 99.99 }
);
```

#### Single Emit — Fanout

```ts
await emitter.emit(
  "UserNotification",
  {
    isFifo: false,
    exchangeType: ExchangeType.Fanout,
  },
  { userId: "user-1", message: "Hello!" }
);
```

#### With Message Attributes (for Filter Policies)

```ts
await emitter.emit(
  "OrderEvents",
  {
    isFifo: false,
    exchangeType: ExchangeType.Fanout,
    MessageAttributes: {
      eventType: { DataType: "String", StringValue: "order_created" },
      tenantId: { DataType: "String", StringValue: "tenant-123" },
    },
  },
  { orderId: "order-456" }
);
```

#### FIFO Emit

```ts
await emitter.emit(
  "OrderUpdates",
  {
    isFifo: true,
    exchangeType: ExchangeType.Fanout,
    partitionKey: "order-123", // ensures ordering per order
    deduplicationId: "unique-update-id", // prevents duplicates within 5 min
  },
  { orderId: "order-123", status: "shipped" }
);
```

#### Compressed Emit

```ts
await emitter.emit(
  "LargePayloadEvent",
  {
    isFifo: false,
    exchangeType: ExchangeType.Queue,
    compressed: true,
  },
  largePayload
);
```

---

### Batch Emit

```ts
const messages = [
  { id: "1", data: { itemId: "item-1" } },
  { id: "2", data: { itemId: "item-2" } },
  { id: "3", data: { itemId: "item-3" } },
];

const failures = await emitter.emitBatch("ItemsProcessed", messages, {
  isFifo: false,
  exchangeType: ExchangeType.Queue,
});

// failures contains any messages that failed to send
if (failures.length > 0) {
  console.error("Failed messages:", failures);
}
```

---

### Hooks

Hooks allow you to intercept and modify messages at various stages of the emit/consume lifecycle.

```ts
const emitter = new Emitter({
  // ...config
  hooks: {
    beforeEmit,
    beforeBatchEmit,
    afterEmit,
    beforeConsume,
    afterConsume,
  },
});
```

#### `beforeEmit` — Dynamic Message Attributes

Called before every single emit. Can modify both the payload and emit options (including `MessageAttributes`).

```ts
const emitter = new Emitter({
  // ...config
  hooks: {
    async beforeEmit(topicName, data, options) {
      // Inject tenantId from payload into MessageAttributes
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
      };
    },
  },
});

// tenantId attribute is automatically injected from payload
await emitter.emit(
  "OrderCreated",
  { isFifo: false, exchangeType: ExchangeType.Fanout },
  { tenantId: "tenant-123", orderId: "order-1" }
);
```

**Backward compatible**: You can also return just the modified data (without options):

```ts
hooks: {
  async beforeEmit(topicName, data) {
    return { ...data, timestamp: Date.now() };
  },
}
```

#### `beforeBatchEmit` — Dynamic Attributes per Batch Message

Called before a batch emit. Receives the full array of messages and can modify each one individually.

```ts
const emitter = new Emitter({
  // ...config
  hooks: {
    async beforeBatchEmit(topicName, messages) {
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
```

#### `afterEmit`

Called after a message is successfully sent.

```ts
hooks: {
  async afterEmit(topicName, data) {
    logger.info(`Emitted to ${topicName}`, data);
  },
}
```

#### `beforeConsume`

Called before the consumer function is invoked. Can transform the payload.

```ts
hooks: {
  async beforeConsume(topicName, data) {
    return { ...data, receivedAt: Date.now() };
  },
}
```

#### `afterConsume`

Called after the consumer function completes successfully.

```ts
hooks: {
  async afterConsume(topicName, data) {
    logger.info(`Consumed from ${topicName}`, data);
  },
}
```

#### Skipping Hooks

Use `skipBeforeEmitHook: true` in emit options to bypass `beforeEmit` or `beforeBatchEmit` for a specific call:

```ts
// Single emit — skip hook
await emitter.emit(
  "OrderCreated",
  {
    isFifo: false,
    exchangeType: ExchangeType.Fanout,
    skipBeforeEmitHook: true,
  },
  payload
);

// Batch emit — skip hook
await emitter.emitBatch("OrderCreated", messages, {
  isFifo: false,
  exchangeType: ExchangeType.Fanout,
  skipBeforeEmitHook: true,
});
```

---

### SNS Filter Policies

Filter policies allow consumers to receive only messages that match specific attribute criteria. This is evaluated at the SNS subscription level — non-matching messages are never delivered to the queue.

**Producer** — set attributes on emit:

```ts
await emitter.emit(
  "OrderEvents",
  {
    isFifo: false,
    exchangeType: ExchangeType.Fanout,
    MessageAttributes: {
      eventType: { DataType: "String", StringValue: "order_created" },
    },
  },
  { orderId: "order-1" }
);
```

**Consumer** — set filter policy on subscription:

```ts
emitter.on(
  "OrderEvents",
  async (data) => {
    // Only receives messages where eventType matches
  },
  {
    isFifo: false,
    exchangeType: ExchangeType.Fanout,
    separateConsumerGroup: "order-creation-handler",
    filterPolicy: {
      eventType: ["order_created"],
    },
  }
);
```

**Combining with `beforeEmit` for automatic attribute injection:**

```ts
const emitter = new Emitter({
  hooks: {
    async beforeEmit(topicName, data, options) {
      return {
        data,
        options: {
          ...options,
          MessageAttributes: {
            ...options?.MessageAttributes,
            tenantId: { DataType: "String", StringValue: data.tenantId },
          },
        },
      };
    },
  },
});

// Consumer only receives messages for tenant-123
emitter.on("OrderEvents", handler, {
  exchangeType: ExchangeType.Fanout,
  separateConsumerGroup: "tenant-123-handler",
  filterPolicy: { tenantId: ["tenant-123"] },
});
```

---

### Bootstrap (Resource Deployment)

Call `bootstrap()` once during deployment to create all AWS resources (topics, queues, subscriptions, event source mappings):

```ts
// Register consumers first
emitter.on("OrderCreated", handler, options);
emitter.on("UserNotification", handler, options);

// Create all resources
await emitter.bootstrap();

// Start polling consumers
await emitter.startConsumers();
```

Alternatively, pass topics directly to bootstrap (useful for serverless where `on` may not be called):

```ts
await emitter.bootstrap([
  { name: "OrderCreated", isFifo: false, exchangeType: ExchangeType.Queue },
  { name: "UserNotification", isFifo: false, exchangeType: ExchangeType.Fanout },
]);
```

---

### Consumer Idempotency

The broker supports DynamoDB-backed consumer idempotency to prevent duplicate processing:

```ts
const emitter = new Emitter({
  // ...config
  useIdempotency: true,
  consumerIdempotencyOptions: {
    strategy: ConsumerIdempotencyStrategy.DeduplicationId,
    expiry: 300, // 5 minutes
  },
});
```

Strategies:
- `DeduplicationId` — uses the message's deduplication ID
- `PayloadHash` — generates a SHA-256 hash of the payload
- `Custom` — provide a custom key function:

```ts
consumerIdempotencyOptions: {
  strategy: ConsumerIdempotencyStrategy.Custom,
  key: (payload, metadata) => `${payload.orderId}-${payload.version}`,
  expiry: 600,
}
```

---

### Outbox Pattern

For transactional guarantees, the broker supports the outbox pattern:

```ts
const emitter = new Emitter({
  // ...config
  outboxConfig: {
    // your outbox configuration
  },
});

// Emit via outbox — event is saved to DB first, then relayed
await emitter.emit(
  "OrderCreated",
  {
    isFifo: false,
    exchangeType: ExchangeType.Queue,
    outboxData: { transaction: myDbTransaction },
  },
  payload
);
```

---

### Serverless Support

Topics can be mapped to Lambda functions:

```ts
emitter.on("OrderCreated", async () => {}, {
  isFifo: false,
  exchangeType: ExchangeType.Queue,
  lambdaHandler: {
    functionName: "order-service-prod-processOrder",
    maximumConcurrency: 10,
  },
  batchSize: 5,
});

await emitter.bootstrap();
```

The broker exposes helper methods for getting internal ARNs:

```ts
const topicArn = emitter.getTopicReference({ name: "OrderCreated", isFifo: false, exchangeType: ExchangeType.Queue });
const queueArn = emitter.getQueueReference({ name: "OrderCreated", isFifo: false, exchangeType: ExchangeType.Queue });
```

For Lambda consumers, use `processMessage` or `processMessages` in your handler:

```ts
export const handler = async (event) => {
  return await emitter.processMessages(event.Records);
};
```

---

### Default Queue Options

When using fanout topics without specifying a `separateConsumerGroup`, messages are consumed from default queues:

```ts
const emitter = new Emitter({
  // ...config
  defaultQueueOptions: {
    fifo: {
      name: "default-fifo-queue",
      batchSize: 10,
      visibilityTimeout: 360,
    },
    standard: {
      name: "default-standard-queue",
      batchSize: 10,
      visibilityTimeout: 360,
    },
  },
});
```

---

### Updating Topic Properties

The following properties are automatically updated when bootstrapping if changed:

| Property | Updated |
| --- | --- |
| visibilityTimeout | Yes |
| batchSize | Yes |
| maxRetryCount | Yes |
| deadLetterQueueEnabled | Yes (attaches/detaches DLQ, doesn't delete) |
| separateConsumerGroup | Yes (creates new queue, old not deleted) |
| enableHighThroughput | Yes |
| retentionPeriod | Yes |
| contentBasedDeduplication | Yes |
| tags | Yes |

---

### Graceful Shutdown

The broker handles `SIGTERM` for graceful consumer draining:

```ts
await emitter.startConsumers();
// On SIGTERM, consumers will drain inflight messages before stopping
```

You can also drain manually:

```ts
await emitter.drainConsumers();
```

---

## API Reference

### `Emitter`

| Method | Description |
| --- | --- |
| `bootstrap(topics?)` | Create AWS resources (topics, queues, subscriptions) |
| `emit(eventName, options, payload)` | Emit a single message |
| `emitBatch(eventName, messages, options)` | Emit up to 10 messages in a batch |
| `on(eventName, listener, options)` | Register a consumer for an event |
| `startConsumers()` | Start polling SQS queues |
| `drainConsumers()` | Gracefully stop all consumers |
| `processMessage(message, options)` | Process a single SQS message (for Lambda) |
| `processMessages(messages, options)` | Process a batch of SQS messages (for Lambda) |
| `removeListener(eventName, listener)` | Remove a specific listener |
| `removeAllListener()` | Remove all listeners |
| `getTopicReference(topic)` | Get the internal ARN of a topic |
| `getQueueReference(topic)` | Get the internal ARN of a queue |
| `getInternalTopicName(topic)` | Get the internal name of a topic |
| `getInternalQueueName(topic)` | Get the internal name of a queue |
| `getQueues()` | Get all registered queues |
| `getEmitPayload(eventName, options, payload)` | Get the raw AWS request payload for a single emit |
| `getBatchEmitPayload(eventName, messages, options)` | Get the raw AWS request payload for a batch emit |
| `parseDataFromMessage(message)` | Parse a raw SQS message into the broker's message format |

---

## Running Integration Tests

The project uses LocalStack for integration testing:

```bash
npm run test:integration
```

This will:
1. Start LocalStack via docker-compose
2. Run all integration tests
3. Tear down LocalStack

---

## Roadmap

- **Schema Registry**: Adding a layer for registering topics and providing validations for schema. This will also help in eliminating the options that are required to be provided while emitting or attaching a consumer.
