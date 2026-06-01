import {
  GetSubscriptionAttributesCommand,
  ListSubscriptionsByTopicCommand,
  SNSClient,
} from "@aws-sdk/client-sns";
import { Emitter } from "../src/emitters/emitter";
import { ExchangeType } from "../src/types";
import { createTestEmitter } from "./helpers/emitter-factory";

const snsClient = new SNSClient({
  endpoint: "http://localhost:4566/",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

const getFilterPolicy = async (
  topicArn: string,
  queueArn: string
): Promise<string | undefined> => {
  const { Subscriptions } = await snsClient.send(
    new ListSubscriptionsByTopicCommand({ TopicArn: topicArn })
  );
  const sub = (Subscriptions ?? []).find((s) => s.Endpoint === queueArn);
  if (!sub?.SubscriptionArn) return undefined;
  const { Attributes } = await snsClient.send(
    new GetSubscriptionAttributesCommand({
      SubscriptionArn: sub.SubscriptionArn,
    })
  );
  return Attributes?.FilterPolicy;
};

describe("FilterPolicy reconciliation on existing subscription - Integration", () => {
  let emitter: Emitter;

  afterAll(() => {
    emitter.removeAllListener();
  });

  it("updates the FilterPolicy when re-subscribing an existing subscription", async () => {
    const eventName = "FilterPolicyReconcileEvent";

    // First bootstrap creates the subscription with an initial policy.
    const first = createTestEmitter("filter-policy-service");
    first.on(
      eventName,
      async () => undefined,
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
        deadLetterQueueEnabled: true,
        separateConsumerGroup: "filter_policy_consumer",
        filterPolicy: { tenantId: ["tenant-1"] },
      }
    );
    await first.bootstrap();

    const topicArn = first.getTopicReference({
      name: eventName,
      isFifo: false,
      exchangeType: ExchangeType.Fanout,
    });
    const queueArn = first.getQueueReference({
      name: eventName,
      isFifo: false,
      exchangeType: ExchangeType.Fanout,
      separateConsumerGroup: "filter_policy_consumer",
    });

    expect(JSON.parse((await getFilterPolicy(topicArn, queueArn))!)).toEqual({
      tenantId: ["tenant-1"],
    });

    // Second bootstrap on the SAME subscription with a DIFFERENT policy.
    // Previously this threw "Subscription already exists with different
    // attributes"; now it must reconcile the policy in place.
    emitter = createTestEmitter("filter-policy-service");
    emitter.on(
      eventName,
      async () => undefined,
      {
        isFifo: false,
        exchangeType: ExchangeType.Fanout,
        deadLetterQueueEnabled: true,
        separateConsumerGroup: "filter_policy_consumer",
        filterPolicy: { tenantId: ["tenant-1", "tenant-2"] },
      }
    );
    await expect(emitter.bootstrap()).resolves.not.toThrow();

    expect(JSON.parse((await getFilterPolicy(topicArn, queueArn))!)).toEqual({
      tenantId: ["tenant-1", "tenant-2"],
    });

    first.removeAllListener();
  });
});
