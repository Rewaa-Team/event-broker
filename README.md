# event-broker

A broker for all the events that Rewaa will ever produce or consume.

This package is intended to abstract out the functionality of an event broker keeping the underlying client hidden.

Currently the underlying client is SQS.
Under the hood, this broker maps events to queues on SQS. Multiple events may belong to a single queue. A queue can only be consumed by one type of consumer. This means that currently Fan-out is not supported. To send the same type of events, multiple queues would be required.


# Usage

```ts
const eventTopics: IEventTopicMap = {
	["SEND_NOTIFICATION"]: {
		name:  "NOTIFICATION",
		isConsuming:  true,
		isFifo:  false,
	},
	["SEND_NOTIFICATION_FIFO"]: {
		name:  "NOTIFICATION_FIFO",
		isConsuming:  true, //will consume this topic
		isFifo:  true,
		visibilityTimeout:  10
	}
}

const emitterOptions: = {
	emitterType:  EmitterType.SQS,
	environment:  `local`,
	eventTopicMap:  eventTopics,
	localEmitter:  new  EventEmitter(),
	useExternalBroker: true,
	isConsumer:  true, //If consuming events
	deadLetterQueueEnabled:  true
};

const emitter = new Emitter(emitterOptions);

emitter.on<{payload: ISendNotificationPayload}>("SEND_NOTIFICATION", async (...data) => {
	//Process event
});

await  emitter.initialize(emitterOptions);
```
