
# event-broker

A broker for producing and consuming messages across multiple micro-services.

This package is intended to abstract out the functionality of an event broker keeping the underlying client hidden.

For now, the underlying client is SNS+SQS. So this package requires AWS and is limited in functionality by the quotas for SNS and SQS respectively.

# Supported Exchange Types

The broker supports 2 exchange types currently:

 - **Queue**: This means that the topic will have 1 - 1 mapping between the producer and consumer i.e. only 1 consumer can consume the messages on this type of topic.
 
 - **Fanout**: This type of exchange means a 1 - Many mapping between the producer and consumer. So messages on this type of topic can be consumed by multiple consumers. For fanout topics, message filtering is also supported.

# Serverless Support

Topics can be mapped to lambda functions for consumption. The broker supports specifying lambda functions along with the batch size in which case it does the mapping by itself. However it is recommended to do the mapping in the serverless file. For this purpose, the broker also exposes helper functions to get the internally generated ARNs of both the topic and queues.
A serverless plugin can be used in this case.
The broker also exposes a method `processMessage` which takes in any consumed message and executes it as per the mapping it has. This can be used in case you are using only 1 lambda for all types of topics. Since the broker knows about the mapping of events to functions, it will handle the execution.

# Offline Support

This broker package can be simulated on an offline platform like localstack. As long as Lambda, SQS and SNS are on the same network the broker will work. For offline support, the broker takes as input any options for sqs, sns or lambda like the endpoint and a flag isLocal which when true will use the provided endpoints.

For serverless, [serverless-offline-sqs](https://www.npmjs.com/package/serverless-offline-sqs) can be used to redirect sqs to localstack so that the event source mapping can be entertained by serverless itself.

# Usage

## Initialising the broker

```ts
import {
	Emitter,
	IEmitterOptions,
} from  "@rewaa/event-broker";
import { EventEmitter } from  'events';

const env = `local`;
const region = `us-east-1`;
const emitterOptions: IEmitterOptions = {
	environment: env,
	localEmitter: new EventEmitter(),
	useExternalBroker: `true`,
	awsConfig: {
		accountId: `000000000000`,
		region
	},
	log:  true,
};

if(env === 'local') {
	emitterOptions.isLocal = true;
	emitterOptions.lambdaConfig = {
		endpoint: `http://localhost:4000`,
		region
	}
	emitterOptions.sqsConfig = {
		endpoint: `http://localhost:4566`,
		region
	}
	emitterOptions.snsConfig = {
		endpoint:  `http://localhost:4566`,
		region
	}
}

const emitter = new Emitter(this.emitterOptions);
```

## Consuming from topics

```ts
import { ExchangeType } from  '@rewaa/event-broker';

interface Notification {
	name: string;
	payload: any;
}

emitter.on<Notification>("Notification",
	async (...data) => {
		const  input = data[0]; // typeof === Notification
		// Do something with the Notification
	}, {
		isFifo: false,
		exchangeType:  ExchangeType.Queue,
		deadLetterQueueEnabled:  true,
	}
);
```

## Emitting to topics

```ts
import { ExchangeType } from  '@rewaa/event-broker';

const notification = {
	name: `Some notification`,
	payload: {
		text: `Hello`
	}
}

await emitter.emit("Notification", {
	exchangeType:  ExchangeType.Queue,
	isFifo:  false
}, notification);
```

## Fanout

For fanout, the exchange type of message must be 'Fanout'.

Emitter:
```ts
import { ExchangeType } from  '@rewaa/event-broker';

const notification = {
	name: `Some notification`,
	payload: {
		text: `Hello`
	}
}

await emitter.emit("Notification", {
	exchangeType:  ExchangeType.Fanout,
	isFifo:  false
}, notification);
```

Consumer 1:
```ts
import { ExchangeType } from  '@rewaa/event-broker';

emitter.on<Notification>("Notification",
	async (...data) => {
		const  input = data[0]; // typeof === Notification
		// Do something with the Notification
	}, {
		isFifo: false,
		exchangeType:  ExchangeType.Fanout,
		deadLetterQueueEnabled:  true,
		separateConsumerGroup: "consumer_group_1"
	}
);
```

Consumer 2:
```ts
import { ExchangeType } from  '@rewaa/event-broker';

emitter.on<Notification>("Notification",
	async (...data) => {
		const  input = data[0]; // typeof === Notification
		// Do something with the Notification
	}, {
		isFifo: false,
		exchangeType:  ExchangeType.Fanout,
		deadLetterQueueEnabled:  true,
		separateConsumerGroup: "consumer_group_2"
	}
);
```

## Deploying

The deployment of resources created by the broker is a separate process extracted out in the method `bootstrap`.

This method takes an optional array of topics which can be useful for serverless case where we might not have attached the consumer by calling the `on` method.
if you have called the `on` method on the emitter object for all the topics before calling `bootstrap`, then the topics array is not required.

`bootsrap` can be called only once during deployment. The APIs used internally are idempotent so providing the same topics won't create duplicate resources. However currently, updates are not supported, so updating an attribute of the topic like retention period won't change anything. This is part of the roadmap.

# Roadmap

The following things are part of the roadmap for the broker:

- Batch message consumption

- Schema Registry: Adding a layer for registering topics and providing validations for 		schema. This will also help in eliminating the options that are required to be provided while emitting using `emit` or attaching a consumer using `on`. The registration part can be part of the deployment phase.
