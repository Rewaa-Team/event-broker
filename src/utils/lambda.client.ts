import {
  CreateEventSourceMappingCommandInput,
  CreateEventSourceMappingCommandOutput,
  ListEventSourceMappingsCommandInput,
  UpdateEventSourceMappingCommandInput,
  UpdateEventSourceMappingCommandOutput,
  Lambda,
  LambdaClientConfig,
} from "@aws-sdk/client-lambda";
import { ICreateQueueLambdaEventSourceInput } from "../types";
import { Logger } from "./utils";

export class LambdaClient {
  private readonly lambda: Lambda;
  constructor(config: LambdaClientConfig) {
    this.lambda = new Lambda(config);
  }

  get client(): Lambda {
    return this.lambda;
  }
  createQueueMappingForLambda = async (
    input: ICreateQueueLambdaEventSourceInput
  ): Promise<CreateEventSourceMappingCommandOutput | void> => {
    const params: CreateEventSourceMappingCommandInput = {
      EventSourceArn: input.queueARN,
      FunctionName: input.functionName,
      ScalingConfig: {
        MaximumConcurrency: input.maximumConcurrency,
      },
      BatchSize: input.batchSize,
    };
    try {
      const { EventSourceMappings } = await this.getEventSourceMapping(
        input.queueARN
      );
      let functionName: string | undefined;
      if (EventSourceMappings?.length) {
        functionName = (EventSourceMappings[0].FunctionArn || "").split(
          "function:"
        )[1];
        if (functionName && functionName !== input.functionName) {
          throw new Error(
            `Lambda Function ${functionName} is already listening for Queue ${input.queueARN}`
          );
        }
      }
      if (functionName === input.functionName) {
        if (
          EventSourceMappings?.[0].ScalingConfig?.MaximumConcurrency !==
          input.maximumConcurrency
        ) {
          return await this.updateQueueMappingForLambda(
            EventSourceMappings?.[0].UUID!,
            input
          );
        }
        return;
      }
      return await this.client.createEventSourceMapping(params);
    } catch (error: any) {
      Logger.error(
        `Event Source Mapping Creation failed: Function: ${
          input.functionName
        } ${JSON.stringify(error)}`
      );
      if (error?.name === "ResourceConflictException") {
        Logger.warn(
          `Event Source Mapping already exists: Function: ${input.functionName}`
        );
        return;
      }
      throw error;
    }
  };

  updateQueueMappingForLambda = async (
    uuid: string,
    input: ICreateQueueLambdaEventSourceInput
  ) => {
    const params: UpdateEventSourceMappingCommandInput = {
      UUID: uuid,
      ScalingConfig: {
        MaximumConcurrency: input.maximumConcurrency,
      },
    };
    return await this.client.updateEventSourceMapping(params);
  };

  getEventSourceMapping = async (queueARN: string) => {
    const params: ListEventSourceMappingsCommandInput = {
      EventSourceArn: queueARN,
    };
    try {
      return await this.client.listEventSourceMappings(params);
    } catch (error) {
      Logger.error(
        `Failed to list Event Source Mapping for Queue: ${queueARN}`
      );
      throw error;
    }
  };
}
