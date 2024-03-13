import {
  Lambda,
  LambdaClientConfig,
  EventSourceMappingConfiguration,
  CreateEventSourceMappingRequest,
  ListEventSourceMappingsRequest,
} from "@aws-sdk/client-lambda";
import { ICreateQueueLambdaEventSourceInput, Logger } from "../types";

export class LambdaClient {
  private readonly lambda: Lambda;
  constructor(
    private readonly logger: Logger,
    config: LambdaClientConfig
  ) {
    this.lambda = new Lambda(config);
  }

  get client(): Lambda {
    return this.lambda;
  }
  createQueueMappingForLambda = async (
    input: ICreateQueueLambdaEventSourceInput
  ): Promise<EventSourceMappingConfiguration | void> => {
    const params: CreateEventSourceMappingRequest = {
      EventSourceArn: input.queueARN,
      FunctionName: input.functionName,
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
        return;
      }
      return await this.client.createEventSourceMapping(params);
    } catch (error: any) {
      this.logger.error(
        `Event Source Mapping Creation failed: Function: ${input.functionName}`
      );
      if (error?.name === "ResourceConflictException") {
        this.logger.warn(
          `Event Source Mapping already exists: Function: ${input.functionName}`
        );
        return;
      }
      throw error;
    }
  };

  getEventSourceMapping = async (queueARN: string) => {
    const params: ListEventSourceMappingsRequest = {
      EventSourceArn: queueARN,
    };
    try {
      return await this.client.listEventSourceMappings(params);
    } catch (error) {
      this.logger.error(
        `Failed to list Event Source Mapping for Queue: ${queueARN}`
      );
      throw error;
    }
  };
}
