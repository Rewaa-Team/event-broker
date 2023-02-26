import { Lambda } from "aws-sdk";
import { ICreateQueueLambdaEventSourceInput } from "../types";
import { Logger } from "./utils";

export class LambdaClient {
  private readonly lambda: Lambda;
  constructor(config: Lambda.ClientConfiguration) {
    this.lambda = new Lambda(config);
  }

  get client(): Lambda {
    return this.lambda;
  }
  createQueueMappingForLambda = async (
    input: ICreateQueueLambdaEventSourceInput
  ): Promise<Lambda.EventSourceMappingConfiguration | void> => {
    const params: Lambda.CreateEventSourceMappingRequest = {
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
      return await this.client.createEventSourceMapping(params).promise();
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

  getEventSourceMapping = async (queueARN: string) => {
    const params: Lambda.ListEventSourceMappingsRequest = {
      EventSourceArn: queueARN,
    };
    try {
      return await this.client.listEventSourceMappings(params).promise();
    } catch (error) {
      Logger.error(
        `Failed to list Event Source Mapping for Queue: ${queueARN}`
      );
      throw error;
    }
  };
}
