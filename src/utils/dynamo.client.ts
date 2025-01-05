import {
  AttributeValue,
  GetItemCommand,
  GetItemCommandInput,
  PutItemCommand,
  DynamoDB,
  DynamoDBClientConfig,
  CreateTableCommand,
  CreateTableCommandOutput,
  PutItemCommandInput,
  CreateTableCommandInput,
} from "@aws-sdk/client-dynamodb";
import { DynamoTablesStructure } from "./constants";
import { DynamoTable } from "./types";
import { Logger } from "../types";

export class DynamoClient {
  private readonly dynamoDB: DynamoDB;
  constructor(private readonly logger: Logger, config: DynamoDBClientConfig) {
    this.dynamoDB = new DynamoDB(config);
  }

  get client(): DynamoDB {
    return this.dynamoDB;
  }

  public async exists(input: GetItemCommandInput): Promise<boolean> {
    const item = await this.getItem(input);
    return !!item;
  }

  public async getItem(
    input: GetItemCommandInput
  ): Promise<Record<string, AttributeValue> | undefined> {
    const item = await this.client.getItem(input);
    return item.Item;
  }

  public async putItem(
    input: PutItemCommandInput,
    expiry?: number
  ): Promise<void> {
    try {
      let expiresAt: number | undefined = undefined;
      const expiryKey =
        DynamoTablesStructure[input.TableName as DynamoTable].expiryKey;

      // Default expiry is 5s
      expiresAt = Math.floor(
        (new Date().getTime() + (expiry || 5) * 1000) / 1000
      );
      await this.client.putItem({
        ...input,
        Item: {
          ...input.Item,
          ...(expiryKey && { [expiryKey]: { N: expiresAt.toString() } }),
        },
      });
    } catch (error) {
      this.logger.error(`Failed to put item: ${JSON.stringify(error)}`);
      throw error;
    }
  }

  public async createTable(
    command: CreateTableCommandInput
  ): Promise<CreateTableCommandOutput | undefined> {
    try {
      const output = await this.client.createTable(command);
      const expiryKey =
        DynamoTablesStructure[command.TableName as DynamoTable].expiryKey;
      if (expiryKey) {
        await this.client.updateTimeToLive({
          TableName: command.TableName,
          TimeToLiveSpecification: {
            AttributeName: expiryKey,
            Enabled: true,
          },
        });
      }
      this.logger.info(`Table ${command.TableName} Created`);
      return output;
    } catch (error: any) {
      if (error.name === "ResourceInUseException") {
        this.logger.warn(
          `Table ${command.TableName} already exists, ignoring creation`
        );
        return;
      }
      this.logger.error(
        `Table ${command.TableName} Creation failed: ${JSON.stringify(error)}`
      );
      throw error;
    }
  }
}
