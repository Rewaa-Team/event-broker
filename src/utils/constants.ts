import { DynamoTable } from "./types";
import { CreateTableCommandInput } from "@aws-sdk/client-dynamodb";

export const DynamoTablesStructure: Record<
  DynamoTable,
  CreateTableCommandInput & {
    expiryKey?: string;
  }
> = {
  [DynamoTable.Idempotency]: {
    TableName: DynamoTable.Idempotency,
    KeySchema: [{ AttributeName: "partitionKey", KeyType: "HASH" }],
    AttributeDefinitions: [
      { AttributeName: "partitionKey", AttributeType: "S" },
    ],
    BillingMode: "PAY_PER_REQUEST",
    expiryKey: 'expiresAt',
  },
};
