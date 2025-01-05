export enum DynamoTable {
  Idempotency = "event-broker-idempotency",
}

export interface DynamoTableStructure {
  tableName: DynamoTable;
  partitionKey: string;
  sortKey?: string;
  expiryKey?: string,
  attributes: Record<string, string>;
  expiry?: number;
}
