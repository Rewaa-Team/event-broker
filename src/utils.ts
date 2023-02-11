import { readFileSync, writeFileSync } from "fs";

export const logger = (message: any) =>
  console.log(`EventBrokerLog :::
${message}
`);

export const mapReplacer = (key: string, value: any) => {
  if (value instanceof Map) {
    return {
      dataType: "Map",
      value: Array.from(value.entries()),
    };
  }
  return value;
};

export const mapReviver = (key: string, value: any) => {
  if (typeof value === "object" && value !== null) {
    if (value.dataType === "Map") {
      return new Map(value.value);
    }
  }
  return value;
};

export const loadDataFromFile = (filePath: string) => {
  try {
    let data: any = readFileSync(filePath, {
      encoding: "utf-8",
    });
    return data;
  } catch (error: any) {
    if (error.code !== "ENOENT") {
      throw error;
    }
  }
}

export const writeDataToFile = (filePath: string, data: any) => {
  writeFileSync(filePath, data);
}
