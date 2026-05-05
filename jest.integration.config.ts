import type { Config } from "jest";

const config: Config = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["<rootDir>/tests"],
  testMatch: ["**/*.integration.test.ts"],
  transform: {
    "^.+\\.ts$": [
      "ts-jest",
      {
        tsconfig: "tsconfig.test.json",
        diagnostics: {
          // Ignore errors in source files (pre-existing issues)
          exclude: ["src/**"],
        },
      },
    ],
  },
  testTimeout: 60000,
  globalSetup: "<rootDir>/tests/setup/global-setup.ts",
  globalTeardown: "<rootDir>/tests/setup/global-teardown.ts",
};

export default config;
