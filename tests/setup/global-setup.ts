import { execSync } from "child_process";

export default async function globalSetup() {
  console.log("\n🚀 Starting LocalStack via docker-compose...");
  execSync("docker compose up -d --wait", { stdio: "inherit" });

  // Wait for LocalStack to be ready
  const maxRetries = 30;
  for (let i = 0; i < maxRetries; i++) {
    try {
      execSync("curl -s http://localhost:4566/_localstack/health", {
        stdio: "pipe",
      });
      console.log("✅ LocalStack is ready");
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
  throw new Error("LocalStack failed to start within timeout");
}
