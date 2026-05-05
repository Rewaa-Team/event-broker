import { execSync } from "child_process";

export default async function globalTeardown() {
  console.log("\n🧹 Stopping LocalStack...");
  execSync("docker compose down -v", { stdio: "inherit" });
  console.log("✅ LocalStack stopped");
}
