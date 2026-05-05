import { runCollectorOnce, type CollectorSource } from "./index.js";

type Args = {
  once: boolean;
  source: CollectorSource;
  eventSku?: string;
  teamNumber?: string;
  intervalSeconds: number;
};

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (args.once) {
    const result = await runCollectorOnce({
      source: args.source,
      eventSku: args.eventSku,
      teamNumber: args.teamNumber,
      requestSource: "collector-cli"
    });
    log(result.status === "success" ? "info" : result.status === "skipped" ? "warn" : "error", "collector once complete", result);
    if (result.status === "failed") process.exitCode = 1;
    return;
  }

  log("info", "collector scheduler starting", { intervalSeconds: args.intervalSeconds, source: args.source });
  for (;;) {
    const result = await runCollectorOnce({
      source: args.source,
      eventSku: args.eventSku,
      teamNumber: args.teamNumber,
      requestSource: "collector-scheduler"
    });
    log(result.status === "success" ? "info" : result.status === "skipped" ? "warn" : "error", "collector scheduler cycle complete", result);
    await sleep(args.intervalSeconds * 1000);
  }
}

function parseArgs(args: string[]): Args {
  const parsed: Args = {
    once: false,
    source: process.env.COLLECTOR_SOURCE === "live" ? "live" : "mock",
    intervalSeconds: readInt(process.env.COLLECTOR_INTERVAL_SECONDS, 600)
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--once") parsed.once = true;
    if (arg === "--source") parsed.source = args[index + 1] === "live" ? "live" : "mock";
    if (arg === "--event") parsed.eventSku = args[index + 1];
    if (arg === "--team") parsed.teamNumber = args[index + 1];
  }
  return parsed;
}

function log(level: "info" | "warn" | "error", message: string, fields: Record<string, unknown>): void {
  process.stdout.write(JSON.stringify({ timestamp: new Date().toISOString(), level, message, ...fields }) + "\n");
}

function readInt(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((error) => {
  process.stderr.write(JSON.stringify({ timestamp: new Date().toISOString(), level: "error", message: "collector crashed", error: String(error) }) + "\n");
  process.exitCode = 1;
});
