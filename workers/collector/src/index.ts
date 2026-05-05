import { randomUUID } from "node:crypto";
import {
  createPool,
  createRefreshJob,
  ensureEventBundle,
  readConfig,
  recordCollectionRun,
  releaseRefreshLock,
  tryAcquireRefreshLock,
  updateRefreshJob,
  upsertMatchSnapshots,
  upsertRankSnapshots,
  type AppConfig
} from "@vex-ranker/db";
import { normalizeEventSku, normalizeTeamNumber } from "@vex-ranker/ranking-engine";
import { RobotEventsClient } from "@vex-ranker/vex-client";

export type CollectorSource = "live" | "mock";

export type CollectorOptions = {
  eventSku?: string;
  teamNumber?: string;
  divisionName?: string;
  source?: CollectorSource;
  requestSource?: string;
  requestedBy?: string;
  config?: AppConfig;
};

export type CollectorResult = {
  status: "success" | "failed" | "skipped";
  eventSku: string;
  teamNumber: string;
  rankings: number;
  matches: number;
  jobId?: string;
  errorSummary?: string;
};

export async function runCollectorOnce(options: CollectorOptions = {}): Promise<CollectorResult> {
  const config = options.config ?? readConfig();
  const eventSku = normalizeEventSku(options.eventSku ?? config.defaultEventSku);
  const teamNumber = normalizeTeamNumber(options.teamNumber ?? config.defaultTeamNumber);
  const divisionName = options.divisionName ?? config.defaultDivisionName;
  const source = options.source ?? sourceFromEnv();
  const pool = createPool(config.databaseUrl);
  const owner = randomUUID();
  const lockKey = `refresh:${eventSku}:${divisionName}`;
  const startedAt = new Date();
  const jobId = await createRefreshJob(pool, {
    requestSource: options.requestSource ?? "collector",
    requestedBy: options.requestedBy ?? "system",
    eventSku,
    teamNumber
  });

  try {
    await updateRefreshJob(pool, jobId, "running");
    const acquired = await tryAcquireRefreshLock(pool, lockKey, owner, config.refreshLockSeconds);
    if (!acquired) {
      await updateRefreshJob(pool, jobId, "skipped", "refresh lock is already held");
      await recordCollectionRun(pool, {
        source: sourceLabel(source),
        eventSku,
        teamNumber,
        status: "skipped",
        startedAt,
        itemCount: 0,
        errorSummary: "refresh lock is already held"
      });
      return { status: "skipped", eventSku, teamNumber, rankings: 0, matches: 0, jobId };
    }

    const client = new RobotEventsClient({
      apiBase: apiBaseForSource(source),
      apiKey: apiKeyForSource(source),
      timeoutMs: readInt(process.env.REQUEST_TIMEOUT_MS, 20_000),
      maxRetries: readInt(process.env.HTTP_MAX_RETRIES, 3),
      rateLimitPerMinute: readInt(process.env.HTTP_RATE_LIMIT_PER_MINUTE, 30)
    });
    const bundle = await client.fetchEventBundle(eventSku, divisionName);
    const dbClient = await pool.connect();
    let rankings = 0;
    let matches = 0;
    try {
      await dbClient.query("begin");
      const ids = await ensureEventBundle(dbClient, bundle);
      rankings = await upsertRankSnapshots(dbClient, bundle, ids);
      matches = await upsertMatchSnapshots(dbClient, bundle, ids);
      await dbClient.query("commit");
    } catch (error) {
      await dbClient.query("rollback");
      throw error;
    } finally {
      dbClient.release();
    }

    await recordCollectionRun(pool, {
      source: sourceLabel(source),
      eventSku,
      teamNumber,
      status: "success",
      startedAt,
      itemCount: rankings + matches,
      metadata: { rankings, matches, divisionName }
    });
    await updateRefreshJob(pool, jobId, "success", "", { rankings, matches });
    return { status: "success", eventSku, teamNumber, rankings, matches, jobId };
  } catch (error) {
    const errorSummary = safeError(error);
    await updateRefreshJob(pool, jobId, "failed", errorSummary).catch(() => undefined);
    await recordCollectionRun(pool, {
      source: sourceLabel(source),
      eventSku,
      teamNumber,
      status: "failed",
      startedAt,
      itemCount: 0,
      errorSummary
    }).catch(() => undefined);
    return { status: "failed", eventSku, teamNumber, rankings: 0, matches: 0, jobId, errorSummary };
  } finally {
    await releaseRefreshLock(pool, lockKey, owner).catch(() => undefined);
    await pool.end();
  }
}

export function sourceFromEnv(): CollectorSource {
  return process.env.COLLECTOR_SOURCE === "live" ? "live" : "mock";
}

export function apiBaseForSource(source: CollectorSource): string {
  if (source === "mock") {
    return process.env.MOCK_ROBOTEVENTS_API_BASE ?? process.env.ROBOTEVENTS_API_BASE ?? "http://mock-vex-api:4010/api/v2";
  }
  return process.env.ROBOTEVENTS_API_BASE ?? "https://www.robotevents.com/api/v2";
}

export function apiKeyForSource(source: CollectorSource): string {
  if (source === "mock") return process.env.ROBOTEVENTS_API_KEY ?? "local-mock-key";
  if (!process.env.ROBOTEVENTS_API_KEY) {
    throw new Error("ROBOTEVENTS_API_KEY is required for live collection");
  }
  return process.env.ROBOTEVENTS_API_KEY;
}

export function sourceLabel(source: CollectorSource): string {
  return source === "mock" ? "mock_robotevents" : "robotevents";
}

export function safeError(error: unknown): string {
  const text = error instanceof Error ? error.message : String(error);
  return text
    .replace(/Bearer\s+[A-Za-z0-9._-]+/g, "Bearer [redacted]")
    .replace(/ROBOTEVENTS_API_KEY=[^&\s]+/g, "ROBOTEVENTS_API_KEY=[redacted]")
    .slice(0, 500);
}

function readInt(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}
