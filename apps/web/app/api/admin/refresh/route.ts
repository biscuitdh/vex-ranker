import { enforceRateLimit, readConfig, verifyAdminToken } from "@vex-ranker/db";
import { runCollectorOnce } from "@vex-ranker/collector";
import { getWebPool } from "../../../../src/lib/db";
import {
  getBearerToken,
  getClientRateKey,
  jsonError,
  refreshBodySchema
} from "../../../../src/lib/validation";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const config = readConfig();
  const body = await readBody(request);
  const token = getBearerToken(request) || body.adminToken || "";
  if (!verifyAdminToken(token, config.adminToken)) {
    return jsonError(401, "unauthorized", "Admin token is required");
  }

  const pool = getWebPool();
  const rate = await enforceRateLimit(
    pool,
    getClientRateKey(request),
    config.adminRateLimitRequests,
    config.adminRateLimitWindowSeconds
  );
  if (!rate.allowed) return jsonError(429, "rate_limited", "Refresh rate limit exceeded");

  const parsed = refreshBodySchema.safeParse(body);
  if (!parsed.success) return jsonError(400, "invalid_refresh_request", "Invalid refresh request");

  const result = await runCollectorOnce({
    eventSku: parsed.data.eventSku ?? config.defaultEventSku,
    teamNumber: parsed.data.teamNumber ?? config.defaultTeamNumber,
    source: parsed.data.source ?? "mock",
    requestSource: "web-admin",
    requestedBy: getClientRateKey(request),
    config
  });

  if (result.status === "failed") {
    return Response.json({ result }, { status: 502 });
  }
  return Response.json({ result, rateLimitRemaining: rate.remaining });
}

async function readBody(request: Request): Promise<Record<string, string>> {
  const contentType = request.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    const raw = await request.json().catch(() => ({}));
    return normalizeRecord(raw);
  }
  if (contentType.includes("application/x-www-form-urlencoded") || contentType.includes("multipart/form-data")) {
    const form = await request.formData();
    return Object.fromEntries(Array.from(form.entries()).map(([key, value]) => [key, String(value)]));
  }
  return {};
}

function normalizeRecord(value: unknown): Record<string, string> {
  if (!value || typeof value !== "object") return {};
  const output: Record<string, string> = {};
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    if (item != null) output[key] = String(item);
  }
  return output;
}
