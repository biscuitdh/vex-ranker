import { getHealth } from "@vex-ranker/db";
import { getWebPool } from "../../../src/lib/db";
import { jsonError } from "../../../src/lib/validation";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const health = await getHealth(getWebPool());
    return Response.json(health, { status: health.ok ? 200 : 503 });
  } catch {
    return jsonError(503, "health_failed", "Health check failed");
  }
}
