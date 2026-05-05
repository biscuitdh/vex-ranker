import { getEventRankings } from "@vex-ranker/db";
import { getWebPool } from "../../../../../src/lib/db";
import { eventSkuSchema, jsonError } from "../../../../../src/lib/validation";

export const dynamic = "force-dynamic";

export async function GET(_request: Request, { params }: { params: Promise<{ eventSku: string }> }) {
  const parsedParams = await params;
  const parsed = eventSkuSchema.safeParse(parsedParams.eventSku);
  if (!parsed.success) return jsonError(400, "invalid_event_sku", "Invalid event SKU");
  try {
    const rankings = await getEventRankings(getWebPool(), parsed.data, undefined, 250);
    return Response.json({ eventSku: parsed.data, rankings });
  } catch {
    return jsonError(500, "rankings_lookup_failed", "Unable to load rankings");
  }
}
