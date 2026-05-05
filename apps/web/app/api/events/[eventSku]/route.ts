import { getEventView } from "@vex-ranker/db";
import { getWebPool } from "../../../../src/lib/db";
import { eventSkuSchema, jsonError } from "../../../../src/lib/validation";

export const dynamic = "force-dynamic";

export async function GET(_request: Request, { params }: { params: Promise<{ eventSku: string }> }) {
  const parsedParams = await params;
  const parsed = eventSkuSchema.safeParse(parsedParams.eventSku);
  if (!parsed.success) return jsonError(400, "invalid_event_sku", "Invalid event SKU");
  try {
    const view = await getEventView(getWebPool(), parsed.data);
    if (!view.event) return jsonError(404, "event_not_found", "Event not found");
    return Response.json(view);
  } catch {
    return jsonError(500, "event_lookup_failed", "Unable to load event");
  }
}
