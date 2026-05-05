import { getTeamView } from "@vex-ranker/db";
import { getWebPool } from "../../../../src/lib/db";
import { jsonError, teamNumberSchema } from "../../../../src/lib/validation";

export const dynamic = "force-dynamic";

export async function GET(_request: Request, { params }: { params: Promise<{ teamNumber: string }> }) {
  const parsedParams = await params;
  const parsed = teamNumberSchema.safeParse(parsedParams.teamNumber);
  if (!parsed.success) return jsonError(400, "invalid_team_number", "Invalid team number");
  try {
    const view = await getTeamView(getWebPool(), parsed.data);
    if (!view.team && !view.latestRank) return jsonError(404, "team_not_found", "Team not found");
    return Response.json(view);
  } catch {
    return jsonError(500, "team_lookup_failed", "Unable to load team");
  }
}
