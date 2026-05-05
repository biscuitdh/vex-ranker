import Link from "next/link";
import { redirect } from "next/navigation";
import { readConfig } from "@vex-ranker/db";
import { Shell } from "../../src/components/Shell";
import { Section } from "../../src/components/Section";
import { teamNumberSchema } from "../../src/lib/validation";

export const dynamic = "force-dynamic";

type TeamSearchParams = {
  teamNumber?: string | string[];
};

export default async function TeamsPage({ searchParams }: { searchParams: Promise<TeamSearchParams> }) {
  const config = readConfig();
  const params = await searchParams;
  const rawTeamNumber = Array.isArray(params.teamNumber) ? params.teamNumber[0] : params.teamNumber;
  const requestedTeam = rawTeamNumber?.trim() ?? "";

  if (requestedTeam) {
    const parsed = teamNumberSchema.safeParse(requestedTeam);
    if (parsed.success) {
      redirect(`/teams/${encodeURIComponent(parsed.data)}`);
    }
  }

  return (
    <Shell active="team">
      <div className="grid gap-5">
        <div>
          <div className="text-xs font-bold uppercase tracking-[0.08em] text-emerald-300">Teams</div>
          <h2 className="mt-1 text-2xl font-semibold">Open Any Team</h2>
          <p className="mt-2 max-w-3xl text-sm text-slate-400">
            Enter a valid VEX team number to view stored rank history, match state, and derived rankings.
          </p>
        </div>

        <Section label="Lookup" title="Team Selector">
          <form action="/teams" method="get" className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_auto]">
            <label className="sr-only" htmlFor="team-number">Team number</label>
            <input
              id="team-number"
              name="teamNumber"
              autoCapitalize="characters"
              autoComplete="off"
              className="rounded-xl border border-slate-500/20 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition placeholder:text-slate-500 focus:border-emerald-300/50"
              defaultValue={requestedTeam}
              placeholder={`Example: ${config.defaultTeamNumber}`}
            />
            <button
              className="rounded-xl border border-emerald-300/30 bg-emerald-300/15 px-4 py-3 text-sm font-semibold text-slate-50 transition hover:bg-emerald-300/20"
              type="submit"
            >
              View Team
            </button>
          </form>
          {requestedTeam ? (
            <div className="mt-3 rounded-xl border border-red-400/20 bg-red-400/10 p-3 text-sm text-red-100">
              Invalid team number. Expected a VEX team format like <span className="font-semibold">{config.defaultTeamNumber}</span>.
            </div>
          ) : null}
          <div className="mt-4 flex flex-wrap gap-2 text-sm">
            <Link href={`/teams/${config.defaultTeamNumber}`} className="rounded-xl border border-slate-500/20 bg-slate-950 px-4 py-3 text-slate-200">
              Open Default Team
            </Link>
            <Link href={`/events/${config.defaultEventSku}/rankings`} className="rounded-xl border border-slate-500/20 bg-slate-950 px-4 py-3 text-slate-200">
              Browse Loaded Rankings
            </Link>
          </div>
        </Section>
      </div>
    </Shell>
  );
}
