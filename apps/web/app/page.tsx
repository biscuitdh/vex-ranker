import Link from "next/link";
import { readConfig, getEventRankings, getEventView, getTeamView } from "@vex-ranker/db";
import { Shell } from "../src/components/Shell";
import { MetricCard } from "../src/components/MetricCard";
import { Section } from "../src/components/Section";
import { StatusBadge } from "../src/components/StatusBadge";
import { displayNumber, formatDate } from "../src/lib/format";
import { getWebPool } from "../src/lib/db";

export const dynamic = "force-dynamic";

export default async function HomePage() {
  const config = readConfig();
  const pool = getWebPool();
  const [teamView, eventView, rankings] = await Promise.all([
    getTeamView(pool, config.defaultTeamNumber),
    getEventView(pool, config.defaultEventSku),
    getEventRankings(pool, config.defaultEventSku, config.defaultDivisionName, 12)
  ]);
  const power = teamView.derivedRankings.find((row) => row.teamNumber === config.defaultTeamNumber);

  return (
    <Shell active="home">
      <div className="grid gap-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="text-xs font-bold uppercase tracking-[0.08em] text-emerald-300">Overview</div>
            <h2 className="mt-1 text-2xl font-semibold">Current Team Status</h2>
            <p className="mt-2 max-w-3xl text-sm text-slate-400">
              Public rank, match state, and collector freshness from server-side Postgres snapshots.
            </p>
          </div>
          <Link href="/admin" className="rounded-xl border border-slate-500/20 bg-slate-900 px-4 py-3 text-sm text-slate-200">
            Admin Refresh
          </Link>
        </div>

        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
          <MetricCard label="Official Rank" value={teamView.latestRank?.rank ? `#${teamView.latestRank.rank}` : "N/A"} />
          <MetricCard label="Power Rank" value={power?.powerRank ? `#${power.powerRank}` : "N/A"} />
          <MetricCard label="Record" value={teamView.latestRank?.recordText ?? "Unknown"} />
          <MetricCard label="Rank Direction" value={teamView.delta.rankDirection} />
          <MetricCard label="Latest Run" value={<StatusBadge status={eventView.latestRun?.status ?? "none"} />} />
        </div>

        <div className="grid gap-5 lg:grid-cols-2">
          <Section label="Competition" title="Latest Snapshot">
            {teamView.latestRank ? (
              <div className="overflow-x-auto">
                <table>
                  <tbody>
                    <tr><th>Event</th><td>{teamView.latestRank.eventName}</td></tr>
                    <tr><th>Division</th><td>{teamView.latestRank.divisionName}</td></tr>
                    <tr><th>Team</th><td>{teamView.latestRank.teamNumber} / {teamView.latestRank.teamName || "Unknown"}</td></tr>
                    <tr><th>WP / AP / SP</th><td>{displayNumber(teamView.latestRank.wp)} / {displayNumber(teamView.latestRank.ap)} / {displayNumber(teamView.latestRank.sp)}</td></tr>
                    <tr><th>Last Seen</th><td>{formatDate(teamView.latestRank.lastSeenAt)}</td></tr>
                    <tr><th>Source</th><td>{teamView.latestRank.source}</td></tr>
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="rounded-xl border border-dashed border-slate-500/30 bg-slate-950/50 p-4 text-slate-400">
                No rank snapshot yet. Run the local worker or admin refresh.
              </div>
            )}
          </Section>

          <Section label="Event" title="Status">
            <div className="overflow-x-auto">
              <table>
                <tbody>
                  <tr><th>Name</th><td>{eventView.event?.name ?? "Unknown"}</td></tr>
                  <tr><th>Teams Loaded</th><td>{eventView.teamCount}</td></tr>
                  <tr><th>Divisions</th><td>{eventView.divisions.map((division) => division.name).join(", ") || "None"}</td></tr>
                  <tr><th>Last Collector</th><td>{eventView.latestRun ? `${eventView.latestRun.source} / ${formatDate(eventView.latestRun.completedAt)}` : "Never"}</td></tr>
                </tbody>
              </table>
            </div>
          </Section>
        </div>

        <Section label="Teams" title="Loaded Team Lookup">
          {rankings.length ? (
            <div className="overflow-x-auto">
              <table>
                <thead>
                  <tr><th>Rank</th><th>Team</th><th>Name</th><th>Record</th><th>Last Seen</th></tr>
                </thead>
                <tbody>
                  {rankings.map((team) => (
                    <tr key={team.id}>
                      <td>{team.rank ? `#${team.rank}` : "N/A"}</td>
                      <td>
                        <Link href={`/teams/${team.teamNumber}`} className="font-semibold text-emerald-300 hover:text-slate-50">
                          {team.teamNumber}
                        </Link>
                      </td>
                      <td>{team.teamName || team.organization || "Unknown"}</td>
                      <td>{team.recordText}</td>
                      <td>{formatDate(team.lastSeenAt)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="rounded-xl border border-dashed border-slate-500/30 bg-slate-950/50 p-4 text-slate-400">
              No loaded teams yet. Run a refresh, then use the team lookup above or browse rankings.
            </div>
          )}
        </Section>
      </div>
    </Shell>
  );
}
