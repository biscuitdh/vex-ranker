import { notFound } from "next/navigation";
import { getTeamView } from "@vex-ranker/db";
import { Shell } from "../../../src/components/Shell";
import { MetricCard } from "../../../src/components/MetricCard";
import { Section } from "../../../src/components/Section";
import { StatusBadge } from "../../../src/components/StatusBadge";
import { displayNumber, formatDate } from "../../../src/lib/format";
import { getWebPool } from "../../../src/lib/db";
import { teamNumberSchema } from "../../../src/lib/validation";

export const dynamic = "force-dynamic";

export default async function TeamPage({ params }: { params: Promise<{ teamNumber: string }> }) {
  const parsedParams = await params;
  const parsed = teamNumberSchema.safeParse(parsedParams.teamNumber);
  if (!parsed.success) notFound();
  const teamView = await getTeamView(getWebPool(), parsed.data);
  const power = teamView.derivedRankings.find((row) => row.teamNumber === parsed.data);

  return (
    <Shell active="team">
      <div className="grid gap-5">
        <div className="min-w-0">
          <div className="text-xs font-bold uppercase tracking-[0.08em] text-emerald-300">Team</div>
          <h2 className="mt-1 break-words text-2xl font-semibold">{parsed.data} / {teamView.team?.teamName || "Unknown"}</h2>
          <p className="mt-2 break-words text-sm text-slate-400">{teamView.team?.organization || "Organization unavailable"}</p>
        </div>

        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
          <MetricCard label="Official Rank" value={teamView.latestRank?.rank ? `#${teamView.latestRank.rank}` : "N/A"} />
          <MetricCard label="Power Rank" value={power?.powerRank ? `#${power.powerRank}` : "N/A"} />
          <MetricCard label="Record" value={teamView.latestRank?.recordText ?? "Unknown"} />
          <MetricCard label="Direction" value={teamView.delta.rankDirection} />
          <MetricCard label="Snapshots" value={teamView.rankHistory.length} />
        </div>

        <div className="grid gap-5 xl:grid-cols-2">
          <Section label="Rank History" title="Stored Snapshots">
            <div className="overflow-x-auto">
              <table>
                <thead>
                  <tr><th>Seen</th><th>Rank</th><th>Record</th><th>WP/AP/SP</th><th>Count</th></tr>
                </thead>
                <tbody>
                  {teamView.rankHistory.map((item) => (
                    <tr key={item.id}>
                      <td>{formatDate(item.lastSeenAt)}</td>
                      <td>{item.rank ? `#${item.rank}` : "N/A"}</td>
                      <td>{item.recordText}</td>
                      <td>{displayNumber(item.wp)} / {displayNumber(item.ap)} / {displayNumber(item.sp)}</td>
                      <td>{item.seenCount}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Section>

          <Section label="Matches" title="Recent Match State">
            <div className="overflow-x-auto">
              <table>
                <thead>
                  <tr><th>Match</th><th>Status</th><th>Alliance</th><th>Opponents</th><th>Score</th><th>Time</th></tr>
                </thead>
                <tbody>
                  {teamView.matches.map((match) => (
                    <tr key={match.id}>
                      <td>{match.roundLabel ?? match.matchKey}</td>
                      <td><StatusBadge status={match.status} /></td>
                      <td>{match.participant?.alliance ?? "N/A"}</td>
                      <td>{match.participant?.opponentTeamNumbers.join(", ") || "TBD"}</td>
                      <td>{displayNumber(match.participant?.scoreFor)} - {displayNumber(match.participant?.scoreAgainst)}</td>
                      <td>{formatDate(match.scheduledTime ?? match.completedTime)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Section>
        </div>
      </div>
    </Shell>
  );
}
