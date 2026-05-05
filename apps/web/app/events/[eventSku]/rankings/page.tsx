import Link from "next/link";
import { notFound } from "next/navigation";
import { getEventRankings, getEventView } from "@vex-ranker/db";
import { Shell } from "../../../../src/components/Shell";
import { MetricCard } from "../../../../src/components/MetricCard";
import { Section } from "../../../../src/components/Section";
import { displayNumber, formatDate } from "../../../../src/lib/format";
import { getWebPool } from "../../../../src/lib/db";
import { eventSkuSchema } from "../../../../src/lib/validation";

export const dynamic = "force-dynamic";

export default async function EventRankingsPage({ params }: { params: Promise<{ eventSku: string }> }) {
  const parsedParams = await params;
  const parsed = eventSkuSchema.safeParse(parsedParams.eventSku);
  if (!parsed.success) notFound();
  const pool = getWebPool();
  const [eventView, rankings] = await Promise.all([
    getEventView(pool, parsed.data),
    getEventRankings(pool, parsed.data, undefined, 250)
  ]);

  return (
    <Shell active="rankings">
      <div className="grid gap-5">
        <div className="min-w-0">
          <div className="text-xs font-bold uppercase tracking-[0.08em] text-emerald-300">Rankings</div>
          <h2 className="mt-1 break-words text-2xl font-semibold">{eventView.event?.name ?? parsed.data}</h2>
          <p className="mt-2 text-sm text-slate-400">Latest deduplicated rank state for loaded teams.</p>
        </div>

        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <MetricCard label="Rows" value={rankings.length} />
          <MetricCard label="Top Team" value={rankings[0]?.teamNumber ?? "N/A"} />
          <MetricCard label="Latest Source" value={rankings[0]?.source ?? "N/A"} />
          <MetricCard label="Last Seen" value={formatDate(rankings[0]?.lastSeenAt)} />
        </div>

        <Section label="Official" title="Current Rankings">
          <div className="overflow-x-auto">
            <table>
              <thead>
                <tr><th>Rank</th><th>Team</th><th>Name</th><th>Record</th><th>WP</th><th>AP</th><th>SP</th><th>Seen</th></tr>
              </thead>
              <tbody>
                {rankings.map((item) => (
                  <tr key={item.id}>
                    <td>{item.rank ? `#${item.rank}` : "N/A"}</td>
                    <td>
                      <Link href={`/teams/${item.teamNumber}`} className="font-semibold text-emerald-300 hover:text-slate-50">
                        {item.teamNumber}
                      </Link>
                    </td>
                    <td>{item.teamName || item.organization || "Unknown"}</td>
                    <td>{item.recordText}</td>
                    <td>{displayNumber(item.wp)}</td>
                    <td>{displayNumber(item.ap)}</td>
                    <td>{displayNumber(item.sp)}</td>
                    <td>{formatDate(item.lastSeenAt)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Section>
      </div>
    </Shell>
  );
}
