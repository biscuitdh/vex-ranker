import Link from "next/link";
import { notFound } from "next/navigation";
import { getEventView } from "@vex-ranker/db";
import { Shell } from "../../../src/components/Shell";
import { MetricCard } from "../../../src/components/MetricCard";
import { Section } from "../../../src/components/Section";
import { StatusBadge } from "../../../src/components/StatusBadge";
import { formatDate } from "../../../src/lib/format";
import { getWebPool } from "../../../src/lib/db";
import { eventSkuSchema } from "../../../src/lib/validation";

export const dynamic = "force-dynamic";

export default async function EventPage({ params }: { params: Promise<{ eventSku: string }> }) {
  const parsedParams = await params;
  const parsed = eventSkuSchema.safeParse(parsedParams.eventSku);
  if (!parsed.success) notFound();
  const eventView = await getEventView(getWebPool(), parsed.data);

  return (
    <Shell active="event">
      <div className="grid gap-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="text-xs font-bold uppercase tracking-[0.08em] text-emerald-300">Event</div>
            <h2 className="mt-1 text-2xl font-semibold">{eventView.event?.name ?? parsed.data}</h2>
            <p className="mt-2 text-sm text-slate-400">
              {eventView.event?.city ?? "Unknown city"} / {eventView.event?.region ?? "Unknown region"}
            </p>
          </div>
          <Link href={`/events/${parsed.data}/rankings`} className="rounded-xl border border-slate-500/20 bg-slate-900 px-4 py-3 text-sm text-slate-200">
            Open Rankings
          </Link>
        </div>

        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <MetricCard label="Teams Loaded" value={eventView.teamCount} />
          <MetricCard label="Divisions" value={eventView.divisions.length} />
          <MetricCard label="Last Run" value={<StatusBadge status={eventView.latestRun?.status ?? "none"} />} />
          <MetricCard label="Updated" value={formatDate(eventView.latestRun?.completedAt)} />
        </div>

        <Section label="Source State" title="Collector Status">
          <div className="overflow-x-auto">
            <table>
              <tbody>
                <tr><th>Event SKU</th><td>{parsed.data}</td></tr>
                <tr><th>Event Dates</th><td>{formatDate(eventView.event?.startAt)} - {formatDate(eventView.event?.endAt)}</td></tr>
                <tr><th>Latest Collector</th><td>{eventView.latestRun?.source ?? "Never"}</td></tr>
                <tr><th>Latest Error</th><td>{eventView.latestRun?.errorSummary || "None"}</td></tr>
                <tr><th>Divisions</th><td>{eventView.divisions.map((division) => division.name).join(", ") || "None"}</td></tr>
              </tbody>
            </table>
          </div>
        </Section>
      </div>
    </Shell>
  );
}
