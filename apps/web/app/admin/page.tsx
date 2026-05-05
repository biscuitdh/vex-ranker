import { readConfig, getHealth } from "@vex-ranker/db";
import { Shell } from "../../src/components/Shell";
import { Section } from "../../src/components/Section";
import { StatusBadge } from "../../src/components/StatusBadge";
import { formatDate } from "../../src/lib/format";
import { getWebPool } from "../../src/lib/db";

export const dynamic = "force-dynamic";

export default async function AdminPage() {
  const config = readConfig();
  const health = await getHealth(getWebPool()).catch(() => null);

  return (
    <Shell active="admin">
      <div className="grid gap-5">
        <div>
          <div className="text-xs font-bold uppercase tracking-[0.08em] text-emerald-300">Admin</div>
          <h2 className="mt-1 text-2xl font-semibold">Refresh Control</h2>
          <p className="mt-2 max-w-3xl text-sm text-slate-400">
            Uses the same server-side refresh endpoint locally and in production. Token stays server-side or operator-supplied.
          </p>
        </div>

        <Section label="Manual" title="Trigger Refresh">
          <form action="/api/admin/refresh" method="post" className="grid gap-3 md:grid-cols-2">
            <label className="grid gap-2 text-sm text-slate-300">
              Admin token
              <input name="adminToken" type="password" className="rounded-xl border border-slate-500/20 bg-slate-950 px-3 py-3 text-slate-100" placeholder="local-admin-token" />
            </label>
            <label className="grid gap-2 text-sm text-slate-300">
              Source
              <select name="source" className="rounded-xl border border-slate-500/20 bg-slate-950 px-3 py-3 text-slate-100" defaultValue="mock">
                <option value="mock">mock</option>
                <option value="live">live</option>
              </select>
            </label>
            <label className="grid gap-2 text-sm text-slate-300">
              Event SKU
              <input name="eventSku" className="rounded-xl border border-slate-500/20 bg-slate-950 px-3 py-3 text-slate-100" defaultValue={config.defaultEventSku} />
            </label>
            <label className="grid gap-2 text-sm text-slate-300">
              Team number
              <input name="teamNumber" className="rounded-xl border border-slate-500/20 bg-slate-950 px-3 py-3 text-slate-100" defaultValue={config.defaultTeamNumber} />
            </label>
            <button className="rounded-xl border border-emerald-300/30 bg-emerald-300/15 px-4 py-3 text-sm font-semibold text-slate-50 md:col-span-2" type="submit">
              Run Refresh
            </button>
          </form>
        </Section>

        <Section label="Health" title="Runtime State">
          <div className="overflow-x-auto">
            <table>
              <tbody>
                <tr><th>Database</th><td><StatusBadge status={health?.database ?? "error"} /></td></tr>
                <tr><th>Latest Migration</th><td>{health?.latestMigration ?? "Unknown"}</td></tr>
                <tr><th>Latest Collector</th><td>{health?.latestRun ? `${health.latestRun.source} / ${health.latestRun.status}` : "Never"}</td></tr>
                <tr><th>Latest Collector Time</th><td>{formatDate(health?.latestRun?.completedAt)}</td></tr>
                <tr><th>Stale Snapshot</th><td>{health?.staleSnapshot ? "Yes" : "No"}</td></tr>
              </tbody>
            </table>
          </div>
        </Section>
      </div>
    </Shell>
  );
}
