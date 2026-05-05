import Link from "next/link";
import type { ReactNode } from "react";
import { readConfig } from "@vex-ranker/db";

export function Shell({ children, active }: { children: ReactNode; active?: string }) {
  const config = readConfig();
  const nav = [
    { key: "home", href: "/", label: "Dashboard" },
    { key: "team", href: "/teams", label: "Teams" },
    { key: "event", href: `/events/${config.defaultEventSku}`, label: "Event" },
    { key: "rankings", href: `/events/${config.defaultEventSku}/rankings`, label: "Rankings" },
    { key: "admin", href: "/admin", label: "Admin" }
  ];

  return (
    <div className="min-h-screen overflow-x-hidden bg-[radial-gradient(circle_at_top_left,rgba(57,160,255,0.16),transparent_30%),radial-gradient(circle_at_top_right,rgba(105,240,143,0.1),transparent_24%),linear-gradient(180deg,#07101a_0%,#081018_100%)] text-slate-100">
      <div className="mx-auto grid w-full min-w-0 max-w-[1480px] gap-5 px-4 py-4 sm:px-6 lg:px-8">
        <header className="min-w-0 rounded-2xl border border-slate-400/20 bg-slate-950/70 p-4 shadow-2xl shadow-black/30 backdrop-blur sm:rounded-3xl sm:p-5">
          <div className="flex min-w-0 flex-wrap items-start justify-between gap-5">
            <div className="min-w-0">
              <span className="inline-flex rounded-full border border-emerald-300/20 bg-emerald-300/10 px-3 py-1 text-xs font-bold uppercase tracking-[0.08em] text-emerald-300">
                VEX Ranker
              </span>
              <h1 className="mt-3 break-words text-2xl font-bold tracking-normal text-slate-50 sm:text-4xl">
                VEX Team Monitoring Console
              </h1>
              <p className="mt-2 break-words text-sm text-slate-400">
                {config.defaultDivisionName} Division / {config.defaultEventSku}
              </p>
            </div>
            <form action="/teams" method="get" className="grid w-full min-w-0 max-w-full gap-2 sm:max-w-md sm:grid-cols-[minmax(0,1fr)_auto]">
              <label className="sr-only" htmlFor="shell-team-number">Team number</label>
              <input
                id="shell-team-number"
                name="teamNumber"
                autoCapitalize="characters"
                autoComplete="off"
                className="min-w-0 rounded-xl border border-slate-500/20 bg-slate-950 px-3 py-3 text-sm text-slate-100 outline-none transition placeholder:text-slate-500 focus:border-emerald-300/50"
                placeholder={`Team number, e.g. ${config.defaultTeamNumber}`}
              />
              <button
                className="rounded-xl border border-emerald-300/30 bg-emerald-300/15 px-4 py-3 text-sm font-semibold text-slate-50 transition hover:bg-emerald-300/20"
                type="submit"
              >
                View Team
              </button>
              <div className="text-xs text-slate-500 sm:col-span-2">Server-side lookup. No browser VEX API calls.</div>
            </form>
          </div>
          <nav className="mt-5 grid min-w-0 grid-cols-2 gap-2 sm:flex sm:flex-wrap">
            {nav.map((item) => (
              <Link
                key={item.key}
                href={item.href}
                className={`min-w-0 rounded-xl border px-3 py-3 text-center text-sm font-medium transition sm:px-4 ${
                  active === item.key
                    ? "border-emerald-300/30 bg-emerald-300/15 text-slate-50"
                    : "border-slate-500/20 bg-slate-900/80 text-slate-400 hover:text-slate-50"
                }`}
              >
                {item.label}
              </Link>
            ))}
          </nav>
        </header>
        <main className="min-w-0 overflow-hidden rounded-2xl border border-slate-400/20 bg-slate-950/70 p-4 shadow-2xl shadow-black/30 backdrop-blur sm:rounded-3xl sm:p-5">
          {children}
        </main>
      </div>
    </div>
  );
}
