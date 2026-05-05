import type { ReactNode } from "react";

export function MetricCard({ label, value, subtext }: { label: string; value: ReactNode; subtext?: ReactNode }) {
  return (
    <div className="min-w-0 rounded-2xl border border-emerald-300/10 bg-gradient-to-br from-sky-400/10 to-emerald-300/10 p-4">
      <div className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-400">{label}</div>
      <div className="mt-2 overflow-hidden break-words text-2xl font-bold text-slate-50">{value}</div>
      {subtext ? <div className="mt-2 text-sm text-slate-400">{subtext}</div> : null}
    </div>
  );
}
