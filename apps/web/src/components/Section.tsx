import type { ReactNode } from "react";

export function Section({
  label,
  title,
  children
}: {
  label: string;
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="min-w-0 rounded-2xl border border-slate-500/20 bg-slate-900/80 p-4">
      <div className="mb-4">
        <div className="text-xs font-bold uppercase tracking-[0.08em] text-emerald-300">{label}</div>
        <h2 className="mt-1 break-words text-xl font-semibold text-slate-50">{title}</h2>
      </div>
      {children}
    </section>
  );
}
