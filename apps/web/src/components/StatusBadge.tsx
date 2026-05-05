import { statusTone } from "../lib/format";

export function StatusBadge({ status }: { status: string | null | undefined }) {
  return (
    <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold ${statusTone(status)}`}>
      {status ?? "unknown"}
    </span>
  );
}
