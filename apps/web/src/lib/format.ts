export function formatDate(value: string | null | undefined): string {
  if (!value) return "Unknown";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(parsed);
}

export function displayNumber(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return "N/A";
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
}

export function statusTone(status: string | null | undefined): string {
  switch (status) {
    case "success":
    case "completed":
    case "healthy":
      return "text-emerald-300 border-emerald-400/30 bg-emerald-400/10";
    case "failed":
      return "text-red-300 border-red-400/30 bg-red-400/10";
    case "scheduled":
    case "running":
    case "skipped":
      return "text-amber-200 border-amber-300/30 bg-amber-300/10";
    default:
      return "text-slate-300 border-slate-500/30 bg-slate-500/10";
  }
}
