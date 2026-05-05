import { createHash, timingSafeEqual } from "node:crypto";

export type RankState = {
  eventSku: string;
  divisionName: string;
  teamNumber: string;
  rank: number | null;
  wins: number | null;
  losses: number | null;
  ties: number | null;
  wp: number | null;
  ap: number | null;
  sp: number | null;
  averageScore: number | null;
  recordText: string;
};

export type MatchState = {
  eventSku: string;
  divisionName: string;
  matchKey: string;
  matchType: string | null;
  roundLabel: string | null;
  instance: number | null;
  status: "scheduled" | "completed" | "unknown";
  scheduledTime: string | null;
  completedTime: string | null;
  fieldName: string | null;
  redScore: number | null;
  blueScore: number | null;
  redTeams: string[];
  blueTeams: string[];
};

export type RankDelta = {
  rankChange: number | null;
  rankDirection: "up" | "down" | "no change" | "unknown";
  recordChanged: boolean;
};

export type DerivedRankingInput = {
  teamNumber: string;
  officialRank: number | null;
  wins: number | null;
  losses: number | null;
  ties: number | null;
  averageScore: number | null;
  wp: number | null;
  ap: number | null;
  sp: number | null;
};

export type DerivedRanking = DerivedRankingInput & {
  compositeScore: number;
  powerRank: number;
};

export function normalizeTeamNumber(value: string): string {
  return value.trim().toUpperCase();
}

export function normalizeEventSku(value: string): string {
  return value.trim().toUpperCase();
}

export function isValidTeamNumber(value: string): boolean {
  return /^[0-9]{1,6}[A-Z]{1,3}$/.test(normalizeTeamNumber(value));
}

export function isValidEventSku(value: string): boolean {
  return /^RE-[A-Z0-9]+-[0-9]{2}-[0-9]{3,6}$/.test(normalizeEventSku(value));
}

export function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

export function contentHash(value: unknown): string {
  return createHash("sha256").update(stableStringify(value)).digest("hex");
}

export function rankContent(rank: RankState): RankState {
  return {
    eventSku: normalizeEventSku(rank.eventSku),
    divisionName: rank.divisionName.trim(),
    teamNumber: normalizeTeamNumber(rank.teamNumber),
    rank: nullableNumber(rank.rank),
    wins: nullableNumber(rank.wins),
    losses: nullableNumber(rank.losses),
    ties: nullableNumber(rank.ties),
    wp: nullableNumber(rank.wp),
    ap: nullableNumber(rank.ap),
    sp: nullableNumber(rank.sp),
    averageScore: nullableNumber(rank.averageScore),
    recordText: rank.recordText || recordText(rank.wins, rank.losses, rank.ties)
  };
}

export function matchContent(match: MatchState): MatchState {
  return {
    eventSku: normalizeEventSku(match.eventSku),
    divisionName: match.divisionName.trim(),
    matchKey: match.matchKey.trim(),
    matchType: match.matchType,
    roundLabel: match.roundLabel,
    instance: nullableNumber(match.instance),
    status: match.status,
    scheduledTime: match.scheduledTime,
    completedTime: match.completedTime,
    fieldName: match.fieldName,
    redScore: nullableNumber(match.redScore),
    blueScore: nullableNumber(match.blueScore),
    redTeams: match.redTeams.map(normalizeTeamNumber).sort(),
    blueTeams: match.blueTeams.map(normalizeTeamNumber).sort()
  };
}

export function rankSnapshotHash(rank: RankState): string {
  return contentHash(rankContent(rank));
}

export function matchSnapshotHash(match: MatchState): string {
  return contentHash(matchContent(match));
}

export function rankDelta(
  latest: Pick<RankState, "rank" | "recordText"> | null,
  previous: Pick<RankState, "rank" | "recordText"> | null
): RankDelta {
  if (!latest || !previous || latest.rank == null || previous.rank == null) {
    return {
      rankChange: null,
      rankDirection: "unknown",
      recordChanged: Boolean(latest && previous && latest.recordText !== previous.recordText)
    };
  }
  const rankChange = previous.rank - latest.rank;
  return {
    rankChange,
    rankDirection: rankChange > 0 ? "up" : rankChange < 0 ? "down" : "no change",
    recordChanged: latest.recordText !== previous.recordText
  };
}

export function computeDerivedRankings(rows: DerivedRankingInput[]): DerivedRanking[] {
  const scored = rows.map((row) => {
    const officialComponent = row.officialRank == null ? 0 : 1 / Math.max(row.officialRank, 1);
    const recordGames = (row.wins ?? 0) + (row.losses ?? 0) + (row.ties ?? 0);
    const recordComponent = recordGames === 0 ? 0 : ((row.wins ?? 0) + (row.ties ?? 0) * 0.5) / recordGames;
    const wpComponent = safeScale(row.wp, rows.map((item) => item.wp));
    const apComponent = safeScale(row.ap, rows.map((item) => item.ap));
    const spComponent = safeScale(row.sp, rows.map((item) => item.sp));
    const scoreComponent = safeScale(row.averageScore, rows.map((item) => item.averageScore));
    const compositeScore =
      officialComponent * 0.35 +
      recordComponent * 0.25 +
      wpComponent * 0.15 +
      apComponent * 0.1 +
      spComponent * 0.1 +
      scoreComponent * 0.05;
    return { ...row, compositeScore: round(compositeScore, 6), powerRank: 0 };
  });
  scored.sort((a, b) => {
    if (b.compositeScore !== a.compositeScore) return b.compositeScore - a.compositeScore;
    return (a.officialRank ?? 9999) - (b.officialRank ?? 9999) || a.teamNumber.localeCompare(b.teamNumber);
  });
  return scored.map((row, index) => ({ ...row, powerRank: index + 1 }));
}

export function safeTokenEquals(candidate: string, expected: string): boolean {
  const candidateBuffer = Buffer.from(candidate);
  const expectedBuffer = Buffer.from(expected);
  if (candidateBuffer.length !== expectedBuffer.length) return false;
  return timingSafeEqual(candidateBuffer, expectedBuffer);
}

export function recordText(wins: number | null, losses: number | null, ties: number | null): string {
  if (wins == null && losses == null && ties == null) return "Unknown";
  return `${wins ?? 0}-${losses ?? 0}-${ties ?? 0}`;
}

function nullableNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function safeScale(value: number | null | undefined, values: Array<number | null | undefined>): number {
  if (value == null) return 0;
  const clean = values.filter((item): item is number => item != null && Number.isFinite(Number(item))).map(Number);
  if (clean.length === 0) return 0;
  const min = Math.min(...clean);
  const max = Math.max(...clean);
  if (max === min) return 0;
  return (Number(value) - min) / (max - min);
}

function round(value: number, digits: number): number {
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}
