import {
  normalizeEventSku,
  normalizeTeamNumber,
  recordText,
  type MatchState,
  type RankState
} from "@vex-ranker/ranking-engine";

export type VexClientOptions = {
  apiBase: string;
  apiKey?: string;
  timeoutMs?: number;
  maxRetries?: number;
  rateLimitPerMinute?: number;
  userAgent?: string;
};

export type NormalizedEvent = {
  sourceEventId: number;
  eventSku: string;
  name: string;
  startAt: string | null;
  endAt: string | null;
  city: string | null;
  region: string | null;
  country: string | null;
  divisions: NormalizedDivision[];
  raw: unknown;
};

export type NormalizedDivision = {
  sourceDivisionId: number;
  name: string;
};

export type NormalizedTeam = {
  teamNumber: string;
  teamName: string;
  organization: string;
};

export type NormalizedRanking = RankState & {
  teamName: string;
  organization: string;
  source: string;
  snapshotAt: string;
  raw: unknown;
};

export type NormalizedMatch = MatchState & {
  source: string;
  snapshotAt: string;
  raw: unknown;
};

export type EventBundle = {
  event: NormalizedEvent;
  division: NormalizedDivision;
  rankings: NormalizedRanking[];
  matches: NormalizedMatch[];
  fetchedAt: string;
};

type ApiPayload<T> = {
  data?: T[];
};

export class RobotEventsClient {
  private readonly apiBase: string;
  private readonly apiKey: string;
  private readonly timeoutMs: number;
  private readonly maxRetries: number;
  private readonly minimumIntervalMs: number;
  private readonly userAgent: string;
  private lastRequestAt = 0;

  constructor(options: VexClientOptions) {
    this.apiBase = options.apiBase.replace(/\/$/, "");
    this.apiKey = options.apiKey ?? "";
    this.timeoutMs = options.timeoutMs ?? 20_000;
    this.maxRetries = options.maxRetries ?? 3;
    this.minimumIntervalMs = 60_000 / Math.max(options.rateLimitPerMinute ?? 30, 1);
    this.userAgent = options.userAgent ?? "vex-ranker/0.1";
  }

  async fetchEventBundle(eventSku: string, divisionName?: string): Promise<EventBundle> {
    const event = await this.getEventBySku(eventSku);
    const division = selectDivision(event, divisionName);
    const fetchedAt = new Date().toISOString();
    const [rankingsPayload, matchesPayload] = await Promise.all([
      this.getJson<ApiPayload<unknown>>(`/events/${event.sourceEventId}/divisions/${division.sourceDivisionId}/rankings`),
      this.getJson<ApiPayload<unknown>>(`/events/${event.sourceEventId}/divisions/${division.sourceDivisionId}/matches`)
    ]);
    const rankings = normalizeRankings({
      eventSku: event.eventSku,
      divisionName: division.name,
      eventName: event.name,
      fetchedAt,
      payload: rankingsPayload
    });
    const matches = normalizeMatches({
      eventSku: event.eventSku,
      divisionName: division.name,
      fetchedAt,
      payload: matchesPayload
    });

    return { event, division, rankings, matches, fetchedAt };
  }

  async getEventBySku(eventSku: string): Promise<NormalizedEvent> {
    const sku = normalizeEventSku(eventSku);
    const payload = await this.getJson<ApiPayload<unknown>>(`/events?sku[]=${encodeURIComponent(sku)}`);
    const event = payload.data?.[0];
    if (!event || typeof event !== "object") {
      throw new Error(`RobotEvents event not found for ${sku}`);
    }
    return normalizeEvent(event);
  }

  async getJson<T>(path: string): Promise<T> {
    let lastError: Error | null = null;
    for (let attempt = 1; attempt <= this.maxRetries; attempt += 1) {
      await this.waitForRateLimit();
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
      try {
        const response = await fetch(`${this.apiBase}${path}`, {
          headers: this.headers(),
          signal: controller.signal
        });
        if (response.status >= 500) {
          throw new Error(`RobotEvents ${response.status} ${response.statusText}`);
        }
        if (!response.ok) {
          const body = await response.text().catch(() => "");
          throw new Error(`RobotEvents ${response.status} ${response.statusText}: ${body.slice(0, 200)}`);
        }
        return (await response.json()) as T;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt === this.maxRetries) break;
        await sleep(2 ** attempt * 250);
      } finally {
        clearTimeout(timeout);
      }
    }
    throw lastError ?? new Error("RobotEvents request failed");
  }

  private headers(): HeadersInit {
    const headers: Record<string, string> = {
      Accept: "application/json",
      "User-Agent": this.userAgent
    };
    if (this.apiKey) headers.Authorization = this.apiKey;
    return headers;
  }

  private async waitForRateLimit(): Promise<void> {
    const elapsed = Date.now() - this.lastRequestAt;
    const delay = this.minimumIntervalMs - elapsed;
    if (delay > 0) await sleep(delay);
    this.lastRequestAt = Date.now();
  }
}

export function normalizeEvent(item: unknown): NormalizedEvent {
  const event = asRecord(item);
  const location = asRecord(event.location);
  const divisions = Array.isArray(event.divisions)
    ? event.divisions.map((division) => {
        const row = asRecord(division);
        return {
          sourceDivisionId: toNumber(row.id) ?? 0,
          name: String(row.name ?? "").trim()
        };
      }).filter((division) => division.sourceDivisionId > 0 && division.name)
    : [];

  return {
    sourceEventId: requireNumber(event.id, "event.id"),
    eventSku: normalizeEventSku(String(event.sku ?? "")),
    name: String(event.name ?? ""),
    startAt: nullableString(event.start),
    endAt: nullableString(event.end),
    city: nullableString(location.city),
    region: nullableString(location.region),
    country: nullableString(location.country),
    divisions,
    raw: item
  };
}

export function normalizeRankings(input: {
  eventSku: string;
  divisionName: string;
  eventName: string;
  fetchedAt: string;
  payload: ApiPayload<unknown>;
}): NormalizedRanking[] {
  return (input.payload.data ?? []).map((item) => {
    const row = asRecord(item);
    const team = asRecord(row.team);
    const wins = toNumber(row.wins);
    const losses = toNumber(row.losses);
    const ties = toNumber(row.ties);
    const normalizedTeam = normalizeTeam(team);
    return {
      eventSku: normalizeEventSku(input.eventSku),
      divisionName: input.divisionName,
      teamNumber: normalizedTeam.teamNumber,
      teamName: normalizedTeam.teamName,
      organization: normalizedTeam.organization,
      rank: toNumber(row.rank),
      wins,
      losses,
      ties,
      wp: toNumber(row.wp),
      ap: toNumber(row.ap),
      sp: toNumber(row.sp),
      averageScore: toNumber(row.average_score),
      recordText: recordText(wins, losses, ties),
      source: "robotevents",
      snapshotAt: input.fetchedAt,
      raw: item
    };
  }).filter((ranking) => ranking.teamNumber);
}

export function normalizeMatches(input: {
  eventSku: string;
  divisionName: string;
  fetchedAt: string;
  payload: ApiPayload<unknown>;
}): NormalizedMatch[] {
  return (input.payload.data ?? []).map((item) => {
    const row = asRecord(item);
    const alliances = Array.isArray(row.alliances) ? row.alliances.map(asRecord) : [];
    const red = alliances[0] ?? {};
    const blue = alliances[1] ?? {};
    const redScore = toNumber(red.score);
    const blueScore = toNumber(blue.score);
    const status: MatchState["status"] = redScore == null || blueScore == null ? "scheduled" : "completed";
    return {
      eventSku: normalizeEventSku(input.eventSku),
      divisionName: input.divisionName,
      matchKey: String(row.id ?? row.name ?? "").trim(),
      matchType: nullableString(row.round),
      roundLabel: nullableString(row.name ?? row.round),
      instance: toNumber(row.instance),
      status,
      scheduledTime: nullableString(row.scheduled),
      completedTime: nullableString(row.started),
      fieldName: nullableString(asRecord(row.field).name),
      redScore,
      blueScore,
      redTeams: extractAllianceTeams(red),
      blueTeams: extractAllianceTeams(blue),
      source: "robotevents",
      snapshotAt: input.fetchedAt,
      raw: item
    };
  }).filter((match) => match.matchKey);
}

export function normalizeTeam(item: unknown): NormalizedTeam {
  const team = asRecord(item);
  const teamNumber = normalizeTeamNumber(String(team.number ?? team.name ?? ""));
  return {
    teamNumber,
    teamName: String(team.team_name ?? team.name ?? "").trim(),
    organization: String(team.organization ?? "").trim()
  };
}

export function selectDivision(event: NormalizedEvent, divisionName?: string): NormalizedDivision {
  if (divisionName) {
    const found = event.divisions.find((division) => division.name.toLowerCase() === divisionName.toLowerCase());
    if (found) return found;
  }
  const first = event.divisions[0];
  if (!first) throw new Error(`Event ${event.eventSku} has no divisions`);
  return first;
}

function extractAllianceTeams(alliance: Record<string, unknown>): string[] {
  const teams = Array.isArray(alliance.teams) ? alliance.teams : [];
  return teams
    .map((entry) => normalizeTeam(asRecord(entry).team).teamNumber)
    .filter(Boolean);
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function toNumber(value: unknown): number | null {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function requireNumber(value: unknown, label: string): number {
  const parsed = toNumber(value);
  if (parsed == null) throw new Error(`Missing numeric ${label}`);
  return parsed;
}

function nullableString(value: unknown): string | null {
  if (value == null || value === "") return null;
  return String(value);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
