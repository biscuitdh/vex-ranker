import pg from "pg";
import {
  computeDerivedRankings,
  matchContent,
  matchSnapshotHash,
  normalizeEventSku,
  normalizeTeamNumber,
  rankDelta,
  rankSnapshotHash,
  safeTokenEquals,
  type MatchState,
  type RankState
} from "@vex-ranker/ranking-engine";
import type { EventBundle, NormalizedMatch, NormalizedRanking } from "@vex-ranker/vex-client";

const { Pool } = pg;

export type DbPool = pg.Pool;
export type DbClient = pg.PoolClient | pg.Pool;

export type AppConfig = {
  databaseUrl: string;
  defaultEventSku: string;
  defaultDivisionName: string;
  defaultTeamNumber: string;
  adminToken: string;
  refreshLockSeconds: number;
  adminRateLimitRequests: number;
  adminRateLimitWindowSeconds: number;
};

export type LatestRankSnapshot = RankState & {
  id: string;
  eventName: string;
  teamName: string;
  organization: string;
  source: string;
  snapshotAt: string;
  firstSeenAt: string;
  lastSeenAt: string;
  seenCount: number;
  contentHash: string;
};

export type MatchSnapshotView = MatchState & {
  id: string;
  source: string;
  firstSeenAt: string;
  lastSeenAt: string;
  seenCount: number;
  participant?: {
    alliance: "red" | "blue";
    partnerTeamNumbers: string[];
    opponentTeamNumbers: string[];
    scoreFor: number | null;
    scoreAgainst: number | null;
    margin: number | null;
  };
};

export type TeamView = {
  team: { teamNumber: string; teamName: string; organization: string } | null;
  latestRank: LatestRankSnapshot | null;
  previousRank: LatestRankSnapshot | null;
  delta: ReturnType<typeof rankDelta>;
  rankHistory: LatestRankSnapshot[];
  matches: MatchSnapshotView[];
  derivedRankings: ReturnType<typeof computeDerivedRankings>;
};

export type EventView = {
  event: {
    eventSku: string;
    name: string;
    startAt: string | null;
    endAt: string | null;
    city: string | null;
    region: string | null;
    country: string | null;
  } | null;
  divisions: Array<{ name: string; sourceDivisionId: number | null }>;
  latestRun: CollectionRun | null;
  teamCount: number;
};

export type CollectionRun = {
  id: string;
  source: string;
  eventSku: string | null;
  teamNumber: string | null;
  status: string;
  startedAt: string;
  completedAt: string | null;
  itemCount: number;
  errorSummary: string;
};

export function readConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return {
    databaseUrl: requireEnv(env.DATABASE_URL, "DATABASE_URL"),
    defaultEventSku: normalizeEventSku(env.DEFAULT_EVENT_SKU ?? "RE-V5RC-26-4025"),
    defaultDivisionName: env.DEFAULT_DIVISION_NAME ?? "Technology",
    defaultTeamNumber: normalizeTeamNumber(env.DEFAULT_TEAM_NUMBER ?? "7157B"),
    adminToken: env.ADMIN_TOKEN ?? "",
    refreshLockSeconds: readInt(env.REFRESH_LOCK_SECONDS, 300),
    adminRateLimitRequests: readInt(env.ADMIN_RATE_LIMIT_REQUESTS, 5),
    adminRateLimitWindowSeconds: readInt(env.ADMIN_RATE_LIMIT_WINDOW_SECONDS, 600)
  };
}

export function createPool(databaseUrl = readConfig().databaseUrl): DbPool {
  return new Pool({
    connectionString: databaseUrl,
    max: 8,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000
  });
}

export async function withPool<T>(fn: (pool: DbPool) => Promise<T>, databaseUrl?: string): Promise<T> {
  const pool = createPool(databaseUrl);
  try {
    return await fn(pool);
  } finally {
    await pool.end();
  }
}

export async function ensureEventBundle(client: DbClient, bundle: EventBundle): Promise<{
  eventId: string;
  divisionId: string;
}> {
  const eventRow = await client.query<{ id: string }>(
    `
    insert into events (
      event_sku, source_event_id, name, start_at, end_at, city, region, country, raw_json, updated_at
    ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, now())
    on conflict (event_sku) do update set
      source_event_id = excluded.source_event_id,
      name = excluded.name,
      start_at = excluded.start_at,
      end_at = excluded.end_at,
      city = excluded.city,
      region = excluded.region,
      country = excluded.country,
      raw_json = excluded.raw_json,
      updated_at = now()
    returning id
    `,
    [
      bundle.event.eventSku,
      bundle.event.sourceEventId,
      bundle.event.name,
      bundle.event.startAt,
      bundle.event.endAt,
      bundle.event.city,
      bundle.event.region,
      bundle.event.country,
      JSON.stringify(bundle.event.raw)
    ]
  );
  const eventId = eventRow.rows[0].id;

  for (const division of bundle.event.divisions) {
    await client.query(
      `
      insert into event_divisions (event_id, source_division_id, name, updated_at)
      values ($1, $2, $3, now())
      on conflict (event_id, name) do update set
        source_division_id = excluded.source_division_id,
        updated_at = now()
      `,
      [eventId, division.sourceDivisionId, division.name]
    );
  }

  const divisionRow = await client.query<{ id: string }>(
    "select id from event_divisions where event_id = $1 and name = $2 limit 1",
    [eventId, bundle.division.name]
  );
  if (!divisionRow.rows[0]) throw new Error(`Division ${bundle.division.name} was not persisted`);
  return { eventId, divisionId: divisionRow.rows[0].id };
}

export async function upsertRankSnapshots(
  client: DbClient,
  bundle: EventBundle,
  ids: { eventId: string; divisionId: string }
): Promise<number> {
  let count = 0;
  for (const ranking of bundle.rankings) {
    const teamId = await ensureTeam(client, ranking);
    await ensureEventTeam(client, ids.eventId, ids.divisionId, teamId);
    const content = rankingToState(ranking);
    const hash = rankSnapshotHash(content);
    await client.query(
      `
      insert into rank_snapshots (
        event_id, division_id, team_id, snapshot_at, rank, wins, losses, ties,
        wp, ap, sp, average_score, record_text, source, content_hash, raw_json
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb)
      on conflict (event_id, division_id, team_id, content_hash) do update set
        last_seen_at = greatest(rank_snapshots.last_seen_at, excluded.snapshot_at),
        seen_count = rank_snapshots.seen_count + 1,
        raw_json = excluded.raw_json
      `,
      [
        ids.eventId,
        ids.divisionId,
        teamId,
        ranking.snapshotAt,
        content.rank,
        content.wins,
        content.losses,
        content.ties,
        content.wp,
        content.ap,
        content.sp,
        content.averageScore,
        content.recordText,
        ranking.source,
        hash,
        JSON.stringify(ranking.raw)
      ]
    );
    count += 1;
  }
  return count;
}

export async function upsertMatchSnapshots(
  client: DbClient,
  bundle: EventBundle,
  ids: { eventId: string; divisionId: string }
): Promise<number> {
  let count = 0;
  for (const match of bundle.matches) {
    const content = matchToState(match);
    const hash = matchSnapshotHash(content);
    const row = await client.query<{ id: string }>(
      `
      insert into match_snapshots (
        event_id, division_id, match_key, match_type, round_label, instance,
        status, scheduled_time, completed_time, field_name, red_score, blue_score,
        red_teams, blue_teams, source, content_hash, raw_json
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb, $15, $16, $17::jsonb)
      on conflict (event_id, division_id, match_key, content_hash) do update set
        last_seen_at = greatest(match_snapshots.last_seen_at, excluded.last_seen_at),
        seen_count = match_snapshots.seen_count + 1,
        raw_json = excluded.raw_json
      returning id
      `,
      [
        ids.eventId,
        ids.divisionId,
        content.matchKey,
        content.matchType,
        content.roundLabel,
        content.instance,
        content.status,
        content.scheduledTime,
        content.completedTime,
        content.fieldName,
        content.redScore,
        content.blueScore,
        JSON.stringify(content.redTeams),
        JSON.stringify(content.blueTeams),
        match.source,
        hash,
        JSON.stringify(match.raw)
      ]
    );
    await upsertParticipants(client, row.rows[0].id, content);
    count += 1;
  }
  return count;
}

export async function recordCollectionRun(
  client: DbClient,
  input: {
    source: string;
    eventSku: string;
    teamNumber: string;
    status: "success" | "failed" | "skipped";
    startedAt: Date;
    itemCount: number;
    errorSummary?: string;
    metadata?: unknown;
  }
): Promise<void> {
  await client.query(
    `
    insert into collection_runs (
      source, event_sku, team_number, status, started_at, completed_at, item_count, error_summary, metadata
    ) values ($1, $2, $3, $4, $5, now(), $6, $7, $8::jsonb)
    `,
    [
      input.source,
      normalizeEventSku(input.eventSku),
      normalizeTeamNumber(input.teamNumber),
      input.status,
      input.startedAt,
      input.itemCount,
      input.errorSummary ?? "",
      JSON.stringify(input.metadata ?? {})
    ]
  );
}

export async function tryAcquireRefreshLock(
  client: DbClient,
  lockKey: string,
  owner: string,
  ttlSeconds: number
): Promise<boolean> {
  const result = await client.query<{ owner: string }>(
    `
    insert into refresh_locks (lock_key, owner, acquired_at, locked_until)
    values ($1, $2, now(), now() + ($3::text || ' seconds')::interval)
    on conflict (lock_key) do update set
      owner = excluded.owner,
      acquired_at = now(),
      locked_until = excluded.locked_until
    where refresh_locks.locked_until < now() or refresh_locks.owner = $2
    returning owner
    `,
    [lockKey, owner, ttlSeconds]
  );
  return result.rowCount === 1;
}

export async function releaseRefreshLock(client: DbClient, lockKey: string, owner: string): Promise<void> {
  await client.query("delete from refresh_locks where lock_key = $1 and owner = $2", [lockKey, owner]);
}

export async function createRefreshJob(
  client: DbClient,
  input: { requestSource: string; requestedBy: string; eventSku: string; teamNumber: string }
): Promise<string> {
  const row = await client.query<{ id: string }>(
    `
    insert into refresh_jobs (request_source, requested_by, event_sku, team_number, status)
    values ($1, $2, $3, $4, 'queued')
    returning id
    `,
    [
      input.requestSource,
      input.requestedBy,
      normalizeEventSku(input.eventSku),
      normalizeTeamNumber(input.teamNumber)
    ]
  );
  return row.rows[0].id;
}

export async function updateRefreshJob(
  client: DbClient,
  id: string,
  status: "running" | "success" | "failed" | "skipped",
  errorSummary = "",
  metadata: unknown = {}
): Promise<void> {
  await client.query(
    `
    update refresh_jobs
    set status = $2,
        started_at = coalesce(started_at, now()),
        completed_at = case when $2 in ('success', 'failed', 'skipped') then now() else completed_at end,
        error_summary = $3,
        metadata = $4::jsonb
    where id = $1
    `,
    [id, status, errorSummary, JSON.stringify(metadata)]
  );
}

export async function enforceRateLimit(
  client: DbClient,
  rateKey: string,
  limit: number,
  windowSeconds: number
): Promise<{ allowed: boolean; remaining: number }> {
  const row = await client.query<{ request_count: number }>(
    `
    insert into admin_rate_limits (rate_key, window_start, request_count, updated_at)
    values ($1, now(), 1, now())
    on conflict (rate_key) do update set
      window_start = case
        when admin_rate_limits.window_start < now() - ($2::text || ' seconds')::interval then now()
        else admin_rate_limits.window_start
      end,
      request_count = case
        when admin_rate_limits.window_start < now() - ($2::text || ' seconds')::interval then 1
        else admin_rate_limits.request_count + 1
      end,
      updated_at = now()
    returning request_count
    `,
    [rateKey, windowSeconds]
  );
  const requestCount = Number(row.rows[0].request_count);
  return { allowed: requestCount <= limit, remaining: Math.max(limit - requestCount, 0) };
}

export function verifyAdminToken(candidate: string, expected: string): boolean {
  if (!expected || !candidate) return false;
  return safeTokenEquals(candidate, expected);
}

export async function getHealth(client: DbClient): Promise<{
  ok: boolean;
  database: "ok" | "error";
  latestMigration: string | null;
  latestRun: CollectionRun | null;
  staleSnapshot: boolean;
}> {
  await client.query("select 1");
  const migration = await client.query<{ version: string }>(
    "select version from schema_migrations order by version desc limit 1"
  ).catch(() => ({ rows: [] as Array<{ version: string }> }));
  const latestRun = await getLatestCollectionRun(client);
  const snapshot = await client.query<{ last_seen_at: string }>(
    "select last_seen_at from rank_snapshots order by last_seen_at desc limit 1"
  );
  const lastSeen = snapshot.rows[0]?.last_seen_at ? new Date(snapshot.rows[0].last_seen_at) : null;
  const staleSnapshot = !lastSeen || Date.now() - lastSeen.getTime() > 1000 * 60 * 90;
  return {
    ok: true,
    database: "ok",
    latestMigration: migration.rows[0]?.version ?? null,
    latestRun,
    staleSnapshot
  };
}

export async function getTeamView(client: DbClient, teamNumber: string): Promise<TeamView> {
  const team = await getTeam(client, teamNumber);
  const latestRank = await getLatestRankSnapshot(client, teamNumber, 0);
  const previousRank = await getLatestRankSnapshot(client, teamNumber, 1);
  const rankHistory = await getRankHistory(client, teamNumber, 25);
  const matches = await getTeamMatches(client, teamNumber, 25);
  const rankings = latestRank
    ? await getEventRankings(client, latestRank.eventSku, latestRank.divisionName, 250)
    : [];
  const derivedRankings = computeDerivedRankings(rankings.map((row) => ({
    teamNumber: row.teamNumber,
    officialRank: row.rank,
    wins: row.wins,
    losses: row.losses,
    ties: row.ties,
    averageScore: row.averageScore,
    wp: row.wp,
    ap: row.ap,
    sp: row.sp
  })));
  return {
    team,
    latestRank,
    previousRank,
    delta: rankDelta(latestRank, previousRank),
    rankHistory,
    matches,
    derivedRankings
  };
}

export async function getEventView(client: DbClient, eventSku: string): Promise<EventView> {
  const sku = normalizeEventSku(eventSku);
  const eventRow = await client.query(
    `
    select event_sku, name, start_at, end_at, city, region, country
    from events
    where event_sku = $1
    limit 1
    `,
    [sku]
  );
  const divisions = await client.query<{ name: string; source_division_id: number | null }>(
    `
    select d.name, d.source_division_id
    from event_divisions d
    join events e on e.id = d.event_id
    where e.event_sku = $1
    order by d.name
    `,
    [sku]
  );
  const teamCount = await client.query<{ count: string }>(
    `
    select count(distinct et.team_id)::text as count
    from event_teams et
    join events e on e.id = et.event_id
    where e.event_sku = $1
    `,
    [sku]
  );

  return {
    event: eventRow.rows[0]
      ? {
          eventSku: eventRow.rows[0].event_sku,
          name: eventRow.rows[0].name,
          startAt: isoOrNull(eventRow.rows[0].start_at),
          endAt: isoOrNull(eventRow.rows[0].end_at),
          city: eventRow.rows[0].city,
          region: eventRow.rows[0].region,
          country: eventRow.rows[0].country
        }
      : null,
    divisions: divisions.rows.map((row) => ({ name: row.name, sourceDivisionId: row.source_division_id })),
    latestRun: await getLatestCollectionRun(client, sku),
    teamCount: Number(teamCount.rows[0]?.count ?? 0)
  };
}

export async function getEventRankings(
  client: DbClient,
  eventSku: string,
  divisionName?: string,
  limit = 100
): Promise<LatestRankSnapshot[]> {
  const params: unknown[] = [normalizeEventSku(eventSku)];
  const divisionFilter = divisionName ? "and d.name = $2" : "";
  if (divisionName) params.push(divisionName);
  const rows = await client.query(
    `
    select distinct on (t.team_number)
      rs.id, e.event_sku, e.name as event_name, d.name as division_name,
      t.team_number, t.team_name, t.organization,
      rs.snapshot_at, rs.rank, rs.wins, rs.losses, rs.ties, rs.wp, rs.ap, rs.sp,
      rs.average_score, rs.record_text, rs.source, rs.content_hash,
      rs.first_seen_at, rs.last_seen_at, rs.seen_count
    from rank_snapshots rs
    join events e on e.id = rs.event_id
    join event_divisions d on d.id = rs.division_id
    join teams t on t.id = rs.team_id
    where e.event_sku = $1 ${divisionFilter}
    order by t.team_number, rs.last_seen_at desc, rs.snapshot_at desc
    `,
    params
  );
  return rows.rows
    .map(rankRowToView)
    .sort((a, b) => (a.rank ?? 9999) - (b.rank ?? 9999) || a.teamNumber.localeCompare(b.teamNumber))
    .slice(0, limit);
}

async function ensureTeam(client: DbClient, ranking: NormalizedRanking): Promise<string> {
  const row = await client.query<{ id: string }>(
    `
    insert into teams (team_number, team_name, organization, updated_at)
    values ($1, $2, $3, now())
    on conflict (team_number) do update set
      team_name = case when excluded.team_name <> '' then excluded.team_name else teams.team_name end,
      organization = case when excluded.organization <> '' then excluded.organization else teams.organization end,
      updated_at = now()
    returning id
    `,
    [normalizeTeamNumber(ranking.teamNumber), ranking.teamName, ranking.organization]
  );
  return row.rows[0].id;
}

async function ensureTeamByNumber(client: DbClient, teamNumber: string): Promise<string> {
  const row = await client.query<{ id: string }>(
    `
    insert into teams (team_number)
    values ($1)
    on conflict (team_number) do update set updated_at = now()
    returning id
    `,
    [normalizeTeamNumber(teamNumber)]
  );
  return row.rows[0].id;
}

async function ensureEventTeam(client: DbClient, eventId: string, divisionId: string, teamId: string): Promise<void> {
  await client.query(
    `
    insert into event_teams (event_id, division_id, team_id)
    values ($1, $2, $3)
    on conflict do nothing
    `,
    [eventId, divisionId, teamId]
  );
}

async function upsertParticipants(client: DbClient, matchSnapshotId: string, match: MatchState): Promise<void> {
  await client.query("delete from match_participants where match_snapshot_id = $1", [matchSnapshotId]);
  for (const [alliance, teams, opponents, scoreFor, scoreAgainst] of [
    ["red", match.redTeams, match.blueTeams, match.redScore, match.blueScore],
    ["blue", match.blueTeams, match.redTeams, match.blueScore, match.redScore]
  ] as const) {
    for (const teamNumber of teams) {
      const teamId = await ensureTeamByNumber(client, teamNumber);
      const partners = teams.filter((team) => team !== teamNumber);
      await client.query(
        `
        insert into match_participants (
          match_snapshot_id, team_id, alliance, partner_team_numbers, opponent_team_numbers,
          score_for, score_against, margin
        ) values ($1, $2, $3, $4, $5, $6, $7, $8)
        on conflict (match_snapshot_id, team_id) do update set
          alliance = excluded.alliance,
          partner_team_numbers = excluded.partner_team_numbers,
          opponent_team_numbers = excluded.opponent_team_numbers,
          score_for = excluded.score_for,
          score_against = excluded.score_against,
          margin = excluded.margin
        `,
        [
          matchSnapshotId,
          teamId,
          alliance,
          partners,
          opponents,
          scoreFor,
          scoreAgainst,
          scoreFor == null || scoreAgainst == null ? null : scoreFor - scoreAgainst
        ]
      );
    }
  }
}

async function getTeam(client: DbClient, teamNumber: string): Promise<TeamView["team"]> {
  const row = await client.query(
    "select team_number, team_name, organization from teams where team_number = $1 limit 1",
    [normalizeTeamNumber(teamNumber)]
  );
  if (!row.rows[0]) return null;
  return {
    teamNumber: row.rows[0].team_number,
    teamName: row.rows[0].team_name,
    organization: row.rows[0].organization
  };
}

async function getLatestRankSnapshot(
  client: DbClient,
  teamNumber: string,
  offset: number
): Promise<LatestRankSnapshot | null> {
  const rows = await client.query(
    `
    select
      rs.id, e.event_sku, e.name as event_name, d.name as division_name,
      t.team_number, t.team_name, t.organization,
      rs.snapshot_at, rs.rank, rs.wins, rs.losses, rs.ties, rs.wp, rs.ap, rs.sp,
      rs.average_score, rs.record_text, rs.source, rs.content_hash,
      rs.first_seen_at, rs.last_seen_at, rs.seen_count
    from rank_snapshots rs
    join events e on e.id = rs.event_id
    join event_divisions d on d.id = rs.division_id
    join teams t on t.id = rs.team_id
    where t.team_number = $1
    order by rs.last_seen_at desc, rs.snapshot_at desc
    limit 1 offset $2
    `,
    [normalizeTeamNumber(teamNumber), offset]
  );
  return rows.rows[0] ? rankRowToView(rows.rows[0]) : null;
}

async function getRankHistory(client: DbClient, teamNumber: string, limit: number): Promise<LatestRankSnapshot[]> {
  const rows = await client.query(
    `
    select
      rs.id, e.event_sku, e.name as event_name, d.name as division_name,
      t.team_number, t.team_name, t.organization,
      rs.snapshot_at, rs.rank, rs.wins, rs.losses, rs.ties, rs.wp, rs.ap, rs.sp,
      rs.average_score, rs.record_text, rs.source, rs.content_hash,
      rs.first_seen_at, rs.last_seen_at, rs.seen_count
    from rank_snapshots rs
    join events e on e.id = rs.event_id
    join event_divisions d on d.id = rs.division_id
    join teams t on t.id = rs.team_id
    where t.team_number = $1
    order by rs.last_seen_at desc, rs.snapshot_at desc
    limit $2
    `,
    [normalizeTeamNumber(teamNumber), limit]
  );
  return rows.rows.map(rankRowToView);
}

async function getTeamMatches(client: DbClient, teamNumber: string, limit: number): Promise<MatchSnapshotView[]> {
  const rows = await client.query(
    `
    select distinct on (ms.match_key)
      ms.id, e.event_sku, d.name as division_name, ms.match_key, ms.match_type, ms.round_label,
      ms.instance, ms.status, ms.scheduled_time, ms.completed_time, ms.field_name,
      ms.red_score, ms.blue_score, ms.red_teams, ms.blue_teams, ms.source,
      ms.first_seen_at, ms.last_seen_at, ms.seen_count,
      mp.alliance, mp.partner_team_numbers, mp.opponent_team_numbers, mp.score_for, mp.score_against, mp.margin
    from match_snapshots ms
    join events e on e.id = ms.event_id
    join event_divisions d on d.id = ms.division_id
    join match_participants mp on mp.match_snapshot_id = ms.id
    join teams t on t.id = mp.team_id
    where t.team_number = $1
    order by ms.match_key, ms.last_seen_at desc
    limit $2
    `,
    [normalizeTeamNumber(teamNumber), limit]
  );
  return rows.rows
    .map(matchRowToView)
    .sort((a, b) => String(b.scheduledTime ?? b.completedTime ?? "").localeCompare(String(a.scheduledTime ?? a.completedTime ?? "")));
}

async function getLatestCollectionRun(client: DbClient, eventSku?: string): Promise<CollectionRun | null> {
  const params = eventSku ? [normalizeEventSku(eventSku)] : [];
  const where = eventSku ? "where event_sku = $1" : "";
  const row = await client.query(
    `
    select id, source, event_sku, team_number, status, started_at, completed_at, item_count, error_summary
    from collection_runs
    ${where}
    order by started_at desc
    limit 1
    `,
    params
  );
  return row.rows[0] ? collectionRunToView(row.rows[0]) : null;
}

function rankingToState(ranking: NormalizedRanking): RankState {
  return {
    eventSku: ranking.eventSku,
    divisionName: ranking.divisionName,
    teamNumber: ranking.teamNumber,
    rank: ranking.rank,
    wins: ranking.wins,
    losses: ranking.losses,
    ties: ranking.ties,
    wp: ranking.wp,
    ap: ranking.ap,
    sp: ranking.sp,
    averageScore: ranking.averageScore,
    recordText: ranking.recordText
  };
}

function matchToState(match: NormalizedMatch): MatchState {
  return matchContent(match);
}

function rankRowToView(row: Record<string, unknown>): LatestRankSnapshot {
  return {
    id: String(row.id),
    eventSku: String(row.event_sku),
    eventName: String(row.event_name),
    divisionName: String(row.division_name),
    teamNumber: String(row.team_number),
    teamName: String(row.team_name ?? ""),
    organization: String(row.organization ?? ""),
    snapshotAt: isoOrNull(row.snapshot_at) ?? "",
    rank: numberOrNull(row.rank),
    wins: numberOrNull(row.wins),
    losses: numberOrNull(row.losses),
    ties: numberOrNull(row.ties),
    wp: numberOrNull(row.wp),
    ap: numberOrNull(row.ap),
    sp: numberOrNull(row.sp),
    averageScore: numberOrNull(row.average_score),
    recordText: String(row.record_text ?? "Unknown"),
    source: String(row.source ?? "unknown"),
    contentHash: String(row.content_hash ?? ""),
    firstSeenAt: isoOrNull(row.first_seen_at) ?? "",
    lastSeenAt: isoOrNull(row.last_seen_at) ?? "",
    seenCount: Number(row.seen_count ?? 1)
  };
}

function matchRowToView(row: Record<string, unknown>): MatchSnapshotView {
  return {
    id: String(row.id),
    eventSku: String(row.event_sku),
    divisionName: String(row.division_name),
    matchKey: String(row.match_key),
    matchType: nullableString(row.match_type),
    roundLabel: nullableString(row.round_label),
    instance: numberOrNull(row.instance),
    status: row.status === "scheduled" || row.status === "completed" ? row.status : "unknown",
    scheduledTime: isoOrNull(row.scheduled_time),
    completedTime: isoOrNull(row.completed_time),
    fieldName: nullableString(row.field_name),
    redScore: numberOrNull(row.red_score),
    blueScore: numberOrNull(row.blue_score),
    redTeams: jsonArray(row.red_teams),
    blueTeams: jsonArray(row.blue_teams),
    source: String(row.source ?? "unknown"),
    firstSeenAt: isoOrNull(row.first_seen_at) ?? "",
    lastSeenAt: isoOrNull(row.last_seen_at) ?? "",
    seenCount: Number(row.seen_count ?? 1),
    participant: row.alliance
      ? {
          alliance: row.alliance === "red" ? "red" : "blue",
          partnerTeamNumbers: arrayFromPg(row.partner_team_numbers),
          opponentTeamNumbers: arrayFromPg(row.opponent_team_numbers),
          scoreFor: numberOrNull(row.score_for),
          scoreAgainst: numberOrNull(row.score_against),
          margin: numberOrNull(row.margin)
        }
      : undefined
  };
}

function collectionRunToView(row: Record<string, unknown>): CollectionRun {
  return {
    id: String(row.id),
    source: String(row.source),
    eventSku: nullableString(row.event_sku),
    teamNumber: nullableString(row.team_number),
    status: String(row.status),
    startedAt: isoOrNull(row.started_at) ?? "",
    completedAt: isoOrNull(row.completed_at),
    itemCount: Number(row.item_count ?? 0),
    errorSummary: String(row.error_summary ?? "")
  };
}

function isoOrNull(value: unknown): string | null {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  const date = new Date(String(value));
  return Number.isNaN(date.getTime()) ? String(value) : date.toISOString();
}

function nullableString(value: unknown): string | null {
  if (value == null || value === "") return null;
  return String(value);
}

function numberOrNull(value: unknown): number | null {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function jsonArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed.map(String) : [];
    } catch {
      return [];
    }
  }
  return [];
}

function arrayFromPg(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String) : [];
}

function requireEnv(value: string | undefined, name: string): string {
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function readInt(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}
