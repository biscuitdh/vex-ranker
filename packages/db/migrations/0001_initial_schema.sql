create extension if not exists pgcrypto;

create table if not exists teams (
  id uuid primary key default gen_random_uuid(),
  team_number text not null unique,
  team_name text not null default '',
  organization text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists events (
  id uuid primary key default gen_random_uuid(),
  event_sku text not null unique,
  source_event_id integer,
  name text not null,
  start_at timestamptz,
  end_at timestamptz,
  city text,
  region text,
  country text,
  raw_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists event_divisions (
  id uuid primary key default gen_random_uuid(),
  event_id uuid not null references events(id) on delete cascade,
  source_division_id integer,
  name text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (event_id, name)
);

create table if not exists event_teams (
  event_id uuid not null references events(id) on delete cascade,
  division_id uuid not null references event_divisions(id) on delete cascade,
  team_id uuid not null references teams(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (event_id, division_id, team_id)
);

create table if not exists collection_runs (
  id uuid primary key default gen_random_uuid(),
  source text not null,
  event_sku text,
  team_number text,
  status text not null check (status in ('started', 'success', 'failed', 'skipped')),
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  item_count integer not null default 0,
  error_summary text not null default '',
  metadata jsonb not null default '{}'::jsonb
);

create table if not exists source_cache (
  cache_key text primary key,
  payload jsonb not null,
  fetched_at timestamptz not null default now(),
  expires_at timestamptz not null
);

create table if not exists refresh_locks (
  lock_key text primary key,
  owner text not null,
  acquired_at timestamptz not null default now(),
  locked_until timestamptz not null
);

create table if not exists refresh_jobs (
  id uuid primary key default gen_random_uuid(),
  request_source text not null,
  requested_by text not null default '',
  event_sku text not null,
  team_number text not null,
  status text not null check (status in ('queued', 'running', 'success', 'failed', 'skipped')),
  started_at timestamptz,
  completed_at timestamptz,
  error_summary text not null default '',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists rank_snapshots (
  id uuid primary key default gen_random_uuid(),
  event_id uuid not null references events(id) on delete cascade,
  division_id uuid not null references event_divisions(id) on delete cascade,
  team_id uuid not null references teams(id) on delete cascade,
  snapshot_at timestamptz not null,
  rank integer,
  wins integer,
  losses integer,
  ties integer,
  wp numeric,
  ap numeric,
  sp numeric,
  average_score numeric,
  record_text text not null default 'Unknown',
  source text not null,
  content_hash text not null,
  raw_json jsonb not null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  seen_count integer not null default 1,
  unique (event_id, division_id, team_id, content_hash)
);

create table if not exists match_snapshots (
  id uuid primary key default gen_random_uuid(),
  event_id uuid not null references events(id) on delete cascade,
  division_id uuid not null references event_divisions(id) on delete cascade,
  match_key text not null,
  match_type text,
  round_label text,
  instance integer,
  status text not null check (status in ('scheduled', 'completed', 'unknown')),
  scheduled_time timestamptz,
  completed_time timestamptz,
  field_name text,
  red_score numeric,
  blue_score numeric,
  red_teams jsonb not null default '[]'::jsonb,
  blue_teams jsonb not null default '[]'::jsonb,
  source text not null,
  content_hash text not null,
  raw_json jsonb not null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  seen_count integer not null default 1,
  unique (event_id, division_id, match_key, content_hash)
);

create table if not exists match_participants (
  match_snapshot_id uuid not null references match_snapshots(id) on delete cascade,
  team_id uuid not null references teams(id) on delete cascade,
  alliance text not null check (alliance in ('red', 'blue')),
  partner_team_numbers text[] not null default '{}',
  opponent_team_numbers text[] not null default '{}',
  score_for numeric,
  score_against numeric,
  margin numeric,
  primary key (match_snapshot_id, team_id)
);

create table if not exists admin_rate_limits (
  rate_key text primary key,
  window_start timestamptz not null,
  request_count integer not null default 0,
  updated_at timestamptz not null default now()
);

create index if not exists idx_rank_snapshots_team_latest
  on rank_snapshots (team_id, last_seen_at desc);

create index if not exists idx_rank_snapshots_event_latest
  on rank_snapshots (event_id, division_id, last_seen_at desc, rank asc);

create index if not exists idx_match_snapshots_event_latest
  on match_snapshots (event_id, division_id, last_seen_at desc);

create index if not exists idx_collection_runs_latest
  on collection_runs (started_at desc);

create index if not exists idx_refresh_jobs_latest
  on refresh_jobs (created_at desc);
