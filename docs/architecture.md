# Architecture

VEX Ranker is split into explicit packages so source access, persistence, ranking logic, and UI do not bleed into each other.

## Request Flow

1. Browser requests public pages or `/api/*` route handlers.
2. Next.js reads Postgres through `packages/db`.
3. Browser never calls RobotEvents.
4. Admin refresh calls `POST /api/admin/refresh`, validates `ADMIN_TOKEN`, rate-limits, and invokes the shared collector path.
5. Collector fetches RobotEvents data, normalizes it, and writes deduplicated snapshots.

## Data Model

- `teams`, `events`, `event_divisions`, `event_teams`: normalized entities
- `rank_snapshots`: rank state over time, deduped by `content_hash`
- `match_snapshots`: match state over time, deduped by `content_hash`
- `match_participants`: team-specific match context
- `collection_runs`: collector telemetry
- `refresh_locks`: Postgres lock rows for idempotent refresh
- `refresh_jobs`: manual/scheduled refresh status
- `admin_rate_limits`: refresh abuse control

## Dedupe

The ranking engine builds deterministic content hashes from normalized rank and match state. If the source returns the same state repeatedly, the existing row updates `last_seen_at` and `seen_count`. Changed state inserts a new snapshot.

## Production

Primary production target:

- OCI A1 Flex VM running Docker Compose
- Caddy reverse proxy for HTTPS ingress
- `apps/web` production container
- `workers/collector` scheduler container
- Self-hosted Postgres 16 on a private Docker network
- OCI Vault for credentials
- Nightly Postgres backups from `pg_dump`
