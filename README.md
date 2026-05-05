# VEX Ranker

Production-ready VEX Robotics ranking tracker built as a TypeScript monorepo.

## Stack

- Next.js App Router + TypeScript
- Tailwind CSS
- Postgres/Supabase-compatible SQL
- Server-only RobotEvents access
- Background collector worker
- Docker Compose local production-style environment
- OCI A1 Flex VM target for production web, collector, and self-hosted Postgres

## Quick Start

```bash
cp .env.local.example .env.local
corepack enable
corepack prepare pnpm@9.15.4 --activate
pnpm install
pnpm db:migrate
pnpm db:seed
pnpm dev
```

Use Node 24.x. If `corepack` is not available locally, install `pnpm@9.15.4` directly or use the Docker path below.

## Local Production-Like Stack

This path runs production-style containers, not `next dev`.

```bash
pnpm local:up
pnpm local:worker
open http://localhost:3000
```

Useful commands:

```bash
pnpm local:logs
pnpm local:test
pnpm local:reset
pnpm local:down
```

Admin refresh uses:

- URL: `http://localhost:3000/admin`
- Token: `local-admin-token`
- Default source: `mock`

## Environment

Required production values:

```dotenv
DATABASE_URL=<set locally; do not commit>
ROBOTEVENTS_API_BASE=https://www.robotevents.com/api/v2
ROBOTEVENTS_API_KEY=<set locally; do not commit>
ADMIN_TOKEN=<set locally; do not commit>
DEFAULT_EVENT_SKU=RE-V5RC-26-4025
DEFAULT_DIVISION_NAME=Technology
DEFAULT_TEAM_NUMBER=7157B
```

Never expose `ROBOTEVENTS_API_KEY`, `DATABASE_URL`, or `ADMIN_TOKEN` as `NEXT_PUBLIC_*`.

## Tests

```bash
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm local:test
```

## Layout

- `apps/web`: Next.js pages and route handlers
- `packages/vex-client`: RobotEvents API client and source normalization
- `packages/ranking-engine`: snapshot hashing, dedupe, validation, derived rankings
- `packages/db`: SQL migrations, seed, typed DB helpers
- `workers/collector`: polling and one-shot collector
- `workers/mock-vex-api`: fixture-backed local RobotEvents mock
- `docs`: architecture and operational notes

## OCI Production Prep

The production target is a small OCI A1 Flex VM running Docker Compose, Caddy, Postgres, the web app, and the collector scheduler.

```bash
pnpm oci:config
pnpm oci:build
```

OCI resources to create:

- VCN, public subnet, internet gateway, route table
- Network security group allowing `80/tcp`, `443/tcp`, and SSH only from your admin IP
- A1 Flex compute VM, Ubuntu 24.04, 1 OCPU / 6 GB minimum
- 100 GB block volume or boot volume space for app data and Postgres
- OCI Vault secrets for RobotEvents API key, admin token, and Postgres password
- Optional Object Storage bucket for encrypted Postgres backups
- DNS `A` record pointing your app hostname to the VM public IP

See `docs/oci-deployment.md` for the build checklist and cutover steps.

## Git Remote

Intended remote:

```bash
git remote add origin https://github.com/biscuitdh/hex-vex.git
```

Commit and push only after local checks pass.
