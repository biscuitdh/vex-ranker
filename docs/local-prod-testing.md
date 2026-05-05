# Local Production-Style Testing

The local stack intentionally runs containers close to the OCI Docker Compose target.

## Start

```bash
pnpm local:up
```

This starts:

- Postgres 16
- mock RobotEvents API
- migration job
- seed job
- production-built Next.js web container

## Validate Compose Files

The test compose file is an override and should be validated together with the base file:

```bash
docker compose --env-file .env.example -f docker-compose.yml -f docker-compose.test.yml config --quiet
```

OCI example validation needs the env file path relative to `deploy/oci/docker-compose.oci.yml`:

```bash
VEX_RANKER_ENV_FILE=env.production.example \
  docker compose --env-file deploy/oci/env.production.example \
  -f deploy/oci/docker-compose.oci.yml config --quiet
```

## Run Collector Once

```bash
pnpm local:worker
```

The collector defaults to `mock` source. Live RobotEvents collection is opt-in:

```bash
docker compose --profile jobs run --rm \
  -e COLLECTOR_SOURCE=live \
  -e ROBOTEVENTS_API_BASE=https://www.robotevents.com/api/v2 \
  -e ROBOTEVENTS_API_KEY="$ROBOTEVENTS_API_KEY" \
  collector-once
```

## Reset

```bash
pnpm local:reset
```

This drops the Postgres volume and rebuilds from migrations and seed data.

## Troubleshooting

```bash
pnpm local:logs
curl http://localhost:3000/api/health
curl http://localhost:3000/api/teams/7157B
curl http://localhost:3000/api/events/RE-V5RC-26-4025/rankings
```

Admin refresh:

```bash
curl -X POST http://localhost:3000/api/admin/refresh \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer local-admin-token' \
  -d '{"eventSku":"RE-V5RC-26-4025","teamNumber":"7157B","source":"mock"}'
```
