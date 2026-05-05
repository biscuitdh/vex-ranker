# Runbook

## Local First Checks

```bash
pnpm local:up
pnpm local:worker
curl http://localhost:3000/api/health
curl http://localhost:3000/api/teams/7157B
```

## Common Failures

### Web shows no rank snapshots

Run:

```bash
pnpm local:worker
pnpm local:logs
```

Check `collection_runs` through `/api/health`.

### Admin refresh returns 401

Use the local token:

```text
local-admin-token
```

Production token comes from OCI Vault or the root-only production env file generated from Vault.

### Live collection fails

Confirm:

- `ROBOTEVENTS_API_KEY` is set
- `ROBOTEVENTS_API_BASE` is `https://www.robotevents.com/api/v2`
- collector source is `live`

### Repeated scheduler executions

Expected. The scheduler is at-least-once. `refresh_locks` and snapshot content hashes prevent duplicate harmful writes.

### OCI web is down after reboot

Check:

```bash
sudo systemctl status vex-ranker.service
docker compose -f deploy/oci/docker-compose.oci.yml ps
docker compose -f deploy/oci/docker-compose.oci.yml logs --tail=200
```

### Backup timer is not running

Check:

```bash
systemctl list-timers vex-ranker-backup.timer
sudo systemctl status vex-ranker-backup.service
```
