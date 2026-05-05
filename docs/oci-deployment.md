# OCI Deployment

Primary production target is OCI, not GCP or AWS.

## What To Build In OCI

Create these resources before deploying the app:

- VCN with one public subnet, internet gateway, and route table.
- Network security group for the VM:
  - allow `80/tcp` from `0.0.0.0/0`
  - allow `443/tcp` from `0.0.0.0/0`
  - allow `22/tcp` only from your admin IP, or use OCI Bastion
  - allow outbound HTTPS/DNS/NTP
- Compute instance:
  - shape: `VM.Standard.A1.Flex`
  - image: Ubuntu 24.04 LTS
  - minimum: 1 OCPU / 6 GB RAM
  - comfortable: 2 OCPU / 12 GB RAM
- Storage:
  - at least 100 GB total disk
  - mount persistent app data under `/opt/vex-ranker`
- Vault:
  - `ROBOTEVENTS_API_KEY`
  - `ADMIN_TOKEN`
  - `POSTGRES_PASSWORD`
- Optional Object Storage bucket for `pg_dump` archives.
- DNS record for the public hostname, for example `vex.example.com`.

## VM Bootstrap

Use `deploy/oci/cloud-init.yaml` when creating the VM. It installs Docker, enables the firewall, and prepares `/opt/vex-ranker`.

After SSH:

```bash
cd /opt/vex-ranker
git clone https://github.com/biscuitdh/hex-vex.git .
cp deploy/oci/env.production.example deploy/oci/.env.production
chmod 600 deploy/oci/.env.production
```

Edit `deploy/oci/.env.production` with real values. Keep Postgres private by using:

```dotenv
DATABASE_URL=<set locally; do not commit>
```

## Deploy

From `/opt/vex-ranker` on the VM:

```bash
docker compose --env-file deploy/oci/.env.production -f deploy/oci/docker-compose.oci.yml config
docker compose --env-file deploy/oci/.env.production -f deploy/oci/docker-compose.oci.yml build
docker compose --env-file deploy/oci/.env.production -f deploy/oci/docker-compose.oci.yml up -d
```

Install the systemd service if you want the stack to recover after reboot:

```bash
sudo cp deploy/oci/systemd/vex-ranker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now vex-ranker.service
```

## Backups

Install the backup timer:

```bash
sudo cp deploy/oci/systemd/vex-ranker-backup.service /etc/systemd/system/
sudo cp deploy/oci/systemd/vex-ranker-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now vex-ranker-backup.timer
```

Manual backup:

```bash
deploy/oci/scripts/backup-postgres.sh
```

Restore requires an explicit safety flag:

```bash
CONFIRM_RESTORE=yes deploy/oci/scripts/restore-postgres.sh /opt/vex-ranker/backups/vexranker-YYYYMMDDTHHMMSSZ.dump
```

## Smoke Checks

```bash
curl -fsS https://$APP_DOMAIN/api/health
curl -fsS https://$APP_DOMAIN/api/teams/7157B
curl -fsS https://$APP_DOMAIN/api/events/RE-V5RC-26-4025/rankings
```

Admin refresh:

```bash
curl -fsS -X POST https://$APP_DOMAIN/api/admin/refresh \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"eventSku":"RE-V5RC-26-4025","teamNumber":"7157B","source":"live"}'
```

## Hardening

- Do not expose Postgres outside Docker.
- Restrict SSH to your IP or use OCI Bastion.
- Rotate `ADMIN_TOKEN` and `ROBOTEVENTS_API_KEY` through Vault.
- Keep `deploy/oci/.env.production` mode `600`.
- Verify backups by restoring locally before trusting them.
- Add OCI budget alerts at 10, 25, and 50 USD.

## Cost Shape

For roughly 100 users/day, the A1 VM is the only meaningful cost driver. Always Free can run this at 0 USD if A1 and block-volume capacity are available in the tenancy. Paid fallback is typically a small single-VM bill plus block storage.
