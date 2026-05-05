#!/usr/bin/env sh
set -eu

ROOT_DIR=${ROOT_DIR:-/opt/vex-ranker}
COMPOSE_FILE=${COMPOSE_FILE:-$ROOT_DIR/deploy/oci/docker-compose.oci.yml}
ENV_FILE=${ENV_FILE:-$ROOT_DIR/deploy/oci/.env.production}
BACKUP_DIR=${BACKUP_DIR:-$ROOT_DIR/backups}
RETENTION_DAYS=${RETENTION_DAYS:-14}

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

POSTGRES_USER=${POSTGRES_USER:-vex}
POSTGRES_DB=${POSTGRES_DB:-vexranker}
timestamp=$(date -u +"%Y%m%dT%H%M%SZ")
backup_file="$BACKUP_DIR/$POSTGRES_DB-$timestamp.dump"

mkdir -p "$BACKUP_DIR"
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc > "$backup_file"

find "$BACKUP_DIR" -type f -name "$POSTGRES_DB-*.dump" -mtime +"$RETENTION_DAYS" -delete
printf '{"event":"postgres_backup","status":"success","file":"%s"}\n' "$backup_file"
