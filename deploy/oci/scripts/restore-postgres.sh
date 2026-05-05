#!/usr/bin/env sh
set -eu

if [ "${CONFIRM_RESTORE:-}" != "yes" ]; then
  echo "Refusing restore. Set CONFIRM_RESTORE=yes to overwrite the target database." >&2
  exit 2
fi

if [ "$#" -ne 1 ]; then
  echo "Usage: CONFIRM_RESTORE=yes $0 /path/to/backup.dump" >&2
  exit 2
fi

backup_file=$1
ROOT_DIR=${ROOT_DIR:-/opt/vex-ranker}
COMPOSE_FILE=${COMPOSE_FILE:-$ROOT_DIR/deploy/oci/docker-compose.oci.yml}
ENV_FILE=${ENV_FILE:-$ROOT_DIR/deploy/oci/.env.production}

if [ ! -f "$backup_file" ]; then
  echo "Backup file not found: $backup_file" >&2
  exit 2
fi

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

POSTGRES_USER=${POSTGRES_USER:-vex}
POSTGRES_DB=${POSTGRES_DB:-vexranker}

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  dropdb -U "$POSTGRES_USER" --if-exists "$POSTGRES_DB"
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  createdb -U "$POSTGRES_USER" "$POSTGRES_DB"
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists < "$backup_file"

printf '{"event":"postgres_restore","status":"success","file":"%s"}\n' "$backup_file"
