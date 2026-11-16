#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi
: "${MYSQL_ADMIN_USER:=root}"
: "${MYSQL_HOST:=127.0.0.1}"
: "${MYSQL_PORT:=3306}"
MYSQL_PWD="${MYSQL_ADMIN_PASSWORD:-${MYSQL_PASSWORD:-orcaterm2024}}" \
  mysql --protocol=tcp --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_ADMIN_USER" \
  < "$ROOT/sql/001_schema.sql"
printf 'MySQL schema initialized: binance_futures\n'
