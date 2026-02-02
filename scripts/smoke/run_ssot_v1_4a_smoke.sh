#!/usr/bin/env bash
set -euo pipefail

DB="${1:-tirehub_etl}"
RUN_ID="${2:-}"
LIST_FILE="scripts/smoke/ssot_v1_4a_migrations.list"
SQL_FILE="scripts/smoke/ssot_v1_4a_smoke.sql"

ts(){ date '+%Y-%m-%d %H:%M:%S'; }
die(){ echo "[$(ts)] ERROR: $*" >&2; exit 1; }

[[ -n "$RUN_ID" ]] || die "RUN_ID is required. Usage: $0 <db> <run_id>"

LOG_DIR="/tmp/ssot_smoke_logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$(date +%Y%m%d_%H%M%S)_${DB}_run_${RUN_ID}.log"

{
  echo "[$(ts)] == SSOT v1.4a smoke =="
  echo "[$(ts)] DB=$DB"
  echo "[$(ts)] RUN_ID=$RUN_ID"
  echo "[$(ts)] SQL=$SQL_FILE"
  echo "[$(ts)] LOG=$LOG_FILE"

  echo "ok|db|$DB|user|$(psql -d "$DB" -Atc "select current_user")"

  sudo -n true 2>/dev/null || die "sudo -n is required for migrations as postgres"
  echo "[$(ts)] Applying migrations as postgres (sudo -n)..."
  while IFS= read -r f; do
    [[ -z "$f" ]] && continue
    echo "[$(ts)]   apply: $f"
    sudo -n -u postgres psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off -f "sql/migrations/$f" >/dev/null
  done < "$LIST_FILE"
  echo "[$(ts)] Migrations: OK"

  echo "[$(ts)] Running smoke SQL as current user (no sudo)..."
  psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off -v run_id="$RUN_ID" -f "$SQL_FILE"
  echo "[$(ts)] OK: smoke passed"
} 2>&1 | tee "$LOG_FILE"

echo "SAVED=$LOG_FILE"
