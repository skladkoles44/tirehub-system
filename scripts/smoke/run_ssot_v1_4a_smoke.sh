#!/usr/bin/env bash
set -euo pipefail

DB="${1:-tirehub_etl}"
SQL_FILE="scripts/smoke/ssot_v1_4a_smoke.sql"
LOG_DIR="/tmp/ssot_smoke_logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$(date +%Y%m%d_%H%M%S)_${DB}_nosudo.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "== SSOT v1.4a smoke (NO SUDO for SQL run, migrations as postgres) =="
log "DB=$DB"
log "SQL=$SQL_FILE"
log "LOG=$LOG_FILE"

# preflight: DB access as current user
if ! psql -d "$DB" -At -P pager=off -v ON_ERROR_STOP=1 <<'SQL' >>"$LOG_FILE" 2>&1; then
SELECT 'ok|db|'||current_database()||'|user|'||current_user;
SELECT 'ok|schema|ssot_ingestion|'||EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='ssot_ingestion');
SELECT 'ok|schema|ssot_curated_internal|'||EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='ssot_curated_internal');
SELECT 'ok|schema|ssot_curated_api|'||EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='ssot_curated_api');
SQL
  log "ERROR: psql access failed (see log tail below)"
  tail -80 "$LOG_FILE" || true
  exit 1
fi

# migrations: must run as postgres (owner)
log "Applying migrations as postgres (sudo -n)..."
for f in $(ls -1 sql/migrations/*.sql 2>/dev/null | sort); do
  log "  apply: $(basename "$f")"
  if ! sudo -n -u postgres psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off -f "$f" >>"$LOG_FILE" 2>&1; then
    log "ERROR: migration failed: $f"
    tail -120 "$LOG_FILE" || true
    exit 1
  fi
done
log "Migrations: OK"

# smoke itself: run as current user (etl) WITHOUT sudo
log "Running smoke SQL as current user (no sudo)..."
if ! psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off -f "$SQL_FILE" >>"$LOG_FILE" 2>&1; then
  log "ERROR: smoke failed"
  tail -160 "$LOG_FILE" || true
  exit 1
fi

log "OK: smoke passed"
echo "SAVED=$LOG_FILE"
