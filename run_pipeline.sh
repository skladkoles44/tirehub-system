#!/usr/bin/env bash
set -e

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VAR="${ETL_VAR_ROOT:?ETL_VAR_ROOT is not set}"

INBOX="$VAR/queue/inbox"
PROC="$VAR/queue/processing"
DONE="$VAR/queue/done"
FAIL="$VAR/queue/failed"
LOCKS="$VAR/queue/locks"

mkdir -p "$INBOX" "$PROC" "$DONE" "$FAIL" "$LOCKS"

HASH_DIR="$VAR/seen_hashes"
mkdir -p "$HASH_DIR"

log() { echo "[$(date +%H:%M:%S)] $1"; }

# ================= VERIFY =================
verify_ndjson() {
  local f="$1"
  [ -f "$f" ] || return 1
  [ -s "$f" ] || return 0  # empty is OK
  python3 -c "
import json, sys
try:
    for i, l in enumerate(open('$f')):
        json.loads(l)
except Exception as e:
    print(f'Invalid JSON at line {i+1}: {e}', file=sys.stderr)
    sys.exit(1)
" 2>/dev/null
}

# ================= STEP 1: MAIL → QUEUE =================
log "STEP 1: CHECK MAIL"

python3 scripts/connectors/mail_ingest_worker_v1.py

# Move attachments to queue
find /storage/emulated/0/Download/ETL/unpacked/attachments -type f 2>/dev/null | while read f; do
  name=$(basename "$f")
  hash=$(sha256sum "$f" | awk '{print $1}')
  marker="$HASH_DIR/$hash"

  if [ -f "$marker" ]; then
    log "SKIP DUPLICATE: $name"
    rm -f "$f"
    continue
  fi

  dest="$INBOX/$name"
  if [ ! -f "$dest" ]; then
    mv "$f" "$dest"
    log "QUEUE ADD: $name"
  fi
done

# ================= STEP 2: PROCESS QUEUE =================
process_file() {
  local FILE="$1"
  local NAME="$2"

  local LOCK="$LOCKS/$NAME.lock"

  # atomic lock with cleanup trap
  if ! mkdir "$LOCK" 2>/dev/null; then
    log "SKIP LOCKED: $NAME"
    return 99
  fi

  cleanup() { rm -rf "$LOCK"; }
  trap cleanup RETURN

  local hash=$(sha256sum "$FILE" | awk '{print $1}')
  local RUN_ID="$(date +%s)_$$"
  local WORK="$VAR/artifacts/$RUN_ID"
  mkdir -p "$WORK"

  log "PROCESS $NAME (run_id=$RUN_ID)"

  cp "$FILE" "$PROC/$NAME"

  # ---------- RUNNER (with state) ----------
  python3 scripts/etl/runner_with_fs_state.py "$PROC/$NAME" "$WORK"
  verify_ndjson "$WORK/atomic_rows.ndjson" || { log "RUNNER FAIL"; return 10; }

  # ---------- ENRICH ----------
  python3 scripts/etl/enrich_roles.py \
    "$WORK/atomic_rows.ndjson" \
    "$WORK/enriched.ndjson"
  verify_ndjson "$WORK/enriched.ndjson" || { log "ENRICH FAIL"; return 20; }

  # ---------- IDENTITY ----------
  python3 scripts/etl/identity_key_v2.py \
    "$WORK/enriched.ndjson" \
    "$WORK/identity.ndjson"
  verify_ndjson "$WORK/identity.ndjson" || { log "IDENTITY FAIL"; return 30; }

  # ---------- LOAD ----------
  # TODO: replace with idempotent loader when ready
  python3 scripts/etl/apply_to_postgres_v2.py "$WORK/identity.ndjson"

  mv "$PROC/$NAME" "$DONE/$NAME"
  touch "$HASH_DIR/$hash"
  rm -f "$FILE"

  log "DONE $NAME"
  return 0
}

# ================= LOOP (sequential, safe) =================
log "STEP 2: PROCESS QUEUE"

while true; do
  FILE=$(find "$INBOX" -type f | head -n1)

  if [ -z "$FILE" ]; then
    log "QUEUE EMPTY → EXIT"
    break
  fi

  NAME=$(basename "$FILE")

  if process_file "$FILE" "$NAME"; then
    :
  else
    log "FAIL $NAME"
    mv "$PROC/$NAME" "$FAIL/$NAME" 2>/dev/null || true
  fi
done

log "PIPELINE FINISHED"
