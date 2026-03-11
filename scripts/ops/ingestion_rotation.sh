#!/usr/bin/env bash
set -euo pipefail

VAR_ROOT="/data/data/com.termux/files/home/var/tirehub-system"
EVIDENCE_DIR="$VAR_ROOT/evidence"
LANDING_DIR="$VAR_ROOT/landing"
LOG_DIR="$VAR_ROOT/logs"
RUN_DIR="$VAR_ROOT/run"
ROUTING_LOG="$LOG_DIR/routing.log"
ARCHIVE_DIR="$LOG_DIR/archive"
LOCK_FILE="$RUN_DIR/ingestion_rotation.lock"

EVIDENCE_TTL_DAYS=30
LANDING_TTL_DAYS=14
ARCHIVE_TTL_DAYS=30

LOG_ROTATE_AGE_DAYS=1
LOG_ROTATE_SIZE_BYTES=$((5 * 1024 * 1024))

mkdir -p "$LOG_DIR" "$RUN_DIR" "$ARCHIVE_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "rotation locked: $LOCK_FILE"
  exit 0
fi

echo "=== INGESTION ROTATION $(date '+%Y-%m-%d %H:%M:%S') ==="

echo "[1] evidence cleanup > ${EVIDENCE_TTL_DAYS}d"
if [ -d "$EVIDENCE_DIR" ]; then
  while IFS= read -r -d '' f; do
    echo "delete evidence: $f"
    rm -f -- "$f"
  done < <(find "$EVIDENCE_DIR" -type f -mtime +${EVIDENCE_TTL_DAYS} -print0)
else
  echo "evidence dir missing"
fi

echo "[2] landing cleanup > ${LANDING_TTL_DAYS}d"
if [ -d "$LANDING_DIR" ]; then
  while IFS= read -r -d '' f; do
    echo "delete landing: $f"
    rm -f -- "$f"
  done < <(find "$LANDING_DIR" -type f -mtime +${LANDING_TTL_DAYS} -print0)
else
  echo "landing dir missing"
fi

echo "[3] routing.log smart rotate"
if [ -f "$ROUTING_LOG" ]; then
  if [ -s "$ROUTING_LOG" ]; then
    NOW_EPOCH=$(date +%s)
    LOG_MTIME=$(stat -c %Y "$ROUTING_LOG" 2>/dev/null || echo 0)
    LOG_SIZE=$(stat -c %s "$ROUTING_LOG" 2>/dev/null || echo 0)
    AGE_DAYS=$(( (NOW_EPOCH - LOG_MTIME) / 86400 ))

    ROTATE_REASON=""
    if [ "$AGE_DAYS" -ge "$LOG_ROTATE_AGE_DAYS" ]; then
      ROTATE_REASON="age=${AGE_DAYS}d"
    fi
    if [ "$LOG_SIZE" -ge "$LOG_ROTATE_SIZE_BYTES" ]; then
      if [ -n "$ROTATE_REASON" ]; then
        ROTATE_REASON="${ROTATE_REASON},size=${LOG_SIZE}B"
      else
        ROTATE_REASON="size=${LOG_SIZE}B"
      fi
    fi

    if [ -n "$ROTATE_REASON" ]; then
      TS=$(date +%Y%m%d_%H%M%S)
      TARGET="$ARCHIVE_DIR/routing_${TS}.log.gz"
      gzip -c "$ROUTING_LOG" > "$TARGET"
      : > "$ROUTING_LOG"
      echo "rotated -> $TARGET ($ROTATE_REASON)"
    else
      echo "skip rotate: age=${AGE_DAYS}d size=${LOG_SIZE}B"
    fi
  else
    echo "routing.log exists but empty"
  fi
else
  echo "routing.log missing"
fi

echo "[4] archive cleanup > ${ARCHIVE_TTL_DAYS}d with keep-last invariant"
mapfile -t ARCHIVES < <(find "$ARCHIVE_DIR" -maxdepth 1 -type f -name 'routing_*.log.gz' -printf '%f\n' | sort)
COUNT=${#ARCHIVES[@]}

if [ "$COUNT" -eq 0 ]; then
  echo "archives: none"
elif [ "$COUNT" -eq 1 ]; then
  echo "archives: keep only existing newest=${ARCHIVES[0]}"
else
  NEWEST="${ARCHIVES[$((COUNT - 1))]}"
  echo "newest archive preserved: $NEWEST"
  NOW_EPOCH=$(date +%s)
  for ((i=0; i<COUNT-1; i++)); do
    F="$ARCHIVE_DIR/${ARCHIVES[$i]}"
    FILE_MTIME=$(stat -c %Y "$F" 2>/dev/null || echo 0)
    AGE_DAYS=$(( (NOW_EPOCH - FILE_MTIME) / 86400 ))
    if [ "$AGE_DAYS" -ge "$ARCHIVE_TTL_DAYS" ]; then
      echo "delete old archive: $F"
      rm -f -- "$F"
    else
      echo "keep archive: $F age=${AGE_DAYS}d"
    fi
  done
fi

echo "=== ROTATION DONE ==="
