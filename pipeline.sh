#!/usr/bin/env bash
set -euo pipefail

# ==================== PROGRESS ====================
show_progress() {
    local current=$1
    local total=$2
    if [ "$total" -eq 0 ]; then return; fi
    local percent=$((current * 100 / total))
    local filled=$((percent / 2))
    local empty=$((50 - filled))
    printf "\r  ["
    printf "%${filled}s" | tr " " "█"
    printf "%${empty}s" | tr " " "░"
    printf "] %3d%% (%d/%d)" "$percent" "$current" "$total"
}

# ==================== CONCURRENCY GUARD ====================
LOCK_FILE="$ETL_VAR_ROOT/run/etl_pipeline.lock"
PID_FILE="$ETL_VAR_ROOT/run/etl_pipeline.pid"

if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
    if [ -n "$OLD_PID" ] && ! kill -0 "$OLD_PID" 2>/dev/null; then
        echo "⚠️ STALE_LOCK_DETECTED → cleaning"
        rm -f "$LOCK_FILE" "$PID_FILE"
    fi
fi

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "⚠️ PIPELINE_ALREADY_RUNNING"
    exit 0
fi
echo $$ > "$PID_FILE"
trap 'rm -f "$PID_FILE"' EXIT

# ==================== LOG ROTATION ====================
LOG="$ETL_VAR_ROOT/logs/pipeline.log"
LOG_SIZE=0
if [ -f "$LOG" ]; then
    LOG_SIZE=$(wc -c < "$LOG" 2>/dev/null || echo 0)
fi
if [ "$LOG_SIZE" -gt 5000000 ]; then
    cp "$LOG" "$LOG.old" 2>/dev/null || true
    : > "$LOG"
fi

# ==================== CONFIG ====================
ETL_VAR_ROOT="${ETL_VAR_ROOT:-$ETL_VAR_ROOT}"
export ETL_VAR_ROOT

EVIDENCE="$ETL_VAR_ROOT/evidence"
UNPACK="$ETL_VAR_ROOT/drop/unpacked"
ARTIFACTS="$ETL_VAR_ROOT/artifacts"
MAX_PARALLEL="${MAX_PARALLEL:-2}"
BIG_FILE_THRESHOLD_MB=1.5

echo "=== XRAY BEGIN ==="
echo "ETL_VAR_ROOT=$ETL_VAR_ROOT"
echo "EVIDENCE=$EVIDENCE"
echo "UNPACK=$UNPACK"
echo "ARTIFACTS=$ARTIFACTS"
echo "=== XRAY END ==="

# ==================== INTAKE ====================
echo "=== INTAKE ==="
set -a
source secrets/mail_ingest.env 2>/dev/null || true
set +a
if ! python scripts/connectors/mail_ingest_worker_v1.py; then
    echo "⚠️ INTAKE_FAIL (continue)"
fi

# ==================== UNPACK ====================
echo "=== UNPACK ==="
rm -f "$UNPACK/unpacker.lock"
python scripts/connectors/mail_unpacker_v1.py "$EVIDENCE" "$UNPACK"

# ==================== RUNNER ====================
echo "=== RUNNER ==="
mkdir -p "$ARTIFACTS"

if [ ! -d "$UNPACK/attachments" ]; then
    echo "⚠️ NO_ATTACHMENTS_DIR"
    exit 0
fi

TMP_LIST=$(mktemp $ETL_VAR_ROOT/run/etl_files.XXXXXX)
find "$UNPACK/attachments" -type f \( -iname "*.xls" -o -iname "*.xlsx" -o -iname "*.xlsm" -o -iname "*.ods" -o -iname "*.csv" \) -print0 > "$TMP_LIST" 2>/dev/null || true

FILE_COUNT=$(tr -cd '\0' < "$TMP_LIST" 2>/dev/null | wc -c || echo 0)
echo "files_to_process_total=$FILE_COUNT"

FILES_TO_PROCESS=()
while IFS= read -r -d '' f; do
    FILES_TO_PROCESS+=("$f")
done < "$TMP_LIST"

TOTAL_FILES=${#FILES_TO_PROCESS[@]}
CURRENT=0
pids=()

for f in "${FILES_TO_PROCESS[@]}"; do
    CURRENT=$((CURRENT + 1))
    show_progress "$CURRENT" "$TOTAL_FILES"
    log="$ARTIFACTS/log_$(basename "$f" | sed 's/\.[^.]*$//' | tr '.' '_').txt"
    python scripts/etl/runner_with_fs_state.py "$f" "$ARTIFACTS" > "$log" 2>&1 &
    pids+=($!)
    
    if [ "${#pids[@]}" -ge "$MAX_PARALLEL" ]; then
        wait -n 2>/dev/null || wait "${pids[0]}"
        new_pids=()
        for pid in "${pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                new_pids+=("$pid")
            fi
        done
        pids=("${new_pids[@]}")
    fi
done

for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null
done
echo ""

rm -f "$TMP_LIST"

# ==================== ENRICH ROLES ====================
echo "=== ENRICH ROLES ==="
if ls "$ARTIFACTS"/run_*/ >/dev/null 2>&1; then
    for artifact_dir in "$ARTIFACTS"/run_*/; do
        if [ -d "$artifact_dir" ]; then
            atomic="$artifact_dir/atomic_rows.ndjson"
            enriched="$artifact_dir/atomic_rows_enriched.ndjson"
            if [ -f "$atomic" ] && [ ! -f "$enriched" ]; then
                python scripts/etl/enrich_roles.py "$atomic" "$enriched" 2>/dev/null
            fi
        fi
    done
else
    echo "  (нет артефактов)"
fi

# ==================== NORMALIZER ====================
echo "=== NORMALIZER ==="
if ls "$ARTIFACTS"/run_*/ >/dev/null 2>&1; then
    for artifact_dir in "$ARTIFACTS"/run_*/; do
        if [ -d "$artifact_dir" ]; then
            enriched="$artifact_dir/atomic_rows_enriched.ndjson"
            if [ -f "$enriched" ] && [ ! -f "$artifact_dir/good.ndjson" ]; then
                python scripts/normalization/normalizer_v3_1.py --atomic "$enriched" --out-dir "$artifact_dir" 2>/dev/null
            fi
        fi
    done
else
    echo "  (нет артефактов)"
fi

# ==================== XRAY RESULT ====================
echo "=== XRAY RESULT ==="
echo "--- artifacts ---"
ls "$ARTIFACTS" 2>/dev/null | wc -l
echo "--- good total ---"
find "$ARTIFACTS" -name "good.ndjson" -exec wc -l {} + 2>/dev/null | tail -1
echo "--- reject total ---"
find "$ARTIFACTS" -name "reject.ndjson" -exec wc -l {} + 2>/dev/null | tail -1
echo "=== DONE ==="

# ==================== TRUE STREAMING ENRICH ====================
echo "=== ENRICH ROLES (streaming) ==="
if ls "$ARTIFACTS"/run_*/ >/dev/null 2>&1; then
    for artifact_dir in "$ARTIFACTS"/run_*/; do
        if [ -d "$artifact_dir" ]; then
            atomic="$artifact_dir/atomic_rows.ndjson"
            enriched="$artifact_dir/atomic_rows_enriched.ndjson"
            if [ -f "$atomic" ] && [ ! -f "$enriched" ]; then
                echo "  Enriching: $(basename "$artifact_dir")"
                # True streaming: roles вычисляются по sample, затем применяются к потоку
                cat "$atomic" | tee "$artifact_dir/tmp_atomic.ndjson" > /dev/null
                head -20 "$artifact_dir/tmp_atomic.ndjson" | python scripts/etl/compute_roles.py > "$artifact_dir/roles.json"
                cat "$artifact_dir/tmp_atomic.ndjson" | python scripts/etl/apply_roles.py "$artifact_dir/roles.json" > "$enriched"
                rm -f "$artifact_dir/tmp_atomic.ndjson"
            fi
        fi
    done
else
    echo "  (нет артефактов)"
fi
