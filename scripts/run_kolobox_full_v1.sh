#!/usr/bin/env bash
set -euo pipefail

# run_kolobox_full_v1.sh — полный оркестратор Kolobox v1
# emitter → gate → ingest → curated

# Defaults (override via env)
PY="${PY:-/home/etl/apps/tirehub/.venv/bin/python}"
SUPPLIER="kolobox"
INPUT_FILE="${INPUT_FILE:-inputs/inbox/Kolobox/Прайс_Колобокс_Шины_2026-02-03 (XLS).xls}"
MAPPING_FILE="${MAPPING_FILE:-mappings/suppliers/kolobox.yaml}"
RUN_ID="${RUN_ID:-kolobox_shiny_$(date -u +%Y%m%dT%H%M%SZ)_full}"

OUT_DIR="out/${SUPPLIER}/ndjson_v1final/${RUN_ID}"
mkdir -p "$OUT_DIR"

GOOD_NDJSON="$OUT_DIR/good.ndjson"
STATS_JSON="$OUT_DIR/stats.json"
VERDICT_JSON="$OUT_DIR/verdict.json"

CURATED_OUT_DIR="curated_v1/out/${RUN_ID}"
CURATED_NDJSON="${CURATED_OUT_DIR}/curated.ndjson"
CURATED_STATS="${CURATED_OUT_DIR}/stderr.log"

echo "python:  $PY"
echo "run_id:  $RUN_ID"
echo "input:   $INPUT_FILE"
echo "mapping: $MAPPING_FILE"
echo "out_dir: $OUT_DIR"

# 1) Emitter
echo "=== 1) Emitter ==="
"$PY" scripts/ingestion/kolobox/emit_kolobox_ndjson_v1_FINAL.py \
  --file "$INPUT_FILE" \
  --mapping "$MAPPING_FILE" \
  --out "$GOOD_NDJSON" \
  --stats-out "$STATS_JSON" \
  --run-id "$RUN_ID"

test -s "$GOOD_NDJSON" || { echo "NOT_FOUND_OR_EMPTY: $GOOD_NDJSON"; exit 1; }
test -s "$STATS_JSON"  || { echo "NOT_FOUND_OR_EMPTY: $STATS_JSON";  exit 1; }

# 2) Gate
echo "=== 2) Gate ==="
"$PY" scripts/ingestion/kolobox/tirehub_gate_v1.py \
  --stats "$STATS_JSON" \
  --baseline rulesets/gate_baselines/kolobox_xls_v1.baseline.json \
  --out "$VERDICT_JSON"

VERDICT="$(jq -r '.verdict' "$VERDICT_JSON")"
echo "gate_verdict: $VERDICT"
if [ "$VERDICT" = "FAIL" ]; then
  echo "ERROR: gate FAIL: $VERDICT_JSON"
  cat "$VERDICT_JSON"
  exit 1
fi

# 3) Ingestion
echo "=== 3) Ingestion ==="
"$PY" scripts/ingestion/tirehub_ingest_v1.py \
  --good "$GOOD_NDJSON" \
  --stats "$STATS_JSON" \
  --verdict "$VERDICT_JSON" \
  --mapping "$MAPPING_FILE"

MANIFEST_JSON="ssot/manifests/${RUN_ID}.json"
test -s "$MANIFEST_JSON" || { echo "NOT_FOUND_OR_EMPTY: $MANIFEST_JSON"; exit 1; }

# 4) Curated
echo "=== 4) Curated ==="
"$PY" scripts/curated/tirehub_curate_v1.py \
  --manifest "$MANIFEST_JSON" \
  --out-dir "$CURATED_OUT_DIR"

test -s "$CURATED_NDJSON" || { echo "NOT_FOUND_OR_EMPTY: $CURATED_NDJSON"; exit 1; }
test -s "$CURATED_STATS"  || { echo "NOT_FOUND_OR_EMPTY: $CURATED_STATS";  exit 1; }

echo "=== DONE ==="
echo "good:     $GOOD_NDJSON"
echo "stats:    $STATS_JSON"
echo "verdict:  $VERDICT_JSON"
echo "manifest: $MANIFEST_JSON"
echo "curated:  $CURATED_NDJSON"
echo "cstats:   $CURATED_STATS"
cat "$CURATED_STATS" | jq .
