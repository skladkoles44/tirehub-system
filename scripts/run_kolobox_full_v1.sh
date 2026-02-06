#!/usr/bin/env bash
set -euo pipefail

# VPS only (etl): /home/etl/apps/tirehub
cd /home/etl/apps/tirehub

# 0) update code
git pull --ff-only

# 1) venv python
. .venv/bin/activate
PY="$(command -v python)"
echo "python: $PY"

# 2) inputs (caller must export these or edit defaults here)
: "${IN_FILE:?set IN_FILE to the Kolobox XLS path}"
: "${MAPPING:?set MAPPING to mapping yaml path}"
: "${EFFECTIVE_AT:?set EFFECTIVE_AT like 2026-02-04T10:15:00Z}"
: "${RUN_ID:?set RUN_ID like kolobox_shiny_YYYYMMDDTHHMMSSZ}"

OUT_DIR="${OUT_DIR:-out/kolobox/ndjson_v1final}"
BASELINE="${BASELINE:-rulesets/gate_baselines/kolobox_xls_v1.baseline.json}"

mkdir -p "$OUT_DIR/$RUN_ID"

NDJSON="$OUT_DIR/$RUN_ID/segment.ndjson"
STATS="$OUT_DIR/$RUN_ID/stats.json"
VERDICT="$OUT_DIR/$RUN_ID/verdict.json"

echo "run_id:   $RUN_ID"
echo "input:    $IN_FILE"
echo "mapping:  $MAPPING"
echo "out_dir:  $OUT_DIR"
echo "ndjson:   $NDJSON"
echo "stats:    $STATS"
echo "baseline: $BASELINE"
echo "verdict:  $VERDICT"

# 3) emit
python scripts/ingestion/kolobox/emit_kolobox_ndjson_v1_FINAL.py \
  --input "$IN_FILE" \
  --mapping "$MAPPING" \
  --effective-at "$EFFECTIVE_AT" \
  --run-id "$RUN_ID" \
  --out-dir "$OUT_DIR"

# 4) gate
python scripts/ingestion/kolobox/tirehub_gate_v1.py \
  --stats "$STATS" \
  --baseline "$BASELINE" \
  --out "$VERDICT"

cat "$VERDICT"

# 5) ingest (only PASS/WARN allowed by ingest script itself)
python scripts/ingestion/tirehub_ingest_v1.py \
  --manifest "/home/etl/apps/tirehub/ssot/manifests/${RUN_ID}.json" \
  --verdict "$VERDICT"

# 6) curated (qty-only eligibility)
python scripts/curated/tirehub_curate_v1.py \
  --manifest "/home/etl/apps/tirehub/ssot/manifests/${RUN_ID}.json" \
  --out-dir "curated_v1/out" \
  --max-dropped-samples 50
