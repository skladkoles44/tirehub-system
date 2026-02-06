#!/usr/bin/env bash
set -euo pipefail

# full orchestrator: emitter → gate → ingest → curated
# runs on VPS (etl) inside /home/etl/apps/tirehub, but file is committed from phone.

PY="${PY:-/home/etl/apps/tirehub/.venv/bin/python}"

SUPPLIER="kolobox"
IN_FILE="${IN_FILE:-inputs/inbox/Kolobox/Прайс_Колобокс_Шины_2026-02-03 (XLS).xls}"
MAPPING="${MAPPING:-mappings/suppliers/kolobox.yaml}"
EFFECTIVE_AT="${EFFECTIVE_AT:-2026-02-04T10:15:00Z}"
RUN_ID="${RUN_ID:-kolobox_shiny_$(date -u +%Y%m%dT%H%M%SZ)_full}"

OUT_DIR="out/${SUPPLIER}/ndjson_v1final/${RUN_ID}"
mkdir -p "$OUT_DIR"

GOOD_NDJSON="${OUT_DIR}/good.ndjson"
STATS_JSON="${OUT_DIR}/stats.json"
VERDICT_JSON="${OUT_DIR}/verdict.json"

echo "python:  $PY"
echo "run_id:  $RUN_ID"
echo "input:   $IN_FILE"
echo "mapping: $MAPPING"
echo "out_dir: $OUT_DIR"

echo "=== 1) Emitter ==="
"$PY" scripts/ingestion/kolobox/emit_kolobox_ndjson_v1_FINAL.py \
  --input "$IN_FILE" \
  --mapping "$MAPPING" \
  --effective-at "$EFFECTIVE_AT" \
  --run-id "$RUN_ID" \
  --out-dir "$OUT_DIR"

test -s "$GOOD_NDJSON" || { echo "NOT_FOUND: $GOOD_NDJSON"; exit 2; }
test -s "$STATS_JSON"  || { echo "NOT_FOUND: $STATS_JSON"; exit 2; }

echo "=== 2) Gate ==="
"$PY" scripts/ingestion/kolobox/tirehub_gate_v1.py \
  --stats "$STATS_JSON" \
  --baseline rulesets/gate_baselines/kolobox_xls_v1.baseline.json \
  --out "$VERDICT_JSON"

VERDICT="$(jq -r '.verdict' "$VERDICT_JSON")"
echo "gate verdict: $VERDICT"
if [ "$VERDICT" = "FAIL" ]; then
  cat "$VERDICT_JSON" | jq .
  exit 3
fi

echo "=== 3) Ingestion (SSOT) ==="
"$PY" scripts/ingestion/tirehub_ingest_v1.py \
  --good "$GOOD_NDJSON" \
  --stats "$STATS_JSON" \
  --verdict "$VERDICT_JSON" \
  --mapping "$MAPPING"

MANIFEST_JSON="$(ls -1t ssot/manifests/${RUN_ID}*.json | head -n 1)"
test -s "$MANIFEST_JSON" || { echo "NOT_FOUND: $MANIFEST_JSON"; exit 4; }

echo "=== 4) Curated ==="
"$PY" scripts/curated/tirehub_curate_v1.py \
  --manifest "$MANIFEST_JSON" \
  --out-dir "curated_v1/out/${RUN_ID}"

echo "=== DONE ==="
echo "verdict:  $VERDICT_JSON"
echo "manifest: $MANIFEST_JSON"
echo "curated:  curated_v1/out/${RUN_ID}/curated.ndjson"
echo "curated_stats: curated_v1/out/${RUN_ID}/stderr.log"
cat "$VERDICT_JSON" | jq .
cat "curated_v1/out/${RUN_ID}/stderr.log" | jq .
