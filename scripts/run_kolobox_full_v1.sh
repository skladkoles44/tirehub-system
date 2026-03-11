#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$REPO_ROOT" ] || { echo "Repo root not found"; exit 1; }

set -euo pipefail

# run_kolobox_full_v1.sh — оркестратор Kolobox v1
# emitter → gate → ingest → curated

PY="${PY:-${ETL_VENV_ROOT:?ETL_VENV_ROOT not set}/bin/python}"

SUPPLIER="kolobox"
IN_FILE="${IN_FILE:-${ETL_VAR_ROOT:?ETL_VAR_ROOT not set}/inputs/inbox/Kolobox/Прайс_Колобокс_Шины_2026-02-03 (XLS).xls}"
MAPPING="${MAPPING:-$REPO_ROOT/mappings/suppliers/kolobox.yaml}"
EFFECTIVE_AT="${EFFECTIVE_AT:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
RUN_ID="${RUN_ID:-kolobox_shiny_$(date -u +%Y%m%dT%H%M%SZ)_full}"

OUT_ROOT="${ETL_VAR_ROOT:?ETL_VAR_ROOT not set}/out/${SUPPLIER}/ndjson_v1final"
OUT_DIR="${OUT_ROOT}/${RUN_ID}"

GOOD_NDJSON="${OUT_DIR}/good.ndjson"
STATS_JSON="${OUT_DIR}/stats.json"
VERDICT_JSON="${OUT_DIR}/verdict.json"

BASELINE="$REPO_ROOT/rulesets/gate_baselines/kolobox_xls_v1.baseline.json"

echo "python:  $PY"
echo "run_id:  $RUN_ID"
echo "input:   $IN_FILE"
echo "mapping: $MAPPING"
echo "out_dir: $OUT_DIR"

echo "=== 0) git pull (VPS step is separate; here just file content) ==="

echo "=== 1) Emitter ==="
"$PY" "$REPO_ROOT/scripts/ingestion/emit_generic_ndjson_v1.py" \
  --supplier-id "kolobox" \
  --input "$IN_FILE" \
  --mapping "$MAPPING" \
  --effective-at "$EFFECTIVE_AT" \
  --run-id "$RUN_ID" \
  --out-dir "$OUT_ROOT"

test -d "$OUT_DIR" || { echo "NOT_FOUND: $OUT_DIR"; exit 1; }
test -s "$GOOD_NDJSON" || { echo "NOT_FOUND: $GOOD_NDJSON"; ls -la "$OUT_DIR" || true; exit 1; }
test -s "$STATS_JSON" || { echo "NOT_FOUND: $STATS_JSON"; ls -la "$OUT_DIR" || true; exit 1; }

echo "=== 2) Gate ==="
"$PY" "$REPO_ROOT/scripts/ingestion/kolobox/tirehub_gate_v1.py" \
  --stats "$STATS_JSON" \
  --baseline "$BASELINE" \
  --out "$VERDICT_JSON"

VERDICT="$(jq -r '.verdict' "$VERDICT_JSON")"
echo "gate_verdict: $VERDICT"
if [ "$VERDICT" = "FAIL" ]; then
  echo "FAIL verdict.json:"
  cat "$VERDICT_JSON"
  exit 1
fi

echo "=== 3) Ingestion (SSOT) ==="
"$PY" "$REPO_ROOT/scripts/ingestion/tirehub_ingest_v1.py" \
  --good "$GOOD_NDJSON" \
  --stats "$STATS_JSON" \
  --verdict "$VERDICT_JSON" \
  --mapping "$MAPPING"

MANIFEST_JSON="${SSOT_ROOT:?SSOT_ROOT not set}/manifests/${RUN_ID}.json"
test -f "$MANIFEST_JSON" || {
  echo "NOT_FOUND: $MANIFEST_JSON"
  echo "== manifests tail =="
  ls -la "${SSOT_ROOT:?SSOT_ROOT not set}/manifests" 2>/dev/null | tail -n 50 || true
  exit 1
}

echo "=== 4) Curated ==="
"$PY" "$REPO_ROOT/scripts/curated/tirehub_curate_v1.py" \
  --manifest "$MANIFEST_JSON" \
  --out-dir "${CURATED_ROOT:?CURATED_ROOT not set}"

echo "=== DONE ==="
echo "OUT_DIR:     $OUT_DIR"
echo "GOOD:        $GOOD_NDJSON"
echo "STATS:       $STATS_JSON"
echo "VERDICT:     $VERDICT_JSON"
echo "MANIFEST:    $MANIFEST_JSON"
echo "CURATED_DIR: ${CURATED_ROOT:?CURATED_ROOT not set}/${RUN_ID}"
