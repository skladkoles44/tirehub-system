#!/usr/bin/env bash
set -euo pipefail

# run_kolobox_full_v1.sh — оркестратор Kolobox v1 (PROD layout)
# emitter → gate → ingest(SSOT) → curated
#
# Инварианты:
# - repo = только код
# - data = $ETL_DATA_ROOT
# - ops  = $ETL_OPS_ROOT
#
# Требования окружения (желательно):
# - ETL_DATA_ROOT=/home/etl/etl_data
# - ETL_OPS_ROOT=/home/etl/etl_ops
#
# Опционально:
# - PY=/path/to/python (venv) — приоритет №1
# - SSOT_ROOT=/path/to/ssot — если нужно переопределить

ETL_DATA_ROOT="${ETL_DATA_ROOT:-/home/etl/etl_data}"
ETL_OPS_ROOT="${ETL_OPS_ROOT:-/home/etl/etl_ops}"

RAW_ROOT="${RAW_ROOT:-$ETL_DATA_ROOT/raw_v1}"
SSOT_ROOT="${SSOT_ROOT:-$RAW_ROOT/ssot}"
CURATED_ROOT="${CURATED_ROOT:-$ETL_DATA_ROOT/curated_v1/out}"

SUPPLIER="${SUPPLIER:-kolobox}"

# inputs
IN_FILE="${IN_FILE:-$RAW_ROOT/inbox/Kolobox/Прайс_Колобокс_Шины_2026-02-03 (XLS).xls}"
MAPPING="${MAPPING:-mappings/suppliers/kolobox.yaml}"
EFFECTIVE_AT="${EFFECTIVE_AT:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
RUN_ID="${RUN_ID:-kolobox_shiny_$(date -u +%Y%m%dT%H%M%SZ)_full}"

# ops run dir
RUN_DIR="$ETL_OPS_ROOT/runs/$RUN_ID"
mkdir -p "$RUN_DIR"

# logging
exec 1> >(tee "$RUN_DIR/stdout.log") 2> >(tee "$RUN_DIR/stderr.log" >&2)

# repo root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# python selection (stable)
if [ -z "${PY:-}" ]; then
  PY="$(find "$ETL_OPS_ROOT/tmp" -maxdepth 4 -type f -path '*/bin/python' 2>/dev/null | sort -r | head -n 1 || true)"
fi
if [ -z "${PY:-}" ] && [ -x "$REPO_ROOT/.venv/bin/python" ]; then
  PY="$REPO_ROOT/.venv/bin/python"
fi
if [ -z "${PY:-}" ] && command -v python3 >/dev/null 2>&1; then
  PY="$(command -v python3)"
fi
[ -n "${PY:-}" ] && [ -x "$PY" ] || { echo "NOT_FOUND: PY (set env PY=/path/to/python)"; exit 1; }

command -v jq >/dev/null 2>&1 || { echo "NOT_FOUND: jq"; exit 1; }

# git commit (best-effort)
GIT_COMMIT="NOT_FOUND"
if command -v git >/dev/null 2>&1 && [ -d "$REPO_ROOT/.git" ]; then
  GIT_COMMIT="$(cd "$REPO_ROOT" && git rev-parse HEAD 2>/dev/null || echo NOT_FOUND)"
fi

# MAPPING path resolution
if [[ "$MAPPING" != /* ]]; then
  MAPPING_PATH="$REPO_ROOT/$MAPPING"
else
  MAPPING_PATH="$MAPPING"
fi

# derive mapping_version / contract_version
MAPPING_VERSION="$(basename "$MAPPING_PATH")"
MAPPING_VERSION="${MAPPING_VERSION%.yaml}"
MAPPING_VERSION="${MAPPING_VERSION%.yml}"

CONTRACT_VERSION="$(
  (grep -E -i '^\s*(contract_version|contract)\s*:' "$MAPPING_PATH" 2>/dev/null || true) \
  | head -n 1 | sed -E 's/^\s*[^:]+:\s*//; s/\s+$//' || true
)"
[ -n "${CONTRACT_VERSION:-}" ] || CONTRACT_VERSION="NOT_FOUND"

# OUT (OPS tmp, not repo)
OUT_ROOT="$ETL_OPS_ROOT/tmp/out/${SUPPLIER}/ndjson_v1final"
OUT_DIR="$OUT_ROOT/$RUN_ID"
GOOD_NDJSON="$OUT_DIR/good.ndjson"
BAD_NDJSON="$OUT_DIR/bad.ndjson"
STATS_JSON="$OUT_DIR/stats.json"
VERDICT_JSON="$OUT_DIR/verdict.json"

BASELINE_GATE="$REPO_ROOT/rulesets/gate_baselines/kolobox_xls_v1.baseline.json"

echo "=== RUN CONTEXT ==="
echo "REPO_ROOT:       $REPO_ROOT"
echo "GIT_COMMIT:      $GIT_COMMIT"
echo "PY:              $PY"
echo "RUN_ID:          $RUN_ID"
echo "SUPPLIER:        $SUPPLIER"
echo "IN_FILE:         $IN_FILE"
echo "EFFECTIVE_AT:    $EFFECTIVE_AT"
echo "ETL_DATA_ROOT:   $ETL_DATA_ROOT"
echo "ETL_OPS_ROOT:    $ETL_OPS_ROOT"
echo "RAW_ROOT:        $RAW_ROOT"
echo "SSOT_ROOT:       $SSOT_ROOT"
echo "CURATED_ROOT:    $CURATED_ROOT"
echo "MAPPING:         $MAPPING"
echo "MAPPING_PATH:    $MAPPING_PATH"
echo "MAPPING_VERSION: $MAPPING_VERSION"
echo "CONTRACT_VERSION:$CONTRACT_VERSION"
echo "OUT_DIR:         $OUT_DIR"
echo

echo "=== 0) Preconditions ==="
test -s "$IN_FILE" || { echo "NOT_FOUND: $IN_FILE"; exit 1; }
test -f "$MAPPING_PATH" || { echo "NOT_FOUND: $MAPPING_PATH"; exit 1; }
test -f "$BASELINE_GATE" || { echo "NOT_FOUND: $BASELINE_GATE"; exit 1; }
mkdir -p "$OUT_DIR" "$SSOT_ROOT" "$CURATED_ROOT"
echo "INPUT_SHA256:    $(sha256sum "$IN_FILE" | awk '{print $1}')"
echo

echo "=== 1) Emitter (OPS tmp out) ==="
"$PY" "$REPO_ROOT/scripts/ingestion/emit_generic_ndjson_v1.py" \
  --supplier-id "$SUPPLIER" \
  --input "$IN_FILE" \
  --mapping "$MAPPING_PATH" \
  --effective-at "$EFFECTIVE_AT" \
  --run-id "$RUN_ID" \
  --out-dir "$OUT_ROOT"

test -d "$OUT_DIR" || { echo "NOT_FOUND: $OUT_DIR"; exit 1; }
test -s "$GOOD_NDJSON" || { echo "NOT_FOUND: $GOOD_NDJSON"; ls -la "$OUT_DIR" || true; exit 1; }
test -s "$STATS_JSON" || { echo "NOT_FOUND: $STATS_JSON"; ls -la "$OUT_DIR" || true; exit 1; }
test -f "$BAD_NDJSON" || : > "$BAD_NDJSON"
echo

echo "=== 2) Gate ==="
"$PY" "$REPO_ROOT/scripts/ingestion/kolobox/tirehub_gate_v1.py" \
  --stats "$STATS_JSON" \
  --baseline "$BASELINE_GATE" \
  --out "$VERDICT_JSON"

VERDICT="$(jq -r '.verdict' "$VERDICT_JSON")"
echo "gate_verdict: $VERDICT"
if [ "$VERDICT" = "FAIL" ]; then
  echo "FAIL verdict.json:"
  cat "$VERDICT_JSON"
  exit 1
fi
echo

echo "=== 3) Ingestion (SSOT) -> SSOT_ROOT ==="
"$PY" "$REPO_ROOT/scripts/ingestion/tirehub_ingest_v1.py" \
  --good "$GOOD_NDJSON" \
  --stats "$STATS_JSON" \
  --verdict "$VERDICT_JSON" \
  --mapping "$MAPPING_PATH" \
  --ssot-root "$SSOT_ROOT"

MANIFEST_JSON="$SSOT_ROOT/manifests/${RUN_ID}.json"
test -f "$MANIFEST_JSON" || {
  echo "NOT_FOUND: $MANIFEST_JSON"
  echo "== manifests tail =="
  ls -la "$SSOT_ROOT/manifests" 2>/dev/null | tail -n 50 || true
  exit 1
}
echo "MANIFEST_JSON: $MANIFEST_JSON"
echo

echo "=== 4) Curated -> CURATED_ROOT/<run_id> ==="
CUR_OUT_DIR="$CURATED_ROOT/$RUN_ID"
mkdir -p "$CUR_OUT_DIR"

"$PY" "$REPO_ROOT/scripts/curated/tirehub_curate_v1.py" \
  --manifest "$MANIFEST_JSON" \
  --out-dir "$CUR_OUT_DIR"

CURATED_NDJSON="$CUR_OUT_DIR/$RUN_ID/curated.ndjson"
test -s "$CURATED_NDJSON" || { echo "NOT_FOUND: $CURATED_NDJSON"; ls -la "$CUR_OUT_DIR" || true; exit 1; }
echo "CURATED_SHA256: $(sha256sum "$CURATED_NDJSON" | awk '{print $1}')"
echo

echo "=== 5) Write run.manifest.json (OPS) ==="
INPUT_SHA="$(sha256sum "$IN_FILE" | awk '{print $1}')"
GOOD_SHA="$(sha256sum "$GOOD_NDJSON" | awk '{print $1}')"
BAD_SHA="$(sha256sum "$BAD_NDJSON" | awk '{print $1}')"
CUR_SHA="$(sha256sum "$CURATED_NDJSON" | awk '{print $1}')"

PARSER_ID="$(jq -r '.parser_id // empty' "$VERDICT_JSON" 2>/dev/null || true)"
[ -n "$PARSER_ID" ] || PARSER_ID="$(jq -r '.parser_id // empty' "$MANIFEST_JSON" 2>/dev/null || true)"
[ -n "$PARSER_ID" ] || PARSER_ID="NOT_FOUND"

RUN_ID="$RUN_ID" \
RUN_DIR="$RUN_DIR" \
GIT_COMMIT="$GIT_COMMIT" \
PARSER_ID="$PARSER_ID" \
MAPPING_PATH="$MAPPING_PATH" \
MAPPING_VERSION="$MAPPING_VERSION" \
CONTRACT_VERSION="$CONTRACT_VERSION" \
EFFECTIVE_AT="$EFFECTIVE_AT" \
IN_FILE="$IN_FILE" \
INPUT_SHA="$INPUT_SHA" \
GOOD_NDJSON="$GOOD_NDJSON" \
GOOD_SHA="$GOOD_SHA" \
BAD_NDJSON="$BAD_NDJSON" \
BAD_SHA="$BAD_SHA" \
STATS_JSON="$STATS_JSON" \
VERDICT_JSON="$VERDICT_JSON" \
SSOT_ROOT="$SSOT_ROOT" \
MANIFEST_JSON="$MANIFEST_JSON" \
CURATED_DIR="$CUR_OUT_DIR" \
CURATED_NDJSON="$CURATED_NDJSON" \
CURATED_SHA="$CUR_SHA" \
"$PY" - <<'PY'
import json, os, time

run_id = os.environ["RUN_ID"]
run_dir = os.environ["RUN_DIR"]

out = {
  "run_id": run_id,
  "git_commit": os.environ["GIT_COMMIT"],
  "parser_id": os.environ["PARSER_ID"],
  "mapping_version": os.environ["MAPPING_VERSION"],
  "contract_version": os.environ["CONTRACT_VERSION"],
  "effective_at": os.environ["EFFECTIVE_AT"],
  "input_hashes": {
    os.environ["IN_FILE"]: {"sha256": os.environ["INPUT_SHA"]},
  },
  "output_paths": {
    "good_ndjson": os.environ["GOOD_NDJSON"],
    "bad_ndjson": os.environ["BAD_NDJSON"],
    "stats_json": os.environ["STATS_JSON"],
    "verdict_json": os.environ["VERDICT_JSON"],
    "ssot_root": os.environ["SSOT_ROOT"],
    "ssot_manifest": os.environ["MANIFEST_JSON"],
    "curated_dir": os.environ["CURATED_DIR"],
    "curated_ndjson": os.environ["CURATED_NDJSON"],
    "stdout_log": run_dir + "/stdout.log",
    "stderr_log": run_dir + "/stderr.log",
  },
  "output_hashes": {
    os.environ["GOOD_NDJSON"]: {"sha256": os.environ["GOOD_SHA"]},
    os.environ["BAD_NDJSON"]: {"sha256": os.environ["BAD_SHA"]},
    os.environ["CURATED_NDJSON"]: {"sha256": os.environ["CURATED_SHA"]},
  },
  "timestamps": {
    "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
  },
  "exit_code": 0,
}

path = os.path.join(run_dir, "run.manifest.json")
with open(path, "w", encoding="utf-8") as f:
  json.dump(out, f, ensure_ascii=False, indent=2)

print("WROTE:", path)
PY

echo
echo "=== DONE ==="
echo "RUN_DIR:       $RUN_DIR"
echo "OUT_DIR:       $OUT_DIR"
echo "MANIFEST_JSON: $MANIFEST_JSON"
echo "CURATED_DIR:   $CUR_OUT_DIR"
