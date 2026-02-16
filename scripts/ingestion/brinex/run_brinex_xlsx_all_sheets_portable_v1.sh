#!/usr/bin/env bash
set -euo pipefail

PY="${PYTHON_BIN:-$(command -v python3)}"
XLSX="${BRINEX_XLSX_PATH:?BRINEX_XLSX_PATH not set}"
SSOT="${SSOT_ROOT:?SSOT_ROOT not set}"
TMP="${TMP_ROOT:?TMP_ROOT not set}"
RUNS_ROOT="${BRINEX_RUN_ROOT:-$TMP/brinex_xlsx_all_sheets_portable_v1}"

mkdir -p "$RUNS_ROOT"

echo "=== XLSX ==="
echo "$XLSX"
echo "=== SSOT ==="
echo "$SSOT"
echo "=== RUNS_ROOT ==="
echo "$RUNS_ROOT"
echo

SHEETS="$("$PY" scripts/ingestion/brinex/list_nonempty_sheets_v1.py "$XLSX")"

echo "=== SHEETS ==="
echo "$SHEETS"
echo

ok=0
fail=0
idx=0

while IFS= read -r sheet; do
  [ -n "$sheet" ] || continue
  idx=$((idx+1))
  OUT="$(printf "%s/%02d" "$RUNS_ROOT" "$idx")"
  mkdir -p "$OUT"

  echo "=== RUN sheet: $sheet -> $OUT ==="

  # 1) emitter -> good.ndjson + stats.json
  "$PY" scripts/ingestion/brinex/emit_brinex_xlsx_sheet_v1.py "$XLSX" "$sheet" "$OUT" || {
    rc=$?
    echo "FAIL emitter rc=$rc sheet=$sheet"
    fail=$((fail+1))
    continue
  }

  # 2) baseline.json from stats.json (normalized)
  "$PY" - <<'PY'
import json
from pathlib import Path

out = Path(__import__("os").environ["OUTDIR"])
s = json.loads((out/"stats.json").read_text(encoding="utf-8"))

good = float(s.get("good_rows", 0))
bad  = float(s.get("bad_rows", 0))
src  = float(s.get("source_rows_read", good+bad)) or 1.0
expl = float(s.get("exploded_lines", good))
exp_factor = expl / src

baseline = {
  "baseline_version": "1.0",
  "supplier_id": s.get("supplier_id"),
  "parser_id": s.get("parser_id"),
  "expected": {
    "source_rows_read": int(src),
    "exploded_lines": int(expl),
    "explosion_factor_exact": exp_factor
  },
  "tolerance": {
    "source_rows_read": 0,
    "exploded_lines": 0,
    "explosion_factor_exact": 0.001
  }
}
(out/"baseline.json").write_text(json.dumps(baseline, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print("OK: baseline")
PY
  OUTDIR="$OUT" "$PY" - <<'PY' >/dev/null
# no-op placeholder to keep env consumption consistent (ignored)
PY

  # 3) gate (rc 0=PASS, 10=WARN)
  set +e
  "$PY" scripts/ingestion/kolobox/tirehub_gate_v1.py \
    --stats "$OUT/stats.json" \
    --out "$OUT/verdict.json" \
    --baseline "$OUT/baseline.json"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ] && [ "$rc" -ne 10 ]; then
    echo "FAIL gate rc=$rc sheet=$sheet"
    fail=$((fail+1))
    continue
  fi

  # 4) ingest
  "$PY" scripts/ingestion/tirehub_ingest_v1.py \
    --ssot-root "$SSOT" \
    --good "$OUT/good.ndjson" \
    --stats "$OUT/stats.json" \
    --verdict "$OUT/verdict.json" || {
      rc=$?
      echo "FAIL ingest rc=$rc sheet=$sheet"
      fail=$((fail+1))
      continue
    }

  echo "OK: ingested sheet=$sheet"
  ok=$((ok+1))
  echo
done <<< "$SHEETS"

echo "=== SUMMARY ==="
echo "ok=$ok fail=$fail runs_root=$RUNS_ROOT"
