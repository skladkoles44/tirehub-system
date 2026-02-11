#!/usr/bin/env bash
set -euo pipefail

# Repo root (works both on VPS and Termux clone)
REPO="${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
cd "$REPO"

# Python selector:
# - VPS: set PYTHON=/home/.../venvs/.../bin/python
# - Termux: default python (system)
PYTHON="${PYTHON:-python3}"

XLS="${XLS:-inputs/inbox/Brinex/b2bbr.xlsx}"
MAPPING="${MAPPING:-mappings/suppliers/brinex_xlsx_v1.yaml}"

RUN_ID="${RUN_ID:-brinex_tetl_all_$(date +%Y%m%d_%H%M%S)}"
OUTDIR="${OUTDIR:-out/batch/$RUN_ID}"
mkdir -p "$OUTDIR"

# list sheets
"$PYTHON" - <<PY >"$OUTDIR/sheets.txt"
import openpyxl
wb=openpyxl.load_workbook("${XLS}", read_only=True, data_only=True)
for i,name in enumerate(wb.sheetnames, start=1):
    print(f"sheet{i:02d}\t{name}")
PY

{
  echo "RUN_ID=$RUN_ID"
  echo "XLS=$XLS"
  echo "MAPPING=$MAPPING"
  echo "OUTDIR=$OUTDIR"
  echo "TS_START=$(date -Is)"
} >"$OUTDIR/meta.txt"

: >"$OUTDIR/progress.txt"

while IFS=$'\t' read -r KEY SHEET_NAME; do
  [ -n "${KEY:-}" ] || continue
  [ -n "${SHEET_NAME:-}" ] || continue

  echo "=== START ${KEY} ${SHEET_NAME} ===" | tee -a "$OUTDIR/progress.txt" >/dev/null

  LOG="$OUTDIR/${KEY}.log"
  OUT="$OUTDIR/brinex.${KEY}.ndjson"
  STATS="$OUTDIR/brinex.${KEY}.stats.json"

  "$PYTHON" scripts/ingestion/brinex/emit_brinex_xlsx_v1.py \
    --file "$XLS" \
    --layout "category:${KEY}" \
    --sheet "$SHEET_NAME" \
    --run-id "$RUN_ID" \
    --mapping "$MAPPING" \
    --out "$OUT" \
    --stats-out "$STATS" \
    >"$LOG" 2>&1 || true
done <"$OUTDIR/sheets.txt"

echo "TS_END=$(date -Is)" >>"$OUTDIR/meta.txt"
echo "DONE"
echo "OUTDIR=$OUTDIR"
