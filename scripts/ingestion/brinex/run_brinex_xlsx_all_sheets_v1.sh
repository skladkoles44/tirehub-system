#!/data/data/com.termux/files/usr/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$REPO_ROOT" ] || { echo "Repo root not found"; exit 1; }
set -euo pipefail

XLSX="/storage/emulated/0/Download/ETL/прайсотБринэксКозловской (1).xlsx"
SSOT="/storage/emulated/0/Download/ETL/etl_data/raw_v1/ssot"
ROOT="/storage/emulated/0/Download/ETL/etl_ops/tmp/brinex_xlsx_all_sheets"

mkdir -p "$ROOT"

# Build sheet list (same logic as step2)
SHEETS=$(python3 - << 'PY'
from openpyxl import load_workbook
from pathlib import Path
xlsx = Path("/storage/emulated/0/Download/ETL/прайсотБринэксКозловской (1).xlsx")
NEEDED_HEADERS = {"Код товара (goods_id)","Номенклатура","Код товара (product_id)","Артикул","Склад","Цена","Остаток на складе"}
def norm(x): return "" if x is None else str(x).strip()
wb = load_workbook(xlsx, data_only=True, read_only=True)
ok=[]
for s in wb.sheetnames:
    ws=wb[s]
    if ws.max_row is None or ws.max_row < 8: 
        continue
    best=0
    for i,row in enumerate(ws.iter_rows(values_only=True), start=1):
        vals=[norm(v) for v in row]
        hits=sum(1 for v in vals if v in NEEDED_HEADERS)
        best=max(best,hits)
        if i>=80: break
    if best>=4:
        ok.append(s)
print("\n".join(ok))
PY
)

echo "=== SHEETS ==="
echo "$SHEETS"
echo

ok_count=0
fail_count=0

while IFS= read -r sheet; do
  [ -z "$sheet" ] && continue
  ok_count=$((ok_count+1))
  OUT="$ROOT/$(echo "$ok_count" | awk '{printf "%02d", $1}')"
  mkdir -p "$OUT"

  echo "=== RUN sheet: $sheet -> $OUT ==="

  # emitter
  if ! python3 "$REPO_ROOT/scripts/ingestion/brinex/emit_brinex_xlsx_sheet_v1.py" "$XLSX" "$sheet" "$OUT"; then
    echo "SKIP/FAIL emitter sheet=$sheet"
    fail_count=$((fail_count+1))
    continue
  fi

  # baseline from stats
  python3 - << PY
import json
from pathlib import Path
stats_p = Path("$OUT/stats.json")
base_p  = Path("$OUT/baseline.json")
s = json.loads(stats_p.read_text(encoding="utf-8"))
good = float(s.get("good_rows", 0))
src  = float(s.get("source_rows_read", 0)) or 1.0
expl = float(s.get("exploded_lines", good))
exp_factor = expl / src
baseline = {
  "baseline_version": "1.0",
  "supplier_id": s.get("supplier_id"),
  "parser_id": s.get("parser_id"),
  "expected": {"source_rows_read": int(src), "exploded_lines": int(expl), "explosion_factor_exact": exp_factor},
  "tolerance": {"source_rows_read": 0, "exploded_lines": 0, "explosion_factor_exact": 0.001}
}
base_p.write_text(json.dumps(baseline, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print("OK: baseline")
PY

  # gate
  python3 "$REPO_ROOT/scripts/ingestion/kolobox/tirehub_gate_v1.py" \
    --stats "$OUT/stats.json" \
    --out "$OUT/verdict.json" \
    --baseline "$OUT/baseline.json"

  # ingest
  python3 "$REPO_ROOT/scripts/ingestion/tirehub_ingest_v1.py" \
    --ssot-root "$SSOT" \
    --good "$OUT/good.ndjson" \
    --stats "$OUT/stats.json" \
    --verdict "$OUT/verdict.json"

  echo "OK: ingested sheet=$sheet"
  echo
done < <(printf "%s\n" "$SHEETS")

echo "=== SUMMARY ==="
echo "sheets_attempted=$ok_count"
echo "fail_count=$fail_count"
echo "runs_root=$ROOT"
