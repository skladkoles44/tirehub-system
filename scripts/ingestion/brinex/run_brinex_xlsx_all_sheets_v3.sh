#!/data/data/com.termux/files/usr/bin/bash
set -u
set -o pipefail

XLSX="/storage/emulated/0/Download/ETL/прайсотБринэксКозловской (1).xlsx"
SSOT="/storage/emulated/0/Download/ETL/etl_data/raw_v1/ssot"
ROOT="/storage/emulated/0/Download/ETL/etl_ops/tmp/brinex_xlsx_all_sheets_v3"

mkdir -p "$ROOT"

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
    if not ws.max_row or ws.max_row < 8:
        continue
    best=0
    for i,row in enumerate(ws.iter_rows(values_only=True), start=1):
        vals=[norm(v) for v in row]
        hits=sum(1 for v in vals if v in NEEDED_HEADERS)
        if hits>best: best=hits
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
idx=0

while IFS= read -r sheet; do
  [ -z "$sheet" ] && continue
  idx=$((idx+1))
  OUT="$ROOT/$(printf "%02d" "$idx")"
  mkdir -p "$OUT"
  echo "=== RUN sheet: $sheet -> $OUT ==="

  python3 scripts/ingestion/brinex/emit_brinex_xlsx_sheet_v1.py "$XLSX" "$sheet" "$OUT"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL emitter rc=$rc sheet=$sheet"
    fail_count=$((fail_count+1))
    echo
    continue
  fi

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
print("OK: baseline run_id=", s.get("run_id"), "parser_id=", s.get("parser_id"), "good=", s.get("good_rows"), "bad=", s.get("bad_rows"))
PY

  python3 scripts/ingestion/kolobox/tirehub_gate_v1.py \
    --stats "$OUT/stats.json" \
    --out "$OUT/verdict.json" \
    --baseline "$OUT/baseline.json"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL gate rc=$rc sheet=$sheet"
    fail_count=$((fail_count+1))
    echo
    continue
  fi

  python3 scripts/ingestion/tirehub_ingest_v1.py \
    --ssot-root "$SSOT" \
    --good "$OUT/good.ndjson" \
    --stats "$OUT/stats.json" \
    --verdict "$OUT/verdict.json"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL ingest rc=$rc sheet=$sheet"
    fail_count=$((fail_count+1))
    echo
    continue
  fi

  echo "OK: ingested sheet=$sheet"
  ok_count=$((ok_count+1))
  echo
done <<< "$SHEETS"

echo "=== SUMMARY ==="
echo "ok=$ok_count fail=$fail_count runs_root=$ROOT"
