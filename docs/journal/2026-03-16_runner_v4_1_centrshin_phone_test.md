# Runner v4.1 phone test: Centrshin XLSX

Date: 2026-03-16

Context
Tested scripts/ingestion/runner_v4_1.py on phone/Termux with file:

drop/tirehub-system/etl_data/raw_v1/inbox/centrshin/stock_2026-03-16.xlsx

Findings
1. Runner successfully parsed supplier XLSX.
2. Total rows emitted: 5594.
3. Roles detected correctly:
   sku
   name
   brand
   model
   year
   stock
   price
   image

Problem discovered
Trailing empty columns in XLSX layout.

Example sheet Камеры before fix:
column 8  Изображение
columns 9..13 empty header and None values.

Root cause
Supplier XLSX physically contains extra right-side columns.

Fix implemented
Right-trim trailing empty columns before emitting atomic_rows:

while atomic_cols and (atomic_cols[-1].get("header") in ("", None)) and atomic_cols[-1].get("value") is None:
    atomic_cols.pop()

Verification
After patch and cache clear:

RUN_OK
ROWS=5594
UNKNOWN_TAIL_GONE

Sheet Камеры now emits 9 columns instead of 14.

Artifacts produced
artifact/stock_2026-03-16/atomic_rows.ndjson
artifact/stock_2026-03-16/column_profiles.ndjson
artifact/stock_2026-03-16/manifest.json

Remaining issues
1. Runner cache masks repeated runs.
2. CLI out_dir argument is currently ignored.
3. Wholesale and retail prices are both classified as price.
4. Some stock values are textual (example: "больше 30").

Conclusion
Centrshin XLSX successfully processed by runner_v4_1.
Trailing empty XLSX columns fixed and verified.
