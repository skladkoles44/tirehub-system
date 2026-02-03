#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

import xlrd

SHEET_NAME = "TDSheet"
HEADER_ROWS = 2

# KOLOBOX_XLS_MAPPING_V1.yaml -> layouts.komplektatsii_v1.columns (indexes)
C = {
    "supplier_id": "kolobox",
    "parser_id": "kolobox_xls_v1",
    "supplier_article_col": 6,
    "supplier_code_1c_col": 7,
    "brand_col": 8,
    "name_col": 9,
    "price_opt_col": 10,
    "price_retail_col": 11,
    "price_mic_col": 12,
    "warehouse_qty_cols": [14, 15, 16],
    "order_col": 17,
}

def as_num(v):
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace(" ", "").replace(",", ".")
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None

def parse(path: str) -> dict:
    wb = xlrd.open_workbook(path)
    sh = wb.sheet_by_name(SHEET_NAME)

    stats = {
        "rows_total_seen": 0,
        "rows_with_price_opt_gt_0": 0,
        "rows_with_any_qty_gt_0": 0,
    }
    out = []

    for r in range(HEADER_ROWS, sh.nrows):
        stats["rows_total_seen"] += 1

        def cell(ci):
            return sh.cell_value(r, ci)

        price_opt = as_num(cell(C["price_opt_col"]))
        if price_opt is not None and price_opt > 0:
            stats["rows_with_price_opt_gt_0"] += 1

        any_qty = False
        wh = []
        for ci in C["warehouse_qty_cols"]:
            q = as_num(cell(ci))
            if q is not None and q > 0:
                any_qty = True
            wh.append({"col": ci, "qty": q})

        if any_qty:
            stats["rows_with_any_qty_gt_0"] += 1

        rec = {
            "supplier_id": C["supplier_id"],
            "parser_id": C["parser_id"],
            "raw": {
                "supplier_article": cell(C["supplier_article_col"]),
                "supplier_code_1c": cell(C["supplier_code_1c_col"]),
                "brand_raw": cell(C["brand_col"]),
                "name_raw": cell(C["name_col"]),
                "price_opt": price_opt,
                "price_retail": as_num(cell(C["price_retail_col"])),
                "price_mic": as_num(cell(C["price_mic_col"])),
            },
            "warehouses_qty_by_col": wh,
            "order_flag": cell(C["order_col"]),
            "source_row_1based": r + 1,
        }
        out.append(rec)

    return {
        "file": path,
        "sheet": SHEET_NAME,
        "header_rows": HEADER_ROWS,
        "stats": stats,
        "sample_first_3": out[:3],
    }

def main():
    if len(sys.argv) < 2:
        raise SystemExit("Usage: parser_komplektatsii_v1.py <path_to_xls>")
    res = parse(sys.argv[1])
    print(json.dumps(res, ensure_ascii=False, indent=2, default=str))

if __name__ == "__main__":
    main()
