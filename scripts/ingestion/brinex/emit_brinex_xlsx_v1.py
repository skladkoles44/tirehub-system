from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

from openpyxl import load_workbook


def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(" ", "").replace(",", ".")
    if s == "" or s.lower() in ("none", "null", "nan"):
        return None
    try:
        return float(s)
    except Exception:
        return None


def _as_int_qty(x: Any) -> tuple[Optional[int], Optional[int], Optional[str]]:
    # returns: (qty, qty_lower_bound, qty_raw)
    if x is None:
        return (None, None, None)
    if isinstance(x, (int, float)):
        try:
            return (int(float(x)), None, None)
        except Exception:
            return (None, None, str(x))
    s = str(x).strip()
    if s == "":
        return (None, None, "")
    m = re.match(r"^>\s*(\d+)\s*$", s)
    if m:
        lb = int(m.group(1))
        # conservative: use lower bound as qty, and keep raw+lb in raw fields
        return (lb, lb, s)
    # try plain int
    try:
        return (int(float(s.replace(",", ".").replace(" ", ""))), None, s)
    except Exception:
        return (None, None, s)


def _slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9а-яё\-_]+", "", s, flags=re.IGNORECASE)
    s = s.replace("ё", "е")
    return s[:120] if s else "unknown"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--layout", required=True)  # expects category:<key>
    ap.add_argument("--sheet", required=True)  # original sheet name
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--mapping", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--stats-out", required=True)
    args = ap.parse_args()

    in_path = Path(args.file)
    out_nd = Path(args.out)
    out_st = Path(args.stats_out)

    layout = str(args.layout).strip()
    if not layout.startswith("category:"):
        print(f"ERROR: layout must be category:<key>, got {layout}", file=sys.stderr)
        return 2
    category_key = layout.split(":", 1)[1].strip()
    sheet_name = str(args.sheet)

    # Load workbook (read_only for speed; data_only to get computed values if present)
    wb = load_workbook(in_path, read_only=True, data_only=True)
    if sheet_name not in wb.sheetnames:
        print(f"ERROR: sheet not found: {sheet_name}", file=sys.stderr)
        return 3
    ws = wb[sheet_name]

    # Find header row: first cell == "Код товара (goods_id)"
    header_row = None
    for r in range(1, min(80, ws.max_row or 1) + 1):
        v = ws.cell(row=r, column=1).value
        if isinstance(v, str) and v.strip() == "Код товара (goods_id)":
            header_row = r
            break
    if header_row is None:
        print("ERROR: header row not found (A=='Код товара (goods_id)')", file=sys.stderr)
        return 4

    # Build col map from header row
    col_map: Dict[str, int] = {}
    for c in range(1, (ws.max_column or 1) + 1):
        hv = ws.cell(row=header_row, column=c).value
        if isinstance(hv, str):
            key = hv.strip()
            if key:
                col_map[key] = c

    def col(name: str) -> Optional[int]:
        return col_map.get(name)

    need = [
        "Код товара (goods_id)",
        "Номенклатура",
        "Артикул",
        "Цена",
        "Склад",
        "Остаток на складе",
    ]
    missing = [n for n in need if col(n) is None]
    if missing:
        print(f"ERROR: missing required columns: {missing}", file=sys.stderr)
        return 5

    c_goods = col("Код товара (goods_id)") or 1
    c_name = col("Номенклатура") or 2
    c_product = col("Код товара (product_id)")
    c_article = col("Артикул") or 4
    c_kind = col("Вид товара")
    c_price = col("Цена") or 9
    c_price_r = col("Розница")
    c_qty_total = col("Остаток общий")
    c_wh = col("Склад") or 8
    c_qty_wh = col("Остаток на складе") or 10

    emitted = 0
    seen = 0
    skipped_no_id = 0
    skipped_qty_empty = 0
    bad_price = 0
    bad_qty = 0

    out_nd.parent.mkdir(parents=True, exist_ok=True)
    tmp_nd = out_nd.with_suffix(out_nd.suffix + ".tmp")

    # Data rows start after header_row + 2 (usually numeric row exists)
    start_r = header_row + 2

    with tmp_nd.open("w", encoding="utf-8") as w:
        for r in range(start_r, (ws.max_row or start_r) + 1):
            goods_id = ws.cell(row=r, column=c_goods).value
            if goods_id is None:
                continue
            seen += 1
            sid = str(goods_id).strip()
            if sid == "":
                skipped_no_id += 1
                continue

            name = ws.cell(row=r, column=c_name).value
            article = ws.cell(row=r, column=c_article).value
            product_id = ws.cell(row=r, column=c_product).value if c_product else None
            kind = ws.cell(row=r, column=c_kind).value if c_kind else None

            wh = ws.cell(row=r, column=c_wh).value
            wh_name = str(wh).strip() if wh is not None else ""
            if wh_name == "":
                wh_name = "UNKNOWN"

            qty_val = ws.cell(row=r, column=c_qty_wh).value
            qty, qty_lb, qty_raw = _as_int_qty(qty_val)
            if qty is None or qty <= 0:
                if qty_raw not in (None, "", "0"):
                    bad_qty += 1
                else:
                    skipped_qty_empty += 1
                continue

            price_val = ws.cell(row=r, column=c_price).value
            price = _as_float(price_val)
            if price is None or price <= 0:
                bad_price += 1
                continue

            rec: Dict[str, Any] = {
                "supplier_id": "brinex",
                "parser_id": f"brinex__{category_key}__xlsx_v1",
                "layout": f"category:{category_key}",
                "ts_ingested": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
                "item": {
                    "supplier_item_id": sid,
                    "name": str(name).strip() if name is not None else None,
                    "article": str(article).strip() if article is not None else None,
                    "qty": qty,
                    "price_opt": price,
                    "currency": "RUB",
                },
                "raw": {
                    "category_key": category_key,
                    "sheet_name": sheet_name,
                    "warehouse_name": wh_name,
                    "goods_id": goods_id,
                    "product_id": product_id,
                    "kind": kind,
                    "qty_raw": qty_raw,
                    "qty_lower_bound": qty_lb,
                    "price_roznica": _as_float(ws.cell(row=r, column=c_price_r).value) if c_price_r else None,
                    "qty_total": ws.cell(row=r, column=c_qty_total).value if c_qty_total else None,
                },
            }
            w.write(json.dumps(rec, ensure_ascii=False) + "\n")
            emitted += 1

    tmp_nd.replace(out_nd)

    stats = {
        "supplier_id": "brinex",
        "parser_id": f"brinex__{category_key}__xlsx_v1",
        "layout": f"category:{category_key}",
        "category_key": category_key,
        "sheet_name": sheet_name,
        "lines": emitted,
        "seen_items": seen,
        "emitted_items": emitted,
        "skipped_no_id": skipped_no_id,
        "skipped_qty_empty": skipped_qty_empty,
        "bad_price": bad_price,
        "bad_qty": bad_qty,
    }
    _atomic_write_text(out_st, json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
