#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, sys, time
from pathlib import Path
from typing import Any, Dict, Optional

def _as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(" ", "").replace(",", ".")
    if s == "" or s.lower() in ("none","null","nan"):
        return None
    try:
        return float(s)
    except Exception:
        return None

def _as_int(x: Any) -> Optional[int]:
    f = _as_float(x)
    if f is None:
        return None
    try:
        return int(f)
    except Exception:
        return None

def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--layout", required=True)   # expects category:<key>
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
    category = layout.split(":",1)[1]

    data = json.loads(in_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        print(f"ERROR: top-level JSON is not object: {type(data).__name__}", file=sys.stderr)
        return 3

    items = data.get(category)
    if not isinstance(items, list):
        print(f"ERROR: category={category} is not array: {type(items).__name__}", file=sys.stderr)
        return 4

    out_nd.parent.mkdir(parents=True, exist_ok=True)
    tmp_nd = out_nd.with_suffix(out_nd.suffix + ".tmp")

    emitted = 0
    seen = len(items)
    skipped_no_id = 0
    skipped_qty_empty = 0
    bad_price = 0
    bad_qty = 0

    with tmp_nd.open("w", encoding="utf-8") as w:
        for obj in items:
            if not isinstance(obj, dict):
                continue
            sid = str(obj.get("id","") or "").strip()
            if not sid:
                skipped_no_id += 1
                continue

            qty = _as_int(obj.get("stock"))
            if qty is None:
                bad_qty += 1
                skipped_qty_empty += 1
                continue
            if qty <= 0:
                skipped_qty_empty += 1
                continue

            price_opt = _as_float(obj.get("price_minimum"))
            if price_opt is None:
                bad_price += 1

            rec: Dict[str, Any] = {
                "supplier_id": "centrshin",
                "parser_id": f"centrshin__{category}__json_v1",
                "layout": f"category:{category}",
                "ts_ingested": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
                "item": {
                    "supplier_item_id": sid,
                    "name": obj.get("name"),
                    "brand": obj.get("marka"),
                    "model": obj.get("model"),
                    "article": obj.get("article"),
                    "qty": qty,
                    "price_opt": price_opt,
                    "price_roznica": _as_float(obj.get("price_roznica")),
                    "image": obj.get("image") or obj.get("img_url"),
                },
                "raw": {
                    "category_key": category
                },
            }
            w.write(json.dumps(rec, ensure_ascii=False) + "\n")
            emitted += 1

    tmp_nd.replace(out_nd)

    stats = {
        "supplier_id": "centrshin",
        "parser_id": f"centrshin__{category}__json_v1",
        "layout": f"category:{category}",
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
