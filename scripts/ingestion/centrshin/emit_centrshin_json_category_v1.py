#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, sys, time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import re
from json import JSONDecodeError

def sha256_lf_text(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def extract_mapping_version(mapping_text: str) -> str:
    # best-effort: mapping_version: <x> or version: <x>
    for key in ("mapping_version", "version"):
        m = re.search(rf"^\s*{key}\s*:\s*([^#\n]+)", mapping_text, flags=re.MULTILINE)
        if m:
            v = m.group(1).strip().strip('"').strip("'")
            return v if v else "0"
    return "0"


def load_json_relaxed(path: Path) -> Any:
    """
    Centershin stock.json иногда приходит с trailing commas: ",}" или ",]".
    Пытаемся штатно, при ошибке — убираем trailing commas и пробуем снова.
    """
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except JSONDecodeError:
        fixed = re.sub(r",\s*([}\]])", r"\1", text)
        return json.loads(fixed)



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
    ap.add_argument("--layout", required=True)  # expects category:<key>
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--mapping", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--stats-out", required=True)
    args = ap.parse_args()

    run_id = str(args.run_id).strip()

    mapping_path = Path(args.mapping)
    if not mapping_path.exists():
        print(f"ERROR: mapping not found: {mapping_path}", file=sys.stderr)
        return 5
    try:
        mapping_text = mapping_path.read_text(encoding="utf-8", errors="strict")
    except Exception as e:
        print(f"ERROR: mapping read failed: {mapping_path} :: {e}", file=sys.stderr)
        return 5
    mapping_hash = sha256_lf_text(mapping_text)
    mapping_version = extract_mapping_version(mapping_text)


    in_path = Path(args.file)
    run_id = str(args.run_id).strip()

    in_path = Path(args.file)
    out_nd = Path(args.out)
    out_st = Path(args.stats_out)

    layout = str(args.layout).strip()
    if not layout.startswith("category:"):
        print(f"ERROR: layout must be category:<key>, got {layout}", file=sys.stderr)
        return 2
    category = layout.split(":", 1)[1]

    data = load_json_relaxed(in_path)
    if not isinstance(data, dict):
        print(
            f"ERROR: top-level JSON is not object: {type(data).__name__}",
            file=sys.stderr,
        )
        return 3

    items = data.get(category)
    if not isinstance(items, list):
        print(
            f"ERROR: category={category} is not array: {type(items).__name__}",
            file=sys.stderr,
        )
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
            sid = str(obj.get("id", "") or "").strip()
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
                    "category_key": category,
                },
            }
            w.write(json.dumps(rec, ensure_ascii=False) + "\n")
            emitted += 1

    tmp_nd.replace(out_nd)

    stats = {
        "run_id": run_id,
        "mapping_hash": mapping_hash,
        "mapping_version": mapping_version,
        "supplier_id": "centrshin",
        "parser_id": f"centrshin__{category}__json_v1",
        "layout": f"category:{category}",
        "category_key": category,
        "good_rows": emitted,
        "bad_rows": 0,
        "lines": emitted,
        "seen_items": seen,
        "emitted_items": emitted,
        "skipped_no_id": skipped_no_id,
        "skipped_qty_empty": skipped_qty_empty,
        "bad_price": bad_price,
        "bad_qty": bad_qty,
        "effective_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _atomic_write_text(out_st, json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
