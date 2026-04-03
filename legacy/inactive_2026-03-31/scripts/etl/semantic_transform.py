#!/usr/bin/env python3
"""
semantic_transform.py — production semantic layer

Функции:
- склад → (warehouse, stock)
- нормализация значений
- защита структуры base
- подготовка к normalizer
"""

import json
import re
from pathlib import Path


# ==================== HELPERS ====================

def norm(s):
    if not s:
        return ""
    return str(s).strip().lower()


def clean_warehouse(h):
    h = norm(h)
    h = h.replace("склад", "")
    return h.strip()


def parse_stock(v):
    if v is None:
        return 0

    s = norm(v)

    if not s:
        return 0

    # "больше 20", ">20", "20+"
    if "больше" in s or "+" in s or ">" in s:
        nums = re.findall(r"\d+", s)
        return int(nums[0]) if nums else 0

    # "есть", "в наличии"
    if s in {"есть", "в наличии", "много"}:
        return 999

    try:
        return int(float(s.replace(",", ".")))
    except:
        return 0


# ==================== CORE ====================

def transform(inp: Path, out: Path):
    with inp.open() as f, out.open("w") as w:
        for line in f:
            row = json.loads(line)
            cols = row.get("columns", [])

            base = {}
            offers = []

            for c in cols:
                h = c.get("header")
                v = c.get("value")
                role = c.get("role")

                h_norm = norm(h)

                # === WAREHOUSE LOGIC ===
                if "склад" in h_norm:
                    wh = clean_warehouse(h)
                    stock = parse_stock(v)

                    offers.append({
                            "warehouse": wh,
                            "stock": stock
                        })
                    continue

                # === BASE FIELDS ===
                key = role or "unknown"

                # защита от перезаписи
                if key in base:
                    if isinstance(base[key], list):
                        base[key].append(v)
                    else:
                        base[key] = [base[key], v]
                else:
                    base[key] = v

            # === FINAL STRUCTURE ===
            row["base"] = base
            row["offers"] = offers
            row["meta"] = {
                "offers_count": len(offers)
            }

            w.write(json.dumps(row, ensure_ascii=False) + "\n")


# ==================== ENTRY ====================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("usage: semantic_transform.py <in> <out>")
        exit(1)

    transform(Path(sys.argv[1]), Path(sys.argv[2]))
