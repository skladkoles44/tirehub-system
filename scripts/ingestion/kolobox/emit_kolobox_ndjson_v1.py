#!/usr/bin/env python3
# Production NDJSON emitter for Kolobox XLS (v1).
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import xlrd


# ----------------------------
# Normalization / helpers
# ----------------------------

def norm_wh(s: str) -> str:
    s = (s or "").strip().lower()
    s = " ".join(s.split())
    return s


def cell(sheet: xlrd.sheet.Sheet, r0: int, c0: int) -> Any:
    try:
        return sheet.cell_value(r0, c0)
    except Exception:
        return None


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1]
    return s


def choose_sku(article: str, code1c: str, brand: str, name: str) -> Tuple[str, List[str]]:
    q: List[str] = []
    article = (article or "").strip()
    code1c = (code1c or "").strip()
    if article:
        return article, q
    if code1c:
        q.append("missing_article_used_code1c")
        return code1c, q
    base = (brand or "") + "|" + (name or "")
    h = hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:12]
    q.append("missing_sku")
    return f"unknown_sku_{h}", q


def safe_int_str(v: Any) -> Tuple[str, List[str]]:
    flags: List[str] = []
    if v is None or v == "":
        return "", flags
    if isinstance(v, bool):
        return "", ["qty_non_numeric"]
    if isinstance(v, int):
        return str(v), flags
    if isinstance(v, float):
        try:
            if v != int(v):
                return "", ["qty_has_fraction"]
            return str(int(v)), flags
        except Exception:
            return "", ["qty_non_numeric"]

    s = str(v).strip()
    if s.startswith(">"):
        flags.append("qty_approximated")
        s = s.lstrip(">").strip()

    s_clean = re.sub(r"[^0-9\.,\-]", "", s).replace(",", ".").strip()
    if s_clean == "":
        return "", ["qty_non_numeric"]

    try:
        n = float(s_clean)
        if n != int(n):
            return "", ["qty_has_fraction"]
        return str(int(n)), flags
    except Exception:
        return "", ["qty_non_numeric"]


def safe_price_str(v: Any) -> Tuple[str, List[str]]:
    flags: List[str] = []
    if v is None or v == "":
        return "", ["missing_price"]
    if isinstance(v, bool):
        return "", ["bad_price"]
    if isinstance(v, int):
        return str(v), flags
    if isinstance(v, float):
        try:
            if abs(v - int(v)) < 1e-9:
                return str(int(v)), flags
            s = f"{v:.6f}".rstrip("0").rstrip(".")
            return s, flags
        except Exception:
            return "", ["bad_price"]

    s = str(v).strip()
    s_clean = re.sub(r"[^0-9\.,\-]", "", s).replace(",", ".").strip()
    if s_clean == "":
        return "", ["bad_price"]

    try:
        n = float(s_clean)
        if abs(n - int(n)) < 1e-9:
            return str(int(n)), flags
        s2 = f"{n:.6f}".rstrip("0").rstrip(".")
        return s2, flags
    except Exception:
        return "", ["bad_price"]


# ----------------------------
# Minimal YAML loader (supports list of dicts)
# ----------------------------

YAMLScalar = Union[str, int, float, bool, None]
YAMLValue = Union[YAMLScalar, Dict[str, Any], List[Any]]


def yaml_load_minimal(text: str) -> Dict[str, Any]:
    lines = [line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    data: Dict[str, Any] = {}
    stack: List[Tuple[int, YAMLValue]] = [(-1, data)]
    key_stack: List[str] = []

    i = 0
    while i < len(lines):
        raw = lines[i]
        indent = len(raw) - len(raw.lstrip())
        line = raw.strip()

        # pop stack to match indent
        while stack and indent <= stack[-1][0]:
            stack.pop()
            if key_stack:
                key_stack.pop()

        cur = stack[-1][1]

        if line.startswith("- "):
            item_txt = line[2:].strip()
            if not isinstance(cur, list):
                # auto-convert last key to list if needed
                if not key_stack or not isinstance(cur, dict):
                    raise ValueError("minimal YAML: list item outside list")
                last_k = key_stack[-1]
                cur[last_k] = []
                stack.append((indent, cur[last_k]))
                cur = cur[last_k]

            if ":" in item_txt:
                k, v = [x.strip() for x in item_txt.split(":", 1)]
                d: Dict[str, Any] = {}
                if v == "":
                    d = {}
                    cur.append(d)
                    stack.append((indent + 2, d))
                    key_stack.append(k)
                else:
                    d[k] = _strip_quotes(v)
                    cur.append(d)
            else:
                cur.append(_strip_quotes(item_txt))
            i += 1
            continue

        if ":" not in line:
            raise ValueError("minimal YAML: expected key:value")
        k, v = [x.strip() for x in line.split(":", 1)]

        if not isinstance(cur, dict):
            raise ValueError("minimal YAML: key:value in non-dict")

        if v == "":
            nxt = {}
            cur[k] = nxt
            stack.append((indent + 2, nxt))
            key_stack.append(k)
        else:
            cur[k] = _strip_quotes(v)
            key_stack.append(k)
        i += 1

    return data


def load_yaml(path: Path) -> Dict[str, Any]:
    txt = path.read_text(encoding="utf-8")
    try:
        import yaml
        return yaml.safe_load(txt)
    except ImportError:
        return yaml_load_minimal(txt)


# ... (остальной код без изменений: cell, choose_sku, safe_int_str, safe_price_str, main)

def main():
    ap = argparse.ArgumentParser(description="Kolobox XLS -> LINE NDJSON emitter (production)")
    ap.add_argument("--file", required=True)
    ap.add_argument("--layout", required=True, choices=["shiny", "diski", "truck", "komplektatsii"])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--mapping", default="mappings/suppliers/kolobox.yaml")
    ap.add_argument("--out", default="-")
    ap.add_argument("--stats-out", default="")
    args = ap.parse_args()

    xls_path = Path(args.file)
    mapping_path = Path(args.mapping)

    if not xls_path.exists():
        raise SystemExit(f"FATAL: file not found: {xls_path}")
    if not mapping_path.exists():
        raise SystemExit(f"FATAL: mapping not found: {mapping_path}")

    mp = load_yaml(mapping_path)


    parser_id_var = str(mp.get('parser_id') or 'kolobox_xls_v1')
    hints = mp.get("format_hints", {}) or {}
    sheet_name = str(hints.get("sheet", "TDSheet"))
    header_r1 = int(hints.get("header_row_1based", 1))
    data_start_r1 = int(hints.get("data_start_row_1based", 3))

    cols = mp.get("columns", {}) or {}
    col_article_primary = int(cols.get("article_primary", {}).get("column", 7))
    col_article_fallback = int(cols.get("article_fallback", {}).get("column", 8))
    col_brand = int(cols.get("brand", {}).get("column", 9))
    col_name = int(cols.get("name", {}).get("column", 10))
    col_price = int(cols.get("price", {}).get("column", 11))

    warehouses = mp.get("warehouses", [])
    if not isinstance(warehouses, list) or not warehouses:
        raise SystemExit("FATAL: no warehouses[] in mapping")

    defaults = mp.get("defaults", {}) or {}
    currency_default = str(defaults.get("currency", "RUB"))

    wb = xlrd.open_workbook(str(xls_path))
    sh = wb.sheet_by_name(sheet_name)

    header_cells = [str(cell(sh, header_r1 - 1, c) or "").strip() for c in range(sh.ncols)]

    out_f = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")
    stats = {
        "supplier_id": "kolobox",
        "parser_id": parser_id_var,
        "parser_version": "1.0",
        "run_id": args.run_id,
        "file": str(xls_path),
        "layout": args.layout,
        "lines": 0,
        "bad_json": 0,
        "bad_qty": 0,
        "skipped_qty_empty": 0,
        "skipped_qty_zero": 0,
        "skipped_qty_invalid": 0,
        "bad_price": 0,
        "missing_sku": 0,
        "flags_counts": {},
    }

    for r0 in range(data_start_r1 - 1, sh.nrows):
        art1 = cell(sh, r0, col_article_primary - 1)
        art2 = cell(sh, r0, col_article_fallback - 1)
        brand = cell(sh, r0, col_brand - 1)
        name = cell(sh, r0, col_name - 1)
        price_v = cell(sh, r0, col_price - 1)

        a1s = str(art1).strip() if art1 is not None else ""
        a2s = str(art2).strip() if art2 is not None else ""
        article = a1s if a1s else a2s
        supplier_code_1c = a2s

        brand_raw = str(brand).strip() if brand is not None else ""
        name_raw = str(name).strip() if name is not None else ""

        sku_candidate_key, sku_flags = choose_sku(article, supplier_code_1c, brand_raw, name_raw)
        if "missing_sku" in sku_flags:
            stats["missing_sku"] += 1

        price_s, price_flags = safe_price_str(price_v)
        if "missing_price" in price_flags or "bad_price" in price_flags or not price_s:
            stats["bad_price"] += 1

        base_qflags = sku_flags + price_flags

        for w in warehouses:
            wcol = int(w["column"])
            wh_map_name = w.get("warehouse_name", f"c{wcol}")
            wh_raw = header_cells[wcol - 1] if 0 <= wcol - 1 < len(header_cells) else wh_map_name
            wh_norm = norm_wh(wh_raw)

            qty_v = cell(sh, r0, wcol - 1)
            qty_s, qty_flags = safe_int_str(qty_v)

            qflags = base_qflags + qty_flags

            # qty gating (SSOT): emit only if qty is a positive integer
            # safe_int_str() returns digits-only string or "".
            if qty_s == "":
                # qty missing OR invalid (non-numeric / fraction / etc.) -> skip emission
                raw_s = "" if qty_v is None else str(qty_v).strip()
                if raw_s == "":
                    stats["skipped_qty_empty"] += 1
                else:
                    stats["skipped_qty_invalid"] += 1
                    stats["bad_qty"] += 1
                continue
            
            try:
                q_int = int(qty_s)
            except Exception:
                stats["skipped_qty_invalid"] += 1
                stats["bad_qty"] += 1
                continue
            
            if q_int == 0:
                stats["skipped_qty_zero"] += 1
                continue
            if q_int < 0:
                stats["skipped_qty_invalid"] += 1
                stats["bad_qty"] += 1
                continue
            if qty_s and (("." in qty_s) or ("," in qty_s)):
                stats["bad_qty"] += 1
                qflags.append("qty_has_decimal_separator")

            for f in qflags:
                stats["flags_counts"][f] = stats["flags_counts"].get(f, 0) + 1

            row = {
                "supplier_id": "kolobox",
                "parser_id": parser_id_var,
                "parser_version": "1.0",
                "run_id": args.run_id,
                "quality_flags": qflags,
                "raw": {
                    "supplier_warehouse_name": wh_norm,
                    "sku_candidate_key": sku_candidate_key,
                    "price": price_s,
                    "qty": qty_s,
                    "currency": currency_default,
                    "supplier_article": article,
                    "supplier_code_1c": supplier_code_1c,
                    "brand_raw": brand_raw,
                    "name_raw": name_raw,
                },
                "_meta": {
                    "source_row_1based": r0 + 1,
                    "passthrough": {
                        "supplier_warehouse_name_raw": wh_raw,
                    },
                },
            }

            try:
                out_f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
                stats["lines"] += 1
            except Exception:
                stats["bad_json"] += 1

    if out_f is not sys.stdout:
        out_f.close()

    sys.stderr.write(json.dumps(stats, ensure_ascii=False, indent=2) + "\n")
    if args.stats_out:
        Path(args.stats_out).write_text(json.dumps(stats, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
