#!/usr/bin/env python3
import argparse
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "1.7"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def norm_str(x):
    if x is None:
        return None
    x = str(x).strip()
    return x or None


def norm_price(x):
    if x is None:
        return None
    s = str(x).strip().replace(" ", "").replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def norm_int(x):
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(str(x).strip().replace(",", ".")))
        except Exception:
            return None


def write_ndjson_line(fh, obj):
    fh.write(json.dumps(obj, ensure_ascii=False) + "\n")


def make_reject(reason, line_no=None, sku=None, payload=None, reject_mode="minimal"):
    rr = {"reason": reason}
    if line_no is not None:
        rr["original_line_no"] = line_no
    if sku:
        rr["sku"] = sku
    if reject_mode == "full" and payload is not None:
        rr["payload"] = payload
    return rr


def iter_input_rows(input_path: Path, reject_fh, reject_mode: str, reject_stats: Counter):
    line_no = 0
    with input_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line_no += 1
            line = raw_line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                reject_stats["json_parse_error"] += 1
                rr = make_reject(
                    reason="json_parse_error",
                    line_no=line_no,
                    payload={"raw": line},
                    reject_mode=reject_mode,
                )
                write_ndjson_line(reject_fh, rr)
                continue

            if not isinstance(row, dict):
                reject_stats["row_not_object"] += 1
                rr = make_reject(
                    reason="row_not_object",
                    line_no=line_no,
                    payload=row,
                    reject_mode=reject_mode,
                )
                write_ndjson_line(reject_fh, rr)
                continue

            row["_line_no"] = line_no
            yield row


def extract_first(columns, roles):
    for role in roles:
        for c in columns:
            if c.get("role") == role:
                v = c.get("value")
                if v not in (None, ""):
                    return v
    return None


def extract_all(columns, role):
    out = []
    for c in columns:
        if c.get("role") == role:
            v = c.get("value")
            if v not in (None, ""):
                out.append(v)
    return out


def extract_price(columns):
    vals = []
    for v in extract_all(columns, "price"):
        pv = norm_price(v)
        if pv is not None:
            vals.append(pv)
    positive = [v for v in vals if v > 0]
    if positive:
        return min(positive)
    return vals[0] if vals else None


def build_raw_fields(columns):
    raw = {}
    for c in columns:
        header = c.get("header")
        value = c.get("value")
        if header in (None, ""):
            continue
        key = str(header).strip()
        if not key:
            continue
        if key not in raw:
            raw[key] = value
        else:
            prev = raw[key]
            if isinstance(prev, list):
                prev.append(value)
            else:
                raw[key] = [prev, value]
    return raw


def norm_brand(v):
    s = str(v or "").lower().strip()
    mapping = {
        "good year": "goodyear",
        "good-year": "goodyear",
        "goodyear": "goodyear",
        "пирелли": "pirelli",
        "бриджстоун": "bridgestone",
        "роудстоун": "roadstone",
        "нексен": "nexen",
        "йокохама": "yokohama",
        "континенталь": "continental",
        "мишелин": "michelin",
        "нокиан": "nokian",
        "айкон": "ikon",
        "белшина": "belshina",
    }
    return mapping.get(s, s) if s else None


def norm_season(v):
    s = str(v or "").lower().strip()
    if "лет" in s:
        return "summer"
    if "зим" in s:
        return "winter"
    if "всесез" in s:
        return "allseason"
    return s or None


def norm_stud(v):
    s = str(v or "").lower().strip()
    return ("шип" in s) or (s in {"ш.", "да", "1", "true"})


def parse_size_raw(size_raw: str) -> dict:
    if not size_raw:
        return {"width": None, "height": None, "diameter": None, "status": "missing"}

    s = str(size_raw).upper().strip().replace(",", ".")

    m = re.search(r"(\d{3})/(\d{2,3})R(\d{2})", s)
    if m:
        w, h, d = m.groups()
        return {
            "width": norm_int(w),
            "height": norm_int(h),
            "diameter": norm_int(d),
            "status": "parsed",
        }

    m = re.search(r"(\d+(?:\.\d+)?)/(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)", s)
    if m:
        w, h, d = m.groups()
        return {
            "width": norm_int(float(w) * 10),
            "height": norm_int(h),
            "diameter": norm_int(d),
            "status": "parsed",
        }

    m = re.search(r"(\d+(?:\.\d+)?)[/-](\d+(?:\.\d+)?)", s)
    if m:
        w, d = m.groups()
        return {
            "width": norm_int(float(w) * 10),
            "height": None,
            "diameter": norm_int(d),
            "status": "partial",
        }

    return {"width": None, "height": None, "diameter": None, "status": "raw_only"}


def detect_identity_basis(source_sku, raw_name):
    if source_sku:
        return "sku"
    if raw_name:
        return "raw_name"
    return "none"


def canonical(row: dict) -> dict:
    cols = row.get("columns") or []

    size_raw = norm_str(extract_first(cols, ["size", "size_raw"]))
    size_parsed = parse_size_raw(size_raw)

    source_sku = norm_str(extract_first(cols, ["sku", "article", "supplier_sku"]))
    raw_name = norm_str(extract_first(cols, ["name", "model"]))

    return {
        "source_sku": source_sku,
        "brand": norm_brand(extract_first(cols, ["brand"])),
        "model": norm_str(extract_first(cols, ["model"])),
        "raw_name": raw_name,
        "size_raw": size_raw,
        "width": size_parsed["width"],
        "height": size_parsed["height"],
        "diameter": size_parsed["diameter"],
        "size_parse_status": size_parsed["status"],
        "load": norm_str(extract_first(cols, ["load"])),
        "speed": norm_str(extract_first(cols, ["speed"])),
        "season": norm_season(extract_first(cols, ["season"])),
        "studded": norm_stud(extract_first(cols, ["studded"])),
        "price": extract_price(cols),
        "warehouse": norm_str(extract_first(cols, ["warehouse"])),
        "qty": norm_int(extract_first(cols, ["stock", "qty"])),
        "oem": norm_str(extract_first(cols, ["oem"])),
        "category": norm_str(extract_first(cols, ["category"])),
        "identity_basis": detect_identity_basis(source_sku, raw_name),
        "raw_columns_sample": cols[:20],
        "raw_fields": build_raw_fields(cols),
        "lineage": {
            "source_file": row.get("source_file"),
            "sheet": row.get("sheet_name"),
            "table_index": row.get("table_index"),
            "row_index": row.get("row_index"),
            "fingerprint": (row.get("layout") or {}).get("fingerprint"),
            "original_line_no": row.get("_line_no"),
        },
    }


def reject_reason(r: dict) -> str:
    if not r.get("source_sku") and not r.get("raw_name"):
        return "missing_identity"
    return ""


def group_key(r: dict) -> str:
    return r.get("source_sku") or r.get("raw_name") or "unknown"


def collapse_group(rows_group):
    base = rows_group[0]

    warehouses = []
    seen_wh = set()
    for r in rows_group:
        item = {
            "warehouse": r.get("warehouse"),
            "stock": r.get("qty"),
        }
        key = (item["warehouse"], item["stock"])
        if key not in seen_wh and (item["warehouse"] is not None or item["stock"] is not None):
            seen_wh.add(key)
            warehouses.append(item)

    prices = [r.get("price") for r in rows_group if r.get("price") is not None]
    source_skus = sorted({r.get("source_sku") for r in rows_group if r.get("source_sku")})
    parse_statuses = Counter(r.get("size_parse_status") for r in rows_group)

    return {
        "source_sku": base.get("source_sku"),
        "name": base.get("raw_name"),
        "brand": base.get("brand"),
        "model": base.get("model"),
        "size_raw": base.get("size_raw"),
        "width": base.get("width"),
        "height": base.get("height"),
        "diameter": base.get("diameter"),
        "size_parse_status": base.get("size_parse_status"),
        "size_parse_status_counts": dict(parse_statuses),
        "load": base.get("load"),
        "speed": base.get("speed"),
        "season": base.get("season"),
        "studded": base.get("studded"),
        "oem": base.get("oem"),
        "category": base.get("category"),
        "identity_basis": base.get("identity_basis"),
        "price": min(prices) if prices else None,
        "warehouses": warehouses,
        "source_skus_count": len(source_skus),
        "source_skus": source_skus,
        "rows_in_group": len(rows_group),
        "raw_columns_sample": base.get("raw_columns_sample"),
        "raw_fields": base.get("raw_fields"),
        "lineage_sample": base.get("lineage"),
    }


def run(input_path: Path, out_dir: Path, reject_mode: str = "minimal"):
    started_at = time.time()
    started_at_utc = now_utc()

    out_dir.mkdir(parents=True, exist_ok=True)

    good_path = out_dir / "good.ndjson"
    reject_path = out_dir / "reject.ndjson"
    manifest_path = out_dir / "normalizer_manifest.json"

    stats = Counter()
    reject_stats = Counter()
    groups = defaultdict(list)

    with reject_path.open("w", encoding="utf-8") as reject_fh:
        for row in iter_input_rows(input_path, reject_fh, reject_mode, reject_stats):
            r = canonical(row)
            reason = reject_reason(r)

            if reason:
                rr = make_reject(
                    reason=reason,
                    line_no=row.get("_line_no"),
                    sku=r.get("source_sku"),
                    payload=r,
                    reject_mode=reject_mode,
                )
                write_ndjson_line(reject_fh, rr)
                reject_stats[reason] += 1
                continue

            groups[group_key(r)].append(r)

    with good_path.open("w", encoding="utf-8") as good_fh:
        for key in sorted(groups.keys()):
            rows_group = groups[key]
            obj = collapse_group(rows_group)
            write_ndjson_line(good_fh, obj)
            stats["groups"] += 1
            stats["rows"] += len(rows_group)
            stats["good"] += 1

    manifest = {
        "version": VERSION,
        "input": {
            "path": str(input_path),
            "size_bytes": input_path.stat().st_size,
        },
        "output": {
            "good": str(good_path),
            "reject": str(reject_path),
            "manifest": str(manifest_path),
        },
        "runtime": {
            "start_ts": started_at_utc,
            "end_ts": now_utc(),
            "duration_sec": round(time.time() - started_at, 3),
        },
        "stats": {
            **dict(stats),
            "json_parse_errors": reject_stats.get("json_parse_error", 0),
        },
        "reject_stats": dict(reject_stats),
        "reject_mode": reject_mode,
    }

    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return 0


def main():
    ap = argparse.ArgumentParser(description=f"L1 normalizer v{VERSION}")
    ap.add_argument("--input", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--reject-mode", default="minimal", choices=["minimal", "full"])
    args = ap.parse_args()

    return run(args.input, args.out_dir, args.reject_mode)


if __name__ == "__main__":
    sys.exit(main())
