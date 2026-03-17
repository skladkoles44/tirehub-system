#!/usr/bin/env python3
import os
import re
import json
import time
import hashlib
from pathlib import Path
from collections import Counter

PRICE_HEADER_HINTS_WHOLESALE = {
    "цена оптовая",
    "оптовая цена",
    "оптовая",
    "опт",
    "price wholesale",
    "wholesale price",
}

PRICE_HEADER_HINTS_RETAIL = {
    "цена розничная",
    "розничная цена",
    "розничная",
    "розница",
    "price retail",
    "retail price",
}

TEXT_STOCK_PATTERNS = [
    (re.compile(r"^\s*больше\s+(\d+)\s*$", re.I), "gt"),
    (re.compile(r"^\s*>=\s*(\d+)\s*$", re.I), "gte"),
    (re.compile(r"^\s*(\d+)\+\s*$", re.I), "gte"),
]


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def safe_slug(s: str) -> str:
    s = re.sub(r"[^\w.\-]+", "_", s.strip(), flags=re.U)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown_file"


def to_text(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def to_number(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v.is_integer() else v

    s = str(v).strip()
    if not s:
        return None

    s = s.replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        x = float(s)
        return int(x) if x.is_integer() else x
    except Exception:
        return None


def normalize_stock(v):
    if v is None:
        return {"stock_qty": None, "stock_raw": None, "stock_kind": None}

    n = to_number(v)
    if n is not None:
        return {"stock_qty": int(n), "stock_raw": str(v), "stock_kind": "exact"}

    s = str(v).strip()
    for rx, kind in TEXT_STOCK_PATTERNS:
        m = rx.match(s)
        if m:
            return {"stock_qty": int(m.group(1)), "stock_raw": s, "stock_kind": kind}

    return {"stock_qty": None, "stock_raw": s, "stock_kind": "text"}


def classify_price_kind(header, name, ordinal_price_idx: int) -> str:
    h = (header or "").strip().lower()
    n = (name or "").strip().lower()

    if h in PRICE_HEADER_HINTS_WHOLESALE:
        return "wholesale"
    if h in PRICE_HEADER_HINTS_RETAIL:
        return "retail"

    if n in PRICE_HEADER_HINTS_WHOLESALE:
        return "wholesale"
    if n in PRICE_HEADER_HINTS_RETAIL:
        return "retail"

    if "wholesale" in h or "опт" in h:
        return "wholesale"
    if "retail" in h or "розн" in h or "розниц" in h:
        return "retail"

    if ordinal_price_idx == 0:
        return "primary"
    if ordinal_price_idx == 1:
        return "secondary"
    return f"price_{ordinal_price_idx + 1}"


def index_columns(columns):
    first_by_role = {}
    all_by_role = {}
    prices = {}
    price_ordinal = 0

    for c in columns:
        role = c.get("role") or "unknown"
        value = c.get("value")
        header = c.get("header")
        name = c.get("name")

        all_by_role.setdefault(role, []).append(value)

        if role not in first_by_role:
            first_by_role[role] = value

        if role == "price":
            num = to_number(value)
            if num is not None:
                kind = classify_price_kind(header, name, price_ordinal)
                prices[kind] = num
                price_ordinal += 1

    return first_by_role, all_by_role, prices


def cleaned_text_list(values):
    if not values:
        return []
    out = []
    for v in values:
        t = to_text(v)
        if t is not None:
            out.append(t)
    return out


def build_candidate(first, roles, prices, stock_norm, rec):
    return {
        "source_file": rec.get("source_file"),
        "sheet_name": rec.get("sheet_name"),
        "table_index": rec.get("table_index"),
        "row_index": rec.get("row_index"),
        "fingerprint": ((rec.get("layout") or {}).get("fingerprint")),
        "sku": to_text(first.get("sku")),
        "name": to_text(first.get("name")),
        "brand": to_text(first.get("brand")),
        "model": to_text(first.get("model")),
        "year": to_text(first.get("year")),
        "image": to_text(first.get("image")),
        "size": cleaned_text_list(roles.get("size", [])),
        "season": to_text(first.get("season")),
        "load_index": to_text(first.get("load_index")),
        "speed_index": to_text(first.get("speed_index")),
        "ply_rating": to_text(first.get("ply_rating")),
        "application": to_text(first.get("application")),
        "bolt_count": to_text(first.get("bolt_count")),
        "bolt_pattern": to_text(first.get("bolt_pattern")),
        "center_bore": to_text(first.get("center_bore")),
        "offset": to_text(first.get("offset")),
        "color": to_text(first.get("color")),
        "wheel_type": to_text(first.get("wheel_type")),
        "capacity": to_text(first.get("capacity")),
        "voltage": to_text(first.get("voltage")),
        "current": to_text(first.get("current")),
        "polarity": to_text(first.get("polarity")),
        "configuration": to_text(first.get("configuration")),
        "prices": prices,
        "stock_qty": stock_norm["stock_qty"],
        "stock_raw": stock_norm["stock_raw"],
        "stock_kind": stock_norm["stock_kind"],
        "raw_roles": {k: len(v) for k, v in roles.items()},
    }


def build_good_record(candidate):
    return {
        "supplier_sku": candidate["sku"],
        "is_fully_keyed": bool(candidate["sku"]),
        "identity_basis": candidate["identity_basis"],
        "sku": candidate["sku"],
        "name": candidate["name"],
        "brand": candidate.get("brand"),
        "model": candidate.get("model"),
        "year": candidate.get("year"),
        "image": candidate.get("image"),
        "size": candidate.get("size") or [],
        "season": candidate.get("season"),
        "load_index": candidate.get("load_index"),
        "speed_index": candidate.get("speed_index"),
        "ply_rating": candidate.get("ply_rating"),
        "application": candidate.get("application"),
        "bolt_count": candidate.get("bolt_count"),
        "bolt_pattern": candidate.get("bolt_pattern"),
        "center_bore": candidate.get("center_bore"),
        "offset": candidate.get("offset"),
        "color": candidate.get("color"),
        "wheel_type": candidate.get("wheel_type"),
        "capacity": candidate.get("capacity"),
        "voltage": candidate.get("voltage"),
        "current": candidate.get("current"),
        "polarity": candidate.get("polarity"),
        "configuration": candidate.get("configuration"),
        "prices": candidate.get("prices", {}),
        "stock": {
            "qty": candidate.get("stock_qty"),
            "raw": candidate.get("stock_raw"),
            "kind": candidate.get("stock_kind"),
        },
        "lineage": {
            "source_file": candidate.get("source_file"),
            "sheet_name": candidate.get("sheet_name"),
            "table_index": candidate.get("table_index"),
            "row_index": candidate.get("row_index"),
            "fingerprint": candidate.get("fingerprint"),
        },
    }


def process_row(rec, reject_mode="full"):
    cols = rec.get("columns", [])
    first, roles, prices = index_columns(cols)
    stock_norm = normalize_stock(first.get("stock"))
    candidate = build_candidate(first, roles, prices, stock_norm, rec)

    if not candidate["name"]:
        return "reject", candidate, "missing_name", rec if reject_mode == "full" else None

    if candidate["sku"]:
        candidate["identity_basis"] = "sku"
    elif (
        candidate.get("brand")
        or candidate.get("model")
        or candidate.get("size")
        or candidate.get("bolt_pattern")
        or candidate.get("capacity")
        or candidate.get("image")
    ):
        candidate["identity_basis"] = "derived_no_sku"
    else:
        return "reject", candidate, "missing_identity", rec if reject_mode == "full" else None

    good = build_good_record(candidate)
    return "good", good, None, None


def normalize_atomic_file(atomic_path: Path, out_dir: Path, reject_mode: str = "full"):
    start_ts = time.time()
    start_ts_human = now_utc()

    input_manifest_path = atomic_path.parent / "manifest.json"
    runner_version = "unknown"
    if input_manifest_path.exists():
        try:
            input_manifest = json.loads(input_manifest_path.read_text(encoding="utf-8"))
            runner_version = input_manifest.get("runner", {}).get("version", "unknown")
        except Exception:
            runner_version = "unknown"

    out_dir.mkdir(parents=True, exist_ok=True)
    good_path = out_dir / "good.ndjson"
    reject_path = out_dir / "reject.ndjson"
    manifest_path = out_dir / "normalizer_manifest.json"

    if good_path.exists():
        good_path.unlink()
    if reject_path.exists():
        reject_path.unlink()

    stats = Counter()
    reject_reasons = Counter()
    sheet_counts = Counter()
    fingerprint_counts = Counter()
    identity_basis_counts = Counter()
    missing_supplier_sku_good = 0

    with atomic_path.open("r", encoding="utf-8") as src, \
         good_path.open("a", encoding="utf-8") as good_f, \
         reject_path.open("a", encoding="utf-8") as reject_f:

        for line in src:
            stats["rows_in"] += 1
            rec = json.loads(line)

            sheet_name = rec.get("sheet_name") or "unknown"
            fingerprint = ((rec.get("layout") or {}).get("fingerprint")) or "none"
            sheet_counts[sheet_name] += 1
            fingerprint_counts[fingerprint] += 1

            outcome, obj, reason, original = process_row(rec, reject_mode=reject_mode)

            if outcome == "good":
                good_f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                stats["rows_good"] += 1
                identity_basis_counts[obj.get("identity_basis") or "unknown"] += 1
                if obj.get("supplier_sku") in (None, ""):
                    missing_supplier_sku_good += 1
            else:
                reject_reasons[reason] += 1
                reject = {
                    "reject_timestamp": now_utc(),
                    "normalizer_version": "2.2",
                    "reason": reason,
                    "candidate": obj,
                }
                if original is not None:
                    reject["original_row"] = original
                reject_f.write(json.dumps(reject, ensure_ascii=False) + "\n")
                stats["rows_reject"] += 1

    end_ts_human = now_utc()

    manifest = {
        "normalizer": {
            "version": "2.2",
            "reject_mode": reject_mode,
        },
        "timestamp": now_utc(),
        "input": {
            "atomic_rows": str(atomic_path),
            "atomic_hash": sha256_file(atomic_path),
            "size_bytes": atomic_path.stat().st_size,
            "runner_version": runner_version,
        },
        "output": {
            "good": str(good_path),
            "reject": str(reject_path),
        },
        "stats": {
            **dict(stats),
            "good_missing_supplier_sku_count": missing_supplier_sku_good,
        },
        "processing": {
            "start_ts": start_ts_human,
            "end_ts": end_ts_human,
            "duration_sec": round(time.time() - start_ts, 3),
            "sheets": dict(sheet_counts),
            "fingerprints": dict(fingerprint_counts),
            "reject_reasons": dict(reject_reasons),
            "identity_basis": dict(identity_basis_counts),
        },
    }

    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--atomic", required=True, help="path to atomic_rows.ndjson")
    ap.add_argument("--out-dir", default="", help="output dir; default ETL_VAR_ROOT/normalized/<stem>")
    ap.add_argument("--reject-mode", default="full", choices=["full", "minimal"], help="full: include original_row; minimal: only candidate + reason")
    ap.add_argument("--version", action="version", version="NormalizerV2 2.2")
    args = ap.parse_args()

    atomic_path = Path(args.atomic).resolve()
    if not atomic_path.exists():
        raise SystemExit(f"NOT_FOUND: {atomic_path}")

    if args.out_dir:
        out_dir = Path(args.out_dir).resolve()
    else:
        var_root = Path(os.environ["ETL_VAR_ROOT"])
        stem = safe_slug(atomic_path.parent.name or atomic_path.stem)
        out_dir = var_root / "normalized" / stem

    manifest = normalize_atomic_file(
        atomic_path=atomic_path,
        out_dir=out_dir,
        reject_mode=args.reject_mode,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
