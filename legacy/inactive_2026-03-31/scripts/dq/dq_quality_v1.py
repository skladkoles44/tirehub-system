#!/usr/bin/env python3
import json
from pathlib import Path
from collections import Counter

BAD_NAME_VALUES = {"-", "--", "---", "n/a", "na", "none", "null", "."}

CORE_CATEGORIES = {
    "tires",
    "wheels",
    "battery",
    "oil",
}

SOFT_CATEGORIES = {
    "accessories",
}

def iter_ndjson(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            yield line_no, json.loads(line)

def norm_text(v):
    return "" if v is None else str(v).strip()

def bad_name(name: str) -> bool:
    n = norm_text(name).lower()
    if not n:
        return True
    if n in BAD_NAME_VALUES:
        return True
    if len(n) < 3:
        return True
    return False

def category_bucket(category: str) -> str:
    c = norm_text(category).lower()
    if not c:
        return "unknown"
    if c in SOFT_CATEGORIES:
        return "soft_zero_price"
    if c in CORE_CATEGORIES:
        return "core_goods"
    return "other"

def dq_quality(artifact_dir: Path):
    good_path = artifact_dir / "good.ndjson"
    out_path = artifact_dir / "quality_verdict.json"
    if not good_path.exists():
        raise SystemExit(f"GOOD_NOT_FOUND: {good_path}")

    stats = Counter()
    errors = Counter()
    sku_counter = Counter()
    category_counts = Counter()

    sample_zero_price_core = []
    sample_zero_price_soft = []
    sample_bad_name = []
    sample_bad_stock = []
    sample_dup_sku = []

    for line_no, rec in iter_ndjson(good_path):
        stats["rows"] += 1

        sku = norm_text(rec.get("source_sku"))
        name = norm_text(rec.get("name"))
        price = rec.get("price")
        warehouses = rec.get("warehouses") or []
        category = norm_text(rec.get("category"))
        bucket = category_bucket(category)
        category_counts[bucket] += 1

        if sku:
            sku_counter[sku] += 1
            if sku_counter[sku] == 2 and len(sample_dup_sku) < 20:
                sample_dup_sku.append({
                    "line_no": line_no,
                    "source_sku": sku,
                    "name": name,
                })

        if price is None or not isinstance(price, (int, float)):
            errors["missing_or_bad_price"] += 1
            if len(sample_zero_price_core) < 20:
                sample_zero_price_core.append({
                    "line_no": line_no,
                    "source_sku": sku,
                    "name": name,
                    "category": category,
                    "price": price,
                    "reason": "missing_or_bad_price",
                })
        elif price <= 0:
            if bucket == "soft_zero_price":
                errors["zero_price_soft"] += 1
                if len(sample_zero_price_soft) < 20:
                    sample_zero_price_soft.append({
                        "line_no": line_no,
                        "source_sku": sku,
                        "name": name,
                        "category": category,
                        "price": price,
                    })
            else:
                errors["zero_price_core"] += 1
                if len(sample_zero_price_core) < 20:
                    sample_zero_price_core.append({
                        "line_no": line_no,
                        "source_sku": sku,
                        "name": name,
                        "category": category,
                        "price": price,
                    })

        if bad_name(name):
            errors["bad_name"] += 1
            if len(sample_bad_name) < 20:
                sample_bad_name.append({
                    "line_no": line_no,
                    "source_sku": sku,
                    "name": name,
                    "category": category,
                })

        for w in warehouses:
            stock = w.get("stock")
            if stock is not None and (not isinstance(stock, (int, float)) or stock < 0):
                errors["bad_stock"] += 1
                if len(sample_bad_stock) < 20:
                    sample_bad_stock.append({
                        "line_no": line_no,
                        "source_sku": sku,
                        "name": name,
                        "category": category,
                        "warehouse": w,
                    })
                break

    dup_rows = sum(v - 1 for v in sku_counter.values() if v > 1)
    if dup_rows:
        errors["duplicate_source_sku_rows"] = dup_rows

    blocks = []
    warnings = []

    if errors["bad_name"] > 0:
        blocks.append("bad_name_present")
    if errors["bad_stock"] > 0:
        blocks.append("negative_or_bad_stock_present")
    if errors["zero_price_core"] > 0:
        blocks.append("zero_price_core_goods_present")
    if errors["missing_or_bad_price"] > 0:
        blocks.append("missing_or_bad_price_present")

    if errors["zero_price_soft"] > 0:
        warnings.append("zero_price_soft_goods_present")
    if dup_rows > 0:
        warnings.append("duplicate_source_sku_rows_present")

    verdict = "block" if blocks else ("warn" if warnings else "pass")

    out = {
        "rows": stats["rows"],
        "verdict": verdict,
        "errors": dict(errors),
        "category_buckets": dict(category_counts),
        "blocks": blocks,
        "warnings": warnings,
        "samples": {
            "zero_price_core": sample_zero_price_core,
            "zero_price_soft": sample_zero_price_soft,
            "bad_name": sample_bad_name,
            "bad_stock": sample_bad_stock,
            "duplicate_source_sku": sample_dup_sku,
        },
    }

    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifact-dir", required=True)
    args = ap.parse_args()
    dq_quality(Path(args.artifact_dir))
