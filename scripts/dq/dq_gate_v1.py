#!/usr/bin/env python3
import json
import hashlib
import time
from pathlib import Path
from collections import Counter

ALLOWED_IDENTITY_BASIS = {"sku", "derived_no_sku"}
ALLOWED_STOCK_KINDS = {"exact", "gt", "gte", "text", None}


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def safe_div(a: int, b: int) -> float:
    return round((a / b), 6) if b else 0.0


def norm_text(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def iter_ndjson(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            yield line_no, json.loads(line)


def validate_good_record(rec: dict):
    errors = []

    name = norm_text(rec.get("name"))
    supplier_sku = norm_text(rec.get("supplier_sku"))
    is_fully_keyed = rec.get("is_fully_keyed")
    identity_basis = rec.get("identity_basis")
    prices = rec.get("prices")
    stock = rec.get("stock")

    if not name:
        errors.append("missing_name")

    if identity_basis not in ALLOWED_IDENTITY_BASIS:
        errors.append("bad_identity_basis")

    if bool(supplier_sku) != bool(is_fully_keyed):
        errors.append("keyedness_mismatch")

    if not isinstance(prices, dict):
        errors.append("prices_not_dict")
    else:
        for k, v in prices.items():
            if not isinstance(k, str):
                errors.append("price_key_not_str")
                break
            if not isinstance(v, (int, float)):
                errors.append("price_value_not_number")
                break

    if not isinstance(stock, dict):
        errors.append("stock_not_dict")
    else:
        stock_kind = stock.get("kind")
        if stock_kind not in ALLOWED_STOCK_KINDS:
            errors.append("bad_stock_kind")

    return errors


def dq_gate(artifact_dir: Path):
    start_ts = time.time()
    start_ts_human = now_utc()

    good_path = artifact_dir / "good.ndjson"
    reject_path = artifact_dir / "reject.ndjson"
    normalizer_manifest_path = artifact_dir / "normalizer_manifest.json"
    verdict_path = artifact_dir / "verdict.json"
    stats_path = artifact_dir / "stats.json"

    if not good_path.exists():
        raise SystemExit(f"GOOD_NOT_FOUND: {good_path}")
    if not normalizer_manifest_path.exists():
        fallback_manifest_path = artifact_dir / "manifest.json"
        if fallback_manifest_path.exists():
            normalizer_manifest_path = fallback_manifest_path
        else:
            raise SystemExit(f"NORMALIZER_MANIFEST_NOT_FOUND: {normalizer_manifest_path}")

    normalizer_manifest = json.loads(normalizer_manifest_path.read_text(encoding="utf-8"))
    normalizer_stats = normalizer_manifest.get("stats") or {}
    normalizer_processing = normalizer_manifest.get("processing") or {}
    normalizer_meta = normalizer_manifest.get("normalizer") or {}

    rows_in_manifest = normalizer_stats.get("rows_in") or 0
    rows_good_manifest = normalizer_stats.get("rows_good") or 0
    rows_reject_manifest = normalizer_stats.get("rows_reject")
    if rows_reject_manifest is None:
        rows_reject_manifest = 0

    stats = {
        "rows_scanned": 0,
        "rows_invalid": 0,
        "rows_in_manifest": rows_in_manifest,
        "rows_good_manifest": rows_good_manifest,
        "rows_reject_manifest": rows_reject_manifest,
        "duplicate_supplier_sku_rows": 0,
        "duplicate_derived_identity_rows": 0,
        "good_missing_supplier_sku_count": 0,
    }

    error_counts = Counter()
    identity_basis_counts = Counter()
    stock_kind_counts = Counter()
    supplier_sku_counter = Counter()
    derived_identity_counter = Counter()

    sample_errors = []
    sample_duplicate_supplier_sku = []
    sample_duplicate_derived_identity = []

    for line_no, rec in iter_ndjson(good_path):
        stats["rows_scanned"] += 1

        identity_basis = rec.get("identity_basis")
        identity_basis_counts[identity_basis or "unknown"] += 1

        stock = rec.get("stock") or {}
        stock_kind_counts[stock.get("kind")] += 1

        supplier_sku = norm_text(rec.get("supplier_sku"))
        if not supplier_sku:
            stats["good_missing_supplier_sku_count"] += 1

        errs = validate_good_record(rec)
        if errs:
            stats["rows_invalid"] += 1
            for e in errs:
                error_counts[e] += 1
            if len(sample_errors) < 20:
                sample_errors.append({
                    "line_no": line_no,
                    "errors": errs,
                    "name": rec.get("name"),
                    "supplier_sku": rec.get("supplier_sku"),
                    "identity_basis": rec.get("identity_basis"),
                })

        if supplier_sku:
            supplier_sku_counter[supplier_sku] += 1
            if supplier_sku_counter[supplier_sku] == 2 and len(sample_duplicate_supplier_sku) < 20:
                sample_duplicate_supplier_sku.append({
                    "supplier_sku": supplier_sku,
                    "name": rec.get("name"),
                })

        if rec.get("identity_basis") == "derived_no_sku":
            derived_key = (
                norm_text(rec.get("name")).lower(),
                norm_text(rec.get("brand")).lower(),
            )
            derived_identity_counter[derived_key] += 1
            if derived_identity_counter[derived_key] == 2 and len(sample_duplicate_derived_identity) < 20:
                sample_duplicate_derived_identity.append({
                    "name": rec.get("name"),
                    "brand": rec.get("brand"),
                })

    stats["duplicate_supplier_sku_rows"] = sum(v - 1 for v in supplier_sku_counter.values() if v > 1)
    stats["duplicate_derived_identity_rows"] = sum(v - 1 for v in derived_identity_counter.values() if v > 1)

    rows_scanned = stats["rows_scanned"]
    rows_invalid = stats["rows_invalid"]
    derived_no_sku_count = identity_basis_counts.get("derived_no_sku", 0)

    ratios = {
        "good_ratio_vs_manifest_rows_in": safe_div(rows_scanned, rows_in_manifest),
        "reject_ratio_vs_manifest_rows_in": safe_div(rows_reject_manifest, rows_in_manifest),
        "invalid_ratio_vs_scanned": safe_div(rows_invalid, rows_scanned),
        "derived_no_sku_ratio_vs_scanned": safe_div(derived_no_sku_count, rows_scanned),
    }

    warnings = []
    blocks = []

    if rows_scanned == 0:
        blocks.append("zero_good_rows")

    if rows_in_manifest and (rows_scanned + rows_reject_manifest != rows_in_manifest):
        blocks.append("row_accounting_mismatch")

    if rows_invalid > 0:
        blocks.append("invalid_good_records_present")

    if stats["duplicate_supplier_sku_rows"] > 0:
        warnings.append("duplicate_supplier_sku_rows_present")

    if stats["duplicate_derived_identity_rows"] > 0:
        warnings.append("duplicate_derived_identity_rows_present")

    if ratios["derived_no_sku_ratio_vs_scanned"] > 0.10:
        warnings.append("high_derived_no_sku_ratio")

    if ratios["reject_ratio_vs_manifest_rows_in"] > 0.05:
        warnings.append("high_reject_ratio")

    verdict = "block" if blocks else ("warn" if warnings else "pass")
    end_ts_human = now_utc()

    stats_doc = {
        "timestamp": now_utc(),
        "input": {
            "artifact_dir": str(artifact_dir),
            "good_ndjson": str(good_path),
            "good_hash": sha256_file(good_path),
            "reject_ndjson": str(reject_path) if reject_path.exists() else None,
            "reject_hash": (
                sha256_file(reject_path)
                if reject_path.exists() and reject_path.stat().st_size > 0
                else None
            ),
            "normalizer_manifest": str(normalizer_manifest_path),
            "normalizer_version": normalizer_meta.get("version"),
        },
        "stats": stats,
        "ratios": ratios,
        "processing": {
            "start_ts": start_ts_human,
            "end_ts": end_ts_human,
            "duration_sec": round(time.time() - start_ts, 3),
            "identity_basis": dict(identity_basis_counts),
            "stock_kind": {str(k): v for k, v in stock_kind_counts.items()},
            "normalizer_identity_basis": normalizer_processing.get("identity_basis"),
            "normalizer_reject_reasons": normalizer_processing.get("reject_reasons"),
        },
        "errors": dict(error_counts),
        "samples": {
            "invalid_rows": sample_errors,
            "duplicate_supplier_sku": sample_duplicate_supplier_sku,
            "duplicate_derived_identity": sample_duplicate_derived_identity,
        },
    }

    verdict_doc = {
        "timestamp": now_utc(),
        "artifact_dir": str(artifact_dir),
        "verdict": verdict,
        "warnings": warnings,
        "blocks": blocks,
        "summary": {
            "rows_scanned": rows_scanned,
            "rows_invalid": rows_invalid,
            "rows_reject_manifest": rows_reject_manifest,
            "derived_no_sku_count": derived_no_sku_count,
            "duplicate_supplier_sku_rows": stats["duplicate_supplier_sku_rows"],
            "duplicate_derived_identity_rows": stats["duplicate_derived_identity_rows"],
        },
    }

    stats_path.write_text(json.dumps(stats_doc, ensure_ascii=False, indent=2), encoding="utf-8")
    verdict_path.write_text(json.dumps(verdict_doc, ensure_ascii=False, indent=2), encoding="utf-8")
    return verdict_doc


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--artifact-dir", required=True, help="dir with good.ndjson + normalizer_manifest.json")
    ap.add_argument("--version", action="version", version="DQ Gate v1")
    args = ap.parse_args()

    artifact_dir = Path(args.artifact_dir).resolve()
    if not artifact_dir.exists():
        raise SystemExit(f"ARTIFACT_DIR_NOT_FOUND: {artifact_dir}")

    verdict = dq_gate(artifact_dir)
    print(json.dumps(verdict, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
