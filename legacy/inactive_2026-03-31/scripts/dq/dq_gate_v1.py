#!/usr/bin/env python3
import json
from pathlib import Path
from collections import Counter

def iter_ndjson(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            yield line_no, json.loads(line)

def validate(rec: dict):
    errors = []

    source_sku = rec.get("source_sku")
    name = rec.get("name")
    price = rec.get("price")
    warehouses = rec.get("warehouses")
    identity_basis = rec.get("identity_basis")
    lineage = rec.get("lineage_sample")

    # identity
    if not (source_sku or name):
        errors.append("missing_identity")

    if identity_basis not in ("sku", "raw_name"):
        errors.append("bad_identity_basis")

    # price
    if price is not None and not isinstance(price, (int, float)):
        errors.append("bad_price")

    # warehouses
    if not isinstance(warehouses, list):
        errors.append("warehouses_not_list")

    # lineage (критично для трассировки)
    if not isinstance(lineage, dict):
        errors.append("missing_lineage")

    return errors

def dq_gate(artifact_dir: Path):
    good = artifact_dir / "good.ndjson"
    verdict_path = artifact_dir / "verdict.json"

    if not good.exists():
        raise SystemExit(f"GOOD_NOT_FOUND: {good}")

    rows = 0
    invalid = 0
    error_counts = Counter()

    for _, rec in iter_ndjson(good):
        rows += 1
        errs = validate(rec)
        if errs:
            invalid += 1
            for e in errs:
                error_counts[e] += 1

    blocks = []
    if rows == 0:
        blocks.append("zero_rows")
    if invalid > 0:
        blocks.append("invalid_records")

    verdict = "block" if blocks else "pass"

    out = {
        "rows": rows,
        "invalid": invalid,
        "errors": dict(error_counts),
        "verdict": verdict,
    }

    verdict_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifact-dir", required=True)
    args = ap.parse_args()
    dq_gate(Path(args.artifact_dir))
