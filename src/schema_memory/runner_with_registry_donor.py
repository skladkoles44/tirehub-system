from pathlib import Path
import json
import time
from argparse import Namespace
from scripts.common.schema_registry import SchemaRegistry


# ================= SCHEMA =================

def build_schema(atomic_path: Path):
    roles = set()
    headers = set()
    role_keys = set()

    def norm_header(h):
        h = str(h).strip().lower()
        h = " ".join(h.split())
        return h

    with open(atomic_path, encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            cols = row.get("columns", [])

            row_roles = []

            for c in cols:
                r = c.get("role")
                h = c.get("header")

                if r and r != "unknown":
                    roles.add(r)
                    row_roles.append(r)

                if h:
                    headers.add(norm_header(h))

            if row_roles:
                role_keys.add(tuple(row_roles))

    return {
        "roles": sorted(roles),
        "headers": sorted(headers),
        "role_keys": sorted([list(k) for k in role_keys]),
    }


# ================= WRAPPER =================

def run_with_registry(input_file: Path, out_dir: Path):
    from scripts.etl.runner_v5_6_3 import run as base_run

    args = Namespace(
        supplier_id="unknown",
        max_rows=None,
        header_row_index=None,
        skip_header_rows=0,
        dry_run=False,
        ingestion_id=None,
        log_level="INFO"
    )

    # --- run ---
    base_run(input_file, out_dir, args)

    # --- atomic ---
    atomic_path = out_dir / "atomic_rows.ndjson"
    if not atomic_path.exists():
        raise RuntimeError(f"No atomic_rows: {atomic_path}")

    # --- rows ---
    rows = sum(1 for _ in open(atomic_path, encoding="utf-8"))

    # --- build schema ---
    raw = build_schema(atomic_path)

    schema = {
        "field_mappings": {
            "roles": raw["roles"],
            "role_keys": raw["role_keys"],
        },
        "categories": {
            "headers": raw["headers"],
        },
        "_runtime": {
            "rows_analyzed": rows,
            "source_file": str(input_file),
            "generated_at": time.time()
        }
    }

    # --- register ---
    registry = SchemaRegistry()
    cfg_hash, is_new, prev_hash = registry.register(schema)

    # --- save ---
    (out_dir / "schema_hash.txt").write_text(cfg_hash)

    print(f"🧠 Schema registered: {cfg_hash} (new={is_new})")

    return {
        "schema_hash": cfg_hash,
        "rows": rows
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("usage: runner_with_registry.py <input> <out_dir>")
        sys.exit(1)

    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])

    run_with_registry(inp, out)
