#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

VERSION = "1.5"


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def norm(v: Any) -> str:
    if v is None:
        return ""
    return re.sub(r"\s+", " ", str(v).strip())


def low(v: Any) -> str:
    return norm(v).lower()


def clean_num(v: Any) -> str:
    s = norm(v).replace(",", ".")
    if s.endswith(".0"):
        s = s[:-2]
    return s


def norm_brand(v: Any) -> str:
    s = low(v)
    mapping = {
        "good year": "goodyear",
        "good-year": "goodyear",
        "goodyear": "goodyear",
        "good year tire": "goodyear",
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
    return mapping.get(s, s)


def norm_model(v: Any) -> str:
    return norm(v)


def norm_season(v: Any) -> str:
    s = low(v)
    if "лет" in s:
        return "summer"
    if "зим" in s:
        return "winter"
    if "всесез" in s:
        return "allseason"
    return s


def norm_stud(v: Any) -> bool:
    s = low(v)
    return s in {"ш.", "да", "1", "true"} or "шип" in s


def norm_warehouse(v: Any) -> str:
    return norm(v)


def canonical(row: dict) -> dict:
    return {
        "source_sku": norm(
            row.get("source_sku")
            or row.get("sku")
            or row.get("supplier_sku")
            or row.get("article")
        ),
        "brand": norm_brand(row.get("brand")),
        "model": norm_model(row.get("model")),
        "width": clean_num(row.get("width")),
        "height": clean_num(row.get("height")),
        "diameter": clean_num(row.get("diameter")),
        "load": norm(row.get("load")),
        "speed": norm(row.get("speed")),
        "season": norm_season(row.get("season")),
        "studded": norm_stud(row.get("studded")),
        "price": row.get("price"),
        "warehouse": norm_warehouse(row.get("warehouse")),
        "qty": row.get("qty"),
        "oem": norm(row.get("oem")),
        "raw_name": norm(row.get("name")),
        "lineage": {
            "source_file": row.get("source_file"),
            "sheet": row.get("sheet_name"),
            "table_index": row.get("table_index"),
            "row_index": row.get("row_index"),
            "fingerprint": row.get("fingerprint"),
        },
    }


def reject_reason(r: dict) -> str:
    try:
        w = float(r["width"])
        h = float(r["height"])
        d = float(r["diameter"])
    except Exception:
        return "bad_size_parse"

    if not (125 <= w <= 355):
        return "width_range"
    if not (25 <= h <= 95):
        return "height_range"
    if not (12 <= d <= 24):
        return "diameter_range"
    if not r["brand"]:
        return "no_brand"
    if not r["load"]:
        return "no_load"
    if not r["speed"]:
        return "no_speed"

    return ""


def canonical_key_tuple(r: dict) -> tuple:
    return (
        r["brand"],
        r["model"],
        r["width"],
        r["height"],
        r["diameter"],
        r["load"],
        r["speed"],
        r["season"],
        "1" if r["studded"] else "0",
    )


def canonical_key_str(r: dict) -> str:
    return "|".join(canonical_key_tuple(r))


def canonical_id(r: dict) -> str:
    return hashlib.sha1(canonical_key_str(r).encode("utf-8")).hexdigest()[:16]


def stock_key(r: dict) -> tuple:
    return (
        r["warehouse"],
        str(r["qty"]),
        str(r["price"]),
        r["oem"],
        r["source_sku"],
    )


def reject_record(r: dict, mode: str, reason: str, line_no: int) -> dict:
    if mode == "minimal":
        return {
            "reject_reason": reason,
            "original_line_no": line_no,
            "source_sku": r.get("source_sku"),
            "brand": r.get("brand"),
            "model": r.get("model"),
            "width": r.get("width"),
            "height": r.get("height"),
            "diameter": r.get("diameter"),
            "load": r.get("load"),
            "speed": r.get("speed"),
            "lineage": r.get("lineage", {}),
        }

    rr = dict(r)
    rr["reject_reason"] = reason
    rr["original_line_no"] = line_no
    return rr


def write_ndjson(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def run(input_path: Path, out_dir: Path, reject_mode: str) -> None:
    started_at = time.time()
    started_at_utc = now_utc()

    if not input_path.exists():
        raise FileNotFoundError(f"input not found: {input_path}")
    if not input_path.is_file():
        raise ValueError(f"input is not a file: {input_path}")

    grouped: dict[tuple, list[dict]] = defaultdict(list)
    reject: list[dict] = []
    stats: Counter[str] = Counter()

    rows_total = 0
    rows_nonempty = 0
    rows_parsed_ok = 0
    empty_lines = 0
    json_parse_errors = 0

    with input_path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            rows_total += 1
            line = line.strip()

            if not line:
                empty_lines += 1
                continue

            rows_nonempty += 1

            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                json_parse_errors += 1
                stats["bad_json"] += 1
                continue
            except Exception:
                json_parse_errors += 1
                stats["unexpected_json_error"] += 1
                continue

            rows_parsed_ok += 1
            r = canonical(row)
            reason = reject_reason(r)

            if reason:
                reject.append(reject_record(r, reject_mode, reason, line_no))
                stats[reason] += 1
                continue

            grouped[canonical_key_tuple(r)].append(r)

    good: list[dict] = []
    for key in sorted(grouped.keys()):
        rows_group = grouped[key]
        base = rows_group[0]
        dedup_stocks: dict[tuple, dict] = {}

        for r in rows_group:
            k = stock_key(r)
            if k not in dedup_stocks:
                dedup_stocks[k] = {
                    "warehouse": r["warehouse"],
                    "qty": r["qty"],
                    "price": r["price"],
                    "oem": r["oem"],
                    "source_sku": r["source_sku"],
                }

        source_skus = sorted({r["source_sku"] for r in rows_group if r["source_sku"]})
        stocks = [dedup_stocks[k] for k in sorted(dedup_stocks.keys())]

        good.append({
            "canonical_id": canonical_id(base),
            "canonical_key": canonical_key_str(base),
            "brand": base["brand"],
            "model": base["model"],
            "width": base["width"],
            "height": base["height"],
            "diameter": base["diameter"],
            "load": base["load"],
            "speed": base["speed"],
            "season": base["season"],
            "studded": base["studded"],
            "oem": base["oem"],
            "raw_name": base["raw_name"],
            "stocks": stocks,
            "source_skus": source_skus,
            "source_skus_count": len(source_skus),
            "rows": len(rows_group),
            "stock_count": len(stocks),
            "lineage_sample": base["lineage"],
        })

    out_dir.mkdir(parents=True, exist_ok=True)
    write_ndjson(out_dir / "good.ndjson", good)
    write_ndjson(out_dir / "reject.ndjson", reject)

    manifest = {
        "version": VERSION,
        "reject_mode": reject_mode,
        "rows_total": rows_total,
        "rows_nonempty": rows_nonempty,
        "rows_parsed_ok": rows_parsed_ok,
        "empty_lines": empty_lines,
        "json_parse_errors": json_parse_errors,
        "grouped_keys": len(grouped),
        "good": len(good),
        "reject": len(reject),
        "stats": dict(stats),
        "runtime": {
            "start_ts": started_at_utc,
            "end_ts": now_utc(),
            "duration_sec": round(time.time() - started_at, 3),
        },
    }

    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description=f"L1 normalizer v{VERSION}: atomic NDJSON -> good/reject/manifest"
    )
    ap.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    ap.add_argument("--input", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--reject-mode", default="full", choices=["full", "minimal"])
    args = ap.parse_args()

    run(args.input.resolve(), args.out_dir.resolve(), args.reject_mode)


if __name__ == "__main__":
    main()
