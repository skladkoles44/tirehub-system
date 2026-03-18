#!/usr/bin/env python3

if __name__ == "__main__" and __package__ is None:
    raise SystemExit(
        "This script must be run as module:\n"
        "cd repo_root && python -m scripts.etl.unknown_header_harvest"
    )

import json
from collections import defaultdict
from pathlib import Path

from scripts.etl.column_classifier import column_classifier
from scripts.etl.container_reader import container_reader
from scripts.etl.header_detector import detect_header
from scripts.etl.header_flattener import header_flattener
from scripts.etl.row_stream import row_iterator
from scripts.etl.sheet_stream import iter_sheets
from scripts.etl.table_detector import table_detector


FILES = [
    Path("/storage/emulated/0/Download/ETL/прайсотБринэксКозловской.xlsx"),
    Path("/storage/emulated/0/Download/ETL/etl_data/raw_v1/inbox/centrshin/stock_2026-02-15.xlsx"),
]

OUT_PATH = Path("tmp/unknown_headers_report.json")


def norm(x):
    s = "" if x is None else str(x)
    s = s.lower().replace("ё", "е").replace("_", " ").replace("-", " ")
    return " ".join(s.split())


def sample_unknowns_from_table(xlsx_path, sheet_name, table_idx, table, sample_rows):
    info = detect_header(table)
    headers = info["header"]
    flat = header_flattener(headers)
    cols = column_classifier(flat)

    data_samples = []
    for i, row in enumerate(row_iterator(table), 1):
        data_samples.append(row)
        if i >= sample_rows:
            break

    out = []
    for col_idx, col in enumerate(cols):
        if col["role"] != "unknown":
            continue

        header = col["header"]
        examples = []

        for row in data_samples:
            if col_idx < len(row):
                v = row[col_idx]
                if v not in (None, "") and v not in examples:
                    examples.append(v)

        header_norm = norm(header)
        if not header_norm:
            continue

        out.append({
            "file": str(xlsx_path),
            "sheet": sheet_name,
            "table_idx": table_idx,
            "column_idx": col_idx,
            "header": header,
            "header_norm": header_norm,
            "examples": examples[:5],
        })

    return out


def harvest_file(xlsx_path, sample_rows=5):
    out = []
    container = container_reader(xlsx_path)

    for sheet_name, sheet in iter_sheets(container):
        for table_idx, table in enumerate(table_detector(sheet), 1):
            out.extend(
                sample_unknowns_from_table(
                    xlsx_path, sheet_name, table_idx, table, sample_rows
                )
            )

    return out


def aggregate(items):
    buckets = defaultdict(lambda: {
        "header_norm": "",
        "raw_headers": set(),
        "count": 0,
        "files": set(),
        "sheets": set(),
        "examples": [],
    })

    for item in items:
        key = item["header_norm"]
        b = buckets[key]

        b["header_norm"] = key
        b["raw_headers"].add(item["header"])
        b["count"] += 1
        b["files"].add(item["file"])
        b["sheets"].add(item["sheet"])

        for ex in item["examples"]:
            if ex not in b["examples"]:
                b["examples"].append(ex)

    result = []
    for _, b in buckets.items():
        result.append({
            "header_norm": b["header_norm"],
            "raw_headers": sorted(b["raw_headers"]),
            "count": b["count"],
            "files": sorted(b["files"]),
            "sheets": sorted(b["sheets"]),
            "examples": b["examples"][:10],
        })

    result.sort(key=lambda x: (-x["count"], x["header_norm"]))
    return result


def build_report(files, sample_rows=5):
    scanned = []
    raw = []

    for path in files:
        if not path.exists():
            continue

        scanned.append(str(path))
        raw.extend(harvest_file(path, sample_rows))

    return {
        "files_scanned": scanned,
        "unknown_headers": aggregate(raw),
    }


def write_report(report, out_path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def print_summary(report, out_path):
    print("REPORT_PATH:", out_path)
    print("FILES_SCANNED:", len(report["files_scanned"]))
    print("UNKNOWN_HEADERS_COUNT:", len(report["unknown_headers"]))
    print()

    for item in report["unknown_headers"][:20]:
        print("HEADER_NORM:", item["header_norm"])
        print("RAW_HEADERS:", item["raw_headers"])
        print("COUNT:", item["count"])
        print("SHEETS:", item["sheets"][:5])
        print("EXAMPLES:", item["examples"][:5])
        print()


def main():
    report = build_report(FILES, sample_rows=5)
    write_report(report, OUT_PATH)
    print_summary(report, OUT_PATH)


if __name__ == "__main__":
    main()
