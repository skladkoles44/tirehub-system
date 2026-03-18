#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import shutil
import subprocess
from collections import Counter
from pathlib import Path

REPO = Path("/data/data/com.termux/files/home/tirehub-system")
VAR = Path("/data/data/com.termux/files/home/var/tirehub-system")

DEFAULT_SEARCH_ROOTS = [
    Path("/data/data/com.termux/files/home/drop/tirehub-system"),
    Path("/storage/emulated/0/Download/ETL"),
]

ALLOW_EXT = {".xls", ".xlsx"}

DENY_SUBSTRINGS = [
    "/archive_hidden_",
    "/repo_docs_",
    "/snapshot",
    "/manifests/",
    "/ssot/",
    "__pycache__",
    ".evidence.json",
]

KEYWORDS = [
    "centrshin", "centershin",
    "brinex", "бринэкс", "бринекс",
    "linaris", "linearis", "линарис",
    "kolobox", "колобокс",
]

RUN_ROOT = VAR / "android_mass_probe_v2_runner"
NORM_ROOT = VAR / "android_mass_probe_v2_normalized"
CACHE_DIR = VAR / "cache" / "artifacts_runner_v4_1_android_mass_probe_v2"
SUMMARY_PATH = VAR / "android_mass_probe_v2_summary.jsonl"
DUP_PATH = VAR / "android_mass_probe_v2_duplicates.jsonl"


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def supplier_of(p: Path) -> str:
    s = str(p).lower()
    if "centrshin" in s or "centershin" in s:
        return "centrshin"
    if "brinex" in s or "бринэкс" in s or "бринекс" in s:
        return "brinex"
    if "linaris" in s or "linearis" in s or "линарис" in s:
        return "linearis"
    if "kolobox" in s or "колобокс" in s:
        return "kolobox"
    return "unknown"


def is_candidate(p: Path, supplier_filter: set[str] | None) -> bool:
    s = str(p).lower()
    if p.suffix.lower() not in ALLOW_EXT:
        return False
    if any(x in s for x in DENY_SUBSTRINGS):
        return False
    if not any(k in s for k in KEYWORDS):
        return False
    if supplier_filter:
        return supplier_of(p) in supplier_filter
    return True


def safe_stem(s: str) -> str:
    s = re.sub(r"[^0-9A-Za-zА-Яа-я._-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:120] or "file"


def run_cmd(cmd: list[str], cwd: Path, timeout: int):
    return subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def truncate_err(s: str | None, n: int = 800) -> str:
    return (s or "")[-n:]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--root",
        action="append",
        default=[],
        help="scan root, can be passed multiple times; default roots are used if omitted",
    )
    ap.add_argument(
        "--supplier",
        action="append",
        default=[],
        help="limit to supplier(s): centrshin, brinex, linearis, kolobox",
    )
    ap.add_argument("--dry-run", action="store_true", help="scan + dedupe only")
    ap.add_argument("--force", action="store_true", help="rerun all stages even if outputs exist")
    args = ap.parse_args()

    search_roots = [Path(x) for x in args.root] if args.root else DEFAULT_SEARCH_ROOTS
    supplier_filter = {x.strip().lower() for x in args.supplier if x.strip()} or None

    RUN_ROOT.mkdir(parents=True, exist_ok=True)
    NORM_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if SUMMARY_PATH.exists():
        SUMMARY_PATH.unlink()
    if DUP_PATH.exists():
        DUP_PATH.unlink()

    files = []
    for root in search_roots:
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if p.is_file() and is_candidate(p, supplier_filter):
                files.append(p)

    files = sorted(set(files), key=lambda p: str(p).lower())

    print("== SEARCH ROOTS ==")
    for r in search_roots:
        print(r)
    print()

    if supplier_filter:
        print("== SUPPLIER FILTER ==")
        for s in sorted(supplier_filter):
            print(s)
        print()

    print("== CANDIDATE FILES ==")
    for p in files:
        print(p)
    print(f"TOTAL_CANDIDATES={len(files)}")
    print()

    seen_hashes: dict[str, Path] = {}
    unique_files: list[tuple[Path, str]] = []
    duplicates: list[dict] = []

    for p in files:
        h = file_sha256(p)
        if h in seen_hashes:
            duplicates.append({
                "hash": h,
                "kept": str(seen_hashes[h]),
                "duplicate": str(p),
            })
        else:
            seen_hashes[h] = p
            unique_files.append((p, h))

    with DUP_PATH.open("w", encoding="utf-8") as f:
        for rec in duplicates:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("== UNIQUE FILES ==")
    for p, h in unique_files:
        print(f"{h[:16]} | {p}")
    print(f"TOTAL_UNIQUE={len(unique_files)}")
    print(f"TOTAL_DUPLICATES={len(duplicates)}")
    print()

    results = []
    dq_verdicts = Counter()
    dq_warnings = Counter()
    status_counts = Counter()
    ok_by_supplier = Counter()

    for src, h in unique_files:
        supplier = supplier_of(src)
        tag = f"{supplier}__{safe_stem(src.stem)}__{h[:16]}"

        run_out = RUN_ROOT / tag
        norm_out = NORM_ROOT / tag

        atomic_path = run_out / "atomic_rows.ndjson"
        runner_manifest_path = run_out / "manifest.json"
        normalizer_manifest_path = norm_out / "normalizer_manifest.json"
        verdict_path = norm_out / "verdict.json"

        rec = {
            "file": str(src),
            "hash": h,
            "tag": tag,
            "supplier": supplier,
            "status": "skipped_dry" if args.dry_run else None,
        }

        if args.dry_run:
            results.append(rec)
            status_counts[rec["status"]] += 1
            continue

        if args.force:
            if run_out.exists():
                shutil.rmtree(run_out)
            if norm_out.exists():
                shutil.rmtree(norm_out)

        # STAGE 1: RUNNER
        if not atomic_path.exists() or not runner_manifest_path.exists():
            run_out.mkdir(parents=True, exist_ok=True)
            r1 = run_cmd(
                [
                    "python",
                    "scripts/ingestion/runner_v4_1.py",
                    "--file", str(src),
                    "--out-dir", str(run_out),
                    "--cache-dir", str(CACHE_DIR),
                ],
                cwd=REPO,
                timeout=900,
            )
            if r1.returncode != 0:
                rec["status"] = "runner_fail"
                rec["runner_stderr_tail"] = truncate_err(r1.stderr)
                results.append(rec)
                status_counts[rec["status"]] += 1
                continue

        if not atomic_path.exists() or not runner_manifest_path.exists():
            rec["status"] = "runner_output_missing"
            results.append(rec)
            status_counts[rec["status"]] += 1
            continue

        runner_manifest = load_json(runner_manifest_path)
        rec["runner_rows_emitted"] = (runner_manifest.get("stats") or {}).get("rows_emitted")
        rec["runner_fingerprints"] = (runner_manifest.get("layout") or {}).get("fingerprints")

        # STAGE 2: NORMALIZER
        if not normalizer_manifest_path.exists():
            norm_out.mkdir(parents=True, exist_ok=True)
            r2 = run_cmd(
                [
                    "python",
                    "scripts/normalization/normalizer_v2.py",
                    "--atomic", str(atomic_path),
                    "--out-dir", str(norm_out),
                    "--reject-mode", "minimal",
                ],
                cwd=REPO,
                timeout=600,
            )
            if r2.returncode != 0:
                rec["status"] = "normalizer_fail"
                rec["normalizer_stderr_tail"] = truncate_err(r2.stderr)
                results.append(rec)
                status_counts[rec["status"]] += 1
                continue

        if not normalizer_manifest_path.exists():
            rec["status"] = "normalizer_output_missing"
            results.append(rec)
            status_counts[rec["status"]] += 1
            continue

        nm = load_json(normalizer_manifest_path)
        nm_stats = nm.get("stats") or {}
        nm_proc = nm.get("processing") or {}

        rows_reject = nm_stats.get("rows_reject")
        if rows_reject is None:
            rows_reject = 0

        rec["rows_in"] = nm_stats.get("rows_in")
        rec["rows_good"] = nm_stats.get("rows_good")
        rec["rows_reject"] = rows_reject
        rec["good_missing_supplier_sku_count"] = nm_stats.get("good_missing_supplier_sku_count")
        rec["identity_basis"] = nm_proc.get("identity_basis")
        rec["reject_reasons"] = nm_proc.get("reject_reasons")

        # STAGE 3: DQ
        if not verdict_path.exists():
            r3 = run_cmd(
                [
                    "python",
                    "scripts/dq/dq_gate_v1.py",
                    "--artifact-dir", str(norm_out),
                ],
                cwd=REPO,
                timeout=120,
            )
            if r3.returncode != 0:
                rec["status"] = "dq_fail"
                rec["dq_stderr_tail"] = truncate_err(r3.stderr)
                results.append(rec)
                status_counts[rec["status"]] += 1
                continue

        if not verdict_path.exists():
            rec["status"] = "dq_output_missing"
            results.append(rec)
            status_counts[rec["status"]] += 1
            continue

        verdict = load_json(verdict_path)
        rec["status"] = "ok"
        rec["dq_verdict"] = verdict.get("verdict")
        rec["dq_warnings"] = verdict.get("warnings")
        rec["dq_blocks"] = verdict.get("blocks")
        rec["dq_summary"] = verdict.get("summary")

        results.append(rec)
        status_counts[rec["status"]] += 1
        ok_by_supplier[supplier] += 1
        dq_verdicts[rec["dq_verdict"]] += 1
        for w in rec["dq_warnings"] or []:
            dq_warnings[w] += 1

    with SUMMARY_PATH.open("w", encoding="utf-8") as f:
        for rec in results:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("== SUMMARY ==")
    for rec in results:
        print(json.dumps(rec, ensure_ascii=False))

    print()
    print("== OUTPUT FILES ==")
    print(SUMMARY_PATH)
    print(DUP_PATH)

    print()
    print("== STATUS COUNTS ==")
    for k, v in sorted(status_counts.items()):
        print(f"{k}: {v}")

    print()
    print("== DQ VERDICTS ==")
    for k, v in sorted(dq_verdicts.items()):
        print(f"{k}: {v}")

    print()
    print("== TOP DQ WARNINGS ==")
    for k, v in dq_warnings.most_common(10):
        print(f"{k}: {v}")

    print()
    print("== OK BY SUPPLIER ==")
    for k, v in sorted(ok_by_supplier.items()):
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
