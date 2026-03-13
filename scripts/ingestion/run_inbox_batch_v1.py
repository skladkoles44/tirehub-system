#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os, re, subprocess, sys, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

_BOOTSTRAP_ROOT = next(
    (
        c for c in (
            Path(__file__).resolve().parent,
            *Path(__file__).resolve().parent.parents,
        )
        if (c / "common" / "paths.py").exists()
    ),
    None,
)
if _BOOTSTRAP_ROOT and str(_BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_ROOT))

from common.paths import repo_path
from scripts.ingestion.adapters import ADAPTERS, Plan


def resolve_cli_path(value: str) -> Path:
    p = Path(value)
    if p.is_absolute():
        return p
    return repo_path(*p.parts, start=Path(__file__))


def _repo_str(*parts: str) -> str:
    return str(repo_path(*parts, start=Path(__file__)))


def _norm_supplier_name(s: str) -> str:
    return (s or '').strip().lower()
import secrets
from datetime import datetime, timezone

def now_run_id(supplier: str) -> str:
    supplier = (supplier or "").strip().lower()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rand6 = secrets.token_hex(3)
    return f"{supplier}_{ts}_{rand6}"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def read_xls_shape_and_row0(path: Path) -> Tuple[int, int, List[str]]:
    import xlrd  # type: ignore
    book = xlrd.open_workbook(str(path))
    sh = book.sheet_by_index(0)
    nrows, ncols = sh.nrows, sh.ncols
    row0 = []
    if nrows > 0:
        for c in range(min(80, ncols)):
            row0.append(norm(str(sh.cell_value(0, c))))
    return nrows, ncols, row0

def safe_relpath(p: Path) -> str:
    try:
        return str(p.relative_to(Path.cwd()))
    except Exception:
        return str(p)

def sanitize_tag(s: str) -> str:
    s = s.strip()
    s = s.replace(" ", "_").replace("/", "_").replace("\\", "_").replace("(", "_").replace(")", "_")
    s = re.sub(r"[^0-9A-Za-zА-Яа-я._-]+", "_", s)
    return s

def detect_supplier(file: Path, inbox_root: Path) -> str:
    # expected structure: inputs/inbox/<SupplierName>/<file>
    try:
        rel = file.relative_to(inbox_root)
        parts = rel.parts
        if len(parts) >= 2:
            return parts[0]
    except Exception:
        pass
    return "UNKNOWN"

def run_plan(pl: Plan) -> Dict[str, Any]:
    ensure_dir(pl.out_ndjson.parent)
    cmd = [
        sys.executable,
        str(pl.emitter),
        "--file", str(pl.file),
        "--layout", pl.layout,
        "--run-id", pl.run_id,
        "--mapping", str(pl.mapping),
        "--out", str(pl.out_ndjson),
        "--stats-out", str(pl.out_stats),
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    pl.out_log.write_text(proc.stdout, encoding="utf-8")
    rec: Dict[str, Any] = {
        "supplier_id": pl.supplier_id,
        "file": safe_relpath(pl.file),
        "layout": pl.layout,
        "mapping": safe_relpath(pl.mapping),
        "emitter": safe_relpath(pl.emitter),
        "out_ndjson": str(pl.out_ndjson),
        "out_stats": str(pl.out_stats),
        "out_log": str(pl.out_log),
        "exit_code": proc.returncode,
    }
    if pl.out_stats.exists():
        try:
            rec["stats"] = json.loads(pl.out_stats.read_text(encoding="utf-8"))
        except Exception as e:
            rec["stats_error"] = f"{type(e).__name__}: {e}"
    return rec

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="inputs/inbox", help="inbox root dir (default: inputs/inbox)")
    ap.add_argument("--out", default="out/batch", help="output root (default: out/batch)")
    ap.add_argument("--run-id", default="", help="override run_id (default: now)")
    ap.add_argument("--known-only", action="store_true", help="skip unknown suppliers instead of marking as unsupported")
    ap.add_argument("--only-supplier", default="", help="process only this supplier_id (normalized, e.g. centrshin)")
    args = ap.parse_args()

    only_supplier = (args.only_supplier or "").strip().lower()

    inbox_root = resolve_cli_path(args.root)
    out_root = resolve_cli_path(args.out)

    files: List[Path] = []
    if inbox_root.exists():
        for p in inbox_root.rglob("*"):
            if p.is_file():
                files.append(p)
    files.sort(key=lambda x: str(x))

    report: Dict[str, Any] = {
        "run_id": "MULTI_SUPPLIER",
        "inbox_root": safe_relpath(inbox_root),
        "out_root": str(out_root),
        "items_total": len(files),
        "items": [],
        "unsupported": [],
    }
    for f in files:
        supplier_raw = detect_supplier(f, inbox_root)
        supplier = _norm_supplier_name(supplier_raw)
        run_id = args.run_id.strip() or now_run_id(supplier)
        out_dir = out_root / run_id
        ensure_dir(out_dir)
        if only_supplier and supplier != only_supplier:
            continue

        adapter = next((a for a in ADAPTERS if a.can_handle(supplier)), None)
        if not adapter:
            if args.known_only:
                continue
            report["unsupported"].append({
                "file": safe_relpath(f),
                "supplier_guess": supplier,
                "supplier_guess_raw": supplier_raw,
                "reason": "no adapter",
            })
            continue

        pl = adapter.plan(f, run_id, out_dir)
        if not pl:
            if args.known_only:
                continue
            report["unsupported"].append({
                "file": safe_relpath(f),
                "supplier_guess": supplier,
                "supplier_guess_raw": supplier_raw,
                "reason": "no plan",
            })
            continue

        plans = pl if isinstance(pl, list) else [pl]
        for one in plans:
            report["items"].append(run_plan(one))


    rep_path = out_dir / "report.json"
    rep_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # short summary to stdout (stable)
    print("RUN_ID=", run_id)
    print("OUT_DIR=", str(out_dir))
    print("TOTAL_FILES=", report["items_total"])
    print("PROCESSED=", len(report["items"]))
    print("UNSUPPORTED=", len(report["unsupported"]))
    for it in report["items"]:
        st = it.get("stats") or {}
        print("OK",
              "supplier=", it.get("supplier_id"),
              "layout=", it.get("layout"),
              "file=", Path(it.get("file","")).name,
              "parser_id=", st.get("parser_id"),
              "lines=", st.get("lines"),
              "bad_price=", st.get("bad_price"),
              "skipped_qty_empty=", st.get("skipped_qty_empty"),
        )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
