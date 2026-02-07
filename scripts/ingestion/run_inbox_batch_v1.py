#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os, re, subprocess, sys, time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

def now_run_id() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())

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

@dataclass
class Plan:
    supplier_id: str
    file: Path
    emitter: Path
    layout: str
    mapping: Path
    run_id: str
    out_ndjson: Path
    out_stats: Path
    out_log: Path

class SupplierAdapter:
    supplier_id: str = "UNKNOWN"
    def can_handle(self, supplier_id: str) -> bool: return False
    def plan(self, file: Path, run_id: str, out_dir: Path) -> Optional[Plan]: return None

class KoloboxAdapter(SupplierAdapter):
    supplier_id = "Kolobox"
    def can_handle(self, supplier_id: str) -> bool:
        return supplier_id.lower() == "kolobox"

    def _detect_layout(self, file: Path) -> str:
        name = file.name.lower()
        if "груз" in name:
            return "truck"
        if "диск" in name:
            return "diski"
        if "комплект" in name:
            return "komplektatsii"
        if "шин" in name:
            return "shiny"
        # fallback: shiny (самый частый)
        return "shiny"

    def _detect_mapping(self, file: Path, layout: str) -> Path:
        # kolobox mappings we created:
        # - kolobox_truck_xls_v1.yaml (truck 16 cols)
        # - kolobox_diski_xls_v1.yaml (diski 17 cols)
        # - kolobox_komplektatsii_xls_v1.yaml (komplektatsii 18 cols)
        # - kolobox.yaml (baseline, e.g. shiny 21 cols)
        mp_dir = Path("mappings/suppliers")
        if layout == "truck":
            return mp_dir / "kolobox_truck_xls_v1.yaml"
        if layout == "diski":
            return mp_dir / "kolobox_diski_xls_v1.yaml"
        if layout == "komplektatsii":
            return mp_dir / "kolobox_komplektatsii_xls_v1.yaml"
        return mp_dir / "kolobox.yaml"

    def plan(self, file: Path, run_id: str, out_dir: Path) -> Optional[Plan]:
        emitter = Path("scripts/ingestion/kolobox/emit_kolobox_ndjson_v1.py")
        if not emitter.exists():
            return None
        layout = self._detect_layout(file)
        mapping = self._detect_mapping(file, layout)
        tag = sanitize_tag(f"kolobox__{file.stem}__{layout}")
        nd = out_dir / f"{tag}.{run_id}.ndjson"
        st = out_dir / f"{tag}.{run_id}.stats.json"
        lg = out_dir / f"{tag}.{run_id}.log"
        return Plan(
            supplier_id="kolobox",
            file=file,
            emitter=emitter,
            layout=layout,
            mapping=mapping,
            run_id=run_id,
            out_ndjson=nd,
            out_stats=st,
            out_log=lg,
        )


class CentrshinAdapter(SupplierAdapter):
    supplier_id = "Centrshin"
    def can_handle(self, supplier_id: str) -> bool:
        return supplier_id.lower() in ("centrshin","центршин")

    def plan(self, file: Path, run_id: str, out_dir: Path) -> Optional[Plan]:
        # пока: только один файл-агрегат, лист "Шины"
        emitter = Path("scripts/ingestion/generic/emit_xlsx_rows_v1.py")
        # Если универсального xlsx-emitter ещё нет — будем вызывать kolobox emitter нельзя.
        # Поэтому на этом шаге делаем через встроенный мини-emitter в batch (см. ниже).
        return None

ADAPTERS: List[SupplierAdapter] = [KoloboxAdapter()]

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
        "--file", safe_relpath(pl.file),
        "--layout", pl.layout,
        "--run-id", pl.run_id,
        "--mapping", safe_relpath(pl.mapping),
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
    args = ap.parse_args()

    inbox_root = Path(args.root)
    out_root = Path(args.out)
    run_id = args.run_id.strip() or now_run_id()
    out_dir = out_root / run_id
    ensure_dir(out_dir)

    files: List[Path] = []
    if inbox_root.exists():
        for p in inbox_root.rglob("*"):
            if p.is_file():
                files.append(p)
    files.sort(key=lambda x: str(x))

    report: Dict[str, Any] = {
        "run_id": run_id,
        "inbox_root": safe_relpath(inbox_root),
        "out_dir": str(out_dir),
        "items_total": len(files),
        "items": [],
        "unsupported": [],
    }

    for f in files:
        supplier = detect_supplier(f, inbox_root)
        adapter = next((a for a in ADAPTERS if a.can_handle(supplier)), None)
        if not adapter:
            if args.known_only:
                continue
            report["unsupported"].append({"file": safe_relpath(f), "supplier_guess": supplier})
            continue
        pl = adapter.plan(f, run_id, out_dir)
        if not pl:
            if args.known_only:
                continue
            report["unsupported"].append({"file": safe_relpath(f), "supplier_guess": supplier, "reason": "no plan"})
            continue
        report["items"].append(run_plan(pl))

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
