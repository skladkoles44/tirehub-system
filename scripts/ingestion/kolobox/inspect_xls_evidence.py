#!/usr/bin/env python3
import json, os, re
from pathlib import Path
import xlrd

def norm_cell(v):
    if v is None:
        return ""
    if isinstance(v, float):
        if abs(v - int(v)) < 1e-9:
            return str(int(v))
        return str(v)
    return str(v).replace("\u00a0"," ").strip()

def nonempty_count(row):
    return sum(1 for x in row if x.strip() != "")

def is_numberish(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9]+([.,][0-9]+)?", s.strip()))

def fill_merged_cells(sh, grid):
    # xlrd stores merged ranges as (rlo, rhi, clo, chi)
    # Propagate top-left value into all cells of the merged block if they are empty.
    merged = getattr(sh, "merged_cells", []) or []
    for rlo, rhi, clo, chi in merged:
        tl = grid[rlo][clo].strip() if rlo < len(grid) and clo < len(grid[rlo]) else ""
        if not tl:
            continue
        for r in range(rlo, rhi):
            for c in range(clo, chi):
                if r < len(grid) and c < len(grid[r]):
                    if grid[r][c].strip() == "":
                        grid[r][c] = tl

def load_sheet(path, sheet_name=None):
    wb = xlrd.open_workbook(path, formatting_info=False)
    names = wb.sheet_names()
    sname = sheet_name if sheet_name in names else names[0]
    sh = wb.sheet_by_name(sname)
    rows, cols = sh.nrows, sh.ncols

    grid=[]
    for r in range(rows):
        row=[]
        for c in range(cols):
            row.append(norm_cell(sh.cell_value(r,c)))
        grid.append(row)

    # merged-aware normalization
    fill_merged_cells(sh, grid)

    return names, sname, rows, cols, grid

def trim_right(row):
    j=len(row)
    while j>0 and row[j-1].strip()=="":
        j-=1
    return row[:j]

def infer_header_rows(data, max_scan=20):
    # Better heuristic:
    # - score rows with many nonempty and low numeric ratio
    # - prefer rows containing known header tokens for Kolobox
    known_tokens = ["вид", "резьба", "длина", "ключ", "посад", "секрет", "артикул", "код", "марка", "бренд",
                    "наименование", "опт", "рознич", "миц", "склад", "остат", "заказ"]
    scan = data[:min(max_scan, len(data))]
    scored=[]
    for i,row in enumerate(scan):
        r = [x.strip() for x in row]
        ne = nonempty_count(r)
        if ne == 0:
            continue
        nums = sum(1 for x in r if x and is_numberish(x))
        txt = " ".join(x.lower() for x in r if x)
        tok_hits = sum(1 for t in known_tokens if t in txt)
        # weight: structure > tokens > low numbers
        score = ne*10 + tok_hits*15 - nums*4
        scored.append((score,i,ne,nums,tok_hits))
    if not scored:
        return 1, 0, None
    scored.sort(reverse=True)
    best_i = scored[0][1]

    # Candidate 2-row header if next row has typical secondary labels
    if best_i+1 < len(scan):
        r2 = scan[best_i+1]
        r2_ne = nonempty_count(r2)
        tokens = " ".join(x.lower() for x in r2 if x)
        if r2_ne >= 2 and any(t in tokens for t in ["цена","остаток","шт","руб","заказ"]):
            return 2, best_i, best_i+1
    return 1, best_i, None

def build_evidence(xls_path: str, sheet_name="TDSheet"):
    names, primary, rows, cols, data = load_sheet(xls_path, sheet_name=sheet_name)

    hr, r1i, r2i = infer_header_rows(data, max_scan=20)
    row1 = trim_right(data[r1i]) if r1i is not None else []
    row2 = trim_right(data[r2i]) if (hr==2 and r2i is not None) else []

    # Hard fallback: if heuristic picked something useless, force row0/row1
    if len([x for x in row1 if x.strip()]) < 5:
        r1i = 0
        row1 = trim_right(data[0]) if rows > 0 else []
        hr = 2 if rows > 1 else 1
        r2i = 1 if hr==2 else None
        row2 = trim_right(data[1]) if hr==2 else []

    data_start_0 = (r2i+1) if hr==2 else (r1i+1)

    return {
        "file": xls_path,
        "sheet": primary,
        "sheet_names": names,
        "shape": {"rows": rows, "cols": cols},
        "header": {
            "header_rows": hr,
            "row1_index0": r1i,
            "row2_index0": r2i if hr==2 else None,
            "data_starts_at_row_index0": data_start_0,
            "row1_cells": row1,
            "row2_cells": row2,
        },
    }

def main():
    repo = Path(os.getcwd())
    evidence_dir = repo / "docs/ingestion/kolobox/evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)

    inbox = repo / "inputs/inbox/Kolobox"
    files = sorted(inbox.glob("*.xls"))
    if not files:
        raise SystemExit("NO_XLS_FOUND_UNDER inputs/inbox/Kolobox")

    for p in files:
        ev = build_evidence(str(p), sheet_name="TDSheet")
        out = evidence_dir / (p.name + ".evidence.json")
        out.write_text(json.dumps(ev, ensure_ascii=False, indent=2), encoding="utf-8")
        h=ev["header"]
        print(f"OK: {p.name} | header_rows={h['header_rows']} row1_len={len(h['row1_cells'])} row2_len={len(h['row2_cells'])}")
    return 0

if __name__=="__main__":
    raise SystemExit(main())
