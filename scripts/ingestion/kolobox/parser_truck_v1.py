#!/usr/bin/env python3
import json, re, sys
import xlrd

SHEET_NAME="TDSheet"
HEADER_ROWS=2

# v1 (SSOT по evidence)
COL_ARTICLE=6
COL_CODE_1C=7
COL_BRAND=8
COL_NAME=9
COL_PRICE_OPT=10
CENTER_COL=14  # qty центрального склада для truck

def norm_ws(s):
    s = "" if s is None else str(s)
    return re.sub(r"\s+", " ", s.strip())
def norm_wh(s):
    return norm_ws(s).lower()
def as_num(x):
    if x is None: return None
    if isinstance(x,(int,float)): return float(x)
    s=norm_ws(x)
    if s=="": return None
    try: return float(s.replace(",","."))
    except: return None
def header_cells(sh,r):
    return [norm_ws(sh.cell_value(r,c)) for c in range(sh.ncols)]
def validate_header(sh):
    row1=header_cells(sh,0); row2=header_cells(sh,1)
    assert row2[COL_PRICE_OPT].lower()=="цена", f"bad row2@price_opt {row2[COL_PRICE_OPT]!r}"
    assert "опт" in row1[COL_PRICE_OPT].lower(), f"bad row1@price_opt {row1[COL_PRICE_OPT]!r}"
    assert "центр" in row1[CENTER_COL].lower(), f"bad row1@center {row1[CENTER_COL]!r}"
    # завтра row2 может стать "Остаток" — разрешаем
    if row2[CENTER_COL] and row2[CENTER_COL].lower() not in ("центр. склад","остаток"):
        raise AssertionError(f"bad row2@center {row2[CENTER_COL]!r}")
    return row1,row2
def sku_key(article, code1c):
    a=norm_ws(article); c=norm_ws(code1c)
    return a or c or ""

def parse(xls_path):
    wb=xlrd.open_workbook(xls_path)
    sh=wb.sheet_by_name(SHEET_NAME)
    row1,row2=validate_header(sh)

    stats=dict(rows_total_seen=0, rows_with_price_opt_gt_0=0, rows_with_any_qty_gt_0=0, rows_emitted=0)
    sample=[]
    out=[]

    for r in range(HEADER_ROWS, sh.nrows):
        stats["rows_total_seen"] += 1
        article=sh.cell_value(r,COL_ARTICLE)
        code1c=sh.cell_value(r,COL_CODE_1C)
        sku=sku_key(article, code1c)
        price=as_num(sh.cell_value(r,COL_PRICE_OPT))
        if price and price>0: stats["rows_with_price_opt_gt_0"] += 1

        center_qty=as_num(sh.cell_value(r,CENTER_COL))
        qty = float(center_qty) if (center_qty and center_qty>0) else 0.0
        if qty>0: stats["rows_with_any_qty_gt_0"] += 1

        rec=dict(
            supplier_id="kolobox",
            parser_id="kolobox_xls_v1",
            quality_flags=([] if qty>0 else ["no_qty"]),
            raw=dict(
                supplier_article=norm_ws(article),
                supplier_code_1c=norm_ws(code1c),
                sku_candidate_key=sku,
                brand_raw=norm_ws(sh.cell_value(r,COL_BRAND)),
                name_raw=norm_ws(sh.cell_value(r,COL_NAME)),
                price=price,
                currency="RUB",
                supplier_warehouse_name=norm_wh("Центр. Склад"),
                qty=qty,
            ),
            _passthrough=dict(total_qty=qty),
            source_row_1based=r+1,
        )
        out.append(rec); stats["rows_emitted"] += 1
        if qty>0 and len(sample)<5: sample.append(rec)

    return dict(file=xls_path, sheet=SHEET_NAME, header_rows=HEADER_ROWS, stats=stats, sample_first_5_qty_gt0=sample, records_emitted=len(out))

if __name__=="__main__":
    if len(sys.argv)!=2:
        print("Usage: parser_truck_v1.py <path_to_xls>", file=sys.stderr); sys.exit(2)
    print(json.dumps(parse(sys.argv[1]), ensure_ascii=False, indent=2))
