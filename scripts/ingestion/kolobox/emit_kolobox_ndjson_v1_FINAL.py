#!/usr/bin/env python3
import argparse,sys,hashlib,json,unicodedata,re
from decimal import Decimal,ROUND_HALF_UP,InvalidOperation
from datetime import datetime,timezone
from pathlib import Path
import yaml

NDJSON_CONTRACT_VERSION="1.0"
EMITTER_VERSION="1.0.0"
PARSER_ID="kolobox_xls_v1"
SUPPLIER_ID="kolobox"

EXIT_OK=0
EXIT_ARGS=1
EXIT_FILE_FAIL=2
EXIT_STRUCT_FAIL=3

_RE_WS=re.compile(r"\s+")
_RE_NUM=re.compile(r"-?\d+(?:[.,]\d+)?")

def die(msg:str,code:int=EXIT_ARGS):
    sys.stderr.write(msg+"\n")
    sys.exit(code)

def parse_rfc3339_z(value:str)->str:
    try:
        dt=datetime.fromisoformat(value.replace("Z","+00:00"))
        if dt.tzinfo is None: raise ValueError
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        die(f"Invalid --effective-at RFC3339Z: {value}",EXIT_ARGS)

def sha256_lf_normalized(path:Path)->str:
    data=path.read_bytes().replace(b"\r\n",b"\n")
    return hashlib.sha256(data).hexdigest()

def load_mapping(path:Path)->dict:
    with path.open("r",encoding="utf-8") as f:
        data=yaml.safe_load(f)
    if "mapping_version" not in data: die("mapping_version missing in mapping.yaml",EXIT_ARGS)
    if "format_hints" not in data: die("format_hints missing in mapping.yaml",EXIT_ARGS)
    if "columns" not in data: die("columns missing in mapping.yaml",EXIT_ARGS)
    if "warehouses" not in data: die("warehouses missing in mapping.yaml",EXIT_ARGS)
    if "defaults" not in data: data["defaults"]={}
    if "price_scale" not in data["defaults"]: die("defaults.price_scale missing in mapping.yaml",EXIT_ARGS)
    return data

def normalize_sku(value)->str:
    if value is None: return ""
    s=str(value)
    s=unicodedata.normalize("NFKC",s)
    s=s.replace("\u200b","").replace("\u200c","").replace("\u200d","").replace("\ufeff","")
    s=s.strip()
    s=_RE_WS.sub(" ",s)
    if s.endswith(".0") and all(ch.isdigit() or ch in ".-" for ch in s): s=s[:-2]
    return s

def cell_to_raw_str(v)->str:
    if v is None: return ""
    if isinstance(v,float):
        if v.is_integer(): return str(int(v))
        return format(v,".15g")
    return str(v).strip()

def parse_decimal(s:str):
    if s=="": return None
    return Decimal(s)

def parse_price_to_kop(raw_price:str,price_scale:int,flags:list):
    if raw_price=="":
        flags.append("missing_price")
        return None,None
    try:
        d=parse_decimal(raw_price)
    except InvalidOperation:
        return None,"invalid_price_format"
    kop=(d*Decimal(price_scale)).quantize(Decimal("1"),rounding=ROUND_HALF_UP)
    try:
        val=int(kop)
    except Exception:
        return None,"invalid_price_format"
    if val==0: flags.append("zero_price")
    if val<0: flags.append("negative_price")
    return val,None

def parse_qty_soft(raw_qty:str,flags:list):
    # канон: qty может быть null, но факт всё равно GOOD; текст/мусор = флаг, не BAD
    if raw_qty=="":
        flags.append("no_qty")
        # WBP: пустой/нулевой склад не является фактом наличия → строку не пишем
        # price_missing при qty>0 НЕ режем — это другой кейс
        continue
        return None
    s=raw_qty
    try:
        d=parse_decimal(s)
        if d < 0: flags.append("negative_qty")
        if d == 0: flags.append("qty_zero")
        if d != d.to_integral_value():
            flags.append("qty_fractional")
            d=d.to_integral_value(rounding=ROUND_HALF_UP)
        return int(d)
    except InvalidOperation:
        m=_RE_NUM.search(s)
        if not m:
            flags.append("qty_textual")
            return None
        # восстановление числа из текста => approximated
        token=m.group(0).replace(",",".")
        try:
            d=Decimal(token)
        except InvalidOperation:
            flags.append("qty_textual")
            return None
        flags.append("qty_approximated")
        if d < 0: flags.append("negative_qty")
        if d == 0: flags.append("qty_zero")
        if d != d.to_integral_value():
            flags.append("qty_fractional")
            d=d.to_integral_value(rounding=ROUND_HALF_UP)
        return int(d)

def extract_kolobox_rows(file_path:Path,mapping:dict):
    """Extractor для Kolobox XLS. Только чтение, yield строк. Никаких решений."""
    try:
        import xlrd
    except Exception as e:
        die(f"xlrd import failed: {e}",EXIT_FILE_FAIL)
    try:
        book=xlrd.open_workbook(str(file_path))
    except Exception as e:
        die(f"XLS open failed: {e}",EXIT_FILE_FAIL)
    fh=mapping.get("format_hints",{})
    sheet_name=fh.get("sheet")
    if not sheet_name: die("format_hints.sheet missing",EXIT_STRUCT_FAIL)
    try:
        sh=book.sheet_by_name(sheet_name)
    except Exception:
        die(f"Sheet not found: {sheet_name}",EXIT_STRUCT_FAIL)
    data_start_1based=fh.get("data_start_row_1based")
    if not isinstance(data_start_1based,int) or data_start_1based<=0:
        die("format_hints.data_start_row_1based missing/invalid",EXIT_STRUCT_FAIL)
    data_start0=data_start_1based-1
    for r0 in range(data_start0,sh.nrows):
        yield (r0+1), sh.row_values(r0)

def json_dumps_compact(obj:dict)->str:
    return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(",",":"))

def main():
    ap=argparse.ArgumentParser("Kolobox Emitter v1 FINAL")
    ap.add_argument("--input",required=True)
    ap.add_argument("--mapping",required=True)
    ap.add_argument("--effective-at",required=True)
    ap.add_argument("--run-id",required=True)
    ap.add_argument("--out-dir",required=True)
    args=ap.parse_args()

    input_path=Path(args.input)
    mapping_path=Path(args.mapping)
    if not input_path.exists(): die(f"Input file not found: {input_path}",EXIT_ARGS)
    if not mapping_path.exists(): die(f"Mapping file not found: {mapping_path}",EXIT_ARGS)

    effective_at=parse_rfc3339_z(args.effective_at)
    mapping=load_mapping(mapping_path)
    mapping_version=str(mapping["mapping_version"])
    mapping_hash=sha256_lf_normalized(mapping_path)
    price_scale=int(mapping["defaults"]["price_scale"])

    cols=mapping["columns"]
    def colpos(name:str)->int:
        c=cols.get(name,{})
        if c.get("type")!="column_position": die(f"columns.{name}.type must be column_position",EXIT_STRUCT_FAIL)
        n=c.get("column")
        if not isinstance(n,int) or n<=0: die(f"columns.{name}.column missing/invalid",EXIT_STRUCT_FAIL)
        return n-1

    c_article1=colpos("article_primary")
    c_article2=colpos("article_fallback")
    c_brand=colpos("brand")
    c_name=colpos("name")
    c_price=colpos("price")

    wh_cols=[]
    for w in mapping["warehouses"]:
        if "column" not in w or "warehouse_name" not in w: die("warehouses[*] must have column+warehouse_name",EXIT_STRUCT_FAIL)
        if not isinstance(w["column"],int) or w["column"]<=0: die("warehouses[*].column invalid",EXIT_STRUCT_FAIL)
        wh_cols.append((w["column"]-1,str(w["warehouse_name"])))

    out_dir=Path(args.out_dir)/args.run_id
    out_dir.mkdir(parents=True,exist_ok=True)
    good_path=out_dir/"good.ndjson"
    bad_path=out_dir/"bad_rows.ndjson"
    stats_path=out_dir/"stats.json"
    stderr_path=out_dir/"stderr.log"

    source_rows_read=0
    exploded_lines=0
    good_rows=0
    bad_rows=0
    skipped_rows_all_empty=0
    bad_reasons_counts={}
    flags_counts={}
    file_readable=True
    structure_ok=True

    try:
        with good_path.open("w",encoding="utf-8",newline="\n") as good_f, \
             bad_path.open("w",encoding="utf-8",newline="\n") as bad_f, \
             stderr_path.open("w",encoding="utf-8",newline="\n") as stderr_f:

            for source_row_number,row_cells in extract_kolobox_rows(input_path,mapping):
                source_rows_read+=1

                def cell(idx:int)->str:
                    if idx<0 or idx>=len(row_cells): return ""
                    return cell_to_raw_str(row_cells[idx])

                raw_article=normalize_sku(cell(c_article1)) or normalize_sku(cell(c_article2))
                raw_brand=cell(c_brand)
                raw_name=cell(c_name)
                raw_price=cell(c_price)

                all_qty_empty=True
                for (cwh,_) in wh_cols:
                    if cell(cwh)!="":
                        all_qty_empty=False
                        break

                if raw_article=="" and raw_brand=="" and raw_name=="" and raw_price=="" and all_qty_empty:
                    skipped_rows_all_empty+=1
                    continue

                for (cwh,wh_name_raw) in wh_cols:
                    flags=[]
                    raw_qty=cell(cwh)
                    sku_candidate_key=raw_article

                    if sku_candidate_key=="":
                        bad_rows+=1; exploded_lines+=1
                        bad_reasons_counts["empty_sku"]=bad_reasons_counts.get("empty_sku",0)+1
                        bad_f.write(json_dumps_compact({
                            "run_id":args.run_id,
                            "supplier_id":SUPPLIER_ID,
                            "parser_id":PARSER_ID,
                            "mapping_version":mapping_version,
                            "mapping_hash":mapping_hash,
                            "effective_at":effective_at,
                            "_meta":{"source_row_number":source_row_number},
                            "raw":{
                                "supplier_warehouse_name":wh_name_raw,
                                "supplier_article":cell(c_article1) or cell(c_article2),
                                "price_raw":raw_price,
                                "qty_raw":raw_qty,
                                "brand_raw":raw_brand,
                                "name_raw":raw_name
                            },
                            "reason_code":"empty_sku"
                        })+"\n")
                        continue

                    price_kop,perr=parse_price_to_kop(raw_price,price_scale,flags)
                    if perr:
                        bad_rows+=1; exploded_lines+=1
                        bad_reasons_counts[perr]=bad_reasons_counts.get(perr,0)+1
                        bad_f.write(json_dumps_compact({
                            "run_id":args.run_id,
                            "supplier_id":SUPPLIER_ID,
                            "parser_id":PARSER_ID,
                            "mapping_version":mapping_version,
                            "mapping_hash":mapping_hash,
                            "effective_at":effective_at,
                            "_meta":{"source_row_number":source_row_number},
                            "raw":{
                                "supplier_warehouse_name":wh_name_raw,
                                "sku_candidate_key":sku_candidate_key,
                                "supplier_article":cell(c_article1) or cell(c_article2),
                                "price_raw":raw_price,
                                "qty_raw":raw_qty,
                                "brand_raw":raw_brand,
                                "name_raw":raw_name
                            },
                            "reason_code":perr
                        })+"\n")
                        continue

                    qty_int=parse_qty_soft(raw_qty,flags)

                    for fl in set(flags):
                        flags_counts[fl]=flags_counts.get(fl,0)+1

                    rec={
                        "ndjson_contract_version":NDJSON_CONTRACT_VERSION,
                        "emitter_version":EMITTER_VERSION,
                        "supplier_id":SUPPLIER_ID,
                        "parser_id":PARSER_ID,
                        "mapping_version":mapping_version,
                        "mapping_hash":mapping_hash,
                        "run_id":args.run_id,
                        "effective_at":effective_at,
                        "sku_candidate_key":sku_candidate_key,
                        "raw":{
                            "supplier_article":cell(c_article1) or cell(c_article2),
                            "brand_raw":raw_brand,
                            "name_raw":raw_name,
                            "price_raw":raw_price,
                            "qty_raw":raw_qty,
                            "supplier_warehouse_name":wh_name_raw
                        },
                        "parsed":{
                            "price":price_kop,
                            "qty":qty_int
                        },
                        "quality_flags":sorted(set(flags)),
                        "_meta":{"source_row_number":source_row_number}
                    }
                    good_f.write(json_dumps_compact(rec)+"\n")
                    good_rows+=1; exploded_lines+=1

            explosion_factor_exact = (Decimal(exploded_lines) / Decimal(source_rows_read)) if source_rows_read else Decimal("0")

            stats={
                "run_id":args.run_id,
                "supplier_id":SUPPLIER_ID,
                "parser_id":PARSER_ID,
                "ndjson_contract_version":NDJSON_CONTRACT_VERSION,
                "emitter_version":EMITTER_VERSION,
                "mapping_version":mapping_version,
                "mapping_hash":mapping_hash,
                "effective_at":effective_at,
                "input_file":str(input_path),
                "source_rows_read":source_rows_read,
                "exploded_lines":exploded_lines,
                "explosion_factor_exact": str(explosion_factor_exact),
                "good_rows":good_rows,
                "bad_rows":bad_rows,
                "skipped_rows_all_empty":skipped_rows_all_empty,
                "bad_reasons_counts":bad_reasons_counts,
                "flags_counts":flags_counts,
                "file_readable":file_readable,
                "structure_ok":structure_ok
            }
            stats_path.write_text(json_dumps_compact(stats)+"\n",encoding="utf-8")
            stderr_f.write(json_dumps_compact(stats)+"\n")

    except SystemExit:
        raise
    except Exception as e:
        file_readable=False
        structure_ok=False
        die(f"Unexpected error: {e}",EXIT_FILE_FAIL)

    sys.exit(EXIT_OK)

if __name__=="__main__":
    main()
