#!/usr/bin/env python3
import argparse,sys,hashlib,json
from decimal import Decimal,ROUND_HALF_UP
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
    return data
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
    nrows=sh.nrows
    for r0 in range(data_start0,nrows):
        row_num_1based=r0+1
        row_cells=sh.row_values(r0)  # сырые значения как даёт xlrd (float/str/empty)
        yield row_num_1based,row_cells
def json_dumps_compact(obj:dict)->str:
    return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(",",":"))
def main():
    ap=argparse.ArgumentParser("Kolobox Emitter v1")
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
    out_dir=Path(args.out_dir)/args.run_id
    out_dir.mkdir(parents=True,exist_ok=True)
    good_path=out_dir/"good.ndjson"
    bad_path=out_dir/"bad_rows.ndjson"
    stats_path=out_dir/"stats.json"
    stderr_path=out_dir/"stderr.log"
    good_f=good_path.open("w",encoding="utf-8",newline="\n")
    bad_f=bad_path.open("w",encoding="utf-8",newline="\n")
    stderr_f=stderr_path.open("w",encoding="utf-8",newline="\n")
    total_rows_read=0
    good_rows=0
    bad_rows=0
    skipped_rows_all_empty=0
    bad_reasons_counts={}
    flags_counts={}
    file_readable=True
    structure_ok=True
    try:
        for source_row_number,row_cells in extract_kolobox_rows(input_path,mapping):
            total_rows_read+=1
            # TODO(Emitter): здесь будет маппинг колонок/складов + решение GOOD/BAD
            # Сейчас ничего не решаем и ничего не пишем — только каркас прохода.
            pass
    except SystemExit:
        raise
    except Exception as e:
        file_readable=False
        structure_ok=False
        die(f"Unexpected error: {e}",EXIT_FILE_FAIL)
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
        "total_rows_read":total_rows_read,
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
    good_f.close();bad_f.close();stderr_f.close()
    sys.exit(EXIT_OK)
if __name__=="__main__":
    main()
