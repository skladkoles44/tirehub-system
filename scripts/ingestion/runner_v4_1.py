from pathlib import Path
import logging
from typing import Dict,Any
import itertools
from scripts.ingestion.column_profiler import profile_columns

from scripts.etl.container_reader import container_reader
from scripts.etl.sheet_stream import iter_sheets
from scripts.etl.layout_fingerprint import compute_layout_fingerprint
from scripts.etl.mapping_loader import load_mapping
from scripts.etl.row_parser import parse_row
from scripts.etl.artifact_cache import get_previous_manifest, save_to_cache
from scripts.etl.artifact_manifest import file_sha256, build_manifest, write_manifest
from scripts.etl.table_detector import table_detector
from scripts.etl.header_detector import header_detector
from scripts.etl.merged_cells import merged_cells_propagation
from scripts.etl.header_flattener import header_flattener
from scripts.etl.column_classifier import column_classifier
from scripts.etl.schema_drift import schema_drift_detector
from scripts.etl.row_stream import row_iterator
from scripts.etl.events import emit_event
from scripts.etl.emitters import good_emitter, reject_emitter
import json
import os

logger=logging.getLogger(__name__)

class RunnerV41:
    def __init__(self,layout_registry:Dict[str,Dict[str,Any]]):
        self.layout_registry=layout_registry
        self.last_fingerprint=None

    def run(self,input_file:Path,cache_dir:Path|None=None,output_dir:Path|None=None):
        try:
            var_root=Path(os.environ["ETL_VAR_ROOT"])
            cache_dir=cache_dir or (var_root/"cache"/"artifacts_runner_v4_1")
            output_dir=output_dir or (var_root/"artifacts"/input_file.stem)
            file_hash=file_sha256(input_file)
            prev_manifest=get_previous_manifest(cache_dir,file_hash)
            if prev_manifest:
                logger.info(f"File unchanged {file_hash}, skipping")
                emit_event("FileSkippedUnchanged",{"file":str(input_file),"hash":file_hash})
                return prev_manifest
            container=container_reader(input_file)
            rows_total=0
            rows_emitted=0
            rows_skipped=0
            last_mapping_id="decompose_only"
            fingerprints=set()
            sheets_processed=0
            tables_processed=0
            output_dir.mkdir(parents=True, exist_ok=True)
            atomic_path=output_dir/"atomic_rows.ndjson"
            if atomic_path.exists():
                atomic_path.unlink()
            table_index=0
            for sheet_name,sheet in iter_sheets(container):
                sheets_processed+=1
                for table in table_detector(sheet):
                    tables_processed+=1
                    headers=header_detector(table)
                    headers=merged_cells_propagation(headers,table)
                    flat_headers=header_flattener(headers)
                    columns=column_classifier(flat_headers)
                    role_key=tuple(c["role"] for c in columns)
                    if not role_key:
                        emit_event("EmptyTable",{"sheet":sheet_name})
                        continue
                    fingerprint=compute_layout_fingerprint(role_key)
                    logger.debug(f"Fingerprint {fingerprint} in {sheet_name}")
                    fingerprints.add(fingerprint)
                    last_mapping_id="decompose_only" 
                    if self.last_fingerprint and self.last_fingerprint!=fingerprint:
                        schema_drift_detector(self.last_fingerprint,fingerprint)
                        emit_event("LayoutDrift",{"old":self.last_fingerprint,"new":fingerprint})
                    self.last_fingerprint=fingerprint
                    columns_local=columns
                    row_iter=row_iterator(table)
                    profile_rows,stream_rows=itertools.tee(row_iter)
                    profiles=profile_columns(profile_rows, columns, sample_size=50)
                    try:
                        _pp=output_dir/"column_profiles.ndjson"
                        with _pp.open("a", encoding="utf-8") as _pf:
                            _pf.write(json.dumps({
                                "sheet": sheet_name,
                                "table_index": table_index,
                                "fingerprint": fingerprint,
                                "profiles": profiles
                            }, ensure_ascii=False) + "\n")
                    except Exception as _e:
                        logger.warning(f"ProfileWriteFail:{_e}")
                    for row_idx,row in enumerate(stream_rows):
                        rows_total+=1
                        rows_emitted+=1
                        atomic_cols=[]
                        for idx,col in enumerate(columns_local):
                            value = row[idx] if isinstance(row,(list,tuple)) and idx < len(row) else None
                            atomic_cols.append({
                                "index": idx,
                                "name": col.get("name"),
                                "header": col.get("header"),
                                "role": col.get("role"),
                                "value": value,
                            })
                        while atomic_cols and (atomic_cols[-1].get("header") in ("", None)) and atomic_cols[-1].get("value") is None:
                            atomic_cols.pop()
                        rec = {
                            "source_file": str(input_file),
                            "sheet_name": sheet_name,
                            "table_index": table_index,
                            "row_index": row_idx,
                            "layout": {"fingerprint": fingerprint},
                            "columns": atomic_cols,
                        }
                        with atomic_path.open("a", encoding="utf-8") as _af:
                            _af.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    table_index+=1
            sorted_fingerprints=sorted(fingerprints)
            primary_fingerprint=self.last_fingerprint or (sorted_fingerprints[0] if sorted_fingerprints else "none")
            manifest=build_manifest(
                source_file=input_file,
                fingerprint=primary_fingerprint,
                mapping_id=last_mapping_id,
                stats={
                    "rows_total":rows_total,
                    "rows_emitted":rows_emitted,
                    "rows_skipped":rows_skipped
                },
                fingerprints=sorted_fingerprints,
                processing={
                    "sheets_processed": sheets_processed,
                    "tables_processed": tables_processed
                }
            )
            write_manifest(output_dir,manifest)
            save_to_cache(cache_dir,file_hash,manifest)
            return manifest
        except Exception as e:
            logger.exception(f"Runner failed on {input_file}")
            emit_event("RunnerError",{"file":str(input_file),"error":str(e)})
            raise
if __name__ == "__main__":
    import argparse
    import json

    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--out-dir", default="")
    ap.add_argument("--cache-dir", default="")
    args = ap.parse_args()

    input_file = Path(args.file).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else None
    cache_dir = Path(args.cache_dir).resolve() if args.cache_dir else None

    runner = RunnerV41(None)
    manifest = runner.run(input_file, cache_dir=cache_dir, output_dir=out_dir)

    print(json.dumps(manifest, ensure_ascii=False, indent=2))
