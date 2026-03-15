from pathlib import Path
import logging
from typing import Dict,Any

from scripts.etl.container_reader import container_reader
from scripts.etl.sheet_stream import iter_sheets
from scripts.etl.layout_fingerprint import compute_layout_fingerprint
from scripts.etl.mapping_loader import load_mapping
from scripts.etl.row_parser import parse_row
from scripts.etl.artifact_cache import get_previous_manifest, save_to_cache
from scripts.etl.artifact_manifest import file_sha256, build_manifest, write_manifest

logger=logging.getLogger(__name__)

class RunnerV41:
    def __init__(self,layout_registry:Dict[str,Dict[str,Any]]):
        self.layout_registry=layout_registry
        self.last_fingerprint=None

    def run(self,input_file:Path,cache_dir:Path=Path("cache/artifacts")):
        try:
            file_hash=file_sha256(input_file)
            prev_manifest=get_previous_manifest(cache_dir,file_hash)
            if prev_manifest:
                logger.info(f"File unchanged {file_hash}, skipping")
                emit_event("FileSkippedUnchanged",{"file":str(input_file),"hash":file_hash})
                return prev_manifest
            container=container_reader(input_file)
            rows_total=0
            rows_good=0
            rows_rejected=0
            last_mapping_id="unknown"
            for sheet_name,sheet in iter_sheets(container):
                for table in table_detector(sheet):
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
                    try:
                        mapping=load_mapping(self.layout_registry,fingerprint)
                        last_mapping_id=mapping.get("mapping_id","unknown")
                    except Exception:
                        emit_event("LayoutUnknown",{"fingerprint":fingerprint,"sheet":sheet_name})
                        logger.warning(f"Unknown layout {fingerprint} in {sheet_name}")
                        continue
                    if self.last_fingerprint and self.last_fingerprint!=fingerprint:
                        schema_drift_detector(self.last_fingerprint,fingerprint)
                        emit_event("LayoutDrift",{"old":self.last_fingerprint,"new":fingerprint})
                    self.last_fingerprint=fingerprint
                    columns_local=columns
                    mapping_local=mapping
                    parse=parse_row
                    for row_idx,row in enumerate(row_iterator(table)):
                        rows_total+=1
                        ok,parsed,flags=parse(row,columns_local,mapping_local)
                        if ok:
                            rows_good+=1
                            good_emitter(parsed,flags,input_file,sheet_name,row_idx)
                        else:
                            rows_rejected+=1
                            reject_emitter(row,flags,input_file,sheet_name,row_idx)
            manifest=build_manifest(
                source_file=input_file,
                fingerprint=self.last_fingerprint or "none",
                mapping_id=last_mapping_id,
                stats={
                    "rows_total":rows_total,
                    "rows_good":rows_good,
                    "rows_rejected":rows_rejected
                }
            )
            output_dir=Path("artifact")/input_file.stem
            write_manifest(output_dir,manifest)
            save_to_cache(cache_dir,file_hash,manifest)
            return manifest
        except Exception as e:
            logger.exception(f"Runner failed on {input_file}")
            emit_event("RunnerError",{"file":str(input_file),"error":str(e)})
            raise
