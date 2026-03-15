from pathlib import Path
import logging
from typing import Dict,Any

from scripts.etl.container_reader import container_reader
from scripts.etl.sheet_stream import iter_sheets
from scripts.etl.layout_fingerprint import compute_layout_fingerprint
from scripts.etl.mapping_loader import load_mapping
from scripts.etl.row_parser import parse_row

logger=logging.getLogger(__name__)

class RunnerV41:
    def __init__(self,layout_registry:Dict[str,Dict[str,Any]]):
        self.layout_registry=layout_registry
        self.last_fingerprint=None

    def run(self,input_file:Path):
        try:
            container=container_reader(input_file)
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
                        ok,parsed,flags=parse(row,columns_local,mapping_local)
                        if ok:
                            good_emitter(parsed,flags,input_file,sheet_name,row_idx)
                        else:
                            reject_emitter(row,flags,input_file,sheet_name,row_idx)
        except Exception as e:
            logger.exception(f"Runner failed on {input_file}")
            emit_event("RunnerError",{"file":str(input_file),"error":str(e)})
            raise
