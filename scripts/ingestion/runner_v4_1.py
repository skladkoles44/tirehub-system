from pathlib import Path
import logging

from scripts.etl.container_reader import container_reader
from scripts.etl.sheet_stream import iter_sheets
from scripts.etl.row_stream import row_iterator
from scripts.etl.layout_fingerprint import compute_layout_fingerprint

logger=logging.getLogger(__name__)

class RunnerV41:

    def __init__(self,layout_registry):
        self.layout_registry=layout_registry
        self.last_fingerprint=None

    def run(self,input_file:Path):

        container=container_reader(input_file)

        for sheet_name,sheet in iter_sheets(container):

            headers=None
            columns=None

            for row_idx,row in enumerate(row_iterator(sheet)):

                if headers is None:
                    headers=row
                    columns=[{"role":str(c).strip().lower()} for c in headers]

                    role_key=tuple(c["role"] for c in columns)

                    if not role_key:
                        continue

                    fingerprint=compute_layout_fingerprint(role_key)
                    self.last_fingerprint=fingerprint

                    logger.debug(f"Fingerprint {fingerprint} in {sheet_name}")
                    continue

                parsed={
                    "row_index":row_idx,
                    "values":row
                }

                yield parsed
