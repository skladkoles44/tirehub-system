from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

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
    def can_handle(self, supplier_id: str) -> bool:
        return False
    def plan(self, file: Path, run_id: str, out_dir: Path):
        return None
