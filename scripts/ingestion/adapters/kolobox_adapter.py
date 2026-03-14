from __future__ import annotations
import re
from pathlib import Path
from common.paths import repo_path
from .base_adapter import Plan, SupplierAdapter

def sanitize_tag(s: str) -> str:
    s = s.strip()
    s = s.replace(" ", "_").replace("/", "_").replace("\\", "_").replace("(", "_").replace(")", "_")
    s = re.sub(r"[^0-9A-Za-zА-Яа-я._-]+", "_", s)
    return s

class KoloboxAdapter(SupplierAdapter):
    supplier_id = "Kolobox"
    def can_handle(self, supplier_id: str) -> bool:
        return supplier_id.lower() == "kolobox"

    def _detect_layout(self, file: Path) -> str:
        name = file.name.lower().replace("ё", "е")
        if "масл" in name:
            return "masla"
        if "груз" in name:
            return "truck"
        if "диск" in name:
            return "diski"
        if "комплект" in name:
            return "komplektatsii"
        if "шин" in name:
            return "shiny"
        return "shiny"

    def _detect_mapping(self, file: Path, layout: str) -> Path | None:
        mp_dir = repo_path("mappings", "suppliers", start=Path(__file__))
        if layout == "truck":
            return mp_dir / "kolobox_truck_xls_v1.yaml"
        if layout == "diski":
            return mp_dir / "kolobox_diski_xls_v1.yaml"
        if layout == "komplektatsii":
            return mp_dir / "kolobox_komplektatsii_xls_v1.yaml"
        if layout == "shiny":
            return mp_dir / "kolobox.yaml"
        if layout == "masla":
            return None
        return None

    def plan(self, file: Path, run_id: str, out_dir: Path):
        emitter = repo_path("scripts", "ingestion", "kolobox", "emit_kolobox_ndjson_v1.py", start=Path(__file__))
        if not emitter.exists():
            return None
        layout = self._detect_layout(file)
        mapping = self._detect_mapping(file, layout)
        if mapping is None or not mapping.exists():
            return None
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
