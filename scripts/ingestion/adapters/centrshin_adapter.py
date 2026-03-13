from __future__ import annotations
import json
import re
from pathlib import Path
from common.paths import repo_path
from .base_adapter import Plan, SupplierAdapter

def sanitize_tag(s: str) -> str:
    s = s.strip()
    s = s.replace(" ", "_").replace("/", "_").replace("\\", "_").replace("(", "_").replace(")", "_")
    s = re.sub(r"[^0-9A-Za-zА-Яа-я._-]+", "_", s)
    return s

class CentrshinAdapter(SupplierAdapter):
    supplier_id = "Centrshin"
    def can_handle(self, supplier_id: str) -> bool:
        return supplier_id.lower() in ("centrshin", "центршин")

    def plan(self, file: Path, run_id: str, out_dir: Path):
        if file.suffix.lower() == ".json":
            emitter = repo_path("scripts", "ingestion", "centrshin", "emit_centrshin_json_category_v1.py", start=Path(__file__))
            mapping = repo_path("mappings", "suppliers", "centrshin_json_v1.yaml", start=Path(__file__))
            if not emitter.exists() or not mapping.exists():
                return None
            try:
                data = json.loads(file.read_text(encoding="utf-8"))
            except Exception:
                return None
            if not isinstance(data, dict):
                return None
            cats = [k for k, v in data.items() if isinstance(v, list)]
            cats.sort(key=lambda x: str(x))
            plans = []
            for cat in cats:
                layout = f"category:{cat}"
                tag = sanitize_tag(f"centrshin__{file.stem}__{layout}")
                nd = out_dir / f"{tag}.{run_id}.ndjson"
                st = out_dir / f"{tag}.{run_id}.stats.json"
                lg = out_dir / f"{tag}.{run_id}.log"
                plans.append(Plan(
                    supplier_id="centrshin",
                    file=file,
                    emitter=emitter,
                    layout=layout,
                    mapping=mapping,
                    run_id=run_id,
                    out_ndjson=nd,
                    out_stats=st,
                    out_log=lg,
                ))
            return plans
        return None
