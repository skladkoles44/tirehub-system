from pathlib import Path
import json

def get_previous_manifest(cache_dir: Path,file_hash: str):
    p=cache_dir/f"{file_hash}.manifest.json"
    if p.exists():
        with open(p,encoding="utf-8") as f:
            return json.load(f)
    return None

def save_to_cache(cache_dir: Path,file_hash: str,manifest: dict):
    cache_dir.mkdir(parents=True,exist_ok=True)
    p=cache_dir/f"{file_hash}.manifest.json"
    with open(p,"w",encoding="utf-8") as f:
        json.dump(manifest,f,indent=2,ensure_ascii=False)
