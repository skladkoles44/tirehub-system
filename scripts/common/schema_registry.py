from scripts.common.config import config
#!/usr/bin/env python3
"""
schema_registry.py - Production-Ready Event-Sourced KV Storage
"""

import json
import yaml
import hashlib
import time
import os
import fcntl
import sys
import copy
import threading
from pathlib import Path
from typing import Dict, Optional, Tuple, Any, List
from datetime import datetime
from collections import OrderedDict

# ==================== КОНСТАНТЫ ====================
SCHEMA_VERSION = 3
HASH_LEN = 16
SHARD_DEPTH = 2
CANONICAL_INDENT = 2
SOFT_FAIL_SKIP_CORRUPTED = True
FLOAT_PRECISION = 6
LOCK_TIMEOUT = 10.0
LOCK_RETRY_SLEEP = 0.05
MAX_CONFIG_SIZE = 10_000_000
WAL_ROTATE_SIZE = 10 * 1024 * 1024
SNAPSHOT_INTERVAL = 1000
MAX_CACHE_SIZE = 1000
MAX_SHARD_CACHE = 1000


def log(msg: str):
    try:
        print(msg, flush=True)
    except (BrokenPipeError, OSError):
        pass


def normalize_floats(obj: Any) -> Any:
    if isinstance(obj, float):
        return float(f"{obj:.{FLOAT_PRECISION}g}")
    if isinstance(obj, dict):
        return {k: normalize_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_floats(v) for v in obj]
    return obj


def schema_checksum(schema: dict) -> str:
    schema_copy = dict(schema)
    schema_copy.pop("_runtime", None)
    schema_copy.pop("generated_at", None)
    schema_copy = normalize_floats(schema_copy)
    canonical = json.dumps(schema_copy, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:HASH_LEN]


def hash_config(cfg: dict) -> str:
    return schema_checksum(cfg)


def canonical_yaml_dump(data: dict) -> str:
    data = normalize_floats(data)
    canonical_json = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    canonical_dict = json.loads(canonical_json)
    return yaml.safe_dump(
        canonical_dict,
        sort_keys=True,
        allow_unicode=True,
        default_flow_style=False,
        indent=CANONICAL_INDENT
    )


def atomic_replace(src: Path, dst: Path):
    os.replace(src, dst)
    try:
        fd = os.open(str(dst), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except Exception:
        pass
    try:
        dir_fd = os.open(str(dst.parent), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        pass


def atomic_write(path: Path, content: str):
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        atomic_replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


class RegistryLock:
    def __init__(self, root: Path, timeout: float = LOCK_TIMEOUT):
        self.lock_path = root / ".lock"
        self.timeout = timeout
        self._lock_fd = None

    def __enter__(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_fd = open(self.lock_path, 'w')
        start = time.monotonic()
        while True:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() - start > self.timeout:
                    self._lock_fd.close()
                    self._lock_fd = None
                    raise TimeoutError(f"Registry lock timeout after {self.timeout}s")
                time.sleep(LOCK_RETRY_SLEEP)
        return self

    def __exit__(self, *args):
        if self._lock_fd:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            self._lock_fd.close()
            self._lock_fd = None


class ShardManager:
    def __init__(self, max_cache: int = MAX_SHARD_CACHE):
        self._cache = {}
        self._max_cache = max_cache
    
    def get_shard_path(self, hash_str: str, base_dir: Path, depth: int = SHARD_DEPTH) -> Path:
        if depth <= 0:
            return base_dir
        
        prefix = hash_str[:depth]
        path = base_dir
        for i in range(0, depth, 2):
            chunk = prefix[i:i+2]
            if not chunk:
                break
            path = path / chunk
        
        cache_key = str(path)
        if cache_key not in self._cache:
            if len(self._cache) >= self._max_cache:
                self._cache.clear()
            path.mkdir(parents=True, exist_ok=True)
            self._cache[cache_key] = True
        
        return path


class SchemaRegistry:
    REQUIRED_FIELDS = ["field_mappings", "categories"]
    
    def __init__(self, root: Path = None, shard_depth: int = SHARD_DEPTH):
        if root is None:
            data_root = Path(os.environ.get("ETL_DATA_ROOT", "."))
            root = data_root / "schema_registry"
        
        self.root = Path(root).resolve() if root else config.registry_root
        self.shard_depth = shard_depth
        self.root.mkdir(parents=True, exist_ok=True)
        
        self.configs_dir = self.root / "configs"
        self.wal_dir = self.root / "wal"
        self.snapshots_dir = self.root / "snapshots"
        
        self.configs_dir.mkdir(exist_ok=True)
        self.wal_dir.mkdir(exist_ok=True)
        self.snapshots_dir.mkdir(exist_ok=True)
        
        self.shard_manager = ShardManager()
        
        self._config_cache = OrderedDict()
        self._cache_size = MAX_CACHE_SIZE
        self._cache_lock = threading.RLock()
        
        self.state = {"latest": None, "configs": {}}
        
        self._wal_seq_file = self.wal_dir / ".seq"
        self._current_wal_seq = self._load_wal_seq()
        
        self._recover()
    
    def _get_config_path(self, cfg_hash: str) -> Path:
        shard_dir = self.shard_manager.get_shard_path(cfg_hash, self.configs_dir, self.shard_depth)
        return shard_dir / f"{cfg_hash}.yaml"
    
    def _load_wal_seq(self) -> int:
        if not self._wal_seq_file.exists():
            return 0
        try:
            with open(self._wal_seq_file, 'r') as f:
                return int(f.read().strip())
        except Exception:
            return 0
    
    def _save_wal_seq(self, seq: int):
        tmp = self._wal_seq_file.with_suffix(".tmp")
        with open(tmp, 'w') as f:
            f.write(str(seq))
            f.flush()
            os.fsync(f.fileno())
        atomic_replace(tmp, self._wal_seq_file)
    
    def _append_wal_unsafe(self, record: dict):
        current_seq = self._current_wal_seq
        wal_file = self.wal_dir / f"{current_seq:06d}.log"
        
        record["seq"] = current_seq
        line = json.dumps(record, separators=(",", ":")) + "\n"
        
        with open(wal_file, "a") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
        
        if wal_file.stat().st_size > WAL_ROTATE_SIZE:
            new_seq = current_seq + 1
            self._current_wal_seq = new_seq
            self._save_wal_seq(new_seq)
    
    def _load_config_file(self, path: Path, soft_fail: bool = True) -> Optional[dict]:
        cache_key = str(path)
        
        with self._cache_lock:
            if cache_key in self._config_cache:
                self._config_cache.move_to_end(cache_key)
                return copy.deepcopy(self._config_cache[cache_key])
        
        try:
            with open(path, 'rb') as f:
                if f.seek(0, os.SEEK_END) > MAX_CONFIG_SIZE:
                    if soft_fail:
                        return None
                    raise RuntimeError(f"Config too large")
                f.seek(0)
                data = yaml.safe_load(f)
            
            if not isinstance(data, dict):
                if soft_fail:
                    return None
                raise ValueError("Invalid config format")
            
            if "schema" not in data:
                if soft_fail:
                    return None
                raise ValueError("Missing 'schema' field")
            
            if "checksum" in data:
                actual = schema_checksum(data["schema"])
                if actual != data["checksum"]:
                    if soft_fail:
                        return None
                    raise RuntimeError(f"Schema checksum mismatch")
            
            with self._cache_lock:
                if len(self._config_cache) >= self._cache_size:
                    self._config_cache.popitem(last=False)
                self._config_cache[cache_key] = data
                self._config_cache.move_to_end(cache_key)
            
            return data
        except Exception as e:
            if soft_fail:
                return None
            raise
    
    def _scan_configs(self) -> Dict[str, dict]:
        configs = {}
        for yaml_file in self.configs_dir.rglob("*.yaml"):
            if yaml_file.name.startswith("."):
                continue
            data = self._load_config_file(yaml_file, soft_fail=True)
            if data is None:
                continue
            h = data.get("config_hash")
            if not h:
                try:
                    h = hash_config(data.get("schema", {}))
                except Exception:
                    continue
            configs[h] = {
                "created_at": yaml_file.stat().st_mtime,
                "rows_analyzed": data.get("rows_analyzed", 0),
                "path": str(yaml_file.resolve())
            }
        return configs
    
    def _take_snapshot_unsafe(self):
        snapshot_wal_seq = self._current_wal_seq
        snapshot_state = copy.deepcopy(self.state)
        
        snapshot_file = self.snapshots_dir / f"snapshot_{snapshot_wal_seq:06d}.json"
        tmp_file = snapshot_file.with_suffix(".tmp")
        
        snapshot = {
            "schema_version": SCHEMA_VERSION,
            "wal_seq": snapshot_wal_seq,
            "shard_depth": self.shard_depth,
            "timestamp": time.time(),
            "state": snapshot_state
        }
        
        try:
            with open(tmp_file, 'w') as f:
                json.dump(snapshot, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            atomic_replace(tmp_file, snapshot_file)
            
            snapshots = sorted(self.snapshots_dir.glob("snapshot_*.json"))
            for old in snapshots[:-3]:
                old.unlink()
        except Exception as e:
            log(f"⚠️ Failed to create snapshot: {e}")
    
    def _replay_wal_from(self, start_seq: int) -> Dict:
        state = {"latest": None, "configs": {}}
        wal_files = sorted(self.wal_dir.glob("*.log"), key=lambda x: int(x.stem))
        
        for wal_file in wal_files:
            seq = int(wal_file.stem)
            if seq < start_seq:
                continue
            
            try:
                with open(wal_file, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            op = rec.get("op")
                            rec_seq = rec.get("seq", seq)
                            
                            if rec_seq != seq:
                                continue
                            
                            if op == "put":
                                h = rec["hash"]
                                state["configs"][h] = {
                                    "created_at": rec.get("ts", rec.get("timestamp", 0)),
                                    "rows_analyzed": rec.get("rows_analyzed", 0)
                                }
                                state["latest"] = h
                            elif op == "promote":
                                h = rec["hash"]
                                if h in state["configs"]:
                                    state["latest"] = h
                            elif op == "rollback":
                                h = rec["hash"]
                                if h in state["configs"]:
                                    state["latest"] = h
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                log(f"⚠️ Failed to read WAL {wal_file}: {e}")
                continue
        
        return state
    
    def _recover(self):
        snapshots = sorted(self.snapshots_dir.glob("snapshot_*.json"))
        
        if snapshots:
            last_snapshot = snapshots[-1]
            try:
                with open(last_snapshot, 'r') as f:
                    snapshot = json.load(f)
                    if snapshot.get("schema_version") == SCHEMA_VERSION:
                        self.state = snapshot["state"]
                        snapshot_seq = snapshot["wal_seq"]
                        self._current_wal_seq = snapshot_seq
                        self._save_wal_seq(snapshot_seq)
                        
                        replay_state = self._replay_wal_from(snapshot_seq)
                        for h, info in replay_state["configs"].items():
                            self.state["configs"][h] = info
                        if replay_state["latest"]:
                            self.state["latest"] = replay_state["latest"]
                        
                        log(f"  📸 Recovered from snapshot")
                        return
            except Exception as e:
                log(f"⚠️ Failed to load snapshot: {e}")
        
        log("  🔄 Full WAL replay...")
        self.state = self._replay_wal_from(0)
        
        scanned_configs = self._scan_configs()
        orphans = 0
        for h, info in scanned_configs.items():
            if h not in self.state["configs"]:
                self.state["configs"][h] = info
                orphans += 1
        
        if orphans:
            log(f"  ✅ Recovered {orphans} orphan configs")
        
        if not self.state["latest"] and self.state["configs"]:
            latest = max(self.state["configs"].items(), key=lambda x: (x[1]["created_at"], x[0]))
            self.state["latest"] = latest[0]
    
    def register(self, schema: dict, force: bool = False) -> Tuple[str, bool, Optional[str]]:
        self._validate_schema(schema)
        
        with RegistryLock(self.root):
            prev_hash = self.state.get("latest")
            cfg_hash = hash_config(schema)
            config_path = self._get_config_path(cfg_hash)
            is_new = False
            
            if not config_path.exists():
                data = {
                    "schema_version": SCHEMA_VERSION,
                    "config_hash": cfg_hash,
                    "checksum": schema_checksum(schema),
                    "generated_at": time.time(),
                    "rows_analyzed": schema.get("_runtime", {}).get("rows_analyzed", 0),
                    "schema": schema
                }
                content = canonical_yaml_dump(data)
                atomic_write(config_path, content)
                is_new = True
                log(f"  ✅ Registered new config: {cfg_hash}")
            else:
                existing_data = self._load_config_file(config_path, soft_fail=False)
                existing_schema = existing_data["schema"]
                existing_copy = dict(existing_schema)
                existing_copy.pop("_runtime", None)
                new_copy = dict(schema)
                new_copy.pop("_runtime", None)
                
                if normalize_floats(existing_copy) != normalize_floats(new_copy):
                    raise ValueError(f"Hash collision: {cfg_hash}")
                log(f"  ⏭️ Config already exists: {cfg_hash}")
            
            self._append_wal_unsafe({
                "op": "put",
                "hash": cfg_hash,
                "ts": time.time(),
                "rows_analyzed": schema.get("_runtime", {}).get("rows_analyzed", 0)
            })
            
            self.state["configs"][cfg_hash] = {
                "created_at": time.time(),
                "rows_analyzed": schema.get("_runtime", {}).get("rows_analyzed", 0),
                "path": str(config_path.resolve())
            }
            
            if is_new or force:
                self.state["latest"] = cfg_hash
            
            if len(self.state["configs"]) % SNAPSHOT_INTERVAL == 0:
                self._take_snapshot_unsafe()
            
            return cfg_hash, is_new, prev_hash
    
    def get(self, cfg_hash: str) -> dict:
        config_path = self._get_config_path(cfg_hash)
        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {cfg_hash}")
        
        data = self._load_config_file(config_path, soft_fail=False)
        return copy.deepcopy(data["schema"])
    
    def latest(self) -> dict:
        if not self.state["latest"]:
            raise ValueError("No configs registered")
        return self.get(self.state["latest"])
    
    def latest_hash(self) -> str:
        return self.state["latest"]
    
    def list_configs(self, limit: int = 100) -> dict:
        configs = {}
        for h, info in list(self.state["configs"].items())[:limit]:
            configs[h] = copy.deepcopy(info)
        return configs
    
    def _validate_schema(self, schema: dict):
        for field in self.REQUIRED_FIELDS:
            if field not in schema:
                raise ValueError(f"Missing required field: {field}")
        if not schema.get("field_mappings"):
            # TEMP: allow empty for bootstrap
            pass
    def promote(self, cfg_hash: str):
        with RegistryLock(self.root):
            if cfg_hash not in self.state["configs"]:
                raise FileNotFoundError(f"Config not found: {cfg_hash}")
            
            self._append_wal_unsafe({"op": "promote", "hash": cfg_hash, "ts": time.time()})
            self.state["latest"] = cfg_hash
            log(f"  ✅ Promoted {cfg_hash} to latest")
    
    def rollback(self) -> str:
        with RegistryLock(self.root):
            configs = sorted(
                self.state["configs"].items(),
                key=lambda x: (x[1]["created_at"], x[0]),
                reverse=True
            )
            if len(configs) < 2:
                raise ValueError("Not enough configs to rollback")
            
            prev_hash = configs[1][0]
            self._append_wal_unsafe({"op": "rollback", "hash": prev_hash, "ts": time.time()})
            self.state["latest"] = prev_hash
            log(f"  🔄 Rolled back to {prev_hash}")
            return prev_hash


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--root")
    parser.add_argument("--shard-depth", type=int, default=SHARD_DEPTH)
    parser.add_argument("--register")
    parser.add_argument("--latest", action="store_true")
    parser.add_argument("--show", action="store_true")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--get")
    parser.add_argument("--promote")
    parser.add_argument("--rollback", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()
    
    registry = SchemaRegistry(Path(args.root) if args.root else None, shard_depth=args.shard_depth)
    
    if args.register:
        import yaml
        with open(args.register) as f:
            data = yaml.safe_load(f)
        schema = data.get("schema", data)
        h, new, prev = registry.register(schema)
        print(f"Registered: {h} (new={new})")
    elif args.latest:
        print(registry.latest_hash())
    elif args.show:
        print(canonical_yaml_dump(registry.latest()))
    elif args.list:
        cfgs = registry.list_configs()
        print(f"Total: {len(registry.state['configs'])}")
        print(f"Latest: {registry.latest_hash()}")
        for h, info in list(cfgs.items())[:20]:
            print(f"  {h}: {info['rows_analyzed']} rows")
    elif args.get:
        print(canonical_yaml_dump(registry.get(args.get)))
    elif args.promote:
        registry.promote(args.promote)
    elif args.rollback:
        registry.rollback()
    elif args.compact:
        registry.compact()
    else:
        parser.print_help()
