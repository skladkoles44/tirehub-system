#!/usr/bin/env python3
from pathlib import Path
import sys
import yaml
import re

REPO = Path(".").resolve()
MANIFEST = REPO / "config-manifest.yaml"
ENV_PHONE = REPO / ".env.phone"

IGNORE_DIRS = {".git", "venv", ".venv", "backups", "tmp", "artifacts", "__pycache__", "var"}
IGNORE_FILES = {
    "config-manifest.yaml",
    ".env.phone",
    ".env.phone.example",
    "mail_ingest.env.example",
}

SAFE_PLACEHOLDERS = {"secret", "changeme", "example", "<secret>", "xxx", "***", "redacted", "placeholder"}

def load_yaml(path: Path):
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"FAIL yaml_read {path}: {e}")
        sys.exit(1)

def load_env_keys(path: Path):
    keys = set()
    if not path.exists():
        return keys
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        if "=" in line:
            k = line.split("=", 1)[0].strip()
            if k:
                keys.add(k)
    return keys

def iter_repo_files(root: Path):
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if any(part in IGNORE_DIRS for part in p.parts):
            continue
        if p.name in IGNORE_FILES:
            continue
        if p.name.endswith(".example") or p.name.endswith(".env.example"):
            continue
        yield p

def normalize_value(v: str) -> str:
    return v.strip().strip('"').strip("'").strip()

def is_real_secret_value(v: str) -> bool:
    x = normalize_value(v)
    if not x:
        return False
    if x.lower() in SAFE_PLACEHOLDERS:
        return False
    return True

def main():
    if not MANIFEST.exists():
        print("FAIL missing config-manifest.yaml")
        sys.exit(1)

    data = load_yaml(MANIFEST) or {}
    variables = data.get("variables")
    if not isinstance(variables, dict):
        print("FAIL manifest.variables is not a mapping")
        sys.exit(1)

    env_phone_keys = load_env_keys(ENV_PHONE)
    errors = []

    required_platform = []
    secret_vars = []

    for name, meta in sorted(variables.items()):
        if not isinstance(meta, dict):
            errors.append(f"bad_manifest_entry {name}")
            continue
        level = meta.get("level")
        required = bool(meta.get("required", False))
        derived = bool(meta.get("derived", False))
        secret = bool(meta.get("secret", False))

        if level == "platform" and required and not derived:
            required_platform.append(name)
        if secret:
            secret_vars.append(name)

    for name in required_platform:
        if name not in env_phone_keys:
            errors.append(f"missing_in_.env.phone {name}")

    repo_hits = set()

    for p in iter_repo_files(REPO):
        try:
            text = p.read_text(encoding="utf-8")
        except Exception:
            continue

        for name in secret_vars:
            pats = [
                re.compile(rf'(^|\n)\s*(?:export\s+)?{re.escape(name)}\s*=\s*([^\n#]+)', re.M),
                re.compile(rf'(^|\n)\s*{re.escape(name)}\s*:\s*([^\n#]+)', re.M),
            ]
            for pat in pats:
                for m in pat.finditer(text):
                    value = m.group(2)
                    if is_real_secret_value(value):
                        repo_hits.add(f"secret_value_like_assignment {name} :: {p.relative_to(REPO)}")

    print("== manifest ==")
    print(f"variables_total={len(variables)}")
    print(f"required_platform_nonderived={len(required_platform)}")
    print(f"secret_vars={len(secret_vars)}")
    print("== env ==")
    print(f".env.phone_exists={ENV_PHONE.exists()}")
    print(f".env.phone_keys={len(env_phone_keys)}")

    if repo_hits:
        errors.extend(sorted(repo_hits))

    if errors:
        print("== FAIL ==")
        for e in errors:
            print(e)
        sys.exit(1)

    print("== OK ==")
    print("config manifest v3 validation passed")

if __name__ == "__main__":
    main()
