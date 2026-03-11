#!/usr/bin/env python3
from pathlib import Path
import sys
import re
import yaml

REPO = Path(".").resolve()
MANIFEST = REPO / "config-manifest.yaml"
ENV_PHONE = REPO / ".env.phone"
IGNORE_DIRS = {".git", "venv", ".venv", "backups", "tmp", "artifacts", "__pycache__", "var"}
ALLOWED_SECRET_NAME_FILES = {
    "config-manifest.yaml",
    ".env.phone.example",
    "scripts/connectors/mail_ingest.env.example",
}

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
        parts = set(p.parts)
        if parts & IGNORE_DIRS:
            continue
        yield p

def is_allowed_secret_reference(rel: str) -> bool:
    if rel in ALLOWED_SECRET_NAME_FILES:
        return True
    if rel.endswith(".example"):
        return True
    if rel.endswith(".example.env"):
        return True
    return False

def looks_like_real_secret_value(value: str) -> bool:
    v = value.strip().strip('"').strip("'")
    if v == "":
        return False
    placeholders = {"secret", "changeme", "example", "your_password_here", "***", "<secret>"}
    if v.lower() in placeholders:
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

    assignment_patterns = {
        name: re.compile(rf'(^|\\s)(export\\s+)?{re.escape(name)}\\s*=\\s*(.+)$')
        for name in secret_vars
    }

    repo_hits = []
    for p in iter_repo_files(REPO):
        rel = str(p.relative_to(REPO))
        try:
            lines = p.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        for i, raw in enumerate(lines, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            for name, pat in assignment_patterns.items():
                m = pat.search(raw)
                if not m:
                    continue
                value = m.group(3).strip()
                if is_allowed_secret_reference(rel):
                    continue
                if looks_like_real_secret_value(value):
                    repo_hits.append(f"secret_value_in_repo {name} :: {rel}:{i}")

    if repo_hits:
        errors.extend(repo_hits)

    print("== manifest ==")
    print(f"variables_total={len(variables)}")
    print(f"required_platform_nonderived={len(required_platform)}")
    print(f"secret_vars={len(secret_vars)}")
    print("== env ==")
    print(f".env.phone_exists={ENV_PHONE.exists()}")
    print(f".env.phone_keys={len(env_phone_keys)}")

    if errors:
        print("== FAIL ==")
        for e in errors:
            print(e)
        sys.exit(1)

    print("== OK ==")
    print("config manifest v1 validation passed")

if __name__ == "__main__":
    main()
