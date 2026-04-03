#!/usr/bin/env python3
from pathlib import Path
import sys
import re
import yaml

_BOOTSTRAP_ROOT = next((c for c in (Path(__file__).resolve().parent, *Path(__file__).resolve().parent.parents) if (c / "common" / "paths.py").exists()), None)
if _BOOTSTRAP_ROOT and str(_BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_ROOT))

from common.paths import repo_root

REPO = repo_root(start=Path(__file__))
MANIFEST = REPO / "config-manifest.yaml"
ENV_PHONE = REPO / ".env.phone"

IGNORE_TOP = {".git", "venv", ".venv", "backups", "tmp", "artifacts", "__pycache__"}
IGNORE_REL_PREFIXES = ("var/backup/", "var/tmp/")
EXAMPLE_FILES = {
    "config-manifest.yaml",
    "scripts/connectors/mail_ingest.env.example",
    ".env.phone.example",
}
SAFE_SECRET_PLACEHOLDERS = {
    "",
    '""',
    "''",
    "secret",
    '"secret"',
    "'secret'",
    "<secret>",
    '"<secret>"',
    "'<secret>'",
    "changeme",
    '"changeme"',
    "'changeme'",
}

def fail(msg: str) -> None:
    print(msg)
    sys.exit(1)

def load_yaml(path: Path):
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as e:
        fail(f"FAIL yaml_read {path}: {e}")

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
        rel = str(p.relative_to(root))
        top = rel.split("/", 1)[0]
        if top in IGNORE_TOP:
            continue
        if rel.startswith(IGNORE_REL_PREFIXES):
            continue
        yield p, rel

def strip_inline_comment(value: str) -> str:
    # Good enough for .env-like lines; preserves quoted placeholders.
    if " #" in value:
        value = value.split(" #", 1)[0].rstrip()
    return value.strip()

def is_runtime_passthrough(value: str) -> bool:
    v = (value or "").strip()
    if not v:
        return True
    passthrough_patterns = (
        r'^["\']?\$\{[A-Z][A-Z0-9_]*(:-[^}]*)?\}["\']?$',
        r'^["\']?\$[A-Z][A-Z0-9_]*["\']?$',
    )
    return any(re.match(p, v) for p in passthrough_patterns)

def main():
    if not MANIFEST.exists():
        fail("FAIL missing config-manifest.yaml")

    data = load_yaml(MANIFEST) or {}
    variables = data.get("variables")
    if not isinstance(variables, dict):
        fail("FAIL manifest.variables is not a mapping")

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

    repo_hits = []

    for p, rel in iter_repo_files(REPO):
        try:
            lines = p.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue

        for i, raw in enumerate(lines, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            for name in secret_vars:
                # 1) .env-style assignment: IMAP_PASS=...
                m_env = re.match(rf'^(?:export\s+)?{re.escape(name)}\s*=\s*(.*)$', line)
                if m_env:
                    value = strip_inline_comment(m_env.group(1))
                    if rel in EXAMPLE_FILES:
                        if value not in SAFE_SECRET_PLACEHOLDERS:
                            repo_hits.append(f"secret_value_in_example {name} :: {rel}:{i}")
                    else:
                        if value and value not in SAFE_SECRET_PLACEHOLDERS and not is_runtime_passthrough(value):
                            repo_hits.append(f"secret_assignment_in_repo {name} :: {rel}:{i}")
                    continue

                # 2) YAML/JSON-like key/value: IMAP_PASS: xxx or "IMAP_PASS": "xxx"
                m_map = re.match(
                    rf'^(?:["\']?{re.escape(name)}["\']?)\s*:\s*(.+?)\s*$',
                    line
                )
                if m_map:
                    value = strip_inline_comment(m_map.group(1).strip().rstrip(","))
                    if rel in EXAMPLE_FILES:
                        if value not in SAFE_SECRET_PLACEHOLDERS:
                            repo_hits.append(f"secret_value_in_example {name} :: {rel}:{i}")
                    else:
                        if value and value not in SAFE_SECRET_PLACEHOLDERS and not is_runtime_passthrough(value):
                            repo_hits.append(f"secret_mapping_in_repo {name} :: {rel}:{i}")
                    continue

    print("== manifest ==")
    print(f"variables_total={len(variables)}")
    print(f"required_platform_nonderived={len(required_platform)}")
    print(f"secret_vars={len(secret_vars)}")
    print("== env ==")
    print(f".env.phone_exists={ENV_PHONE.exists()}")
    print(f".env.phone_keys={len(env_phone_keys)}")

    if repo_hits:
        errors.extend(repo_hits)

    if errors:
        print("== FAIL ==")
        for e in errors:
            print(e)
        sys.exit(1)

    print("== OK ==")
    print("config manifest v3 validation passed")

if __name__ == "__main__":
    main()
