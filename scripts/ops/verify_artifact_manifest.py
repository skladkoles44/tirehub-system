"""Artifact Manifest Verifier for 4tochki R1"""
import json
import hashlib
import sys
from pathlib import Path

def main():
    manifest_path = Path("docs/integrations/4tochki/ARTIFACT_MANIFEST.json")
    
    if not manifest_path.exists():
        print("[MANIFEST_VERIFY] STATUS=FAIL")
        print("[MANIFEST_VERIFY] REASON=MANIFEST_NOT_FOUND")
        print(f"[MANIFEST_VERIFY] FILE={manifest_path}")
        sys.exit(1)

    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception as e:
        print("[MANIFEST_VERIFY] STATUS=FAIL")
        print("[MANIFEST_VERIFY] REASON=JSON_INVALID")
        print(f"[MANIFEST_VERIFY] ERROR={e}")
        sys.exit(1)

    if manifest.get("manifest_schema_version") != 1:
        print("[MANIFEST_VERIFY] STATUS=FAIL")
        print("[MANIFEST_VERIFY] REASON=SCHEMA_VERSION_MISMATCH")
        sys.exit(1)

    for file_path, info in manifest.get("artifacts", {}).items():
        p = Path(file_path)
        if not p.exists():
            print("[MANIFEST_VERIFY] STATUS=FAIL")
            print("[MANIFEST_VERIFY] REASON=FILE_MISSING")
            print(f"[MANIFEST_VERIFY] FILE={file_path}")
            sys.exit(1)

        data = p.read_bytes()
        actual = hashlib.sha256(data).hexdigest()
        expected = info.get("sha256")

        if actual != expected:
            print("[MANIFEST_VERIFY] STATUS=FAIL")
            print("[MANIFEST_VERIFY] REASON=HASH_MISMATCH")
            print(f"[MANIFEST_VERIFY] FILE={file_path}")
            print(f"[MANIFEST_VERIFY] EXPECTED={expected}")
            print(f"[MANIFEST_VERIFY] ACTUAL={actual}")
            sys.exit(1)

        print(f"[MANIFEST_VERIFY] FILE={file_path} STATUS=OK")

    print("[MANIFEST_VERIFY] STATUS=SUCCESS")
    sys.exit(0)

if __name__ == "__main__":
    main()
