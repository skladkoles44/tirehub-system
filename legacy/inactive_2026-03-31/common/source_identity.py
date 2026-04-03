#!/usr/bin/env python3
from __future__ import annotations

import fnmatch
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

_REGISTRY_CACHE: dict[str, Any] | None = None
_REGISTRY_CACHE_KEY: str | None = None


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_ingestion_id() -> str:
    """
    Unique ingestion/run id.
    UUID4 is acceptable here; later can be swapped to UUIDv7.
    """
    return str(uuid.uuid4())


def sha256_file(path: str | Path) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def sidecar_path_for(file_path: str | Path) -> Path:
    p = Path(file_path)
    return p.parent / f"{p.name}.metadata.json"


def load_suppliers_registry(registry_path: str | Path = "config/suppliers_registry.yaml") -> dict[str, Any]:
    """
    Load and cache suppliers registry for the current process.
    Returns normalized dict: {"suppliers": [...]}
    """
    global _REGISTRY_CACHE, _REGISTRY_CACHE_KEY

    p = Path(registry_path).resolve()
    cache_key = str(p)

    if _REGISTRY_CACHE is not None and _REGISTRY_CACHE_KEY == cache_key:
        return _REGISTRY_CACHE

    if not p.exists():
        _REGISTRY_CACHE = {"suppliers": []}
        _REGISTRY_CACHE_KEY = cache_key
        return _REGISTRY_CACHE

    with p.open("r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}

    if not isinstance(doc, dict):
        doc = {}

    suppliers = doc.get("suppliers") or []
    if not isinstance(suppliers, list):
        suppliers = []

    normalized: list[dict[str, Any]] = []
    for item in suppliers:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "supplier_id": str(item.get("supplier_id") or "unknown"),
                "display_name": item.get("display_name"),
                "match": {
                    "email_from": list((item.get("match") or {}).get("email_from") or []),
                    "filename_regex": list((item.get("match") or {}).get("filename_regex") or []),
                },
            }
        )

    _REGISTRY_CACHE = {"suppliers": normalized}
    _REGISTRY_CACHE_KEY = cache_key
    return _REGISTRY_CACHE


def resolve_supplier(
    *,
    email_from: str | None,
    filename: str | None,
    registry: dict[str, Any],
) -> dict[str, Any]:
    """
    Resolution order:
      1. email_from via fnmatch
      2. filename via regex
      3. fallback unknown
    """
    email_from = (email_from or "").strip()
    filename = (filename or "").strip()
    suppliers = registry.get("suppliers") or []

    for supplier in suppliers:
        for pat in (supplier.get("match") or {}).get("email_from") or []:
            if fnmatch.fnmatch(email_from.lower(), str(pat).lower()):
                return {
                    "supplier_id": supplier.get("supplier_id", "unknown"),
                    "method": "email",
                    "confidence": 1.0,
                    "evidence": {
                        "email_from": email_from,
                        "matched_pattern": pat,
                    },
                    "status": "resolved",
                }

    for supplier in suppliers:
        for pat in (supplier.get("match") or {}).get("filename_regex") or []:
            try:
                if re.search(str(pat), filename, re.IGNORECASE):
                    return {
                        "supplier_id": supplier.get("supplier_id", "unknown"),
                        "method": "filename_regex",
                        "confidence": 0.9,
                        "evidence": {
                            "filename": filename,
                            "matched_pattern": pat,
                        },
                        "status": "resolved",
                    }
            except re.error:
                continue

    return {
        "supplier_id": "unknown",
        "method": "none",
        "confidence": 0.0,
        "evidence": {
            "email_from": email_from,
            "filename": filename,
        },
        "status": "unresolved",
    }


def build_ingestion_metadata(
    file_path: str | Path,
    *,
    email_from: str | None = None,
    subject: str | None = None,
    received_at: str | None = None,
    registry_path: str | Path = "config/suppliers_registry.yaml",
    channel: str | None = None,
    extra_source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p = Path(file_path).resolve()
    registry = load_suppliers_registry(registry_path)
    resolved = resolve_supplier(
        email_from=email_from,
        filename=p.name,
        registry=registry,
    )

    src_channel = channel or ("email" if (email_from or subject or received_at) else "file")

    source: dict[str, Any] = {
        "channel": src_channel,
        "email_from": email_from,
        "subject": subject,
        "received_at": received_at or now_utc(),
    }
    if extra_source:
        source.update(extra_source)

    return {
        "ingestion_id": generate_ingestion_id(),
        "source": source,
        "file": {
            "original_filename": p.name,
            "path": str(p),
            "sha256": sha256_file(p),
            "size_bytes": p.stat().st_size,
        },
        "resolved": resolved,
        "registry_path": str(Path(registry_path)),
        "created_at": now_utc(),
    }


def write_ingestion_sidecar(
    file_path: str | Path,
    *,
    email_from: str | None = None,
    subject: str | None = None,
    received_at: str | None = None,
    registry_path: str | Path = "config/suppliers_registry.yaml",
    channel: str | None = None,
    extra_source: dict[str, Any] | None = None,
    overwrite: bool = False,
) -> Path:
    p = Path(file_path).resolve()
    sidecar = sidecar_path_for(p)

    if sidecar.exists() and not overwrite:
        return sidecar

    doc = build_ingestion_metadata(
        p,
        email_from=email_from,
        subject=subject,
        received_at=received_at,
        registry_path=registry_path,
        channel=channel,
        extra_source=extra_source,
    )
    sidecar.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return sidecar


def load_ingestion_sidecar(file_path: str | Path) -> dict[str, Any]:
    sidecar = sidecar_path_for(file_path)
    if not sidecar.exists():
        return {}
    try:
        return json.loads(sidecar.read_text(encoding="utf-8"))
    except Exception:
        return {}
