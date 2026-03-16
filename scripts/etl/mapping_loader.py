import yaml

def load_mapping(layout_registry, fingerprint):
    entry = layout_registry.get(fingerprint)
    if not entry:
        raise ValueError(f"LayoutUnknown:{fingerprint}")
    path = entry.get("mapping")
    if not path:
        raise ValueError(f"MappingPathMissing:{fingerprint}")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)
