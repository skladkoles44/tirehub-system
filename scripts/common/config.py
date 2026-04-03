#!/usr/bin/env python3
import os
from pathlib import Path

class Config:
    def __init__(self):
        self.repo_root = self._get("ETL_REPO_ROOT")
        self.data_root = self._get("ETL_DATA_ROOT")
        self.drop_root = self._get("ETL_DROP_ROOT")
        self.var_root = self._get("ETL_VAR_ROOT")

        self.registry_root = self.data_root / "schema_registry"
        self.artifacts_root = self.data_root / "artifacts"

    def _get(self, key: str) -> Path:
        val = os.environ.get(key)
        if not val:
            raise RuntimeError(f"{key} not set")
        return Path(val).resolve()

config = Config()
