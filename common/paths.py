from __future__ import annotations

from pathlib import Path


_REPO_MARKERS = (
    ".git",
    "Makefile",
    "README.md",
)


def repo_root(start: Path | None = None) -> Path:
    cur = (start or Path(__file__)).resolve()
    if cur.is_file():
        cur = cur.parent
    for candidate in (cur, *cur.parents):
        if any((candidate / marker).exists() for marker in _REPO_MARKERS):
            return candidate
    raise RuntimeError(f"Repository root not found from {cur}")


def repo_path(*parts: str, start: Path | None = None) -> Path:
    return repo_root(start=start).joinpath(*parts)
