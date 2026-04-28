from __future__ import annotations

import json
import sys
from pathlib import Path

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Index, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.dialects import postgresql

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from layer_a_r4.db.base import Base  # noqa: E402
import layer_a_r4.models  # noqa: F401,E402
from layer_a_r4.scripts import parity_check_r4 as static_parity  # noqa: E402


def norm_default(value):
    if value is None:
        return None
    text = str(getattr(value, "arg", value)).replace('"', "'").strip()
    if "CURRENT_TIMESTAMP" in text or "current_timestamp" in text.lower():
        return "CURRENT_TIMESTAMP"
    if text.lstrip("-").isdigit():
        return f"'{text}'"
    if text.startswith("'") and text.endswith("'"):
        inner = text[1:-1]
        if inner.lstrip("-").isdigit():
            return text
        return f"'{text}'"
    return text


def norm_type(coltype):
    try:
        text = coltype.compile(dialect=postgresql.dialect()).upper()
    except Exception:
        text = str(coltype).upper()
    if text == "TIMESTAMP WITH TIME ZONE":
        return "DATETIME"
    return text


def collect_orm_metadata():
    schema = {}
    for table in Base.metadata.sorted_tables:
        data = {
            "columns": {},
            "pk": {"columns": ()},
            "fks": [],
            "uniques": [],
            "indexes": [],
            "checks": [],
        }
        for col in table.columns:
            data["columns"][col.name] = {
                "type": norm_type(col.type),
                "nullable": bool(col.nullable),
                "default": norm_default(col.server_default),
            }
        for cons in table.constraints:
            if isinstance(cons, PrimaryKeyConstraint):
                data["pk"] = {"columns": tuple(c.name for c in cons.columns)}
            elif isinstance(cons, ForeignKeyConstraint):
                elems = list(cons.elements)
                data["fks"].append({
                    "name": cons.name,
                    "columns": [e.parent.name for e in elems],
                    "referred_table": elems[0].column.table.name if elems else None,
                    "referred_columns": [e.column.name for e in elems],
                    "options": {
                        k: v for k, v in {
                            "ondelete": cons.ondelete,
                            "onupdate": cons.onupdate,
                        }.items() if v is not None
                    },
                })
            elif isinstance(cons, UniqueConstraint):
                data["uniques"].append({
                    "name": cons.name,
                    "columns": [c.name for c in cons.columns],
                })
            elif isinstance(cons, CheckConstraint):
                sqltext = static_parity.normalize_sql(str(cons.sqltext))
                data["checks"].append({
                    "name": cons.name,
                    "sqltext": sqltext,
                })
        for idx in table.indexes:
            if isinstance(idx, Index):
                data["indexes"].append({
                    "name": idx.name,
                    "columns": [getattr(expr, "name", str(expr)) for expr in idx.expressions],
                    "unique": bool(idx.unique),
                })
        for key in ("fks", "uniques", "indexes", "checks"):
            data[key] = sorted(data[key], key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False))
        schema[table.name] = data
    return schema


def main() -> int:
    expected = static_parity.parse_migration(static_parity.MIGRATION_PATH)
    actual = collect_orm_metadata()
    diff = static_parity.compare_schema(expected, actual)
    green = not diff["missing_tables"] and not diff["extra_tables"] and not diff["mismatched"]
    print(json.dumps({"status": "GREEN" if green else "RED", **diff}, ensure_ascii=False, indent=2))
    return 0 if green else 1


if __name__ == "__main__":
    raise SystemExit(main())
