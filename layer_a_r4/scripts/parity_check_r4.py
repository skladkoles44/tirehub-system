from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "0001_layer_a_init-7.py"


def get_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        left = get_name(node.value)
        return f"{left}.{node.attr}" if left else node.attr
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def get_const(node: ast.AST):
    if isinstance(node, ast.Constant):
        return node.value
    return None


def normalize_sql(sql: str | None) -> str | None:
    if sql is None:
        return None
    sql = re.sub(r"\s+", " ", sql.strip())
    sql = sql.replace('"', "'")
    sql = sql.replace("( ", "(").replace(" )", ")")
    sql = re.sub(r"\b(?:sa\.)?text\((.*)\)$", r"\1", sql)
    sql = sql.replace("now_utc_sql()", "CURRENT_TIMESTAMP")
    sql = sql.replace("_now()", "CURRENT_TIMESTAMP")
    return sql


def unparse(node: ast.AST | None) -> str | None:
    if node is None:
        return None
    return normalize_sql(ast.unparse(node))


def kw_map(call: ast.Call) -> dict[str, ast.AST]:
    return {kw.arg: kw.value for kw in call.keywords if kw.arg is not None}


def call_name(call: ast.Call) -> str | None:
    return get_name(call.func)


def parse_type_expr(node: ast.AST | None) -> str | None:
    if node is None:
        return None
    if isinstance(node, ast.Call):
        fname = get_name(node.func)
        base = fname.split(".")[-1] if fname else ast.unparse(node.func)
        if base == "String":
            if node.args:
                return f"VARCHAR({ast.unparse(node.args[0])})"
            kws = kw_map(node)
            if "length" in kws:
                return f"VARCHAR({ast.unparse(kws['length'])})"
            return "VARCHAR"
        if base == "DateTime":
            return "DATETIME"
        if base == "Text":
            return "TEXT"
        if base == "Integer":
            return "INTEGER"
        if base == "BigInteger":
            return "BIGINT"
        if base == "Boolean":
            return "BOOLEAN"
        if base == "JSON":
            return "JSON"
        return base.upper()
    if isinstance(node, ast.Name):
        mapping = {
            "Text": "TEXT",
            "Integer": "INTEGER",
            "BigInteger": "BIGINT",
            "Boolean": "BOOLEAN",
            "JSON": "JSON",
        }
        return mapping.get(node.id, node.id.upper())
    if isinstance(node, ast.Attribute):
        return parse_type_expr(ast.Call(func=node, args=[], keywords=[]))
    return ast.unparse(node).upper()


def parse_foreign_key_call(call: ast.Call) -> dict | None:
    fname = call_name(call)
    if not fname or fname.split(".")[-1] != "ForeignKey":
        return None
    target = get_const(call.args[0]) if call.args else None
    if not isinstance(target, str):
        return None
    referred_table, referred_column = target.split(".", 1)
    kws = kw_map(call)
    return {
        "name": get_const(kws["name"]) if "name" in kws else None,
        "columns": (),
        "referred_table": referred_table,
        "referred_columns": (referred_column,),
        "options": {k: get_const(v) for k, v in kws.items() if k == "ondelete"},
    }


def parse_migration(migration_path: Path) -> dict:
    tree = ast.parse(migration_path.read_text(encoding="utf-8"))
    schema: dict[str, dict] = {}
    enum_sql_map: dict[str, str] = {}

    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            if isinstance(node.value, ast.Tuple):
                values = []
                for elt in node.value.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        values.append(elt.value)
                if values:
                    quoted = ", ".join(f"'{value}'" for value in values)
                    enum_sql_map[node.targets[0].id] = quoted

    upgrade = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "upgrade")

    pending_indexes: dict[str, list[dict]] = {}
    for stmt in upgrade.body:
        if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Call):
            continue
        call = stmt.value
        name = call_name(call)
        if name == "op.create_table":
            table_name = get_const(call.args[0])
            data = {
                "columns": {},
                "pk": {"columns": []},
                "fks": [],
                "uniques": [],
                "indexes": [],
                "checks": [],
            }
            for arg in call.args[1:]:
                if not isinstance(arg, ast.Call):
                    continue
                cname = call_name(arg)
                short = cname.split(".")[-1] if cname else ""
                kws = kw_map(arg)
                if short == "Column":
                    col_name = get_const(arg.args[0])
                    col_type = parse_type_expr(arg.args[1]) if len(arg.args) > 1 else None
                    nullable = get_const(kws["nullable"]) if "nullable" in kws else None
                    primary_key = get_const(kws["primary_key"]) if "primary_key" in kws else False
                    default = None
                    if "server_default" in kws:
                        default = unparse(kws["server_default"])
                    if primary_key:
                        nullable = False
                        data["pk"]["columns"].append(col_name)
                    fks_inline = []
                    for extra in arg.args[2:]:
                        if isinstance(extra, ast.Call):
                            fk = parse_foreign_key_call(extra)
                            if fk:
                                fk["columns"] = (col_name,)
                                fks_inline.append(fk)
                    data["fks"].extend(fks_inline)
                    data["columns"][col_name] = {
                        "type": col_type,
                        "nullable": True if nullable is None else bool(nullable),
                        "default": default,
                    }
                elif short == "ForeignKeyConstraint":
                    cols = tuple(get_const(e) for e in arg.args[0].elts)
                    refs = tuple(get_const(e) for e in arg.args[1].elts)
                    ref_tables = {ref.split(".")[0] for ref in refs}
                    assert len(ref_tables) == 1
                    ref_table = next(iter(ref_tables))
                    ref_cols = tuple(ref.split(".")[1] for ref in refs)
                    data["fks"].append({
                        "name": get_const(kws["name"]) if "name" in kws else None,
                        "columns": cols,
                        "referred_table": ref_table,
                        "referred_columns": ref_cols,
                        "options": {k: get_const(v) for k, v in kws.items() if k == "ondelete"},
                    })
                elif short == "UniqueConstraint":
                    data["uniques"].append({
                        "name": get_const(kws["name"]) if "name" in kws else None,
                        "columns": tuple(get_const(a) for a in arg.args),
                    })
                elif short == "CheckConstraint":
                    sqltext = get_const(arg.args[0]) if arg.args else None
                    data["checks"].append({
                        "name": get_const(kws["name"]) if "name" in kws else None,
                        "sqltext": normalize_sql(sqltext),
                    })
                elif short == "_enum_check":
                    check_name = get_const(arg.args[0])
                    col_name = get_const(arg.args[1])
                    enum_name = get_name(arg.args[2])
                    nullable = False
                    if len(arg.args) > 3:
                        nullable = bool(get_const(arg.args[3]))
                    if "nullable" in kws:
                        nullable = bool(get_const(kws["nullable"]))
                    quoted = enum_sql_map[enum_name]
                    sql = f"{col_name} IS NULL OR {col_name} IN ({quoted})" if nullable else f"{col_name} IN ({quoted})"
                    data["checks"].append({"name": check_name, "sqltext": normalize_sql(sql)})
            schema[table_name] = data
        elif name == "op.create_index":
            idx_name = get_const(call.args[0])
            table_name = get_const(call.args[1])
            cols = tuple(get_const(e) for e in call.args[2].elts)
            pending_indexes.setdefault(table_name, []).append({
                "name": idx_name,
                "columns": cols,
                "unique": False,
            })

    for table_name, idxs in pending_indexes.items():
        schema[table_name]["indexes"].extend(sorted(idxs, key=lambda x: (x["name"], x["columns"])))

    for table_name, data in schema.items():
        data["pk"]["columns"] = tuple(data["pk"]["columns"])
        data["fks"] = sorted(data["fks"], key=lambda x: (x["name"] or "", x["columns"]))
        data["uniques"] = sorted(data["uniques"], key=lambda x: (x["name"] or "", x["columns"]))
        data["checks"] = sorted(data["checks"], key=lambda x: (x["name"] or "", x["sqltext"] or ""))
    return schema


def parse_mapped_column(call: ast.Call) -> dict:
    kws = kw_map(call)
    type_node = None
    fk_node = None
    for arg in call.args:
        if isinstance(arg, ast.Call) and (call_name(arg) or "").split(".")[-1] == "ForeignKey":
            fk_node = arg
        elif type_node is None:
            type_node = arg
    primary_key = bool(get_const(kws["primary_key"])) if "primary_key" in kws else False
    nullable = get_const(kws["nullable"]) if "nullable" in kws else None
    if primary_key:
        nullable = False
    default = unparse(kws["server_default"]) if "server_default" in kws else None
    fk = None
    if fk_node is not None:
        fk = parse_foreign_key_call(fk_node)
    return {
        "type": parse_type_expr(type_node),
        "nullable": True if nullable is None else bool(nullable),
        "primary_key": primary_key,
        "default": default,
        "fk": fk,
    }


def parse_orm_class(class_node: ast.ClassDef) -> tuple[str | None, dict]:
    table_name = None
    columns = {}
    pk_cols = []
    fks = []
    uniques = []
    indexes = []
    checks = []

    for stmt in class_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "__tablename__":
                    table_name = get_const(stmt.value)
                if isinstance(target, ast.Name) and target.id == "__table_args__":
                    if isinstance(stmt.value, ast.Tuple):
                        elements = stmt.value.elts
                    else:
                        elements = [stmt.value]
                    for elt in elements:
                        if not isinstance(elt, ast.Call):
                            continue
                        name = call_name(elt)
                        short = name.split(".")[-1] if name else ""
                        kws = kw_map(elt)
                        if short == "UniqueConstraint":
                            uniques.append({
                                "name": get_const(kws["name"]) if "name" in kws else None,
                                "columns": tuple(get_const(a) for a in elt.args),
                            })
                        elif short == "CheckConstraint":
                            checks.append({
                                "name": get_const(kws["name"]) if "name" in kws else None,
                                "sqltext": normalize_sql(get_const(elt.args[0]) if elt.args else None),
                            })
                        elif short == "Index":
                            indexes.append({
                                "name": get_const(elt.args[0]),
                                "columns": tuple(get_const(a) for a in elt.args[1:]),
                                "unique": bool(get_const(kws["unique"])) if "unique" in kws else False,
                            })
                        elif short == "enum_check":
                            check_name = get_const(elt.args[0])
                            col_name = get_const(elt.args[1])
                            enum_name = get_name(elt.args[2])
                            nullable = False
                            if len(elt.args) > 3:
                                nullable = bool(get_const(elt.args[3]))
                            if "nullable" in kws:
                                nullable = bool(get_const(kws["nullable"]))
                            enum_file = ROOT / "models" / "enums.py"
                            enum_values = parse_enum_values(enum_file)[enum_name]
                            quoted = ", ".join(f"'{value}'" for value in enum_values)
                            sql = f"{col_name} IS NULL OR {col_name} IN ({quoted})" if nullable else f"{col_name} IN ({quoted})"
                            checks.append({"name": check_name, "sqltext": normalize_sql(sql)})

        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            if isinstance(stmt.value, ast.Call) and (call_name(stmt.value) or "").split(".")[-1] == "mapped_column":
                parsed = parse_mapped_column(stmt.value)
                col_name = stmt.target.id
                columns[col_name] = {
                    "type": parsed["type"],
                    "nullable": parsed["nullable"],
                    "default": parsed["default"],
                }
                if parsed["primary_key"]:
                    pk_cols.append(col_name)
                if parsed["fk"]:
                    fk = parsed["fk"]
                    fk["columns"] = (col_name,)
                    fks.append(fk)

    data = {
        "columns": columns,
        "pk": {"columns": tuple(pk_cols)},
        "fks": sorted(fks, key=lambda x: (x["name"] or "", x["columns"])),
        "uniques": sorted(uniques, key=lambda x: (x["name"] or "", x["columns"])),
        "indexes": sorted(indexes, key=lambda x: (x["name"] or "", x["columns"], x["unique"])),
        "checks": sorted(checks, key=lambda x: (x["name"] or "", x["sqltext"] or "")),
    }
    return table_name, data


def parse_enum_values(path: Path) -> dict[str, tuple[str, ...]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    enum_members: dict[str, tuple[str, ...]] = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            values = []
            for stmt in node.body:
                if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                    val = get_const(stmt.value)
                    if isinstance(val, str):
                        values.append(val)
            if values:
                enum_members[node.name.upper() + "S"] = tuple(values)
                enum_members[node.name] = tuple(values)
    aliases = {}
    aliases["PROCESSING_STATES"] = enum_members["ProcessingState"]
    aliases["OUTCOME_STATUSES"] = enum_members["OutcomeStatus"]
    aliases["MATCH_STATUSES"] = enum_members["MatchStatus"]
    aliases["CARRIER_STRATEGIES"] = enum_members["CarrierStrategy"]
    aliases["EMAIL_EVENT_HANDOFF_STATUSES"] = enum_members["EmailEventHandoffStatus"]
    aliases["HANDOFF_ROW_STATUSES"] = enum_members["HandoffRowStatus"]
    aliases["SEEN_STATUSES"] = enum_members["SeenStatus"]
    aliases["PROCESSING_ERROR_STAGES"] = enum_members["ProcessingErrorStage"]
    aliases["ARTIFACT_KINDS"] = enum_members["ArtifactKind"]
    aliases["ARTIFACT_ROLES"] = enum_members["ArtifactRole"]
    aliases["ARTIFACT_STATUSES"] = enum_members["ArtifactStatus"]
    return aliases


def parse_orm(root: Path) -> dict:
    schema = {}
    for path in sorted((root / "models").glob("*.py")):
        if path.name in {"__init__.py", "enums.py"}:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                table_name, data = parse_orm_class(node)
                if table_name:
                    schema[table_name] = data
    return schema


def compare_named(exp: list[dict], act: list[dict], category: str) -> dict[str, list]:
    if category == "fks":
        key = lambda item: (item.get("name"), tuple(item.get("columns", ())), item.get("referred_table"), tuple(item.get("referred_columns", ())), tuple(sorted((item.get("options") or {}).items())))
    elif category == "checks":
        key = lambda item: (item.get("name"), item.get("sqltext"))
    elif category == "indexes":
        key = lambda item: (item.get("name"), tuple(item.get("columns", ())), bool(item.get("unique")))
    else:
        key = lambda item: (item.get("name"), tuple(item.get("columns", ())))
    exp_map = {key(item): item for item in exp}
    act_map = {key(item): item for item in act}
    return {
        f"missing_{category}": [exp_map[k] for k in sorted(exp_map.keys() - act_map.keys())],
        f"extra_{category}": [act_map[k] for k in sorted(act_map.keys() - exp_map.keys())],
    }


def compare_schema(expected: dict, actual: dict) -> dict:
    diff = {
        "missing_tables": sorted(expected.keys() - actual.keys()),
        "extra_tables": sorted(actual.keys() - expected.keys()),
        "mismatched": {},
    }
    for table in sorted(expected.keys() & actual.keys()):
        exp = expected[table]
        act = actual[table]
        td = {}
        exp_cols = exp["columns"]
        act_cols = act["columns"]

        missing_cols = sorted(exp_cols.keys() - act_cols.keys())
        extra_cols = sorted(act_cols.keys() - exp_cols.keys())
        if missing_cols:
            td["missing_columns"] = missing_cols
        if extra_cols:
            td["extra_columns"] = extra_cols

        mismatched_cols = {}
        for col in sorted(exp_cols.keys() & act_cols.keys()):
            item = {}
            for field in ("type", "nullable", "default"):
                if exp_cols[col].get(field) != act_cols[col].get(field):
                    item[field] = {"expected": exp_cols[col].get(field), "actual": act_cols[col].get(field)}
            if item:
                mismatched_cols[col] = item
        if mismatched_cols:
            td["mismatched_columns"] = mismatched_cols

        if exp["pk"]["columns"] != act["pk"]["columns"]:
            td["pk"] = {"expected": exp["pk"], "actual": act["pk"]}

        for category in ("fks", "uniques", "indexes", "checks"):
            sub = compare_named(exp[category], act[category], category)
            for key, value in sub.items():
                if value:
                    td[key] = value
        if td:
            diff["mismatched"][table] = td
    return diff


def main() -> int:
    expected = parse_migration(MIGRATION_PATH)
    actual = parse_orm(ROOT)
    diff = compare_schema(expected, actual)
    green = not diff["missing_tables"] and not diff["extra_tables"] and not diff["mismatched"]
    print(json.dumps({"status": "GREEN" if green else "RED", **diff}, ensure_ascii=False, indent=2))
    return 0 if green else 1


if __name__ == "__main__":
    raise SystemExit(main())
