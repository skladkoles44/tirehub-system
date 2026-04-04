#!/usr/bin/env python3
"""
layout_fingerprint.py — production-grade self-learning ingestion engine
CONSISTENT: lock на весь read→modify→write цикл
"""

import hashlib
import json
import os
import fcntl
import sys
from pathlib import Path
from datetime import datetime, timezone

REGISTRY_DIR = Path(os.environ["ETL_VAR_ROOT"]) / "registry"
REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
CACHE_PATH = REGISTRY_DIR / "layout_schemas.json"
LOCK_PATH = CACHE_PATH.with_suffix(".lock")

MAX_SCHEMAS = 500
SCHEMA_VERSION = 1


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def norm_cell(x):
    """Умная нормализация ячейки для стабильного fingerprint"""
    if x is None:
        return ""
    
    s = str(x).strip().lower().replace(",", ".")
    
    # Пробуем нормализовать как число
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
        # Округляем до 6 знаков для стабильности
        return str(round(f, 6))
    except ValueError:
        pass
    
    return s


def fsync_dir(path: Path):
    """Гарантированная синхронизация директории"""
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except Exception:
        pass


def load_schema_cache():
    """Загружает кэш без блокировки (только чтение)"""
    if not CACHE_PATH.exists():
        return {"version": SCHEMA_VERSION, "schemas": {}}
    try:
        with open(CACHE_PATH) as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            fcntl.flock(f, fcntl.LOCK_UN)
            
            if data.get("version") != SCHEMA_VERSION:
                print(f"⚠️ Schema cache version mismatch: {data.get('version')} -> {SCHEMA_VERSION}", file=sys.stderr)
                return {"version": SCHEMA_VERSION, "schemas": {}}
            return data
    except json.JSONDecodeError as e:
        print(f"⚠️ Schema cache corrupted (JSON): {e}", file=sys.stderr)
        return {"version": SCHEMA_VERSION, "schemas": {}}
    except Exception as e:
        print(f"⚠️ Schema cache error: {e}", file=sys.stderr)
        return {"version": SCHEMA_VERSION, "schemas": {}}


def save_schema_cache(data):
    """Сохраняет кэш с полной блокировкой от lost update"""
    # Открываем lock-файл
    with open(LOCK_PATH, "w") as lockf:
        fcntl.flock(lockf, fcntl.LOCK_EX)
        
        # Перезагружаем текущее состояние внутри блокировки
        current = load_schema_cache()
        schemas = current.get("schemas", {})
        
        # Обновляем новыми данными
        new_schemas = data.get("schemas", {})
        schemas.update(new_schemas)
        
        # Очистка старых схем
        if len(schemas) > MAX_SCHEMAS:
            schemas = dict(sorted(
                schemas.items(),
                key=lambda x: x[1].get("used_count", 0),
                reverse=True
            )[:MAX_SCHEMAS])
        
        current["version"] = SCHEMA_VERSION
        current["schemas"] = schemas
        current["updated_at"] = now_utc()
        
        # Атомарная запись
        tmp = CACHE_PATH.with_suffix(".tmp")
        with open(tmp, 'w') as f:
            json.dump(current, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        
        tmp.replace(CACHE_PATH)
        
        # ULTRA-SAFE: fsync директории
        fsync_dir(CACHE_PATH.parent)
        
        fcntl.flock(lockf, fcntl.LOCK_UN)


def non_empty_score(ws, max_rows=20):
    """Считает реально заполненные ячейки в первых строках"""
    cnt = 0
    for row in ws.iter_rows(min_row=1, max_row=max_rows, values_only=True):
        cnt += sum(1 for c in row if c not in (None, "", " "))
    return cnt


def get_best_sheet(wb):
    """Выбирает лист с наибольшим количеством данных"""
    sheets = wb.worksheets
    if not sheets:
        return None
    best = max(sheets, key=non_empty_score)
    return best


def calculate_fingerprint(ws, sample_rows=15):
    """Создаёт надёжный отпечаток структуры"""
    if ws is None:
        return None, None
    
    sample_rows = min(sample_rows, ws.max_row)
    
    # Собираем первые sample_rows строк с нормализацией
    rows = []
    for row in ws.iter_rows(min_row=1, max_row=sample_rows, values_only=True):
        rows.append([norm_cell(x) for x in row])
    
    if not rows:
        return None, None
    
    # Ищем строку с заголовками
    header_idx = 0
    keywords = ["артикул", "sku", "наименование", "name", "цена", "price", 
                "бренд", "brand", "модель", "model", "остаток", "stock"]
    
    for i, r in enumerate(rows[:5]):
        text = " ".join(r)
        if any(kw in text for kw in keywords):
            header_idx = i
            break
    
    header = rows[header_idx] if rows else []
    norm_header = [h.strip().lower() for h in header[:15]]
    
    # Определяем роли колонок
    roles = []
    for cell in header[:20]:
        cell_lower = cell.lower()
        role = "unknown"
        if "артикул" in cell_lower or "sku" in cell_lower:
            role = "sku"
        elif "наименование" in cell_lower or "name" in cell_lower:
            role = "name"
        elif "бренд" in cell_lower or "brand" in cell_lower:
            role = "brand"
        elif "цена" in cell_lower or "price" in cell_lower:
            if "опт" in cell_lower or "wholesale" in cell_lower:
                role = "price_wholesale"
            elif "розн" in cell_lower or "retail" in cell_lower:
                role = "price_retail"
            else:
                role = "price"
        elif "остаток" in cell_lower or "stock" in cell_lower:
            role = "stock"
        roles.append(role)
    
    col_count = len(header)
    
    # Маппинг с поддержкой множественных колонок
    mapping = {}
    for idx, role in enumerate(roles):
        if role != "unknown":
            mapping.setdefault(role, []).append(idx)
    
    # Энтропия из данных
    sample_flat = ""
    sample_start = header_idx + 1 if header_idx + 1 < len(rows) else header_idx
    if sample_start < len(rows):
        sample_rows_slice = rows[sample_start:sample_start+2]
        flat = []
        for r in sample_rows_slice:
            flat.extend(str(c)[:30] for c in r[:5])
        sample_flat = "|".join(flat)
    
    # Создаём fingerprint
    fp_str = (
        "|".join(norm_header) +
        "|" +
        "|".join(sorted(set(roles))) +
        "|" +
        sample_flat +
        f":{col_count}"
    )
    fp_hash = hashlib.sha256(fp_str.encode()).hexdigest()[:16]
    
    fp_data = {
        "header_idx": header_idx,
        "col_count": col_count,
        "header": header[:20],
        "roles": roles[:15],
        "mapping": mapping,
        "sample_rows": rows[:5],
        "shape": f"{ws.max_row}x{ws.max_column}",
        "sample_entropy": sample_flat[:100]
    }
    
    return fp_hash, fp_data


def detect_drift(cached, actual):
    """Проверяет изменение структуры с учётом позиций"""
    required_roles = {"sku"}
    
    cached_roles = {r for r in cached.get("roles", []) if r != "unknown"}
    actual_roles = {r for r in actual.get("roles", []) if r != "unknown"}
    
    # Должен быть sku
    if not required_roles.issubset(actual_roles):
        return True, "missing_required_sku"
    
    # Проверка количества колонок
    cached_cols = cached.get("col_count", 0)
    actual_cols = actual.get("col_count", 0)
    if actual_cols < cached_cols * 0.5:
        return True, "col_count_dropped"
    
    # Проверка потери ролей
    if not cached_roles.issubset(actual_roles):
        lost = cached_roles - actual_roles
        return True, f"roles_lost: {lost}"
    
    # Проверка позиций ключевых колонок
    cached_map = cached.get("mapping", {})
    actual_map = actual.get("mapping", {})
    
    for role in ["sku", "price", "stock"]:
        if role in cached_map and role in actual_map:
            cached_pos = cached_map[role][0] if isinstance(cached_map[role], list) else cached_map[role]
            actual_pos = actual_map[role][0] if isinstance(actual_map[role], list) else actual_map[role]
            if abs(cached_pos - actual_pos) > 1:
                return True, f"{role}_moved"
    
    return False, None


def get_or_create_schema(ws, supplier_id=None):
    """Возвращает схему (из кэша или новую)"""
    cache_data = load_schema_cache()
    schemas = cache_data.get("schemas", {})
    
    fp, fp_data = calculate_fingerprint(ws)
    
    if fp is None:
        return None, None
    
    if fp in schemas:
        cached = schemas[fp]
        drift, reason = detect_drift(cached, fp_data)
        if not drift:
            cached["used_count"] = cached.get("used_count", 0) + 1
            save_schema_cache(cache_data)
            return cached, fp
        else:
            print(f"  ⚠️ Drift detected: {reason}", file=sys.stderr)
    
    # Новая схема
    schema = {
        "fingerprint": fp,
        "header_idx": fp_data["header_idx"],
        "col_count": fp_data["col_count"],
        "header": fp_data["header"],
        "roles": fp_data["roles"],
        "mapping": fp_data["mapping"],
        "supplier": supplier_id,
        "shape": fp_data.get("shape"),
        "created_at": now_utc(),
        "used_count": 1
    }
    
    schemas[fp] = schema
    save_schema_cache(cache_data)
    
    return schema, fp
