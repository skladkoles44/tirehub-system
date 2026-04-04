#!/usr/bin/env python3
"""
size_extractor.py — извлекает размеры и индексы из текста
"""

import re

SIZE_RE = re.compile(r"(\d{3})/(\d{2})R?(\d{2})", re.IGNORECASE)
INDEX_RE = re.compile(r"\b(\d{2,3})([A-Z])\b")
WIDTH_RE = re.compile(r"\b(\d{3})\b")
DIAMETER_RE = re.compile(r"\bR?(\d{2})\b", re.IGNORECASE)


def extract_size(text):
    """Извлекает width/height/diameter из строки типа 235/55R20"""
    if not text:
        return {}
    m = SIZE_RE.search(text)
    if m:
        return {
            "width": int(m.group(1)),
            "height": int(m.group(2)),
            "diameter": int(m.group(3)),
        }
    return {}


def extract_index(text):
    """Извлекает load_index и speed_index из строки типа 102T"""
    if not text:
        return {}
    m = INDEX_RE.search(text)
    if m:
        return {
            "load_index": int(m.group(1)),
            "speed_index": m.group(2),
        }
    return {}


def extract_width(text):
    """Извлекает ширину из текста"""
    if not text:
        return None
    m = WIDTH_RE.search(text)
    if m:
        return int(m.group(1))
    return None


def extract_diameter(text):
    """Извлекает диаметр из текста"""
    if not text:
        return None
    m = DIAMETER_RE.search(text)
    if m:
        return int(m.group(1))
    return None


def enrich_from_name(row):
    """Обогащает строку данными из name"""
    name = row.get("name")
    if not name:
        return row
    
    # Извлекаем размер
    size = extract_size(name)
    for k, v in size.items():
        if not row.get(k):
            row[k] = v
    
    # Извлекаем индексы
    idx = extract_index(name)
    for k, v in idx.items():
        if not row.get(k):
            row[k] = v
    
    # Если size не найден, пробуем отдельно
    if not row.get("width"):
        w = extract_width(name)
        if w:
            row["width"] = w
    if not row.get("diameter"):
        d = extract_diameter(name)
        if d:
            row["diameter"] = d
    
    return row
