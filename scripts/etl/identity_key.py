#!/usr/bin/env python3
"""
identity_key.py — создаёт детерминированный ключ для группировки товаров
"""

import hashlib
import re


def normalize_brand(brand: str) -> str:
    """Нормализует название бренда"""
    if not brand:
        return ""
    # Приводим к нижнему регистру, убираем лишние пробелы
    brand = brand.lower().strip()
    # Убираем распространённые суффиксы
    brand = re.sub(r'\s*(llc|inc|ltd|ооо|ип|зао)\s*$', '', brand)
    # Убираем неалфавитные символы
    brand = re.sub(r'[^a-zа-яё0-9]', '', brand)
    return brand


def normalize_season(season: str) -> str:
    """Нормализует сезон"""
    if not season:
        return ""
    season = season.lower().strip()
    if "зим" in season:
        return "winter"
    if "лет" in season:
        return "summer"
    if "всесез" in season:
        return "all_season"
    return "unknown"


def build_identity_key(row: dict) -> str:
    """
    Создаёт уникальный ключ для товара на основе:
    - бренд
    - ширина
    - высота
    - диаметр
    - сезон
    - модель (если есть)
    """
    brand = normalize_brand(row.get("brand", ""))
    width = row.get("width")
    height = row.get("height")
    diameter = row.get("diameter")
    season = normalize_season(row.get("season", ""))
    model = row.get("model", "") or ""
    
    # Нормализуем модель (обрезаем лишнее)
    if model:
        model = model.lower().strip()
        # Убираем размеры из модели, если они там есть
        model = re.sub(r'\d{3}/\d{2}R?\d{2}', '', model)
        model = re.sub(r'\d{2,3}[A-Z]', '', model)
        model = re.sub(r'\s+', ' ', model).strip()
    
    # Собираем компоненты ключа
    parts = []
    if brand:
        parts.append(f"br:{brand}")
    if width:
        parts.append(f"w:{width}")
    if height:
        parts.append(f"h:{height}")
    if diameter:
        parts.append(f"d:{diameter}")
    if season:
        parts.append(f"s:{season}")
    if model:
        parts.append(f"m:{model[:30]}")  # ограничиваем длину
    
    # Если ключ пустой — возвращаем None
    if not parts:
        return None
    
    key_str = "|".join(parts)
    return hashlib.sha256(key_str.encode()).hexdigest()[:16]


if __name__ == "__main__":
    # Тест
    test_row = {
        "brand": "Armstrong",
        "width": 235,
        "height": 55,
        "diameter": 20,
        "season": "Зима",
        "model": "Armstrong 235/55R20 102T Ski-Trac S"
    }
    print(f"Test key: {build_identity_key(test_row)}")
