#!/usr/bin/env python3
"""
identity_key_v2.py — детерминированное построение identity_key
- SKU fast path (максимальная точность)
- Derived path для товаров без SKU
- Семантическая нормализация размеров (сохраняет структуру)
- Транслитерация бренда и модели (рус → латиница)
- BRAND_MAP + fuzzy matching с lru_cache
- Детерминированный empty derived с bounded fallback
- Явная UTF-8 кодировка для hash
- Salt для derived ключей
- Версионирование identity (IDENTITY_SCHEMA_V)
- ИДЕАЛЬНАЯ ФОРМУЛА: identity = brand + model + width + height + diameter
- variant_key для load_index, speed_index, xl, runflat (атрибуты)
- base variant_key: стабильный (identity_key + "base") для детерминизма
- unknown_ модель: включает brand для уменьшения коллизий
- XL detection: стерильный regex (не ловит xl внутри слова)
- speed_index: строго буквы без мусора
- variant_key hash: 16 символов для надёжности
- strength: high при наличии brand+model+diameter
"""

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Tuple, Dict, Any, Optional, Set
from difflib import get_close_matches
from functools import lru_cache


# Версия схемы identity
IDENTITY_SCHEMA_V = "v2"

# Salt для derived ключей
DERIVED_PREFIX = "d2"


# Транслитерация русских символов
RU_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
    "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i",
    "й": "i", "к": "k", "л": "l", "м": "m", "н": "n",
    "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "h", "ц": "c", "ч": "ch",
    "ш": "sh", "щ": "sh", "ы": "y", "э": "e", "ю": "u", "я": "ya"
}

# Базовый канонический маппинг брендов
BRAND_MAP_BASE = {
    "mishlen": "michelin",
    "michelinn": "michelin",
    "michelim": "michelin",
    "michelan": "michelin",
    "bridzston": "bridgestone",
    "brigestone": "bridgestone",
    "bridgeston": "bridgestone",
    "goodyeаr": "goodyear",
    "goodyeaг": "goodyear",
    "conti": "continental",
    "contine": "continental",
    "pirelly": "pirelli",
    "пирелли": "pirelli",
    "hankok": "hankook",
    "nokiann": "nokian",
    "yokohаma": "yokohama",
    "mаtador": "matador",
}

# Известные канонические бренды для fuzzy matching (стабильный порядок)
KNOWN_BRANDS: Set[str] = {
    "michelin", "bridgestone", "goodyear", "continental", "pirelli",
    "hankook", "nokian", "yokohama", "toyo", "matador", "kumho",
    "dunlop", "bfgoodrich", "falken", "general", "gtradial", "kleber",
    "maxxis", "nexen", "roadstone", "sava", "tigar", "vredestein",
    "barum", "firestone", "fulda", "gislaved", "kingstar", "mabor",
}
# Сортированный список для детерминизма
KNOWN_BRANDS_SORTED = sorted(KNOWN_BRANDS)


def translit(text: str) -> str:
    """
    Транслитерация русских символов в латиницу
    """
    if not text:
        return ""
    result = []
    for c in text.lower():
        result.append(RU_MAP.get(c, c))
    return "".join(result)


def normalize_text(value: Any, max_len: int = 0) -> str:
    """
    Нормализация текстовых полей (brand, model)
    """
    if value is None:
        return ""
    text = translit(str(value))
    cleaned = re.sub(r'[^a-z0-9]', '', text.lower())
    if max_len > 0 and len(cleaned) > max_len:
        cleaned = cleaned[:max_len]
    return cleaned


@lru_cache(maxsize=10000)
def canonicalize_brand(brand: str) -> str:
    """
    Канонизация бренда с lru_cache
    """
    if not brand:
        return ""
    
    if brand in BRAND_MAP_BASE:
        return BRAND_MAP_BASE[brand]
    
    if len(brand) >= 5:
        matches = get_close_matches(brand, KNOWN_BRANDS_SORTED, n=1, cutoff=0.9)
        if matches:
            return matches[0]
    
    return brand


def normalize_brand(value: Any) -> str:
    """
    Нормализация бренда
    """
    if value is None:
        return ""
    brand = normalize_text(value)
    brand = canonicalize_brand(brand)
    return brand


def normalize_size(width: Any, height: Any, diameter: Any) -> Tuple[str, str, str]:
    """
    Семантическая нормализация размеров
    """
    def extract_digits(v: Any) -> str:
        if v is None:
            return ""
        return re.sub(r'\D', '', str(v))
    
    w = extract_digits(width)
    h = extract_digits(height) if height else ""
    d = extract_digits(diameter)
    
    return w, h, d


def normalize_sku(value: Any) -> str:
    """
    Нормализация SKU
    """
    if value is None:
        return ""
    text = translit(str(value))
    return re.sub(r'[^a-z0-9]', '', text.lower())


def build_variant_key(row: Dict[str, Any], identity_key: str = "", 
                       brand: str = "", w: str = "", h: str = "", d: str = "") -> str:
    """
    Строит ключ варианта товара (атрибуты, не влияющие на identity)
    - load_index (цифры)
    - speed_index (только буквы, без мусора)
    - xl (extra load) — стерильный regex
    - runflat — только явные "runflat" или "run flat"
    base variant_key: стабильный на основе identity_key (ПРОФЕССИОНАЛЬНЫЙ ЧЕК #1 fixed)
    """
    # load_index
    li = re.sub(r'\D', '', str(row.get("load_index") or ""))
    
    # speed_index — строго буквы, без мусора
    si_raw = row.get("speed_index")
    si = ""
    if si_raw:
        si = re.sub(r'[^a-z]', '', str(si_raw).lower())
    
    # flags: xl, runflat — проверяем по model и name
    flags = []
    
    model = str(row.get("model") or "").lower()
    name = str(row.get("name") or "").lower()
    text = f"{model} {name}"
    
    # XL detection — стерильный regex (не ловит xl внутри слова)
    if re.search(r'(?<![a-z])xl(?![a-z])', text):
        flags.append("xl")
    elif "extra load" in text or "extraload" in text:
        flags.append("xl")
    
    # runflat detection — только явные вхождения
    if "runflat" in text or "run flat" in text:
        flags.append("rf")
    
    flags_str = "|".join(sorted(flags))
    
    raw = f"{li}|{si}|{flags_str}"
    
    # Стабильный base ключ (ПРОФЕССИОНАЛЬНЫЙ ЧЕК #1 fixed)
    if raw == "||":
        if identity_key:
            return f"base:{identity_key}"
        # fallback если identity_key ещё не построен
        return "base"
    
    # 16 символов для надёжности
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def build_identity_key(row: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    """
    Строит identity_key и variant_key
    Возвращает: (key, method, strength, raw_identity, variant_key)
    
    ИДЕАЛЬНАЯ ФОРМУЛА:
    identity = brand + model + width + height + diameter
    variant = load_index + speed_index + flags
    """
    supplier_id = normalize_text(row.get("supplier_id")) or "unknown"
    supplier_sku = row.get("supplier_sku")
    
    # FAST PATH
    if supplier_sku:
        key = f"{supplier_id}:{normalize_sku(supplier_sku)}"
        variant = build_variant_key(row, identity_key=key)
        return key, "sku", "high", normalize_sku(supplier_sku), variant
    
    # DERIVED PATH
    w, h, d = normalize_size(
        row.get("width"),
        row.get("height"),
        row.get("diameter"),
    )
    
    brand = normalize_brand(row.get("brand"))
    model = normalize_text(row.get("model"), max_len=50)
    
    # Fallback модели на размер + brand (ПРОФЕССИОНАЛЬНЫЙ ЧЕК #2 fixed)
    if not model and w and d:
        model = f"unknown_{w}{h}{d}_{brand}" if brand else f"unknown_{w}{h}{d}"
    
    # ИДЕАЛЬНАЯ ФОРМУЛА: только core поля
    parts = [
        brand,
        model,
        w,
        h,
        d,
    ]
    
    # Empty derived
    if all(p == "" for p in parts):
        source_file = row.get("source_file", "")
        row_index = row.get("row_index", "")
        row_id = row.get("row_id")
        
        if not row_id and (source_file or row_index):
            row_id = f"{source_file}:{row_index}"
        
        if not row_id:
            fallback = json.dumps(row, sort_keys=True, ensure_ascii=False)
            if len(fallback) > 1000:
                fallback = fallback[:1000]
            row_id = fallback
        
        raw = f"{IDENTITY_SCHEMA_V}|empty|{supplier_id}|{row_id}"
        h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
        key = f"derived:{DERIVED_PREFIX}:{h}"
        variant = build_variant_key(row, identity_key=key)
        return key, "derived", "low", raw[:200], variant
    
    # Strength: если есть core поля (brand + model + diameter) -> high
    has_core = bool(brand and model and d)
    
    if has_core:
        strength = "high"
    else:
        filled = sum(1 for p in parts if p)
        if filled >= 3:
            strength = "medium"
        else:
            strength = "low"
    
    # IMPORTANT: raw НЕ обрезается до hash
    raw = f"{IDENTITY_SCHEMA_V}|" + "|".join(parts)
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    key = f"derived:{DERIVED_PREFIX}:{h}"
    
    variant = build_variant_key(row, identity_key=key, brand=brand, w=w, h=h, d=d)
    
    return key, "derived", strength, raw[:200], variant


def process_file(inp: Path, out: Path) -> None:
    processed = 0
    skipped = 0
    sku_count = 0
    derived_count = 0
    empty_derived_count = 0
    strength_high = 0
    strength_medium = 0
    strength_low = 0
    variant_base = 0
    variant_custom = 0
    
    with inp.open('r', encoding='utf-8') as f_in, \
         out.open('w', encoding='utf-8') as f_out:
        
        for line_num, line in enumerate(f_in, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"JSON error at line {line_num}: {e}", file=sys.stderr)
                skipped += 1
                continue
            
            key, method, strength, raw, variant = build_identity_key(obj)
            obj["identity_key"] = key
            obj["identity_method"] = method
            obj["identity_strength"] = strength
            obj["_identity_raw"] = raw
            obj["variant_key"] = variant
            
            if variant.startswith("base:"):
                variant_base += 1
            else:
                variant_custom += 1
            
            if method == "sku":
                sku_count += 1
            else:
                derived_count += 1
                if raw.startswith(f"{IDENTITY_SCHEMA_V}|empty|"):
                    empty_derived_count += 1
            
            if strength == "high":
                strength_high += 1
            elif strength == "medium":
                strength_medium += 1
            else:
                strength_low += 1
            
            f_out.write(json.dumps(obj, ensure_ascii=False) + "\n")
            processed += 1
    
    print(f"identity_key_v2: processed={processed}, skipped={skipped}", file=sys.stderr)
    print(f"  method: sku={sku_count}, derived={derived_count}", file=sys.stderr)
    print(f"    derived: empty={empty_derived_count}", file=sys.stderr)
    print(f"  strength: high={strength_high}, medium={strength_medium}, low={strength_low}", file=sys.stderr)
    print(f"  variant_key: base={variant_base}, custom={variant_custom}", file=sys.stderr)
    print(f"  brand_cache_size: {canonicalize_brand.cache_info().currsize}", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: identity_key_v2.py <input.ndjson> <output.ndjson>", file=sys.stderr)
        sys.exit(1)
    
    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])
    
    if not inp.exists():
        print(f"error: input file not found: {inp}", file=sys.stderr)
        sys.exit(1)
    
    out.parent.mkdir(parents=True, exist_ok=True)
    process_file(inp, out)
