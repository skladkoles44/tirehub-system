# 📦 atomic_rows_v1 — Contract (L0 → L1)

---

## 1. TL;DR
Жёсткий контракт между слоями:

- L0 (Runner) = extraction only  
- L1 (Normalizer) = normalization + business logic  

Никакой логики в L0. Вообще.

---

## 2. Scope
Контракт описывает формат данных:
Runner → atomic_rows.ndjson → Normalizer

Цель:
- стабилизировать интерфейс
- исключить скрытую логику
- обеспечить масштабируемость

---

## 3. Format
- NDJSON (UTF-8)
- 1 line = 1 JSON object
- empty lines allowed (ignored)

---

## 4. Data Model

### Top-level (REQUIRED)
- source_file: str              # MUST be non-empty
- columns: list[object]         # MUST exist AND not empty

### Nullable
- sheet_name: str | null
- table_index: int | null
- row_index: int | null
- fingerprint: str | null

---

### columns[]
Each element:

- index: int                      # REQUIRED
- role: str                       # REQUIRED (never null)
- value: any                      # REQUIRED (can be null)
- header: str | null              # raw header
- name: str | null                # flattened header

---

## 5. Invariants

- columns exists AND len(columns) > 0  
- index unique внутри строки  
- index SHOULD be 0..N-1 (без дыр)  
- role всегда строка  
- неизвестная роль → "unknown"  
- value может быть любым JSON типом  
- fingerprint SHOULD be deterministic для одинаковых layout  
- row_index MUST быть монотонным (если есть)

---

## 6. Semantics

### L0 (Runner)
Делает только:
- извлечение
- назначение ролей

НЕ делает:
- нормализацию
- бизнес-логику
- reject
- dedup

👉 L0 = “сканер”

---

### L1 (Normalizer)
Делает:
- извлечение бизнес-полей
- нормализацию
- reject
- grouping

👉 L1 = “мозг”

---

## 7. Rationale (почему так)

### Почему L0 тупой
Чтобы:
- не дублировать логику
- не ломать downstream
- упростить подключение новых источников

### Почему unknown обязателен
Чтобы:
- не терять данные
- не ломать парсинг
- позволить системе развиваться

### Почему нельзя нормализовать в L0
Потому что:
- теряется raw truth
- ломается трассировка
- невозможно переобработать данные

---

## 8. Anti-patterns (как ломают систему)

❌ Нормализация в L0  
→ теряется оригинальное значение  

❌ Dedup в L0  
→ ломается агрегация складов  

❌ Reject в L0  
→ данные исчезают до анализа  

❌ Вычисление season/studded  
→ бизнес-логика размазывается  

---

## 9. Role system

Источник:
config/semantic_roles.yaml

Правило:
- если роль не распознана → "unknown"

---

## 10. Source SKU (L1 responsibility)

- primary: role == "sku"
- fallback: article / supplier_sku / future logic

L0 не гарантирует SKU.

---

## 11. Guarantees

- каждая строка — JSON (или игнорируется)
- malformed JSON не ломает файл
- порядок строк сохраняется
- schema стабилен

---

## 12. L1 behavior (как читать)

L1:
- ищет значения по role
- строит canonical object
- формирует canonical_key
- делает grouping

---

## 13. Reject examples (L1)

- missing_sku  
- bad_size_parse  
- width_range / height_range / diameter_range  
- no_brand / no_load / no_speed  

---

## 14. Example

{"source_file":"file.xlsx","sheet_name":"Sheet1","table_index":0,"row_index":1,"fingerprint":"abc123","columns":[{"index":0,"role":"sku","value":"A1","header":"Артикул","name":"Артикул"},{"index":1,"role":"price","value":5000,"header":"Цена","name":"Цена"}]}

---

## 15. Versioning

version: v1  

Любое breaking изменение → новая версия (v2)

---

## Итог

Это не просто формат.

Это граница:
- где заканчивается extraction
- где начинается логика

Нарушение контракта = деградация всей системы.
