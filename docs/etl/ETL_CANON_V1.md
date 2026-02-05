# ETL CANON v1 — NDJSON-first + SSOT

## 0. Назначение
Каноническая спецификация промышленного ETL-контура. Документ обязателен для разработки, аудита и эксплуатации. Любое отклонение считается дефектом.

---

## 1. Архитектурный принцип
Контур: Emitter → Gate → Ingestion → Curated / Offers  
SSOT — журнал фактов. Бизнес-логика запрещена до Curated.

---

## 2. Emitter

### 2.1 Ответственность
- Чтение входного файла (v1: только XLS, xlrd==1.2.0)
- Применение mapping.yaml
- Генерация NDJSON
- Фиксация проблем через flags
- Никаких бизнес-решений

### 2.2 effective_at
- Обязательный ISO 8601 / RFC3339: `YYYY-MM-DDTHH:MM:SSZ`
- Если не задан → CRITICAL ошибка
- Оркестратор обязан задавать один effective_at на логический раунд
- Ретраи обязаны сохранять original effective_at

### 2.3 Ошибки
Любая ошибка чтения/парсинга:
- CRITICAL лог
- exit code ≠ 0

### 2.4 Пустые строки
- Строки, где все mapped-поля пусты → SKIP
- В NDJSON не пишутся
- Учитываются только в stats-out: skipped_rows_all_empty
- exploded_lines == 0 → Gate FAIL

### 2.5 Нормализация SKU
- Unicode NFKC
- Удаление zero-width
- trim + collapse whitespace
- Без эвристик (/, -, спецсимволы не трогаем)

---

## 3. Mapping

### 3.1 Общие правила
- `version` = mapping_schema_version
- `mapping_version` обязателен и монотонен (контролируется CI)
- Любое изменение → новый mapping_version

### 3.2 header_row_offset
- Смещение от data_start_row_1based
- target_row = data_start_row_1based + offset
- Отрицательное = выше данных

### 3.3 header_probe (опционально)
```yaml
header_probe:
  - {row: 1, column: 1, expected: "Артикул", match: "exact"}
  - {row: 1, column: 2, expected: "Бренд", match: "exact"}
Минимум 2 точки
mismatch → Gate FAIL
4. NDJSON контракт
Копировать код
Json
{
  "ndjson_contract_version": "1.0",
  "supplier_id": "...",
  "parser_id": "...",
  "mapping_version": "...",
  "mapping_hash": "sha256",
  "run_id": "...",
  "effective_at": "2026-02-04T10:15:00Z",
  "sku_candidate_key": "...",
  "raw": {
    "supplier_article": "...",
    "price_raw": "...",
    "qty_raw": "...",
    "supplier_warehouse_name": "..."
  },
  "parsed": {
    "price": 123456,
    "qty": 10
  },
  "quality_flags": [],
  "_meta": {
    "source_row_number": 1
  }
}
currency отсутствует: всегда RUB
price → копейки, Decimal, ROUND_HALF_UP
qty → int или null
source_row_number → 1-based
5. Quality Flags
FAIL:
bad_json
bad_qty
qty_fractional
negative_qty
WARN:
missing_price
price_textual
zero_price
negative_price
warehouse_name_empty
qty_zero
price_out_of_range
qty_out_of_range
INFO:
price_fractional_discarded
Флаги не каскадируются. Первый по приоритету.
6. Gate
FAIL если:
exploded_lines == 0
bad_json > 0
qty_fractional > 0
negative_qty > 0
explosion_factor_exact > 50
exploded_lines > 5_000_000
unique_sku_count > 50_000
total_lines < 0.7 × baseline (если baseline есть)
WARN:
negative_price > 0 (high-priority)
missing_price > 5%
new_warehouses_count > 3
data_density < 0.1
baseline_missing → WARN
7. Ingestion
7.1 Принципы
SSOT = append-only журнал
Уникальности нет
Дубликаты допустимы
7.2 Склады
normalized_name = normalize(raw.supplier_warehouse_name)
Если пусто → warehouse_key="unreviewed:{supplier_id}"
В warehouse_aliases запрещён normalized_name=""
История не переписывается
7.3 Идемпотентность
run_id уникален
distributed lock на run_id
Повтор → очистка незасиленного snapshot
8. Curated / Offers
Оффер создаётся если:
parsed.price > 0
parsed.qty > 0
snapshot PASS/WARN или forced
unreviewed / hold → офферы запрещены
9. Версионирование
breaking NDJSON → новый ndjson_contract_version И новый parser_id
смена библиотеки / логики → новый parser_id
mapping правки → новый mapping_version
10. Hardening (обязательно v1)
max_file_bytes = 100MB
max_rows = 65000
max_cols = 256
max_warehouses = 64
explosion_factor FAIL > 50
11. Monitoring
отсутствие прогонов >24ч считается от последнего PASS/WARN
explosion_factor в UI округлён, проверки — по exact
алерты по трендам (degradation)
12. Границы ответственности
Emitter фиксирует факты
Gate решает допуск
Ingestion хранит
Curated думает
