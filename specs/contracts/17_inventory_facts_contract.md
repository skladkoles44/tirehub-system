# Inventory Facts Contract

## 1. Purpose

Этот контракт вводит единый **location-aware** выходной слой для supplier inventory.

Цель:
- перестать терять измерение склада;
- перестать хранить остатки как одну общую цифру на товар, когда источник уже содержит location-specific inventory;
- одинаково поддерживать как строки, где склад задан отдельным полем, так и строки, где склад зашит в заголовке колонки.

Контракт нужен для устойчивой обработки supplier files при schema drift и для перехода к inventory-by-location модели.

## 2. Scope

Контракт описывает только extraction-level выход `inventory_facts.ndjson`.

Контракт **не решает**:
- канонизацию складов;
- quarantine для сомнительных labels;
- downstream current-state apply;
- reusable tests;
- reconciliation с `stock_total`;
- ценовые tier-ы.

Это следующие capability layers.

## 3. Upstream assumptions

Источник для v1 может быть:
- `atomic_rows.ndjson`, или
- промежуточный product-normalized output.

Выходной контракт при этом остаётся одинаковым.

## 4. Source patterns

Источник может приходить в одном из двух паттернов.

### 4.1 `explicit_row_location`

Склад задан отдельным полем строки, а количество — отдельным числовым полем.

Пример:
- `Склад = "Склад Москва"`
- `Остаток на складе = "1"`

В этом случае одна data-row обычно уже location-aware и чаще требует **projection**, а не explode.

### 4.2 `implicit_header_location`

Склад зашит в названии колонки, а значение в ячейке — количество.

Пример:
- колонка `Центр. Склад`
- значение `6`

В этом случае одна data-row может порождать `0..N` inventory facts и требует **explode** по warehouse-like колонкам.

## 5. Output file

Файл выхода: `inventory_facts.ndjson`

Формат: одна JSON-запись на один inventory fact.

Один и тот же `supplier_sku` может иметь несколько записей в одном файле, если товар присутствует на нескольких складах.

## 6. Record schema

Каждая запись `inventory_facts.ndjson` должна иметь такую схему.

### Required fields

- `_row_index: int`  
  Индекс исходной строки из upstream layer.

- `supplier_sku: string`  
  Supplier SKU / артикул строки. В v1 допустимо пустое значение только если upstream source реально не содержит SKU.

- `warehouse_raw: string`  
  Сырой label склада без канонизации.

- `qty_raw: string | int | float`  
  Сырой quantity token, извлечённый из source. В v1 это поле **не обязано** быть окончательно нормализованным numeric value.

- `warehouse_source_column: string`  
  Имя source-колонки, из которой взят warehouse label.

- `qty_source_column: string`  
  Имя source-колонки, из которой взято количество.

- `source_pattern: string`  
  Одно из:
  - `explicit_row_location`
  - `implicit_header_location`

### Optional fields

- `warehouse_canonical: string | null`  
  Каноническое имя/ID склада. В v1 может отсутствовать или быть `null`.

## 7. Minimal invariants

Для каждой записи должны выполняться такие инварианты:

- `_row_index` присутствует всегда;
- `source_pattern` присутствует всегда;
- `warehouse_raw` не пустой;
- `qty_raw` не пустой;
- `warehouse_source_column` не пустой;
- `qty_source_column` не пустой;
- `supplier_sku` заполнен везде, где он доступен в upstream source.

## 8. Row eligibility guard

Extractor **не должен** эмитить inventory fact из строк, которые выглядят как:
- повтор заголовка;
- legend/service row;
- не-data row.

Минимальные признаки non-data row для v1:
- `supplier_sku` пустой **и**
- `qty_raw` совпадает с текстом заголовка или не выглядит как quantity token **и**
- строка не содержит других item anchors.

Пример non-data row:
- `Центр. Склад = "Центр. Склад"`
- `Опт = "Цена"`
- `supplier_sku = ""`

Такая строка не должна превращаться в inventory fact.

## 9. Pattern detector

Функция:

`detect_inventory_pattern(headers, sample_rows) -> explicit_row_location | implicit_header_location | unknown`

Detector должен работать на уровне **паттерна листа**, а не по одной колонке в изоляции.

### 9.1 Positive evidence

К положительным сигналам относятся:
- наличие text-like location column;
- наличие qty-like numeric column;
- наличие warehouse-like headers;
- повторяемый профиль location/value across rows.

### 9.2 Negative evidence

К отрицательным сигналам относятся колонки, которые числовые, но принадлежат другой бизнес-семантике, например:
- SKU / product codes;
- цены;
- размеры;
- load index и подобные product attributes.

Detector не должен считать warehouse-candidates только потому, что колонка числовая.

### 9.3 Rule for `explicit_row_location`

Возвращать `explicit_row_location`, если одновременно выполняются оба условия:
- есть candidate **text/location column**, где значения похожи на warehouse/location labels;
- есть candidate **qty column**, где значения в основном числовые или пустые.

Типичный пример:
- `Склад`
- `Остаток на складе`

### 9.4 Rule for `implicit_header_location`

Возвращать `implicit_header_location`, если:
- нет подтверждённой row-level location column;
- есть одна или несколько candidate columns, где **header** похож на warehouse/location label;
- значения в этих колонках в основном числовые или пустые;
- это именно warehouse-pattern группы колонок, а не просто одна случайная numeric column.

Типичный пример:
- `Центр. Склад`
- `Москва`
- `СПб`

### 9.5 Rule for `unknown`

Возвращать `unknown`, если ни один паттерн не подтверждён достаточно надёжно.

В v1 `unknown` не должен silently pass дальше как inventory facts. Он должен быть surfaced for review. Формальный quarantine output — следующий слой зрелости.

## 10. Extraction rules

### 10.1 Extraction for `explicit_row_location`

Одна data-row порождает **ровно один** inventory fact, если:
- строка прошла row eligibility guard;
- `warehouse_raw` извлечён;
- `qty_raw` извлечён.

Правила:
- `warehouse_raw` берётся из row-level location column;
- `qty_raw` берётся из qty column;
- `warehouse_source_column` = имя location column;
- `qty_source_column` = имя qty column;
- `source_pattern` = `explicit_row_location`.

Пример.

Вход:
- `Артикул = 4384100`
- `Склад = "Склад Москва"`
- `Остаток на складе = "1"`

Выход:
- `supplier_sku = "4384100"`
- `warehouse_raw = "Склад Москва"`
- `qty_raw = "1"`
- `warehouse_source_column = "Склад"`
- `qty_source_column = "Остаток на складе"`
- `source_pattern = "explicit_row_location"`

### 10.2 Extraction for `implicit_header_location`

Одна data-row порождает `0..N` inventory facts.

Для каждой warehouse-like колонки:
- если value пустое, запись не создаётся;
- если value не пустое, создаётся отдельный inventory fact;
- если строка не прошла row eligibility guard, запись не создаётся.

Правила:
- `warehouse_raw` = header warehouse-like column;
- `qty_raw` = value из этой колонки;
- `warehouse_source_column` = имя этой же колонки;
- `qty_source_column` = имя этой же колонки;
- `source_pattern` = `implicit_header_location`.

Пример.

Вход:
- `Артикул = 1440010`
- `Центр. Склад = "6"`

Выход:
- `supplier_sku = "1440010"`
- `warehouse_raw = "Центр. Склад"`
- `qty_raw = "6"`
- `warehouse_source_column = "Центр. Склад"`
- `qty_source_column = "Центр. Склад"`
- `source_pattern = "implicit_header_location"`

## 11. Type policy for v1

В первом инкременте:
- допускается не приводить `qty_raw` к окончательному numeric type;
- допускается не канонизировать warehouse names;
- extractor отвечает только за честное извлечение inventory facts без потери location dimension.

## 12. Non-goals for v1

В `v1` этот контракт намеренно не включает:
- `price`
- `price_retail`
- `price_tier`
- `stock_total reconciliation`
- `warehouse alias registry`
- `quarantine outputs`
- `current-state merge logic`
- `qty_numeric`

Это отдельные capability layers.

## 13. Future extensions

В следующих версиях поверх этого контракта могут появиться:
- `warehouse_canonical`
- `quarantine_reason`
- `price_tier`
- `price_value`
- `qty_numeric`
- `inventory_fact_id`
- `source_file`
- `artifact_id`
- `sheet_name`

## 14. Design notes

Контракт разделяет **item semantics** и **inventory semantics**. Это нужно потому, что один и тот же товар может существовать в нескольких locations с разными остатками, а location dimension нельзя терять на refined layer.

Detector паттерна вынесен отдельно, потому что source schema может drift-ить: колонки и поля могут добавляться, исчезать и меняться, а ETL, жёстко привязанный к именам source columns, становится хрупким.

Канонизация warehouse labels, quarantine для сомнительных cases и reusable tests не включены в `v1` специально. Это следующий слой зрелости: сначала extractor, потом registry, quarantine и tests.
