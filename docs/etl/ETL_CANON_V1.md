ETL CANON V1 (SSOT / Marketplace-ready)

Назначение
Контур предназначен для приёма прайс-листов поставщиков, фиксации фактов «что поставщик прислал» без бизнес-эвристик и построения витрины только поверх проверенных и валидных данных. SSOT является журналом фактов, а не «актуальным состоянием».

1. Общие принципы
Система построена по модели: Emitter → Gate → Ingestion → Curated / Offers.
NDJSON-first. Каждая строка — атомарный факт.
Emitter не принимает бизнес-решений и ничего не «исправляет».
Gate решает, можно ли писать в SSOT.
Ingestion фиксирует данные навсегда.
Curated / Offers — единственное место бизнес-логики.

2. Термины
parser_id — идентификатор логики парсинга: поставщик + формат/лейаут + логика emitter. Любой breaking change контракта или алгоритма = новый parser_id.
mapping_schema_version — версия схемы mapping.yaml (ключ version в YAML).
mapping_version — версия конкретного mapping-файла как артефакта. Меняется при любом изменении содержимого.
ndjson_contract_version — версия структуры NDJSON. Breaking change требует нового ndjson_contract_version и нового parser_id.
effective_at — момент актуальности данных, задаётся оркестратором.
SSOT — append-only журнал фактов.

3. Emitter
Вход: файл поставщика + mapping.yaml.
Поддержка v1: только XLS через xlrd 1.2.0. Любая проблема чтения файла — CRITICAL лог + exit code != 0.
Оркестратор обязан задавать effective_at в формате RFC3339: YYYY-MM-DDTHH:MM:SSZ. Если не задан — используется run_start_utc (одно значение на прогон).
Emitter нормализует строки источников SKU: Unicode NFKC, удаление zero-width символов, trim, collapse whitespace.
Emitter не делает дедупликацию, не фильтрует строки по бизнес-правилам.
Полностью пустые строки (все сопоставленные поля пусты) пропускаются, не попадают в NDJSON, но учитываются в stats-out как skipped_rows_all_empty.
Emitter формирует stats-out (JSON) с метриками прогона, но не знает «новизну» складов — он только фиксирует найденные имена.
Emitter вычисляет sku_candidate_key, parsed.price, parsed.qty, выставляет флаги.

4. Mapping
Источник правды по колонкам, листам, строкам и складам.
Расположение: mappings/suppliers/{supplier_id}/*.yaml.
Mapping содержит: format_hints (file_type, sheet, header_row_1based, data_start_row_1based), columns, warehouses, header_probe.
header_row_offset считается от data_start_row_1based: target_row = data_start_row_1based + offset.
header_probe обязателен минимум в двух колонках; mismatch = FAIL в Gate.
Mapping не содержит бизнес-логики.
Любое изменение mapping-файла требует увеличения mapping_version.

5. NDJSON контракт
Каждая строка содержит: supplier_id, parser_id, mapping_version, ndjson_contract_version, emitter_version, run_id, effective_at, sku_candidate_key, raw, parsed, quality_flags, _meta.
effective_at — строка строго RFC3339 с Z.
currency не включается: валюта всегда RUB.
parsed.price — целое число в копейках или null. ROUND_HALF_UP.
parsed.qty — целое число или null.
Источники price_raw и qty_raw сохраняются в raw.
supplier_warehouse_name в raw опционален.

6. Флаги качества
Флаги не каскадируются, порядок проверки фиксирован.
price_textual, negative_price, zero_price, missing_price.
qty_textual, qty_fractional, negative_qty, missing_qty.
Диапазоны: price 100..10_000_000 коп., qty 0..100_000; выход за диапазон = WARN.
negative_price — WARN всегда, даже одна строка, как индикатор системной ошибки.
negative_price_share > 1% — FAIL.

7. Gate
Gate работает до записи в SSOT.
FAIL если:
– bad_json > 0
– qty_fractional / negative_qty > 0
– exploded_lines == 0
– total_lines < 0.7 × baseline.total_lines (если baseline есть)
– explosion_factor_exact > 50
– exploded_lines > 5_000_000
– header_probe_mismatch
– collisions_share > 0.1%
WARN если:
– negative_price > 0
– missing_price > 5%
– missing_sku > 1%
– data_density < 0.1
– new_warehouses_count > 3 или доля > 10%
Baseline — зафиксированный эталон, обновляется только вручную.
Если baseline отсутствует — baseline_missing = WARN, но exploded_lines == 0 всё равно FAIL.

8. Ingestion
SSOT — append-only. Бизнес-уникальности нет. Дубликаты допустимы.
warehouse_key фиксируется в момент ingestion и не меняется задним числом.
Маппинг складов:
– нормализация имени
– поиск в warehouse_aliases по (supplier_id, normalized_name)
– не найдено или имя пустое → warehouse_key = "unreviewed:{supplier_id}"
В warehouse_aliases запрещён normalized_name = "".
run_id уникален. Параллельный ingestion одного run_id запрещён.
Повторный запуск с тем же run_id очищает незасиленный snapshot и пишет заново.

9. Curated / Offers
Обрабатывает только снапшоты, прошедшие Ingestion (включая force-ingest).
Условие оффера: parsed.price > 0 и parsed.qty > 0, нет FAIL-флагов.
unreviewed и hold склады не создают офферы.
Дедупликация и выбор актуального предложения — только здесь.

10. Версионирование
Новый parser_id требуется при:
– изменении алгоритмов
– смене библиотеки чтения
– изменении ndjson_contract_version
– изменении обязательных полей NDJSON
Новый mapping_version — при любом изменении mapping-файла.
CI обязан блокировать merge, если mapping_hash изменился, а mapping_version нет.

11. Force-ingest
force_ingest:true и force_reason фиксируются в метаданных snapshot.
Доступ только для админов.
Все случаи логируются и алертятся.

12. Мониторинг
Отсчёт «нет прогонов >24ч» — от последнего PASS/WARN per parser_id.
explosion_factor отображается округлённым, но проверки используют точное значение.
Тренды деградации отслеживаются по окнам.

13. Hardening v1
Лимиты до парсинга: размер файла, строки, колонки, склады.
Unicode-нормализация SKU обязательна.
Лимит уникальных SKU за прогон.
Distributed lock на run_id.
Rate limiting по supplier_id.
Chaos-тесты регулярно.

14. Граница ответственности
Emitter фиксирует реальность.
Gate — качество.
Ingestion — история.
Curated — бизнес.
SKU-семантика, антидемпинг, ценовые гварды — вне ingestion.

Это канон v1. Он предназначен для стабильности, аудита и предсказуемости, а не для «умного исправления» данных.
