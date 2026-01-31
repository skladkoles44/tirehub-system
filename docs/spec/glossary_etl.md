# ETL Glossary (v1)

Термины и определения для ETL Extractor v3.3 и Decomposer v1.7+. Цель: единый словарь для контрактов, схем и отчётов.

## Core pipeline

- **ETL-экстрактор (Extractor)**: компонент, который читает входные файлы поставщиков (XLS/XLSX/CSV/XML), строит IR, маппит колонки, нормализует значения и пишет артефакты canonical/rejected/report.
- **Decomposer**: компонент, который разлагает текстовые поля (прежде всего `name`, при необходимости `raw`) в claims с evidence, пишет аудит конфликтов и quality-сигналы; в учёт не влияет (accounting uses only L1).

## Data representations

- **Raw / Raw Text (L0)**: неизменённая исходная строка/текст, SSOT для дальнейших утверждений.
- **IR (Intermediate Representation)**: унифицированное представление входного файла: таблицы/листы, строки как массивы строковых значений; все null приводятся к пустым строкам, применяется trim.
- **Canonical item / canonical строка**: запись NDJSON, соответствующая JSON Schema `canonical_item`. Содержит trace-поля и нормализованные бизнес-поля (article/name/qty/price/warehouse/currency...).
- **Rejected item / rejected строка**: запись NDJSON с ошибкой (обязательное поле `error`), для строк, которые не могут быть валидно обработаны.

## Traceability and indices

- **Trace-поля**: обязательные поля трассировки: `supplier`, `source_file`, `source_table`, `row_index`, `qty_column_index`, `raw`.
- **row_index**: 1-based индекс строки в таблице IR (после header/subheader правил).
- **qty_column_index**: 1-based индекс колонки, из которой получено значение qty для конкретной канонической строки (важно для explode по складам).
- **1-based**: индексация с 1 (а не с 0) во всех полях индексов и правилах маппинга.

## Mapping and rules

- **Mapping (supplier mapping)**: YAML-правила для сопоставления колонок/полей поставщика с целевыми полями (article/name/price/qty/warehouse...). Хранятся в `mappings/suppliers/<supplier>.yaml`.
- **Ruleset**: версионируемый набор правил (sanity/signature_normalization/impact/token_pattern_fields; позже — brands/sizes и т.п.).
- **ruleset_versions**: JSON-словарь версий rulesets, участвующих в обработке (фиксируется в данных и snapshots).
- **decomposer_version**: версия кода decomposer (фиксируется в данных и snapshots).

## Output artifacts

- **canonical.ndjson**: поток валидных канонических строк для поставщика (детерминированная сортировка).
- **rejected.ndjson**: поток ошибок/отброшенных строк (детерминированная сортировка).
- **report.json**: статистика пайплайна (accepted/rejected/coverage/exploded и т.п.).

## Determinism and validation

- **Детерминизм**: одинаковый вход + одинаковые правила + одинаковая версия кода → бинарно идентичные выходные артефакты.
- **JSON canonicalization**: согласованный режим сериализации JSON (sort_keys/ensure_ascii/separators/newline/Unicode NFC) для воспроизводимости.
- **JSON Schema validation**: строгая проверка выходных строк по схеме (в т.ч. `additionalProperties=false`).

## Decomposer claims and evidence

- **Claim**: утверждение о поле (brand/size/li/si/season/model...), содержащее `value`, `layer`, признаки детерминизма/уверенности и `evidence`.
- **Evidence**: объяснение происхождения claim (dict_match/regex/rule_id/match и т.п.); обязательна для любых нетривиальных преобразований.
- **L1 (Structured Claims)**: детерминированные утверждения (правила/regex/словари).
- **L2 (Enriched Claims)**: вероятностные утверждения/подсказки (модели), не меняют L1 в учёте.

## Conflicts and audit

- **Conflict**: несогласие L1 и L2 по одному полю или отсутствие значения в одном из слоёв.
- **conflict_type**: тип конфликта: `mismatch` | `l1_missing` | `l2_missing`.
- **signature_core**: 16-hex (64-bit digest) идентификатор причины конфликта, полученный хешированием canonical `core_key`.
- **core_key**: каноническая строка-преобраз (preimage) причины конфликта вида `field:rule_id:l1_value:l2_value:conflict_type:token_pattern`.
- **signature_detail**: детализация для расследования (позиции/сниппеты/контекст), не используется для агрегации.

## Quality system

- **quality_flags**: массив строковых флагов качества у записи (например `needs_review`, `blocked_for_aggregation`, `no_qty`).
- **Supplier quality state**: состояние поставщика `normal` | `degraded` | `blocked`.
- **blocked_for_aggregation**: флаг, означающий, что строка не должна попадать в downstream агрегации/витрины.

## Snapshots and backfill

- **Snapshot**: иммутабельная версия набора данных, связанная с конкретными версиями rulesets и decomposer.
- **snapshot_id**: идентификатор snapshot, на который ссылается каждая строка в `canonical_items_source`.
- **Backfill**: повторная обработка части данных под новой версией правил (создаёт новый snapshot; старые данные не мутируются).
- **reprocess_supplier_batch**: backfill по поставщику и временному диапазону.
- **reprocess_by_signature**: backfill по причине конфликта (signature_core) в заданном scope.

## Golden set

- **Golden set**: версионированный набор NDJSON кейсов для проверки контрактности (L1, rejected, conflicts/audit) и применимости патчей/backfill.
- **Baskets**: `positive` (без конфликтов), `negative` (ожидаемый rejected), `conflict` (ожидаемый audit + needs_review, учёт по L1).
