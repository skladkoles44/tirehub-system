Спецификация: ETL-экстрактор прайс-листов (v3.3 FINAL)

---

1. Цель

Преобразование файлов поставщиков (XLS/XLSX/XML/CSV) в единый поток canonical.ndjson. Только извлечение и структурирование.

2. Принципы

· Детерминизм: Идентичные вход и правила → бинарно идентичные выходные файлы.
· Полнота: Все входные строки попадают в canonical.ndjson или rejected.ndjson.
· Строгость: Выход строго соответствует JSON Schema.

3. Выходные артефакты

```
out/<supplier>/canonical.ndjson   # Валидные строки (сортировка: source_file, source_table, row_index, qty_column_index)
out/<supplier>/rejected.ndjson    # Ошибки (сортировка: row_index)
out/<supplier>/report.json        # Отчёт
out/all/canonical.ndjson          # Объединённые данные
data.db (SQLite)                  # Таблица canonical_items, индексы: (supplier, article, warehouse), (supplier, source_file)
```

4. Схемы

canonical_item.schema.json:

```json
{
  "type": "object", "additionalProperties": false,
  "required": ["supplier", "source_file", "source_table", "row_index", "qty_column_index", "raw"],
  "properties": {
    "supplier": {"type": "string"},
    "source_file": {"type": "string"},
    "source_table": {"type": "string"},
    "row_index": {"type": "integer", "minimum": 1},
    "qty_column_index": {"type": "integer", "minimum": 1},
    "raw": {"type": "array", "items": {"type": "string"}},
    "article": {"type": "string"}, "name": {"type": "string"}, "brand": {"type": "string"},
    "qty": {"type": ["number", "null"]}, "price": {"type": ["number", "null"]},
    "warehouse": {"type": "string"}, "currency": {"type": "string"}
  }
}
```

Поля article, name, brand, warehouse, currency — опциональны (могут отсутствовать).
rejected_item.schema.json: + обязательное поле error: string.

5. Пайплайн

1. Reader: Файл → IR {tables: [{name, rows: [[cell,...]]}]}. Приводит все ячейки к string, null → "", trim. Проставляет source_table (string) и row_index (1-based).
2. ColumnMapper: IR + mappings/.yaml → column_index (1-based) → {field, confidence, matched_rule}.
3. ValueNormalizer: Значение ячейки (string) + тип поля (qty/price) → number или null. Правила парсинга — см. п.6.
4. CanonicalRowBuilder: Строка IR + маппинг + нормализованные значения → массив canonical_item/rejected_item. Explode: создаёт строку для каждого qty_column_index, где qty != null (включая 0).
5. Orchestrator: Управление, сортировка вывода, отчёт.

6. Парсинг числовых значений (ValueNormalizer)

1. Десятичные разделители: Если нет . и ровно одна , между цифрами → заменить на .. Иначе запятые — разделители тысяч.
2. Очистка: Удалить все пробелы, символы валют (₽$,€р.руб), оставшиеся запятые.
3. Правила для qty:
   · "", "—", "-", "нет", "отсутствует", "неограничено" → null
   · "0", "0.0" → 0
   · ">N", "N+", ">=N", "M-N" → первое число (N/M)
   · Остальное → float(value) или null
4. Правила для price: После очистки → float(value) или null.

7. Правила маппинга (mappings/.yaml)

· Нормализация заголовков: trim, collapse spaces, lower case.
· Типы правил (1-based column_index): header_exact, header_contains, value_pattern, column_position.
· Приоритеты: exact > contains > pattern > position. Внутри типа: больше priority → выше.

8. Отчёт (report.json)

```json
{
  "statistics": {
    "total_input_rows": 100,
    "rejected_input_rows": 2,
    "accepted_input_rows": 98,
    "rows_with_qty": 95,
    "skipped_no_qty_rows": 3,
    "canonical_rows": 210,
    "exploded_rows_count": 115
  },
  "field_coverage": {"article": 0.92, "price": 0.88, "qty": 0.95},
  "confidence_summary": {"HIGH": 8, "MEDIUM": 2, "LOW": 1},
  "unmapped_columns": [{"column_index": 7, "sample_values": ["Примечание", ""]}],
  "normalization_warnings": {"count": 12, "examples": [{"pattern": ">40", "normalized_to": 40}]},
  "errors": []
}
```

Определения:

· rows_with_qty: входных строк с хотя бы одним qty != null (включая 0).
· exploded_rows_count: canonical_rows - rows_with_qty (добавочные строки).
· skipped_no_qty_rows: accepted_input_rows - rows_with_qty.

Формулы:

1. accepted_input_rows = total_input_rows - rejected_input_rows
2. skipped_no_qty_rows = accepted_input_rows - rows_with_qty
3. canonical_rows = rows_with_qty + exploded_rows_count

9. Критерии приемки

Обязательные:

1. 100% строк canonical.ndjson валидны по схеме.
2. 100% строк имеют заполненные обязательные trace-поля.
3. Детерминизм: Три запуска → бинарно идентичные файлы. Сериализация: sort_keys=True, ensure_ascii=True, separators=(',', ':'), allow_nan=False, newline='\n', Unicode NFC.
4. total_input_rows == rejected_input_rows + accepted_input_rows.
5. Формулы отчёта выполняются точно.

Целевые:

1. ≥80% строк имеют заполненные article, price, qty.
2. Отчёт содержит все секции.

10. Out of Scope

· Автодетект заголовков, fuzzy/ML, контекстный анализ.
· Сопоставление SKU, нормализация брендов.
· Дедупликация, бизнес-логика.

---
