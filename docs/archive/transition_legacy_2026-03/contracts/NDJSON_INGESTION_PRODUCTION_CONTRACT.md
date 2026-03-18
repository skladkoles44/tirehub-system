# NDJSON ingestion / production contract (canonical)

## Scope
- Production ingestion принимает **только LINE NDJSON**.
- Парсеры `parser_*_v1.py` — диагностические (diagnostics/evidence/smoke/debug), **не ingestion**.
- Для Kolobox обязателен эмиттер:
  `scripts/ingestion/kolobox/emit_kolobox_ndjson_v1.py`.

## NDJSON (production) — минимальный контракт строки
{
  "supplier_id": "kolobox",
  "parser_id": "kolobox_xls_v1",
  "raw": {
    "supplier_warehouse_name": "центр. склад",
    "sku_candidate_key": "PS20055",
    "price": "4205",
    "qty": "20",
    "currency": "RUB",
    "supplier_article": "PS20055",
    "supplier_code_1c": "УТ-00295620",
    "brand_raw": "Attar",
    "name_raw": "Attar 175/65R14 82T ATTAR W01"
  },
  "quality_flags": []
}

## Типы (P0)
- qty: строка целого ("4") или "" (NULL). "4.0" запрещено.
- price: строка или число, безопасно кастящееся в numeric.
- supplier_warehouse_name: только trim + lowercase + collapse spaces.

## Warehouse policy
- Допустим временный ключ: `__unreviewed__:<raw_supplier_warehouse_name>`.
- Ingestion не падает из-за новых складов.
- Фильтрация `__unreviewed__:*` — downstream.

## Snapshot lifecycle (production)
1) create snapshot (`parser_id=kolobox_xls_v1`, `status=open`)
2) write NDJSON → `canonical_items_source`
3) seal snapshot
4) curated: `generate_offers_v1(...)`
Seed/smoke всегда исключаются (`parser_id!='seed'`).

## Duplicate policy
- Дубликаты разрешены на source уровне.
- Дедупликация — downstream (ruleset/curated).

## Failure policy
- Одна битая строка не валит ingestion → `quality_flags`.
- Fatal: XLS не читается; layout не определён; NDJSON пустой (неожиданно).

## Layout determination (Kolobox)
- Только явные флаги: shiny | diski | truck | komplektatsii.
- Автодетект не подменяет явный флаг.

## Gates
- repo clean; venv активен; LINE NDJSON; ожидаемый масштаб строк; 0 невалидных JSON.
- После ingestion: count>0; `generate_offers_v1` без ошибок; API возвращает данные.

## Главное
- NDJSON emitter — единственный недостающий компонент.
- Парсеры ≠ ingestion. `parser_id` — ключевой фильтр.
- Типы qty/price — без эвристик.
- Новые склады не блокируют бизнес.
