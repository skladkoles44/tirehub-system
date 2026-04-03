# Canonical Supplier Offer

## Назначение
Canonical Supplier Offer является главной канонической сущностью ingestion/normalization слоя.
Он описывает одно логическое предложение поставщика после нормализации, но до публикации в marketplace layer.

## Обязательные поля
| Поле | Тип |
|------|-----|
| supplier_id | string |
| source_type | enum |
| source_object_id | string |
| run_id | string |
| offer_key | string |
| warehouse_key | string |

## Правила для обязательных полей
- `supplier_id` должен однозначно идентифицировать поставщика.
- `source_type` должен принадлежать поддерживаемому набору типов источников.
- `source_object_id` должен позволять трассировать запись к исходному объекту intake.
- `run_id` должен указывать на конкретный run обработки.
- `offer_key` должен быть стабильным в пределах одного supplier и одного логического предложения.
- `warehouse_key` должен быть нормализованным идентификатором склада.

## offer_key правило
- `offer_key` должен быть стабильным для одного и того же логического предложения поставщика.
- `offer_key` не должен зависеть от нестабильных полей вроде времени обработки, случайного порядка строк или служебного batch sequence, если они не являются частью логической идентичности предложения.

## Nullable поля
| Поле | Тип |
|------|-----|
| supplier_sku | string |
| raw_name | string |
| item_type | string |
| warehouse_raw | string |
| stock_qty_raw | string |
| stock_qty_normalized | integer |
| availability_status | enum |
| price_purchase_cents | integer |
| currency | string |
| identity_key | string |
| quality_flags | array |
| is_reject | boolean |
| reject_reason | string |

## availability_status допустимые значения
- `in_stock`
- `out_of_stock`
- `limited`
- `backorder`
- `unknown`

## Семантика nullable полей
- `supplier_sku` допускается null, если у источника отсутствует стабильный supplier SKU.
- `raw_name` хранит исходное наименование без обязательной очистки до канонического вида.
- `item_type` может быть null до завершения классификации.
- `warehouse_raw` хранит исходное название склада.
- `stock_qty_raw` хранит исходное представление остатка.
- `stock_qty_normalized` хранит нормализованное числовое значение остатка, если нормализация возможна.
- `price_purchase_cents` хранит закупочную цену в минимальных денежных единицах, если цена валидна и определена.
- `identity_key` может быть null до matching.
- `quality_flags` содержит список выявленных проблем качества.
- `is_reject` помечает запись как reject внутри supplier layer.
- `reject_reason` хранит причину reject.

## Запрещённые fallback’и
- Отсутствие цены не превращается в `0`.
- Отсутствие `supplier_id` не превращается в `"unknown"`.
- Нераспознанный склад не считается валидным `warehouse_key`.
- Неразрешённый `item_type` не идёт дальше как валидное значение.
- Отсутствие `identity_key` не делает запись publishable автоматически.

## Инварианты
- Каждая запись должна быть трассируема к intake object через `source_type`, `source_object_id` и `run_id`.
- Каждая запись supplier layer принадлежит ровно одному `supplier_id`.
- Каждая запись supplier layer имеет один `offer_key`.
