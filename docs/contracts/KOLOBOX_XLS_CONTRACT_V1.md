# Kolobox XLS contract v1 (ingestion evidence)

Файл-эталон:
`inputs/inbox/Kolobox/Прайс_Колобокс_Комплектации_2026-01-20 (XLS).xls`

## Sheet
- sheet: `TDSheet`
- header rows: 2 (`row1=1`, `row2=2`)
- data starts: `row3` (1-based)

## Columns (observed, 18)
Товарные атрибуты:
1. `Вид к-ции`
2. `Резьба`
3. `Длина общ`
4. `Ключ`
5. `Посад`
6. `Секрет`
7. `Артикул`
8. `Код 1С`
9. `Марка (бренд)`
10. `Наименование`

Цены:
11. `Опт` / `Цена`
12. `Розничная` / `Цена`
13. `МИЦ` / `Цена`

Остатки/склады и заказ:
14. `Центр. Склад` / `Центр. Склад`
15. `Саранск опт` / `Остаток`
16. `Тольятти опт(АХ)` / `Остаток`
17. `Уфа опт(Соединительное шоссе)` / `Остаток`
18. `Заказ` (факт/квота под заказ; не склад)

## Decisions (фиксируем для ingestion v1)
- base_price: `Опт`
- also_capture_prices: `Розничная`, `МИЦ` (сохранять как raw-поля, не терять)
- currency_default: `RUB`
- warehouse_qty_columns (все, без урезания):
  - `Центр. Склад`
  - `Саранск опт`
  - `Тольятти опт(АХ)`
  - `Уфа опт(Соединительное шоссе)`
- order_column: `Заказ` (сохранять как raw `order_qty`/`order_flag`)
- repeated_header_rows: none (heuristic)

## Notes
- На уровне ingestion: raw + минимальная нормализация. Никаких cleaned/normalized полей.
- Warehouse aliasing по контракту SSOT: `__unreviewed__:<warehouse_name_raw>` до ручного утверждения.
