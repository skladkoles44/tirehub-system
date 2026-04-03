# Граница между Supplier и Marketplace

## Supplier Offers Current
- Отражает реальность поставщика.
- Может содержать технические и неполные данные.
- Является результатом normalize stage.
- Не равен автоматически marketplace offer.

## Marketplace Offers Current
- Формируется только после matching и publish policy.
- Не пополняется напрямую из raw или normalize слоёв.
- Содержит только публикабельные офферы.

## Publish Policy
Оффер попадает в Marketplace только если:
1. `is_reject = false`
2. `identity_key` не null
3. run прошёл quality gates
4. оффер прошёл matching к каталогу
5. `availability_status` определён и publishable по policy

## Publishable availability_status
Следующие значения считаются publishable по умолчанию:
- `in_stock`
- `limited`
- `backorder`

Следующие значения не считаются publishable по умолчанию:
- `out_of_stock`
- `unknown`

## Правило границы
Ingestion и normalization могут готовить supplier reality, но не имеют права напрямую создавать marketplace reality.
