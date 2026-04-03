# Жизненный цикл входящего пакета

Данный документ задаёт expanded normative lifecycle.
README содержит только condensed summary этого lifecycle.

1. Приём данных из источника.
2. Сохранение сырого payload.
3. Структурное извлечение строк.
4. Нормализация в canonical supplier offers.
5. Quality gate.
6. Сборка staging current snapshot.
7. Diff с текущим состоянием.
8. Обновление supplier offers current.
9. Matching к каталогу.
10. Обновление marketplace current.

## Примечание
Webhook и API после intake приводятся к единому incoming object.

## Результат жизненного цикла
- Raw layer фиксирует факт получения и оригинальный payload.
- Supplier current layer отражает текущее состояние предложений поставщика.
- Marketplace current layer отражает только публикабельные офферы.
