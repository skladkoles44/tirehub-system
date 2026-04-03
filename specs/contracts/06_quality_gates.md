# Quality Gates

## Назначение
Данный файл определяет gate action для run.
Классификация schema drift задаётся в `10_schema_drift_handling.md`.

## Результаты run
| Результат | Описание | Действие с current layer |
|-----------|----------|--------------------------|
| ACCEPT | Все пороги соблюдены | Обновляет current layer |
| QUARANTINE | Run завершён, но требует manual review | Не обновляет current layer автоматически |
| REJECT | Критическое нарушение | Завершает run без update current |

## Gate rules
| Условие | Gate action |
|---------|-------------|
| Reject-rate > 5% | REJECT |
| Падение числа офферов > 30% | QUARANTINE |
| Массовое исчезновение складов > 50% | QUARANTINE |
| Скачок медианной цены > 200% | QUARANTINE |
| Рост строк без identity > 10% | QUARANTINE |
| Drift class = optional_added | ACCEPT |
| Drift class = optional_missing | QUARANTINE |
| Drift class = required_missing | REJECT |
| Drift class = type_changed | REJECT |
| Drift class = structure_changed | REJECT |

## Примечание
- Пороги являются initial thresholds и пересматриваются по фактической статистике поставщиков.
- Gate action определяет, может ли run обновить current layer автоматически.
