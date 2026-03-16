# Devlog — 2026-03-16
## Runner v4.1 — decompose-only phase stabilization

Цель
Довести L0-слой ingestion до стабильного состояния:
universal table decomposition → atomic_rows.ndjson.

---

1. Runner v4.1 стабилизация

Runner переведён в strict decompose-only режим.

Функция:
любой Excel → таблицы → атомарные строки.

Артефакты:
- atomic_rows.ndjson
- manifest.json
- column_profiles.ndjson

Runner не выполняет бизнес-нормализацию.

---

2. Manifest контракт

Расширен manifest:

runner.version
timestamp
input.file
input.file_hash
input.size_bytes

layout.fingerprint
layout.fingerprints[]

stats.rows_total
stats.rows_emitted
stats.rows_skipped

processing.sheets_processed
processing.tables_processed

fingerprints[] содержит все layout fingerprints файла.

---

3. column_profiles

Исправлено поведение profiler.

Было:
column_profiles.json (перезаписывался)

Стало:
column_profiles.ndjson

Каждая таблица пишет отдельную запись:

sheet
table_index
fingerprint
profiles

---

4. semantic_roles cleanup

Проблема:
classifier выбирал первое совпадение роли.

Из-за порядка ролей происходили ошибки:

Вид товара -> name  
Страна производитель -> brand  
Тип крепежа -> wheel_type  
Тип клемм -> wheel_type  
Описание цвета -> offset  

Решение:
перестроен порядок ролей.

Новый приоритет:

category
name

country
brand

application

color
offset

terminal_type
fastener_type
wheel_type

Также удалён слишком общий alias:

category:
- Вид

---

5. Проверка на реальных файлах

Brinex

19244 строк  
14 sheets  
14 tables  

Корректная классификация:

Вид товара -> category  
Подвид товара -> category  
Вид техники -> application  
Страна производитель -> country  
Тип крепежа -> fastener_type  
Тип клемм -> terminal_type  
Описание цвета -> color  

Unknown tail:

Оповещение  
Подключение к проводке автомобиля  
Рабочая температура  
Вид  

XRAY показал:
все значения = None → пустые колонки.

Semantic patch не требуется.

---

Linaris

6624 строк  
6 sheets  
6 tables  

Корректно:

Группа -> category  
Страна -> country  
b2b -> price  
Розничные -> price  
Номер -> sku  

unknown = 0.

---

6. Итог

Runner v4.1 (L0 ingestion layer) считается закрытым.

Гарантии:

- универсальная декомпозиция Excel
- streaming
- deterministic manifest
- multi-fingerprint
- role classifier стабилизирован
- semantic_roles.yaml очищен от конфликтов

---

7. Следующий этап

Следующий слой архитектуры:

NormalizerV1

Функция:

atomic_rows.ndjson
      ↓
layout + mapping
      ↓
good.ndjson

Это будет L1 normalization layer.

Runner при этом не изменяется.
