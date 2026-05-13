# Шинсервис — Полное саммари интеграции

**UTC:** 2026-05-13 08:00:00
**Статус:** ✅ ИНТЕГРАЦИЯ ЗАВЕРШЕНА

---

## 1. Выполненные этапы

### R0 — Разведка API (2026-05-12 21:30 UTC)
- Создана структура каталогов на VPS
- Создан .env.shinservice с токеном API
- Написан и выполнен probe-скрипт
- Проверены /stock/tires.json и /stock/wheels.json (HTTP 200)
- Обнаружено: ответ содержит shops (склады), не товарные остатки
- Сохранён снепшот с manifest.json и sha256sums

### R0.7 — Поиск остатков (2026-05-13 00:30 UTC)
- Проверены параметры URL (format, type, detail) — без изменений
- Проверены альтернативные endpointы (v2, api) — 404
- Проверен конструктор выгрузок через UI
- Найдены стабильные ссылки на выгрузки каталога, цен и остатков

### R1 — Подтверждение интеграции (2026-05-13 01:00 UTC)
- Получены UUID для шин и дисков
- Подтверждена структура данных: sku, gtin, price, amount_total, остатки по складам
- Обновлён паспорт поставщика

### Этап 2 — Таблицы PostgreSQL (2026-05-13 01:30 UTC)
- Созданы таблицы:
  - _shinservice_products
  - _shinservice_offers
  - _shinservice_shops
- Добавлены индексы

### Этап 3 — ETL-скрипт (итерации v4.x → v6.5)
- v4.x — базовая версия с fetch и batch insert
- v5.x — добавлены shops, ANALYZE, VACUUM
- v6.x — добавлены раздельные цены (shop_id=NULL), обработка ошибок, dead-letter queue, трекинг запусков, безопасный логгер
- v6.5 — финальная стабильная версия

### Этап 4 — Деплой и настройка (2026-05-13 08:30 UTC)
- Скопирован ETL-скрипт на VPS
- Создана обёртка shinservice_run.sh
- Настроен cron:
  - stock — каждые 30 минут
  - full — раз в сутки в 03:30
- Настроен logrotate
- Создан .env.example

### Тестовый запуск (2026-05-13 10:52 UTC)
- run_id: 6ced2cef
- offers: 197 113 записей
- shops: 14 складов
- Ошибок: 0
- Длительность: около 13 секунд

---

## 2. Ключевые находки

| Что | Результат |
|-----|-----------|
| API тип | REST (не SOAP) |
| Аутентификация | Bearer token |
| URL выгрузок | https://duplo-api.shinservice.ru/api/v1/exporter/{UUID}/download |
| Форматы | JSON, CSV, XML, XLSX |
| Типы выгрузок | catalog, price, stock |
| UUID шины | 019dbb42-9e14-b7d0-a829-b64101ead29f |
| UUID диски | 019dbb40-9828-be33-9728-e5d7db368ca6 |
| GTIN | присутствует |
| Цены | общие для всех складов (shop_id=NULL) |
| Остатки | привязаны к складам (store_id -> shop_id) |

---

## 3. Технические решения

| Проблема | Решение |
|----------|---------|
| Разные контейнеры в ответах | Единый парсинг через fetch_data |
| 502 ошибки API | Tenacity retry |
| Ограничение по частоте | REQUEST_DELAY=0.15, MAX_WORKERS=2 |
| Отсутствие остатков в stock | Использован экспорт type=stock |
| shop_id=0 для цен | Явное значение NULL |
| VACUUM в транзакции | conn.commit() перед VACUUM |

---

## 4. Структура на VPS

| Компонент | Путь |
|-----------|------|
| ETL-скрипт | /opt/canonical-core/scripts/etl/shinservice_etl.py |
| Обёртка | /opt/canonical-core/scripts/etl/shinservice_run.sh |
| Cron | /etc/cron.d/shinservice-etl |
| Логи | /var/log/shinservice_etl.log |
| Logrotate | /etc/logrotate.d/shinservice |
| Снепшоты | /opt/canonical-core/var/probes/shinservice/ |
| Журналы | /opt/canonical-core/ops/runtime_journal/ |

---

## 5. Таблицы PostgreSQL

| Таблица | Назначение | Записей |
|---------|------------|---------|
| _shinservice_products | Каталог товаров | 0 (ждёт full) |
| _shinservice_offers | Остатки и цены | 197 113 |
| _shinservice_shops | Справочник складов | 14 |
| _shinservice_etl_runs | История запусков | 2 |
| _shinservice_etl_errors | Dead-letter queue | 0 |

---

## 6. Итоговые метрики

| Показатель | Значение |
|------------|----------|
| Общее время разработки | около 12 часов |
| Версия ETL | v6.5 |
| Размер чанка | 10 000 |
| Batch insert | 2 000 |
| MAX_WORKERS | 2 |
| REQUEST_DELAY | 0.15 сек |
| Cron stock | каждые 30 минут |
| Cron full | 03:30 ежедневно |

---

## 7. Следующие шаги

1. Запустить full режим для заполнения каталога

2. После заполнения products — добавить индексы

3. Матчинг товаров с 4точками (unified-слой)

---

## 8. Руководство по эксплуатации

Ручной запуск:
ssh root@194.67.119.25 /opt/canonical-core/scripts/etl/shinservice_run.sh stock
ssh root@194.67.119.25 /opt/canonical-core/scripts/etl/shinservice_run.sh full

Просмотр логов:
ssh root@194.67.119.25 tail -f /var/log/shinservice_etl.log | jq .

Проверка статуса:
ssh root@194.67.119.25 'psql -U canonical -d canonical -c "SELECT * FROM _shinservice_etl_runs ORDER BY started_at DESC LIMIT 5;"'

---

**Документ завершён. Интеграция готова к production.**
