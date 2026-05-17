# Паспорт поставщика: 4точки (4tochki.ru)

**Версия:** 1.5
**Дата обновления:** 2026-05-17
**Статус:** Production

## 1. Общая информация

| Поле | Значение |
|------|----------|
| Поставщик | 4точки (4tochki.ru) |
| Сайт | https://www.4tochki.ru |
| Тип API | SOAP (WCF) |
| WSDL | https://api-b2b.4tochki.ru/WCF/ClientService.svc?wsdl |
| Используется методов | 10 read |

## 2. Аутентификация

| Поле | Значение |
|------|----------|
| Тип | Логин/пароль в каждом запросе |
| Переменные в .env | FTO_LOGIN, FTO_PASSWORD |
| Файл | /opt/canonical-core/.env.4tochki |

## 3. Ключевые цифры (17 мая 2026)

| Показатель | Значение |
|------------|----------|
| Типов товаров | 6 |
| Всего товаров | 20 976 |
| Складов | 36 |
| Офферов | 28 420 |
| Время stock | ~65s |
| Время full | ~5 мин |

## 4. Инфраструктура

| Компонент | Путь |
|-----------|------|
| ETL-скрипт | /opt/canonical-core/scripts/etl/4tochki_etl.py |
| Венв | /opt/canonical-core/.venvs/fourtochki-probe |
| .env | /opt/canonical-core/.env.4tochki |
| Лог stock | /var/log/4tochki_etl_stock.log |
| Лог full | /var/log/4tochki_etl_full.log |
| Lock stock | /var/run/4tochki_etl_stock.lock |
| Lock full | /var/run/4tochki_etl_full.lock |
| WSDL-кэш | /var/cache/4tochki_wsdl.db |
| Журнал | /opt/canonical-core/ops/runtime_journal/ |

## 5. Cron

```
*/30 8-20 * * * flock -n /var/run/4tochki_etl_stock.lock ... python 4tochki_etl.py stock
5 3 * * * flock -n /var/run/4tochki_etl_full.lock ... python 4tochki_etl.py full
```

## 6. Таблицы PostgreSQL

| Таблица | Назначение | Ключ |
|---------|-----------|------|
| _4tochki_products | Каталог (is_active) | code |
| _4tochki_offers | Остатки и цены (is_active) | UNIQUE(product_code, warehouse_code) |
| _4tochki_warehouses | Склады | code |
| _etl_state | Состояние ETL | key |

## 7. ETL v3.6.2

| Поле | Значение |
|------|----------|
| Файл | /opt/canonical-core/scripts/etl/4tochki_etl.py |
| Режимы | stock (30 мин, 8-20), full (03:05) |
| Защита запуска | flock + advisory lock (разные ключи для stock/full) |
| Каталог | Параллельный (6 потоков, thread-local клиенты) |
| Остатки | Последовательные чанки по 120 |
| Деактивация | Только по ответу API, через temp table |
| Graceful shutdown | threading.Event + tenacity |
| Защита каталога | MIN_CATALOG=5000 + проверка падения >50% + etl_state |
| Логи | JSON, run_id, /var/log/4tochki_etl_{mode}.log |
| Особенности | orjson, WSDL-кэш, pg_notify, safe_float |

## 8. Конфигурация (.env)

| Переменная | По умолчанию | Назначение |
|------------|-------------|-----------|
| DB_CONN | dbname=canonical user=canonical | БД |
| FTO_CHUNK_SIZE | 120 | Размер чанка |
| FTO_TIMEOUT | 90 | Таймаут SOAP |
| FTO_LOCK_TIMEOUT | 600 | Таймаут блокировки |
| FTO_MIN_CATALOG | 5000 | Мин. каталог |

## 9. Статус

Production. Деплой: 16 мая 2026, 23:04 UTC.
