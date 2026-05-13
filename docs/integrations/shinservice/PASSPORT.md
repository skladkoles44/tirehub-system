# Паспорт поставщика: Шинсервис (shinservice.ru)

**Версия:** 2.0
**Дата обновления:** 2026-05-13
**Статус:** ✅ Интеграция завершена, ETL в production

---

## 1. Общая информация

| Поле | Значение |
|------|----------|
| Поставщик | ООО «ШИНСЕРВИС» |
| Сайт | https://www.shinservice.ru |
| B2B-портал | https://duplo.shinservice.ru |
| Тип API | REST (выгрузки по прямым ссылкам) |
| Версия API | 2.9.1 |
| Версия ETL | 6.5 |

---

## 2. Аутентификация

| Поле | Значение |
|------|----------|
| Тип | Не требуется для выгрузок |
| Способ | UUID выгрузки в URL |
| Токен в .env | не нужен |

---

## 3. Endpoints

### Базовый URL
https://duplo-api.shinservice.ru/api/v1/exporter

### Форматы
- JSON (рекомендован)
- CSV
- XML
- XLSX

### Типы выгрузок

| Тип | Параметр | Содержание |
|-----|----------|------------|
| Каталог | type=catalog | sku, title, brand, model, gtin, характеристики, фото |
| Цены | type=price | sku, price, price_retail, price_msrp |
| Остатки | type=stock | sku, store_id, stock_total |

### Шаблон URL
{BASE}/{UUID}/download?type={catalog|price|stock}&format={json|csv|xml|xlsx}

---

## 4. Ключи доступа

| Назначение | UUID |
|------------|------|
| Шины (все) | 019dbb42-9e14-b7d0-a829-b64101ead29f |
| Диски (все) | 019dbb40-9828-be33-9728-e5d7db368ca6 |

---

## 5. Структура данных

### 5.1 Каталог (type=catalog) — массив tyre / disk

| Поле | Тип | Описание |
|------|-----|----------|
| sku | string | Код товара (1С) |
| title | string | Наименование товара |
| brand | string | Производитель |
| model | string | Модель |
| gtin | string | GTIN (присутствует) |
| season | string | Сезон |
| diameter | string | Диаметр |
| width | integer | Ширина |
| profile | integer | Профиль |
| load_index | string | Индекс нагрузки |
| speed_index | string | Индекс скорости |
| pins | boolean | Шипы |
| runflat | boolean | RunFlat |
| extra_load | boolean | Extra Load |
| photo_url | string | Ссылка на фото |

### 5.2 Цены (type=price) — массив tyre / disk

| Поле | Тип | Описание |
|------|-----|----------|
| sku | string | Код товара |
| price | integer | Цена B2B (руб) |
| price_retail | integer | Розничная цена |
| price_msrp | integer | Рекомендованная цена |

### 5.3 Остатки (type=stock) — массив объектов

| Поле | Тип | Описание |
|------|-----|----------|
| sku | string | Код товара |
| store_id | integer | ID склада |
| stock_total | integer | Остаток на складе |

---

## 6. Инфраструктура на VPS

| Компонент | Путь | Статус |
|-----------|------|--------|
| ETL-скрипт | /opt/canonical-core/scripts/etl/shinservice_etl.py | ✅ |
| Обёртка запуска | /opt/canonical-core/scripts/etl/shinservice_run.sh | ✅ |
| Cron | /etc/cron.d/shinservice-etl | ✅ |
| Логи ETL | /var/log/shinservice_etl.log | ✅ |
| Логи cron | /var/log/shinservice_cron.log | ✅ |
| Logrotate | /etc/logrotate.d/shinservice | ✅ |
| Снепшоты | /opt/canonical-core/var/probes/shinservice/ | ✅ |
| Журналы | /opt/canonical-core/ops/runtime_journal/ | ✅ |

---

## 7. Таблицы PostgreSQL

| Таблица | Назначение | Статус |
|---------|------------|--------|
| _shinservice_products | Каталог товаров | ✅ |
| _shinservice_offers | Остатки и цены | ✅ |
| _shinservice_shops | Справочник складов | ✅ |
| _shinservice_etl_runs | История запусков ETL | ✅ |
| _shinservice_etl_errors | Dead-letter queue | ✅ |

**БД:** canonical
**Схема:** public

---

## 8. Cron

| Расписание | Команда |
|------------|---------|
| */30 * * * * | /opt/canonical-core/scripts/etl/shinservice_run.sh stock |
| 30 3 * * * | /opt/canonical-core/scripts/etl/shinservice_run.sh full |

---

## 9. Статус интеграции

| Этап | Статус | Дата |
|------|--------|------|
| R0 — разведка API | ✅ | 2026-05-12 |
| R1 — подтверждение | ✅ | 2026-05-13 |
| Таблицы PostgreSQL | ✅ | 2026-05-13 |
| ETL-скрипт v6.5 | ✅ | 2026-05-13 |
| Cron | ✅ | 2026-05-13 |
| Logrotate | ✅ | 2026-05-13 |

---

## 10. Контакты поддержки

| Канал | Контакт |
|-------|---------|
| B2B-портал | https://duplo.shinservice.ru |
| Email | b2b_yaroslavl@shinservice.ru |
| Телефон | +7 4852 58-04-68 |

---

## 11. Владельцы

| Роль | Кто |
|------|-----|
| GitHub репозиторий | skladkoles44 |
| VPS | root |
| База данных | postgres |

---

**Документ поддерживается в актуальном состоянии.**
**Версия 2.0 — соответствует ETL v6.5 в production.**
