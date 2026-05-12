
Шинсервис — R0: Разведка API и live probe

САММАРИ (UTC: 2026-05-12 21:30:00)

Выполненные работы

1. Создана структура каталогов на VPS для Шинсервис по шаблону 4точек:
   · /opt/canonical-core/var/probes/shinservice/ — снепшоты probe-запусков
   · /var/tmp/shinservice_stock/ — временные файлы
   · /opt/canonical-core/scripts/probes/ — probe-скрипты
   · /opt/canonical-core/ops/runtime_journal/ — журналы этапов
   · /opt/canonical-core/src/integrations/shinservice/ — адаптер (заглушки)
   · /opt/canonical-core/tests/probes/ — exact-тесты (заглушки)
2. Создан .env.shinservice на VPS с токеном API:
   · Файл: /opt/canonical-core/.env.shinservice
   · Переменная: SHINSERVICE_TOKEN
   · Права: 600
3. Написан и выполнен probe-скрипт (shinservice_probe.sh):
   · Только GET-запросы (без write-операций)
   · Проверка HTTP статуса, валидности JSON, непустого тела
   · trap cleanup для удаления временных файлов
   · Сохранение снепшота с manifest.json и sha256sums
4. Выполнены live-запросы:
   · GET /regular/stock/tires.json → HTTP 200
   · GET /regular/stock/wheels.json → HTTP 200
5. Сохранён снепшот:
   · Путь: /opt/canonical-core/var/probes/shinservice/20260512_211228/
   · Файлы: tires.json, wheels.json, manifest.json, sha256sums.txt
   · Размер: 7.5 МБ
6. Создан журнал этапа:
   · Путь: /opt/canonical-core/ops/runtime_journal/20260512_shinservice_r0_findings.md
7. Закоммичен результат в git:
   · Ветка: work/shinservice-r1-baseline
   · Коммит: 7c93701
   · Сообщение: docs(shinservice): R0 live probe findings

---

Ключевые находки

Что проверяли Результат
Доступность API ✅ Токен рабочий, API доступен
/regular/stock/tires.json ✅ HTTP 200, 3 629 014 байт, валидный JSON
/regular/stock/wheels.json ✅ HTTP 200, 4 161 912 байт, валидный JSON
Формат ответа {"datetime", "generator", "shops"}
Содержимое shops[] {id, title, address, geo_position, delivery_types}
Наличие товарных остатков ❌ НЕТ — ни items, ни stock, ни total, ни sku

---

Технические особенности API (на основе реального ответа)

```yaml
# Структура ответа /stock/tires.json
{
  "datetime": 1778620352,        # UNIX timestamp
  "generator": "b2b-upstream-02",
  "shops": [
    {
      "id": 1,
      "title": "Центр шин и дисков на проспекте Андропова",
      "address": "г. Москва, ЮАО, ул. Садовники, 11Ас1А",
      "geo_position": {"lat": 55.673657, "lng": 37.658694},
      "delivery_types": ["pickup"]
    }
  ]
}
```

Ключевое наблюдение:
Эндпоинты, заявленные в документации как "Остатки для шин и дисков", на деле возвращают справочник складов. Товарных остатков в ответе нет.

---

Что НЕ удалось выяснить (требует дальнейшей разведки)

Вопрос Статус
Как получить остатки товаров? ❌ Неизвестно
Существует ли метод для каталога (все SKU)? ❌ Неизвестно
Поддерживается ли запрос остатков по списку SKU? ❌ Неизвестно
Есть ли пагинация у остатков? ❌ Неизвестно
Какой метод использовать для batch-запроса (чанки по 50)? ❌ Неизвестно

---

Следующие шаги (R0.7 и R0.8)

1. Проверить POST /shipment.json с массивом SKU — возможно, он возвращает остатки
2. Изучить полную документацию API (если есть доступ к WSDL или расширенной OpenAPI)
3. Запросить у Шинсервис метод получения остатков, если не найдём
4. Написать exact-тесты (R1) после подтверждения рабочего endpoint

---

ПАСПОРТ ОБЪЕКТА (Шинсервис, состояние на R0)

```yaml
# Паспорт поставщика: Шинсервис (shinservice.ru)

id: shinservice
full_name: Шинсервис B2B
type: REST (не SOAP)
base_url: http://vendor.shinservice.ru/regular/

authentication:
  type: Bearer token
  env_file: /opt/canonical-core/.env.shinservice
  variable: SHINSERVICE_TOKEN

confirmed_endpoints:
  - path: /stock/tires.json
    method: GET
    status: ✅ рабочий
    response_body: shops (справочник складов)
    size_bytes: 3629014
  - path: /stock/wheels.json
    method: GET
    status: ✅ рабочий
    response_body: shops (справочник складов)
    size_bytes: 4161912

unconfirmed_endpoints_candidates:
  - path: /shipment.json
    method: POST
    purpose: возможно, остатки
    status: ❌ не проверен (write-метод, осторожно)
  - path: /order.json
    method: GET/POST
    purpose: заказы (не нужно на R1)
    status: ❌ не проверен

structure_response:
  tires.json:
    root_keys: [datetime, generator, shops]
    shops_keys: [id, title, address, geo_position, delivery_types]
  wheels.json:
    root_keys: [datetime, generator, shops]
    shops_keys: [id, title, address, geo_position, delivery_types]

missing_required:
  - товарные остатки (stock, rest, quantity)
  - товарный каталог (sku, brand, model, title)
  - цены на товары (price)
  - GTIN
  - пагинация

integration_status:
  phase: R0 — разведка API
  next_action: определить метод получения остатков
  blocker: отсутствует endpoint для остатков товаров

vps_paths:
  env: /opt/canonical-core/.env.shinservice
  probes_root: /opt/canonical-core/var/probes/shinservice/
  last_snapshot: /opt/canonical-core/var/probes/shinservice/20260512_211228/
  journal: /opt/canonical-core/ops/runtime_journal/20260512_shinservice_r0_findings.md

git:
  branch: work/shinservice-r1-baseline
  commit: 7c93701
  worktree: ~/worktrees/shinservice-r1-baseline

rules_followed:
  - VPS — только runtime truth
  - Токен в .env, не в коде
  - Снепшот с manifest.json и sha256sums
  - trap cleanup для временных файлов
  - Никаких POST (write) в probe
  - Проверка RC, HTTP статуса, валидности JSON, непустого тела
  - Git commit только с телефона

created_at_utc: 2026-05-12T21:30:00Z
owner: @Test_etl
```

---

R0 — ЗАВЕРШЁН. ПЕРЕХОДИМ К R0.7 (ПОИСК ОСТАТКОВ) ПОСЛЕ ПОЛУЧЕНИЯ КОМАНДЫ.
