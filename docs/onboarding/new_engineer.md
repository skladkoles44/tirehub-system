# New Engineer Onboarding

## 1. Project definition

Это не “парсер прайсов” и не “просто ETL”.

Это **deterministic supplier current-state engine**.

Система превращает хаотичные входящие supplier payloads в два разных current-state слоя:

1. **supplier reality**
2. **marketplace reality**

Главный принцип:

- \`supplier reality != marketplace reality\`

Короткая модель:

- Raw Intake Layer
- Structural Extraction
- Normalized Supplier Offers Current
- Quality / Idempotency / Diff
- Marketplace Current

Коротко:

**incoming supplier payloads -> canonical supplier current state -> marketplace current state**

---

## 2. Current architectural law

Актуальный закон системы живёт в:

- \`README.md\`
- \`specs/contracts/*\`

Дополнительные документы:
- \`docs/contracts/*\`
- \`docs/legacy_donor/*\`
- \`docs/adr/*\`
- \`docs/plans/*\`

Важно:

- старые архивы
- старые runtime-деревья
- donor-файлы

не являются законом сами по себе.

---

## 3. Layer model

### 3.1 Raw Intake Layer
Система принимает внешний вход:
- email
- вложения
- API payload
- webhook payload
- file payload
- link payload

Задача слоя:
- сохранить факт получения
- сохранить оригинальный payload
- не делать бизнес-нормализацию

Это factual / immutable intake layer.

### 3.2 Structural Extraction
Из разных носителей строится единый поток строк.

Задача слоя:
- привести xlsx/xls/csv/ods/email payload к row-oriented структуре
- сохранить трассировку до источника
- не подменять extraction бизнес-нормализацией

Выход:
- \`atomic_rows\`

### 3.3 Normalized Supplier Offers Current
Атомарные строки превращаются в канонические supplier offers.

Это слой **supplier reality**.

Здесь появляются:
- supplier_id
- source_type
- source_object_id
- run_id
- offer_key
- warehouse_key
- availability_status
- supplier_sku
- raw_name
- item_type
- warehouse_raw
- stock_qty_raw
- stock_qty_normalized
- price_purchase_cents
- currency
- identity_key
- identity_method
- identity_strength
- identity_raw
- variant_key
- quality_flags
- reject_reason
- canonical product attributes

### 3.4 Quality / Idempotency / Diff
После normalize слой проверяется на:
- broken input
- duplicate input
- schema drift
- anomaly
- idempotency
- current-state update correctness

Задача:
- обновлять текущее состояние
- не плодить бессмысленную историю
- гарантировать воспроизводимость

### 3.5 Marketplace Current
Только после:
- matching
- publish policy

supplier reality превращается в marketplace reality.

Нельзя:
- писать в marketplace layer напрямую из intake
- писать в marketplace layer напрямую из extraction
- писать в marketplace layer напрямую из normalize без publish rules

---

## 4. Repository facts

### 4.1 Git remote
- \`git@github.com:skladkoles44/tirehub-system.git\`

### 4.2 Current active branch at time of writing
- \`refactor/new-contract-order\`

Это **текущая активная ветка**, а не вечный архитектурный закон.

### 4.3 Last confirmed commit of the new live-layer
- \`b49d1c2\`
- \`etl: assemble donor-based live normalize layer with smoke hardening\`

---

## 5. Source tree vs runtime

### 5.1 Source tree
Разработка ведётся в git working copy.

Source of truth для истории изменений:
- GitHub repo

Working copy:
- любой корректный git checkout

### 5.2 Runtime
Runtime-контуры не являются местом обычной разработки.

Подтверждённый runtime:
- \`/opt/canonical-core\`

Неподтверждённый / не основной source tree:
- \`/opt/tirehub\`

Если нет отдельной задачи по \`/opt/tirehub\`, его надо считать:
- legacy/runtime-adjacent artifact
- не основным кодовым деревом
- не местом разработки

### 5.3 Hard rule
Не разрабатывать напрямую в:
- \`/opt/canonical-core\`
- \`/opt/tirehub\`

---

## 6. Current live candidate layer

Это текущий **новый активный кодовый кандидат**.

### Active live candidate modules
- \`src/identity\`
- \`src/semantic/roles\`
- \`src/extract/atomic_runner\`
- \`src/normalize/supplier_offer\`

### Main files

#### Identity
- \`src/identity/__init__.py\`
- \`src/identity/identity_key_v2.py\`
- \`src/identity/size_extractor.py\`

#### Semantic roles
- \`src/semantic/roles/__init__.py\`
- \`src/semantic/roles/column_classifier.py\`
- \`src/semantic/roles/enrich_roles.py\`

#### Atomic runner
- \`src/extract/atomic_runner/__init__.py\`
- \`src/extract/atomic_runner/runner.py\`

#### Canonical supplier offer
- \`src/normalize/supplier_offer/__init__.py\`
- \`src/normalize/supplier_offer/price_normalization.py\`
- \`src/normalize/supplier_offer/stock_normalization.py\`
- \`src/normalize/supplier_offer/offer_identity_adapter.py\`
- \`src/normalize/supplier_offer/supplier_offer_builder.py\`

---

## 7. Reference and staged-only code

### 7.1 Donor / reference only
Файлы \`*_donor.py\` нужны как:
- reference
- migration material
- historical donor layer

Они **не считаются production entrypoints**.

### 7.2 Staged, not integrated yet
Слои, которые пока нельзя считать активной частью pipeline:

- \`src/extract/fs_state\`
- \`src/schema_memory\`

Их можно читать и анализировать, но нельзя включать по умолчанию в текущий live path.

---

## 8. Contracts and donor map

### Contracts
- \`docs/contracts/atomic_rows_contract.md\`
- \`docs/contracts/canonical_supplier_offer_contract.md\`

### Existing repo contracts
- \`docs/contracts/CONTRACT_REGISTRY.md\`
- \`docs/contracts/MAPPING_CONTRACT_V1.md\`
- \`docs/contracts/SSOT_V1_4A_INGESTION_PREFLIGHT_LOCK.md\`
- \`docs/contracts/atomic_rows_v1.md\`

### Donor map
- \`docs/legacy_donor/legacy_donor_map.md\`

---

## 9. Current confirmed test path

### 9.1 Confirmed smoke entrypoint
Запускать так:

\`\`\`bash
python3 -m tests.smoke.test_supplier_offer_hardening
\`\`\`

Не запускать так:

\`\`\`bash
python3 tests/smoke/test_supplier_offer_hardening.py
\`\`\`

Иначе пакет \`src\` может не найтись.

### 9.2 What this smoke confirms
Smoke уже подтверждает:
- import path корректен
- \`classify_columns\` работает
- \`enrich_roles\` работает
- \`identity_key_v2\` работает
- \`normalize_stock\` работает
- \`derive_availability\` работает
- \`pick_purchase_price\` работает
- \`build_canonical_supplier_offer\` работает
- \`source_object_id\` обязателен
- fallback на \`raw_name\` работает

### 9.3 What this smoke does NOT prove
Smoke не доказывает:
- real mini E2E на живом supplier sample
- integration path в runtime
- staged activation \`fs_state\`
- staged activation \`schema_memory\`
- production deployment readiness

---

## 10. Current data flow

### 10.1 Conceptual target flow
- Raw Intake Layer
- Structural Extraction
- Normalized Supplier Offers Current
- Quality / Idempotency / Diff
- Marketplace Current

### 10.2 Current practical live flow
Сейчас реально проверяется путь:

1. input / row sample
2. atomic rows
3. semantic role enrichment
4. canonical supplier offer build

Коротко:

- \`atomic_rows\`
- \`enrich_roles\`
- \`canonical supplier offers\`

### 10.3 Current execution understanding
На текущем этапе новый live-layer должен рассматриваться как кандидат на слой:

- \`Normalized Supplier Offers Current\`

А не как финальный end-to-end runtime pipeline.

---

## 11. How to reproduce minimum working state

Это минимальный bootstrap.

### 11.1 Clone
\`\`\`bash
git clone git@github.com:skladkoles44/tirehub-system.git
cd tirehub-system
git checkout refactor/new-contract-order
\`\`\`

### 11.2 Check structure
Убедиться, что существуют:
- \`src/\`
- \`tests/smoke/\`
- \`docs/contracts/\`
- \`docs/legacy_donor/\`

### 11.3 Run smoke
\`\`\`bash
python3 -m tests.smoke.test_supplier_offer_hardening
\`\`\`

### 11.4 Success criteria
Ожидается:
- \`SMOKE=OK\`
- \`IDENTITY_KEY=...\`
- \`SOURCE_OBJECT_ID=...\`
- \`PRICE_PURCHASE_CENTS=...\`
- \`AVAILABILITY=...\`

### 11.5 Honest limitation
Этот bootstrap подтверждает только:
- live-layer code health
- smoke path

Он **не подтверждает**:
- real sample quality
- full E2E
- runtime integration

---

## 12. Current maintainer environment

Этот раздел описывает **среду текущего основного мейнтейнера**, а не обязательную среду для всех.

### Current maintainer working copy
- \`/data/data/com.termux/files/home/tirehub-system\`

### Current maintainer ETL storage
- \`/storage/emulated/0/Download/ETL\`

### Useful maintainer paths
- \`/storage/emulated/0/Download/ETL/_diagnostics\`
- \`/storage/emulated/0/Download/ETL/canonical-core_dump\`
- \`/storage/emulated/0/Download/ETL/out_test1\`

### Current maintainer SSH alias
- \`tetl\`

Этот раздел нужен как операционный контекст, но не должен трактоваться как обязательная форма dev-setup для всех новых инженеров.

---

## 13. Runtime facts

### 13.1 Confirmed VPS
- \`194.67.119.25\`

### 13.2 SSH user
- \`Test_etl\`

### 13.3 Root user
- \`root\`

### 13.4 Home directories
- \`/home/Test_etl\`
- \`/root\`

### 13.5 Active service
- \`canonical-core.service\`

### 13.6 Runtime root
- \`/opt/canonical-core\`

### 13.7 Key runtime paths
- \`/opt/canonical-core/_incoming\`
- \`/opt/canonical-core/var\`
- \`/opt/canonical-core/var/artifacts\`
- \`/opt/canonical-core/data\`
- \`/opt/canonical-core/data/raw\`
- \`/opt/canonical-core/logs\`
- \`/opt/canonical-core/_db_backups\`

### 13.8 Service unit facts
- \`EnvironmentFile=/opt/canonical-core/.env\`
- \`WorkingDirectory=/opt/canonical-core\`
- \`ExecStart=/opt/canonical-core/venv/bin/gunicorn app.main:app -k uvicorn.workers.UvicornWorker --workers 2 --bind 127.0.0.1:8000\`

### 13.9 Runtime safety rule
Не менять runtime без отдельной задачи, отдельного review и отдельного решения.

---

## 14. Legacy / transition facts

### Old normalizer
- \`scripts/normalization/normalizer_v3_1.py\`

Статус:
- отдельный старый/переходный контур
- не входит в commit \`b49d1c2\`
- не должен silently смешиваться с новым \`src/normalize/supplier_offer/*\`

Если с ним работать:
- отдельная задача
- отдельный commit
- отдельное решение

---

## 15. What to ignore unless explicitly tasked

Без отдельной задачи новый инженер должен игнорировать:
- \`/opt/tirehub\`
- staged-only modules
- donor files как entrypoints
- deploy-level runtime patching
- old backup files in runtime
- legacy runtime artifacts

---

## 16. Current merge gates

Изменение считается готовым к следующей фазе только если:

1. smoke green
2. слой \`supplier reality\` не смешан с \`marketplace reality\`
3. нет прямого runtime-edit как части feature work
4. layer boundaries не нарушены
5. для перехода в integration phase пройден real mini E2E

### Review-required changes
Любое изменение, которое затрагивает:
- normalize semantics
- identity semantics
- current-state update semantics
- integration point
- runtime behavior

должно считаться review-required.

---

## 17. Next mandatory milestone

Следующий обязательный шаг:

**real mini E2E on a live \`atomic_rows.ndjson\` sample**

Нужно:
1. взять живой sample \`atomic_rows.ndjson\`
2. прогнать через \`enrich_roles\`
3. собрать canonical supplier offers
4. получить:
   - sample good rows
   - sample reject rows
   - counters
   - xray quality summary

Проверять:
- identity quality
- price normalization
- stock normalization
- availability semantics
- field fill-rate
- reject reasons
- supplier semantics retention

### Do not replace this milestone with:
- deploy
- runtime switch
- \`fs_state\` activation
- \`schema_memory\` activation
- marketplace projection work

---

## 18. Success condition of the current phase

Текущая стратегия считается успешной, если:

1. новый live-layer работает на реальном supplier sample
2. результат пригоден как кандидат на \`Normalized Supplier Offers Current\`
3. contracts не нарушены
4. pipeline детерминирован
5. integration path станет понятным без прямого вмешательства в runtime

---

## 19. Failure condition of the current phase

Работа идёт неправильно, если:

1. donor-code начинает подменять architectural law
2. runtime tree используется как dev tree
3. smoke подменяет реальный mini E2E
4. supplier reality смешивается с marketplace reality
5. staged-only layers включаются раньше времени
6. runtime deploy обсуждается до доказанного результата на живом sample

---

## 20. One-line definition

**хаотичный supplier input -> детерминированное supplier current state -> отдельный marketplace current state**
