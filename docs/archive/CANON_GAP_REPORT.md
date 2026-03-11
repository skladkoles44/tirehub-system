# CANON GAP REPORT

## 1. Canon Commit
3332ec26fb9a0c0103979bd6decdf282162ca226

## 2. Git Status
?? docs/etl/CANON_GAP_REPORT.md

## 3. Repo Tree (depth 3)
[01;34m.[0m
├── [00mARCHITECTURE_v1_LOCK.md[0m
├── [00mMakefile[0m
├── [00mREADME.md[0m
├── [01;34mcurated_v1[0m
│   └── [01;34minput[0m
├── [01;34mdiag[0m
│   └── [00mssot_v1_4a_diag_full_tirehub_etl_20260202_070136.log[0m
├── [01;34mdocs[0m
│   ├── [01;34mcontracts[0m
│   │   ├── [00mKOLOBOX_XLS_CONTRACT_V1.md[0m
│   │   ├── [00mKOLOBOX_XLS_MAPPING_V1.yaml[0m
│   │   ├── [00mNDJSON_INGESTION_PRODUCTION_CONTRACT.md[0m
│   │   ├── [00mSSOT_V1_4A_INGESTION_PREFLIGHT_LOCK.md[0m
│   │   └── [00mSSOT_V1_4A_PARSER_ID_CONTRACT.md[0m
│   ├── [01;34metl[0m
│   │   ├── [00mCANON_GAP_REPORT.md[0m
│   │   ├── [00mCURATED_V1.md[0m
│   │   ├── [00mETL_CANON.md[0m
│   │   ├── [00mETL_CANON_TEST.md[0m
│   │   ├── [00mINGESTION_V1.md[0m
│   │   └── [01;34marchive[0m
│   ├── [01;34mingestion[0m
│   │   └── [01;34mkolobox[0m
│   └── [01;34mspec[0m
│       ├── [00mETL_EXTRACTOR_SPEC_FINAL_v3_3.md[0m
│       ├── [00mbackfill_snapshots_v1.6_plus.md[0m
│       ├── [00mcanonical_storage_contract.md[0m
│       ├── [00mdecomposer_v1.7_content_first_ssot.md[0m
│       ├── [00mglossary_etl.md[0m
│       └── [00mgolden_set_contract_v1.md[0m
├── [01;34mgolden[0m
│   ├── [00mconflict.ndjson[0m
│   ├── [00mmanifest.json[0m
│   ├── [00mnegative.ndjson[0m
│   └── [00mpositive.ndjson[0m
├── [01;34minputs[0m
│   └── [01;34minbox[0m
├── [01;34mmappings[0m
│   └── [01;34msuppliers[0m
│       ├── [00mbrinex_xlsx_v1.yaml[0m
│       ├── [00mcentrshin_diski_xlsx_v1.yaml[0m
│       ├── [00mcentrshin_json_v1.yaml[0m
│       ├── [00mcentrshin_shiny_xlsx_v1.yaml[0m
│       ├── [00mkolobox.yaml[0m
│       ├── [00mkolobox_diski_xls_v1.yaml[0m
│       ├── [00mkolobox_komplektatsii_xls_v1.yaml[0m
│       └── [00mkolobox_truck_xls_v1.yaml[0m
├── [01;34moffers_v1[0m
│   └── [01;34minput[0m
├── [01;34mout[0m
├── [01;34mrulesets[0m
│   ├── [01;34mcurated[0m
│   ├── [01;34mgate_baselines[0m
│   │   └── [00mkolobox_xls_v1.baseline.json[0m
│   ├── [01;34mimpact[0m
│   │   └── [00mimpact_weights_v1.yaml[0m
│   ├── [01;34moffers[0m
│   ├── [01;34msanity[0m
│   │   └── [00msanity_checks_v1.yaml[0m
│   ├── [01;34msignature_normalization[0m
│   │   └── [00msignature_normalization_v1.yaml[0m
│   └── [01;34mtoken_pattern_fields[0m
│       └── [00mtoken_pattern_fields_v1.yaml[0m
├── [01;34mschemas[0m
│   ├── [00mcanonical_item.schema.json[0m
│   └── [00mrejected_item.schema.json[0m
├── [01;34mscripts[0m
│   ├── [01;34mcurated[0m
│   │   └── [00mtirehub_curate_v1.py[0m
│   ├── [01;34mdev[0m
│   │   └── [00mcentrshin_list_arrays.sh[0m
│   ├── [01;34metl[0m
│   │   └── [00mpreflight_ssot_ingestion.sh[0m
│   ├── [01;34mingestion[0m
│   │   ├── [01;34mbrinex[0m
│   │   ├── [01;34mcentrshin[0m
│   │   ├── [00memit_generic_ndjson_v1.py[0m
│   │   ├── [01;34mkolobox[0m
│   │   ├── [00mrun_inbox_batch_v1.py[0m
│   │   └── [00mtirehub_ingest_v1.py[0m
│   ├── [00mrun_kolobox_full_v1.sh[0m
│   └── [01;34msmoke[0m
│       ├── [00mrun_ssot_v1_4a_seed.sh[0m
│       ├── [01;32mrun_ssot_v1_4a_smoke.sh[0m
│       ├── [00mssot_v1_4a_migrations.list[0m
│       └── [00mssot_v1_4a_smoke.sql[0m
├── [01;34msql[0m
│   ├── [01;34mmigrations[0m
│   │   ├── [00m20260201_ssot_v1_4a_fix.sql[0m
│   │   ├── [00m20260202_ssot_v1_4a_add_publish_artifact.sql[0m
│   │   ├── [00m20260202_ssot_v1_4a_fix_constraint.sql[0m
│   │   ├── [00m20260202_ssot_v1_4a_internal_core.sql[0m
│   │   ├── [00m20260202_ssot_v1_4a_smoke_bridge.sql[0m
│   │   ├── [00m20260202_ssot_v1_4a_statusv14a_fix.sql[0m
│   │   └── [00m20260203_ssot_v1_4a_add_parser_id.sql[0m
│   ├── [00mschema_v1.sql[0m
│   └── [00mschema_v1_4a.sql[0m
├── [01;31mssot_v1_4a_bundle_20260202_070653.tar.gz[0m
└── [00mssot_v1_4a_diagnostics_20260201_223953.sql.txt[0m

39 directories, 61 files

## 4. Ingestion Modules (grep)
./README.md:11:Extractor → Emitter → Gate → Ingestion → SSOT
./docs/contracts/NDJSON_INGESTION_PRODUCTION_CONTRACT.md:56:## Gates
./docs/etl/CURATED_V1.md:2:Назначение: потребитель SSOT сегмента (facts/<...>/<run_id>.ndjson). Делает бизнес-отбор “продаваемых” строк поверх валидных фактов (GOOD), не меняя сами факты.
./docs/etl/CURATED_V1.md:3:Вход: manifest.json (ssot/manifests/<run_id>.json) с paths.segment
./docs/etl/CURATED_V1.md:4:Выходы (curated_v1/out/<run_id>/):
./docs/etl/CURATED_V1.md:16:--manifest PATH (обяз.)
./docs/etl/ETL_CANON.md:17:Run-артефакты, quarantine и manifest — операционный след с ограниченным сроком хранения.
./docs/etl/ETL_CANON.md:53:  accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json
./docs/etl/ETL_CANON.md:54:  runs/<run_id>/
./docs/etl/ETL_CANON.md:58:    manifest.json
./docs/etl/ETL_CANON.md:60:  state_store/<supplier>/<run_id>/
./docs/etl/ETL_CANON.md:65:DQ-конфиг:
./docs/etl/ETL_CANON.md:78:4. Файлы, SHA которых уже присутствует в accepted_runs → переместить в quarantine/<supplier>/duplicates/
./docs/etl/ETL_CANON.md:86:Истинная хронология определяется run_id.
./docs/etl/ETL_CANON.md:110:6. run_id
./docs/etl/ETL_CANON.md:123:7. Gate (технический + schema check)
./docs/etl/ETL_CANON.md:139:- создаётся manifest со status=FAIL
./docs/etl/ETL_CANON.md:141:Gate не содержит бизнес-логики.
./docs/etl/ETL_CANON.md:144:8. Data Quality (DQ)
./docs/etl/ETL_CANON.md:156:Если конфиг отсутствует → FAIL (CONFIG_DQ_MISSING).
./docs/etl/ETL_CANON.md:169:Хранятся в runs/<run_id>/
./docs/etl/ETL_CANON.md:175:- run_id
./docs/etl/ETL_CANON.md:191:  "run_id": "brinex_20260213T194500Z_1a2b3c",
./docs/etl/ETL_CANON.md:215:run_id в вычисление не входит.
./docs/etl/ETL_CANON.md:229:- run_id
./docs/etl/ETL_CANON.md:255:- run_id
./docs/etl/ETL_CANON.md:266:manifest хранится как операционный след.
./docs/etl/ETL_CANON.md:269:11. accepted_runs
./docs/etl/ETL_CANON.md:273:accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json
./docs/etl/ETL_CANON.md:282:  "run_id": "...",
./docs/etl/ETL_CANON.md:285:  "manifest_path": "..."
./docs/etl/ETL_CANON.md:295:state_store/<supplier>/<run_id>/facts.ndjson
./docs/etl/ETL_CANON.md:297:Содержимое state_store никогда не изменяется после публикации.
./docs/etl/ETL_CANON.md:309:state_store/<supplier>/<run_id> хранится минимум state_retention_hours
./docs/etl/ETL_CANON.md:327:- accepted_runs до истечения retention
./docs/etl/ETL_CANON.md:343:Повторная обработка того же файла невозможна благодаря accepted_runs.
./docs/etl/ETL_CANON.md:355:Gate → технический + schema check
./docs/etl/ETL_CANON.md:356:DQ → контроль качества
./docs/etl/ETL_CANON.md:363:- запуск без DQ-конфига
./docs/etl/ETL_CANON.md:365:- прямое чтение state_store
./docs/etl/ETL_CANON_TEST.md:15:- проверки DQ-порогов
./docs/etl/ETL_CANON_TEST.md:77:- run_id может быть перезапущен
./docs/etl/ETL_CANON_TEST.md:81:## 6. Gate
./docs/etl/ETL_CANON_TEST.md:83:Gate выполняет технические проверки.
./docs/etl/ETL_CANON_TEST.md:95:DQ-пороги могут быть:
./docs/etl/INGESTION_V1.md:1:INGESTION v1 (SSOT segmented by run_id)
./docs/etl/INGESTION_V1.md:3:Gate rule: ingest only if verdict in PASS/WARN
./docs/etl/INGESTION_V1.md:4:Idempotency: accepted_runs/<run_id>.json exists => exit 0 (already_ingested)
./docs/etl/INGESTION_V1.md:5:Locking: locks/<run_id>.lock created atomically (O_EXCL); if exists => exit 1
./docs/etl/INGESTION_V1.md:6:Validation: every NDJSON line must contain required fields and types; run_id/effective_at/mapping_hash/mapping_version must match stats
./docs/etl/INGESTION_V1.md:7:Commit: write tmp/<run_id>.ndjson.tmp then atomic rename to facts/<supplier>/<parser>/<YYYY-MM>/<run_id>.ndjson
./docs/etl/INGESTION_V1.md:8:After commit: write manifests/<run_id>.json then accepted_runs/<run_id>.json
./docs/etl/archive/ETL_CANON_V1.md:8:Система построена по модели: Extractor → Emitter → Gate → Ingestion → Curated / Offers.  
./docs/etl/archive/ETL_CANON_V1.md:13:Gate решает судьбу файла как события.  
./docs/etl/archive/ETL_CANON_V1.md:28:run_id — идентификатор одного прогона (один входной файл).  
./docs/etl/archive/ETL_CANON_V1.md:58:supplier_id, parser_id, mapping_version, ndjson_contract_version, emitter_version, run_id, effective_at, raw, parsed, quality_flags, _meta.
./docs/etl/archive/ETL_CANON_V1.md:64:Gate  
./docs/etl/archive/ETL_CANON_V1.md:65:Gate работает с файлом как событием.  
./docs/etl/archive/ETL_CANON_V1.md:66:Gate принимает решение PASS / WARN / FAIL.
./docs/etl/archive/ETL_CANON_V1.md:88:Gate решает судьбу файла.  
./docs/etl/archive/ETL_CANON_V1_QA.md:6:Ответ: канон сознательно не задаёт процент. BAD-строки не валят файл. Gate фиксирует PASS/WARN/FAIL по событию файла. Наличие BAD даёт минимум WARN. Алертинг и решение “что делать” лежат выше (оркестратор/процессы). FAIL — только ошибки уровня файла (нечитаемость/сломанная структура/нарушение контракта файла).
./docs/etl/archive/ETL_CANON_V1_QA.md:9:3) Идемпотентность: один и тот же файл обработали дважды разными run_id.
./docs/etl/archive/ETL_CANON_V1_QA.md:10:Ответ: канон даёт механизмы (run_id, effective_at, метрики, артефакты). Защита от повторной обработки — обязанность оркестратора/операционного контура. История допустит два события, а витрина выбирает “что считать актуальным” своими правилами.
./docs/etl/archive/ETL_CANON_V1_QA.md:30:Вопросы по Gate и бизнес-логике
./docs/etl/archive/ETL_CANON_V1_QA.md:44:2) Distributed lock на run_id: как реализовать и что с зависанием?
./docs/etl/archive/ETL_CANON_V1_QA.md:49:Ответ: повтор по run_id с очисткой незасиленного промежуточного состояния. Append-only история и идемпотентные артефакты облегчают восстановление.
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:12:3. run_id генерируется автоматически.
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:14:5. Gate не содержит бизнес-логики.
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:36:      runs/<run_id>/
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:37:      accepted_runs/
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:39:      manifests/
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:44:  runs/<run_id>/
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:66:4. Если (supplier, sha256) уже в accepted_runs:
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:77:## 4. run_id
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:90:## 5. Gate (технический)
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:92:Gate проверяет:
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:102:Gate не содержит бизнес-правил.
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:112:- run.manifest.json
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:120:"run_id": "...",
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:129:"run_id": "...",
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:135:manifest
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:138:"run_id": "...",
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:151:etl_data/raw_v1/ssot/runs/<run_id>/run.manifest.json
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:155:etl_ops/runs/<run_id>/ может содержать symlink.
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:166:- изменять manifest
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:171:accepted_runs/
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:178:DQ выполняется в каждом run.
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:189:empty_file не является метрикой DQ — это FAIL в Gate.
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:198:- accepted_runs/
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:244:- run_id генерируется системой.
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:256:- содержит DQ thresholds
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:264:- читать manifest из etl_ops как источник истины
./docs/etl/archive/ETL_CANON_20260214T114156Z.md:266:- запускать без DQ-порогов
./docs/etl/CANON_GAP_REPORT.md:43:│   ├── [00mmanifest.json[0m
./docs/etl/CANON_GAP_REPORT.md:114:./README.md:11:Extractor → Emitter → Gate → Ingestion → SSOT
./docs/etl/CANON_GAP_REPORT.md:115:./docs/contracts/NDJSON_INGESTION_PRODUCTION_CONTRACT.md:56:## Gates
./docs/etl/CANON_GAP_REPORT.md:116:./docs/etl/CURATED_V1.md:2:Назначение: потребитель SSOT сегмента (facts/<...>/<run_id>.ndjson). Делает бизнес-отбор “продаваемых” строк поверх валидных фактов (GOOD), не меняя сами факты.
./docs/etl/CANON_GAP_REPORT.md:117:./docs/etl/CURATED_V1.md:3:Вход: manifest.json (ssot/manifests/<run_id>.json) с paths.segment
./docs/etl/CANON_GAP_REPORT.md:118:./docs/etl/CURATED_V1.md:4:Выходы (curated_v1/out/<run_id>/):
./docs/etl/CANON_GAP_REPORT.md:119:./docs/etl/CURATED_V1.md:16:--manifest PATH (обяз.)
./docs/etl/CANON_GAP_REPORT.md:120:./docs/etl/ETL_CANON.md:17:Run-артефакты, quarantine и manifest — операционный след с ограниченным сроком хранения.
./docs/etl/CANON_GAP_REPORT.md:121:./docs/etl/ETL_CANON.md:53:  accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json
./docs/etl/CANON_GAP_REPORT.md:122:./docs/etl/ETL_CANON.md:54:  runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:123:./docs/etl/ETL_CANON.md:58:    manifest.json
./docs/etl/CANON_GAP_REPORT.md:124:./docs/etl/ETL_CANON.md:60:  state_store/<supplier>/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:125:./docs/etl/ETL_CANON.md:65:DQ-конфиг:
./docs/etl/CANON_GAP_REPORT.md:126:./docs/etl/ETL_CANON.md:78:4. Файлы, SHA которых уже присутствует в accepted_runs → переместить в quarantine/<supplier>/duplicates/
./docs/etl/CANON_GAP_REPORT.md:127:./docs/etl/ETL_CANON.md:86:Истинная хронология определяется run_id.
./docs/etl/CANON_GAP_REPORT.md:128:./docs/etl/ETL_CANON.md:110:6. run_id
./docs/etl/CANON_GAP_REPORT.md:129:./docs/etl/ETL_CANON.md:123:7. Gate (технический + schema check)
./docs/etl/CANON_GAP_REPORT.md:130:./docs/etl/ETL_CANON.md:139:- создаётся manifest со status=FAIL
./docs/etl/CANON_GAP_REPORT.md:131:./docs/etl/ETL_CANON.md:141:Gate не содержит бизнес-логики.
./docs/etl/CANON_GAP_REPORT.md:132:./docs/etl/ETL_CANON.md:144:8. Data Quality (DQ)
./docs/etl/CANON_GAP_REPORT.md:133:./docs/etl/ETL_CANON.md:156:Если конфиг отсутствует → FAIL (CONFIG_DQ_MISSING).
./docs/etl/CANON_GAP_REPORT.md:134:./docs/etl/ETL_CANON.md:169:Хранятся в runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:135:./docs/etl/ETL_CANON.md:175:- run_id
./docs/etl/CANON_GAP_REPORT.md:136:./docs/etl/ETL_CANON.md:191:  "run_id": "brinex_20260213T194500Z_1a2b3c",
./docs/etl/CANON_GAP_REPORT.md:137:./docs/etl/ETL_CANON.md:215:run_id в вычисление не входит.
./docs/etl/CANON_GAP_REPORT.md:138:./docs/etl/ETL_CANON.md:229:- run_id
./docs/etl/CANON_GAP_REPORT.md:139:./docs/etl/ETL_CANON.md:255:- run_id
./docs/etl/CANON_GAP_REPORT.md:140:./docs/etl/ETL_CANON.md:266:manifest хранится как операционный след.
./docs/etl/CANON_GAP_REPORT.md:141:./docs/etl/ETL_CANON.md:269:11. accepted_runs
./docs/etl/CANON_GAP_REPORT.md:142:./docs/etl/ETL_CANON.md:273:accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json
./docs/etl/CANON_GAP_REPORT.md:143:./docs/etl/ETL_CANON.md:282:  "run_id": "...",
./docs/etl/CANON_GAP_REPORT.md:144:./docs/etl/ETL_CANON.md:285:  "manifest_path": "..."
./docs/etl/CANON_GAP_REPORT.md:145:./docs/etl/ETL_CANON.md:295:state_store/<supplier>/<run_id>/facts.ndjson
./docs/etl/CANON_GAP_REPORT.md:146:./docs/etl/ETL_CANON.md:297:Содержимое state_store никогда не изменяется после публикации.
./docs/etl/CANON_GAP_REPORT.md:147:./docs/etl/ETL_CANON.md:309:state_store/<supplier>/<run_id> хранится минимум state_retention_hours
./docs/etl/CANON_GAP_REPORT.md:148:./docs/etl/ETL_CANON.md:327:- accepted_runs до истечения retention
./docs/etl/CANON_GAP_REPORT.md:149:./docs/etl/ETL_CANON.md:343:Повторная обработка того же файла невозможна благодаря accepted_runs.
./docs/etl/CANON_GAP_REPORT.md:150:./docs/etl/ETL_CANON.md:355:Gate → технический + schema check
./docs/etl/CANON_GAP_REPORT.md:151:./docs/etl/ETL_CANON.md:356:DQ → контроль качества
./docs/etl/CANON_GAP_REPORT.md:152:./docs/etl/ETL_CANON.md:363:- запуск без DQ-конфига
./docs/etl/CANON_GAP_REPORT.md:153:./docs/etl/ETL_CANON.md:365:- прямое чтение state_store
./docs/etl/CANON_GAP_REPORT.md:154:./docs/etl/ETL_CANON_TEST.md:15:- проверки DQ-порогов
./docs/etl/CANON_GAP_REPORT.md:155:./docs/etl/ETL_CANON_TEST.md:77:- run_id может быть перезапущен
./docs/etl/CANON_GAP_REPORT.md:156:./docs/etl/ETL_CANON_TEST.md:81:## 6. Gate
./docs/etl/CANON_GAP_REPORT.md:157:./docs/etl/ETL_CANON_TEST.md:83:Gate выполняет технические проверки.
./docs/etl/CANON_GAP_REPORT.md:158:./docs/etl/ETL_CANON_TEST.md:95:DQ-пороги могут быть:
./docs/etl/CANON_GAP_REPORT.md:159:./docs/etl/INGESTION_V1.md:1:INGESTION v1 (SSOT segmented by run_id)
./docs/etl/CANON_GAP_REPORT.md:160:./docs/etl/INGESTION_V1.md:3:Gate rule: ingest only if verdict in PASS/WARN
./docs/etl/CANON_GAP_REPORT.md:161:./docs/etl/INGESTION_V1.md:4:Idempotency: accepted_runs/<run_id>.json exists => exit 0 (already_ingested)
./docs/etl/CANON_GAP_REPORT.md:162:./docs/etl/INGESTION_V1.md:5:Locking: locks/<run_id>.lock created atomically (O_EXCL); if exists => exit 1
./docs/etl/CANON_GAP_REPORT.md:163:./docs/etl/INGESTION_V1.md:6:Validation: every NDJSON line must contain required fields and types; run_id/effective_at/mapping_hash/mapping_version must match stats
./docs/etl/CANON_GAP_REPORT.md:164:./docs/etl/INGESTION_V1.md:7:Commit: write tmp/<run_id>.ndjson.tmp then atomic rename to facts/<supplier>/<parser>/<YYYY-MM>/<run_id>.ndjson
./docs/etl/CANON_GAP_REPORT.md:165:./docs/etl/INGESTION_V1.md:8:After commit: write manifests/<run_id>.json then accepted_runs/<run_id>.json
./docs/etl/CANON_GAP_REPORT.md:166:./docs/etl/archive/ETL_CANON_V1.md:8:Система построена по модели: Extractor → Emitter → Gate → Ingestion → Curated / Offers.  
./docs/etl/CANON_GAP_REPORT.md:167:./docs/etl/archive/ETL_CANON_V1.md:13:Gate решает судьбу файла как события.  
./docs/etl/CANON_GAP_REPORT.md:168:./docs/etl/archive/ETL_CANON_V1.md:28:run_id — идентификатор одного прогона (один входной файл).  
./docs/etl/CANON_GAP_REPORT.md:169:./docs/etl/archive/ETL_CANON_V1.md:58:supplier_id, parser_id, mapping_version, ndjson_contract_version, emitter_version, run_id, effective_at, raw, parsed, quality_flags, _meta.
./docs/etl/CANON_GAP_REPORT.md:170:./docs/etl/archive/ETL_CANON_V1.md:64:Gate  
./docs/etl/CANON_GAP_REPORT.md:171:./docs/etl/archive/ETL_CANON_V1.md:65:Gate работает с файлом как событием.  
./docs/etl/CANON_GAP_REPORT.md:172:./docs/etl/archive/ETL_CANON_V1.md:66:Gate принимает решение PASS / WARN / FAIL.
./docs/etl/CANON_GAP_REPORT.md:173:./docs/etl/archive/ETL_CANON_V1.md:88:Gate решает судьбу файла.  
./docs/etl/CANON_GAP_REPORT.md:174:./docs/etl/archive/ETL_CANON_V1_QA.md:6:Ответ: канон сознательно не задаёт процент. BAD-строки не валят файл. Gate фиксирует PASS/WARN/FAIL по событию файла. Наличие BAD даёт минимум WARN. Алертинг и решение “что делать” лежат выше (оркестратор/процессы). FAIL — только ошибки уровня файла (нечитаемость/сломанная структура/нарушение контракта файла).
./docs/etl/CANON_GAP_REPORT.md:175:./docs/etl/archive/ETL_CANON_V1_QA.md:9:3) Идемпотентность: один и тот же файл обработали дважды разными run_id.
./docs/etl/CANON_GAP_REPORT.md:176:./docs/etl/archive/ETL_CANON_V1_QA.md:10:Ответ: канон даёт механизмы (run_id, effective_at, метрики, артефакты). Защита от повторной обработки — обязанность оркестратора/операционного контура. История допустит два события, а витрина выбирает “что считать актуальным” своими правилами.
./docs/etl/CANON_GAP_REPORT.md:177:./docs/etl/archive/ETL_CANON_V1_QA.md:30:Вопросы по Gate и бизнес-логике
./docs/etl/CANON_GAP_REPORT.md:178:./docs/etl/archive/ETL_CANON_V1_QA.md:44:2) Distributed lock на run_id: как реализовать и что с зависанием?
./docs/etl/CANON_GAP_REPORT.md:179:./docs/etl/archive/ETL_CANON_V1_QA.md:49:Ответ: повтор по run_id с очисткой незасиленного промежуточного состояния. Append-only история и идемпотентные артефакты облегчают восстановление.
./docs/etl/CANON_GAP_REPORT.md:180:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:12:3. run_id генерируется автоматически.
./docs/etl/CANON_GAP_REPORT.md:181:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:14:5. Gate не содержит бизнес-логики.
./docs/etl/CANON_GAP_REPORT.md:182:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:36:      runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:183:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:37:      accepted_runs/
./docs/etl/CANON_GAP_REPORT.md:184:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:39:      manifests/
./docs/etl/CANON_GAP_REPORT.md:185:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:44:  runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:186:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:66:4. Если (supplier, sha256) уже в accepted_runs:
./docs/etl/CANON_GAP_REPORT.md:187:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:77:## 4. run_id
./docs/etl/CANON_GAP_REPORT.md:188:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:90:## 5. Gate (технический)
./docs/etl/CANON_GAP_REPORT.md:189:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:92:Gate проверяет:
./docs/etl/CANON_GAP_REPORT.md:190:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:102:Gate не содержит бизнес-правил.
./docs/etl/CANON_GAP_REPORT.md:191:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:112:- run.manifest.json
./docs/etl/CANON_GAP_REPORT.md:192:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:120:"run_id": "...",
./docs/etl/CANON_GAP_REPORT.md:193:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:129:"run_id": "...",
./docs/etl/CANON_GAP_REPORT.md:194:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:135:manifest
./docs/etl/CANON_GAP_REPORT.md:195:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:138:"run_id": "...",
./docs/etl/CANON_GAP_REPORT.md:196:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:151:etl_data/raw_v1/ssot/runs/<run_id>/run.manifest.json
./docs/etl/CANON_GAP_REPORT.md:197:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:155:etl_ops/runs/<run_id>/ может содержать symlink.
./docs/etl/CANON_GAP_REPORT.md:198:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:166:- изменять manifest
./docs/etl/CANON_GAP_REPORT.md:199:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:171:accepted_runs/
./docs/etl/CANON_GAP_REPORT.md:200:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:178:DQ выполняется в каждом run.
./docs/etl/CANON_GAP_REPORT.md:201:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:189:empty_file не является метрикой DQ — это FAIL в Gate.
./docs/etl/CANON_GAP_REPORT.md:202:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:198:- accepted_runs/
./docs/etl/CANON_GAP_REPORT.md:203:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:244:- run_id генерируется системой.
./docs/etl/CANON_GAP_REPORT.md:204:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:256:- содержит DQ thresholds
./docs/etl/CANON_GAP_REPORT.md:205:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:264:- читать manifest из etl_ops как источник истины
./docs/etl/CANON_GAP_REPORT.md:206:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:266:- запускать без DQ-порогов
./docs/etl/CANON_GAP_REPORT.md:207:./docs/etl/CANON_GAP_REPORT.md:43:│   ├── [00mmanifest.json[0m
./docs/etl/CANON_GAP_REPORT.md:208:./docs/etl/CANON_GAP_REPORT.md:114:./README.md:11:Extractor → Emitter → Gate → Ingestion → SSOT
./docs/etl/CANON_GAP_REPORT.md:209:./docs/etl/CANON_GAP_REPORT.md:115:./docs/contracts/NDJSON_INGESTION_PRODUCTION_CONTRACT.md:56:## Gates
./docs/etl/CANON_GAP_REPORT.md:210:./docs/etl/CANON_GAP_REPORT.md:116:./docs/etl/CURATED_V1.md:2:Назначение: потребитель SSOT сегмента (facts/<...>/<run_id>.ndjson). Делает бизнес-отбор “продаваемых” строк поверх валидных фактов (GOOD), не меняя сами факты.
./docs/etl/CANON_GAP_REPORT.md:211:./docs/etl/CANON_GAP_REPORT.md:117:./docs/etl/CURATED_V1.md:3:Вход: manifest.json (ssot/manifests/<run_id>.json) с paths.segment
./docs/etl/CANON_GAP_REPORT.md:212:./docs/etl/CANON_GAP_REPORT.md:118:./docs/etl/CURATED_V1.md:4:Выходы (curated_v1/out/<run_id>/):
./docs/etl/CANON_GAP_REPORT.md:213:./docs/etl/CANON_GAP_REPORT.md:119:./docs/etl/CURATED_V1.md:16:--manifest PATH (обяз.)
./docs/etl/CANON_GAP_REPORT.md:214:./docs/etl/CANON_GAP_REPORT.md:120:./docs/etl/ETL_CANON.md:17:Run-артефакты, quarantine и manifest — операционный след с ограниченным сроком хранения.
./docs/etl/CANON_GAP_REPORT.md:215:./docs/etl/CANON_GAP_REPORT.md:121:./docs/etl/ETL_CANON.md:53:  accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json
./docs/etl/CANON_GAP_REPORT.md:216:./docs/etl/CANON_GAP_REPORT.md:122:./docs/etl/ETL_CANON.md:54:  runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:217:./docs/etl/CANON_GAP_REPORT.md:123:./docs/etl/ETL_CANON.md:58:    manifest.json
./docs/etl/CANON_GAP_REPORT.md:218:./docs/etl/CANON_GAP_REPORT.md:124:./docs/etl/ETL_CANON.md:60:  state_store/<supplier>/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:219:./docs/etl/CANON_GAP_REPORT.md:125:./docs/etl/ETL_CANON.md:65:DQ-конфиг:
./docs/etl/CANON_GAP_REPORT.md:220:./docs/etl/CANON_GAP_REPORT.md:126:./docs/etl/ETL_CANON.md:78:4. Файлы, SHA которых уже присутствует в accepted_runs → переместить в quarantine/<supplier>/duplicates/
./docs/etl/CANON_GAP_REPORT.md:221:./docs/etl/CANON_GAP_REPORT.md:127:./docs/etl/ETL_CANON.md:86:Истинная хронология определяется run_id.
./docs/etl/CANON_GAP_REPORT.md:222:./docs/etl/CANON_GAP_REPORT.md:128:./docs/etl/ETL_CANON.md:110:6. run_id
./docs/etl/CANON_GAP_REPORT.md:223:./docs/etl/CANON_GAP_REPORT.md:129:./docs/etl/ETL_CANON.md:123:7. Gate (технический + schema check)
./docs/etl/CANON_GAP_REPORT.md:224:./docs/etl/CANON_GAP_REPORT.md:130:./docs/etl/ETL_CANON.md:139:- создаётся manifest со status=FAIL
./docs/etl/CANON_GAP_REPORT.md:225:./docs/etl/CANON_GAP_REPORT.md:131:./docs/etl/ETL_CANON.md:141:Gate не содержит бизнес-логики.
./docs/etl/CANON_GAP_REPORT.md:226:./docs/etl/CANON_GAP_REPORT.md:132:./docs/etl/ETL_CANON.md:144:8. Data Quality (DQ)
./docs/etl/CANON_GAP_REPORT.md:227:./docs/etl/CANON_GAP_REPORT.md:133:./docs/etl/ETL_CANON.md:156:Если конфиг отсутствует → FAIL (CONFIG_DQ_MISSING).
./docs/etl/CANON_GAP_REPORT.md:228:./docs/etl/CANON_GAP_REPORT.md:134:./docs/etl/ETL_CANON.md:169:Хранятся в runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:229:./docs/etl/CANON_GAP_REPORT.md:135:./docs/etl/ETL_CANON.md:175:- run_id
./docs/etl/CANON_GAP_REPORT.md:230:./docs/etl/CANON_GAP_REPORT.md:136:./docs/etl/ETL_CANON.md:191:  "run_id": "brinex_20260213T194500Z_1a2b3c",
./docs/etl/CANON_GAP_REPORT.md:231:./docs/etl/CANON_GAP_REPORT.md:137:./docs/etl/ETL_CANON.md:215:run_id в вычисление не входит.
./docs/etl/CANON_GAP_REPORT.md:232:./docs/etl/CANON_GAP_REPORT.md:138:./docs/etl/ETL_CANON.md:229:- run_id
./docs/etl/CANON_GAP_REPORT.md:233:./docs/etl/CANON_GAP_REPORT.md:139:./docs/etl/ETL_CANON.md:255:- run_id
./docs/etl/CANON_GAP_REPORT.md:234:./docs/etl/CANON_GAP_REPORT.md:140:./docs/etl/ETL_CANON.md:266:manifest хранится как операционный след.
./docs/etl/CANON_GAP_REPORT.md:235:./docs/etl/CANON_GAP_REPORT.md:141:./docs/etl/ETL_CANON.md:269:11. accepted_runs
./docs/etl/CANON_GAP_REPORT.md:236:./docs/etl/CANON_GAP_REPORT.md:142:./docs/etl/ETL_CANON.md:273:accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json
./docs/etl/CANON_GAP_REPORT.md:237:./docs/etl/CANON_GAP_REPORT.md:143:./docs/etl/ETL_CANON.md:282:  "run_id": "...",
./docs/etl/CANON_GAP_REPORT.md:238:./docs/etl/CANON_GAP_REPORT.md:144:./docs/etl/ETL_CANON.md:285:  "manifest_path": "..."
./docs/etl/CANON_GAP_REPORT.md:239:./docs/etl/CANON_GAP_REPORT.md:145:./docs/etl/ETL_CANON.md:295:state_store/<supplier>/<run_id>/facts.ndjson
./docs/etl/CANON_GAP_REPORT.md:240:./docs/etl/CANON_GAP_REPORT.md:146:./docs/etl/ETL_CANON.md:297:Содержимое state_store никогда не изменяется после публикации.
./docs/etl/CANON_GAP_REPORT.md:241:./docs/etl/CANON_GAP_REPORT.md:147:./docs/etl/ETL_CANON.md:309:state_store/<supplier>/<run_id> хранится минимум state_retention_hours
./docs/etl/CANON_GAP_REPORT.md:242:./docs/etl/CANON_GAP_REPORT.md:148:./docs/etl/ETL_CANON.md:327:- accepted_runs до истечения retention
./docs/etl/CANON_GAP_REPORT.md:243:./docs/etl/CANON_GAP_REPORT.md:149:./docs/etl/ETL_CANON.md:343:Повторная обработка того же файла невозможна благодаря accepted_runs.
./docs/etl/CANON_GAP_REPORT.md:244:./docs/etl/CANON_GAP_REPORT.md:150:./docs/etl/ETL_CANON.md:355:Gate → технический + schema check
./docs/etl/CANON_GAP_REPORT.md:245:./docs/etl/CANON_GAP_REPORT.md:151:./docs/etl/ETL_CANON.md:356:DQ → контроль качества
./docs/etl/CANON_GAP_REPORT.md:246:./docs/etl/CANON_GAP_REPORT.md:152:./docs/etl/ETL_CANON.md:363:- запуск без DQ-конфига
./docs/etl/CANON_GAP_REPORT.md:247:./docs/etl/CANON_GAP_REPORT.md:153:./docs/etl/ETL_CANON.md:365:- прямое чтение state_store
./docs/etl/CANON_GAP_REPORT.md:248:./docs/etl/CANON_GAP_REPORT.md:154:./docs/etl/ETL_CANON_TEST.md:15:- проверки DQ-порогов
./docs/etl/CANON_GAP_REPORT.md:249:./docs/etl/CANON_GAP_REPORT.md:155:./docs/etl/ETL_CANON_TEST.md:77:- run_id может быть перезапущен
./docs/etl/CANON_GAP_REPORT.md:250:./docs/etl/CANON_GAP_REPORT.md:156:./docs/etl/ETL_CANON_TEST.md:81:## 6. Gate
./docs/etl/CANON_GAP_REPORT.md:251:./docs/etl/CANON_GAP_REPORT.md:157:./docs/etl/ETL_CANON_TEST.md:83:Gate выполняет технические проверки.
./docs/etl/CANON_GAP_REPORT.md:252:./docs/etl/CANON_GAP_REPORT.md:158:./docs/etl/ETL_CANON_TEST.md:95:DQ-пороги могут быть:
./docs/etl/CANON_GAP_REPORT.md:253:./docs/etl/CANON_GAP_REPORT.md:159:./docs/etl/INGESTION_V1.md:1:INGESTION v1 (SSOT segmented by run_id)
./docs/etl/CANON_GAP_REPORT.md:254:./docs/etl/CANON_GAP_REPORT.md:160:./docs/etl/INGESTION_V1.md:3:Gate rule: ingest only if verdict in PASS/WARN
./docs/etl/CANON_GAP_REPORT.md:255:./docs/etl/CANON_GAP_REPORT.md:161:./docs/etl/INGESTION_V1.md:4:Idempotency: accepted_runs/<run_id>.json exists => exit 0 (already_ingested)
./docs/etl/CANON_GAP_REPORT.md:256:./docs/etl/CANON_GAP_REPORT.md:162:./docs/etl/INGESTION_V1.md:5:Locking: locks/<run_id>.lock created atomically (O_EXCL); if exists => exit 1
./docs/etl/CANON_GAP_REPORT.md:257:./docs/etl/CANON_GAP_REPORT.md:163:./docs/etl/INGESTION_V1.md:6:Validation: every NDJSON line must contain required fields and types; run_id/effective_at/mapping_hash/mapping_version must match stats
./docs/etl/CANON_GAP_REPORT.md:258:./docs/etl/CANON_GAP_REPORT.md:164:./docs/etl/INGESTION_V1.md:7:Commit: write tmp/<run_id>.ndjson.tmp then atomic rename to facts/<supplier>/<parser>/<YYYY-MM>/<run_id>.ndjson
./docs/etl/CANON_GAP_REPORT.md:259:./docs/etl/CANON_GAP_REPORT.md:165:./docs/etl/INGESTION_V1.md:8:After commit: write manifests/<run_id>.json then accepted_runs/<run_id>.json
./docs/etl/CANON_GAP_REPORT.md:260:./docs/etl/CANON_GAP_REPORT.md:166:./docs/etl/archive/ETL_CANON_V1.md:8:Система построена по модели: Extractor → Emitter → Gate → Ingestion → Curated / Offers.  
./docs/etl/CANON_GAP_REPORT.md:261:./docs/etl/CANON_GAP_REPORT.md:167:./docs/etl/archive/ETL_CANON_V1.md:13:Gate решает судьбу файла как события.  
./docs/etl/CANON_GAP_REPORT.md:262:./docs/etl/CANON_GAP_REPORT.md:168:./docs/etl/archive/ETL_CANON_V1.md:28:run_id — идентификатор одного прогона (один входной файл).  
./docs/etl/CANON_GAP_REPORT.md:263:./docs/etl/CANON_GAP_REPORT.md:169:./docs/etl/archive/ETL_CANON_V1.md:58:supplier_id, parser_id, mapping_version, ndjson_contract_version, emitter_version, run_id, effective_at, raw, parsed, quality_flags, _meta.
./docs/etl/CANON_GAP_REPORT.md:264:./docs/etl/CANON_GAP_REPORT.md:170:./docs/etl/archive/ETL_CANON_V1.md:64:Gate  
./docs/etl/CANON_GAP_REPORT.md:265:./docs/etl/CANON_GAP_REPORT.md:171:./docs/etl/archive/ETL_CANON_V1.md:65:Gate работает с файлом как событием.  
./docs/etl/CANON_GAP_REPORT.md:266:./docs/etl/CANON_GAP_REPORT.md:172:./docs/etl/archive/ETL_CANON_V1.md:66:Gate принимает решение PASS / WARN / FAIL.
./docs/etl/CANON_GAP_REPORT.md:267:./docs/etl/CANON_GAP_REPORT.md:173:./docs/etl/archive/ETL_CANON_V1.md:88:Gate решает судьбу файла.  
./docs/etl/CANON_GAP_REPORT.md:268:./docs/etl/CANON_GAP_REPORT.md:174:./docs/etl/archive/ETL_CANON_V1_QA.md:6:Ответ: канон сознательно не задаёт процент. BAD-строки не валят файл. Gate фиксирует PASS/WARN/FAIL по событию файла. Наличие BAD даёт минимум WARN. Алертинг и решение “что делать” лежат выше (оркестратор/процессы). FAIL — только ошибки уровня файла (нечитаемость/сломанная структура/нарушение контракта файла).
./docs/etl/CANON_GAP_REPORT.md:269:./docs/etl/CANON_GAP_REPORT.md:175:./docs/etl/archive/ETL_CANON_V1_QA.md:9:3) Идемпотентность: один и тот же файл обработали дважды разными run_id.
./docs/etl/CANON_GAP_REPORT.md:270:./docs/etl/CANON_GAP_REPORT.md:176:./docs/etl/archive/ETL_CANON_V1_QA.md:10:Ответ: канон даёт механизмы (run_id, effective_at, метрики, артефакты). Защита от повторной обработки — обязанность оркестратора/операционного контура. История допустит два события, а витрина выбирает “что считать актуальным” своими правилами.
./docs/etl/CANON_GAP_REPORT.md:271:./docs/etl/CANON_GAP_REPORT.md:177:./docs/etl/archive/ETL_CANON_V1_QA.md:30:Вопросы по Gate и бизнес-логике
./docs/etl/CANON_GAP_REPORT.md:272:./docs/etl/CANON_GAP_REPORT.md:178:./docs/etl/archive/ETL_CANON_V1_QA.md:44:2) Distributed lock на run_id: как реализовать и что с зависанием?
./docs/etl/CANON_GAP_REPORT.md:273:./docs/etl/CANON_GAP_REPORT.md:179:./docs/etl/archive/ETL_CANON_V1_QA.md:49:Ответ: повтор по run_id с очисткой незасиленного промежуточного состояния. Append-only история и идемпотентные артефакты облегчают восстановление.
./docs/etl/CANON_GAP_REPORT.md:274:./docs/etl/CANON_GAP_REPORT.md:180:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:12:3. run_id генерируется автоматически.
./docs/etl/CANON_GAP_REPORT.md:275:./docs/etl/CANON_GAP_REPORT.md:181:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:14:5. Gate не содержит бизнес-логики.
./docs/etl/CANON_GAP_REPORT.md:276:./docs/etl/CANON_GAP_REPORT.md:182:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:36:      runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:277:./docs/etl/CANON_GAP_REPORT.md:183:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:37:      accepted_runs/
./docs/etl/CANON_GAP_REPORT.md:278:./docs/etl/CANON_GAP_REPORT.md:184:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:39:      manifests/
./docs/etl/CANON_GAP_REPORT.md:279:./docs/etl/CANON_GAP_REPORT.md:185:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:44:  runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:280:./docs/etl/CANON_GAP_REPORT.md:186:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:66:4. Если (supplier, sha256) уже в accepted_runs:
./docs/etl/CANON_GAP_REPORT.md:281:./docs/etl/CANON_GAP_REPORT.md:187:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:77:## 4. run_id
./docs/etl/CANON_GAP_REPORT.md:282:./docs/etl/CANON_GAP_REPORT.md:188:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:90:## 5. Gate (технический)
./docs/etl/CANON_GAP_REPORT.md:283:./docs/etl/CANON_GAP_REPORT.md:189:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:92:Gate проверяет:
./docs/etl/CANON_GAP_REPORT.md:284:./docs/etl/CANON_GAP_REPORT.md:190:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:102:Gate не содержит бизнес-правил.
./docs/etl/CANON_GAP_REPORT.md:285:./docs/etl/CANON_GAP_REPORT.md:191:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:112:- run.manifest.json
./docs/etl/CANON_GAP_REPORT.md:286:./docs/etl/CANON_GAP_REPORT.md:192:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:120:"run_id": "...",
./docs/etl/CANON_GAP_REPORT.md:287:./docs/etl/CANON_GAP_REPORT.md:193:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:129:"run_id": "...",
./docs/etl/CANON_GAP_REPORT.md:288:./docs/etl/CANON_GAP_REPORT.md:194:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:135:manifest
./docs/etl/CANON_GAP_REPORT.md:289:./docs/etl/CANON_GAP_REPORT.md:195:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:138:"run_id": "...",
./docs/etl/CANON_GAP_REPORT.md:290:./docs/etl/CANON_GAP_REPORT.md:196:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:151:etl_data/raw_v1/ssot/runs/<run_id>/run.manifest.json
./docs/etl/CANON_GAP_REPORT.md:291:./docs/etl/CANON_GAP_REPORT.md:197:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:155:etl_ops/runs/<run_id>/ может содержать symlink.
./docs/etl/CANON_GAP_REPORT.md:292:./docs/etl/CANON_GAP_REPORT.md:198:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:166:- изменять manifest
./docs/etl/CANON_GAP_REPORT.md:293:./docs/etl/CANON_GAP_REPORT.md:199:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:171:accepted_runs/
./docs/etl/CANON_GAP_REPORT.md:294:./docs/etl/CANON_GAP_REPORT.md:200:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:178:DQ выполняется в каждом run.
./docs/etl/CANON_GAP_REPORT.md:295:./docs/etl/CANON_GAP_REPORT.md:201:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:189:empty_file не является метрикой DQ — это FAIL в Gate.
./docs/etl/CANON_GAP_REPORT.md:296:./docs/etl/CANON_GAP_REPORT.md:202:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:198:- accepted_runs/
./docs/etl/CANON_GAP_REPORT.md:297:./docs/etl/CANON_GAP_REPORT.md:203:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:244:- run_id генерируется системой.
./docs/etl/CANON_GAP_REPORT.md:298:./docs/etl/CANON_GAP_REPORT.md:204:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:256:- содержит DQ thresholds
./docs/etl/CANON_GAP_REPORT.md:299:./docs/etl/CANON_GAP_REPORT.md:205:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:264:- читать manifest из etl_ops как источник истины
./docs/etl/CANON_GAP_REPORT.md:300:./docs/etl/CANON_GAP_REPORT.md:206:./docs/etl/archive/ETL_CANON_20260214T114156Z.md:266:- запускать без DQ-порогов
./docs/etl/CANON_GAP_REPORT.md:301:./docs/etl/CANON_GAP_REPORT.md:207:./docs/etl/CANON_GAP_REPORT.md:43:│   ├── [00mmanifest.json[0m
./docs/etl/CANON_GAP_REPORT.md:302:./docs/etl/CANON_GAP_REPORT.md:208:./docs/etl/CANON_GAP_REPORT.md:114:./README.md:11:Extractor → Emitter → Gate → Ingestion → SSOT
./docs/etl/CANON_GAP_REPORT.md:303:./docs/etl/CANON_GAP_REPORT.md:209:./docs/etl/CANON_GAP_REPORT.md:115:./docs/contracts/NDJSON_INGESTION_PRODUCTION_CONTRACT.md:56:## Gates
./docs/etl/CANON_GAP_REPORT.md:304:./docs/etl/CANON_GAP_REPORT.md:210:./docs/etl/CANON_GAP_REPORT.md:116:./docs/etl/CURATED_V1.md:2:Назначение: потребитель SSOT сегмента (facts/<...>/<run_id>.ndjson). Делает бизнес-отбор “продаваемых” строк поверх валидных фактов (GOOD), не меняя сами факты.
./docs/etl/CANON_GAP_REPORT.md:305:./docs/etl/CANON_GAP_REPORT.md:211:./docs/etl/CANON_GAP_REPORT.md:117:./docs/etl/CURATED_V1.md:3:Вход: manifest.json (ssot/manifests/<run_id>.json) с paths.segment
./docs/etl/CANON_GAP_REPORT.md:306:./docs/etl/CANON_GAP_REPORT.md:212:./docs/etl/CANON_GAP_REPORT.md:118:./docs/etl/CURATED_V1.md:4:Выходы (curated_v1/out/<run_id>/):
./docs/etl/CANON_GAP_REPORT.md:307:./docs/etl/CANON_GAP_REPORT.md:213:./docs/etl/CANON_GAP_REPORT.md:119:./docs/etl/CURATED_V1.md:16:--manifest PATH (обяз.)
./docs/etl/CANON_GAP_REPORT.md:308:./docs/etl/CANON_GAP_REPORT.md:214:./docs/etl/CANON_GAP_REPORT.md:120:./docs/etl/ETL_CANON.md:17:Run-артефакты, quarantine и manifest — операционный след с ограниченным сроком хранения.
./docs/etl/CANON_GAP_REPORT.md:309:./docs/etl/CANON_GAP_REPORT.md:215:./docs/etl/CANON_GAP_REPORT.md:121:./docs/etl/ETL_CANON.md:53:  accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json
./docs/etl/CANON_GAP_REPORT.md:310:./docs/etl/CANON_GAP_REPORT.md:216:./docs/etl/CANON_GAP_REPORT.md:122:./docs/etl/ETL_CANON.md:54:  runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:311:./docs/etl/CANON_GAP_REPORT.md:217:./docs/etl/CANON_GAP_REPORT.md:123:./docs/etl/ETL_CANON.md:58:    manifest.json
./docs/etl/CANON_GAP_REPORT.md:312:./docs/etl/CANON_GAP_REPORT.md:218:./docs/etl/CANON_GAP_REPORT.md:124:./docs/etl/ETL_CANON.md:60:  state_store/<supplier>/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:313:./docs/etl/CANON_GAP_REPORT.md:219:./docs/etl/CANON_GAP_REPORT.md:125:./docs/etl/ETL_CANON.md:65:DQ-конфиг:
./docs/etl/CANON_GAP_REPORT.md:314:./docs/etl/CANON_GAP_REPORT.md:220:./docs/etl/CANON_GAP_REPORT.md:126:./docs/etl/ETL_CANON.md:78:4. Файлы, SHA которых уже присутствует в accepted_runs → переместить в quarantine/<supplier>/duplicates/
./docs/etl/CANON_GAP_REPORT.md:315:./docs/etl/CANON_GAP_REPORT.md:221:./docs/etl/CANON_GAP_REPORT.md:127:./docs/etl/ETL_CANON.md:86:Истинная хронология определяется run_id.
./docs/etl/CANON_GAP_REPORT.md:316:./docs/etl/CANON_GAP_REPORT.md:222:./docs/etl/CANON_GAP_REPORT.md:128:./docs/etl/ETL_CANON.md:110:6. run_id
./docs/etl/CANON_GAP_REPORT.md:317:./docs/etl/CANON_GAP_REPORT.md:223:./docs/etl/CANON_GAP_REPORT.md:129:./docs/etl/ETL_CANON.md:123:7. Gate (технический + schema check)
./docs/etl/CANON_GAP_REPORT.md:318:./docs/etl/CANON_GAP_REPORT.md:224:./docs/etl/CANON_GAP_REPORT.md:130:./docs/etl/ETL_CANON.md:139:- создаётся manifest со status=FAIL
./docs/etl/CANON_GAP_REPORT.md:319:./docs/etl/CANON_GAP_REPORT.md:225:./docs/etl/CANON_GAP_REPORT.md:131:./docs/etl/ETL_CANON.md:141:Gate не содержит бизнес-логики.
./docs/etl/CANON_GAP_REPORT.md:320:./docs/etl/CANON_GAP_REPORT.md:226:./docs/etl/CANON_GAP_REPORT.md:132:./docs/etl/ETL_CANON.md:144:8. Data Quality (DQ)
./docs/etl/CANON_GAP_REPORT.md:321:./docs/etl/CANON_GAP_REPORT.md:227:./docs/etl/CANON_GAP_REPORT.md:133:./docs/etl/ETL_CANON.md:156:Если конфиг отсутствует → FAIL (CONFIG_DQ_MISSING).
./docs/etl/CANON_GAP_REPORT.md:322:./docs/etl/CANON_GAP_REPORT.md:228:./docs/etl/CANON_GAP_REPORT.md:134:./docs/etl/ETL_CANON.md:169:Хранятся в runs/<run_id>/
./docs/etl/CANON_GAP_REPORT.md:323:./docs/etl/CANON_GAP_REPORT.md:229:./docs/etl/CANON_GAP_REPORT.md:135:./docs/etl/ETL_CANON.md:175:- run_id
./docs/etl/CANON_GAP_REPORT.md:324:./docs/etl/CANON_GAP_REPORT.md:230:./docs/etl/CANON_GAP_REPORT.md:136:./docs/etl/ETL_CANON.md:191:  "run_id": "brinex_20260213T194500Z_1a2b3c",
./docs/etl/CANON_GAP_REPORT.md:325:./docs/etl/CANON_GAP_REPORT.md:231:./docs/etl/CANON_GAP_REPORT.md:137:./docs/etl/ETL_CANON.md:215:run_id в вычисление не входит.
./docs/etl/CANON_GAP_REPORT.md:326:./docs/etl/CANON_GAP_REPORT.md:232:./docs/etl/CANON_GAP_REPORT.md:138:./docs/etl/ETL_CANON.md:229:- run_id
./docs/etl/CANON_GAP_REPORT.md:327:./docs/etl/CANON_GAP_REPORT.md:233:./docs/etl/CANON_GAP_REPORT.md:139:./docs/etl/ETL_CANON.md:255:- run_id
./docs/etl/CANON_GAP_REPORT.md:328:./docs/etl/CANON_GAP_REPORT.md:234:./docs/etl/CANON_GAP_REPORT.md:140:./docs/etl/ETL_CANON.md:266:manifest хранится как операционный след.
./docs/etl/CANON_GAP_REPORT.md:329:./docs/etl/CANON_GAP_REPORT.md:235:./docs/etl/CANON_GAP_REPORT.md:141:./docs/etl/ETL_CANON.md:269:11. accepted_runs
./docs/etl/CANON_GAP_REPORT.md:330:./docs/etl/CANON_GAP_REPORT.md:236:./docs/etl/CANON_GAP_REPORT.md:142:./docs/etl/ETL_CANON.md:273:accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json
./docs/etl/CANON_GAP_REPORT.md:331:./docs/etl/CANON_GAP_REPORT.md:237:./docs/etl/CANON_GAP_REPORT.md:143:./docs/etl/ETL_CANON.md:282:  "run_id": "...",
./docs/etl/CANON_GAP_REPORT.md:332:./docs/etl/CANON_GAP_REPORT.md:238:./docs/etl/CANON_GAP_REPORT.md:144:./docs/etl/ETL_CANON.md:285:  "manifest_path": "..."
./docs/etl/CANON_GAP_REPORT.md:333:./docs/etl/CANON_GAP_REPORT.md:239:./docs/etl/CANON_GAP_REPORT.md:145:./docs/etl/ETL_CANON.md:295:state_store/<supplier>/<run_id>/facts.ndjson
./docs/etl/CANON_GAP_REPORT.md:334:./docs/etl/CANON_GAP_REPORT.md:240:./docs/etl/CANON_GAP_REPORT.md:146:./docs/etl/ETL_CANON.md:297:Содержимое state_store никогда не изменяется после публикации.
./docs/etl/CANON_GAP_REPORT.md:335:./docs/etl/CANON_GAP_REPORT.md:241:./docs/etl/CANON_GAP_REPORT.md:147:./docs/etl/ETL_CANON.md:309:state_store/<supplier>/<run_id> хранится минимум state_retention_hours
./docs/etl/CANON_GAP_REPORT.md:336:./docs/etl/CANON_GAP_REPORT.md:242:./docs/etl/CANON_GAP_REPORT.md:148:./docs/etl/ETL_CANON.md:327:- accepted_runs до истечения retention
./docs/etl/CANON_GAP_REPORT.md:337:./docs/etl/CANON_GAP_REPORT.md:243:./docs/etl/CANON_GAP_REPORT.md:149:./docs/etl/ETL_CANON.md:343:Повторная обработка того же файла невозможна благодаря accepted_runs.
./docs/etl/CANON_GAP_REPORT.md:338:./docs/etl/CANON_GAP_REPORT.md:244:./docs/etl/CANON_GAP_REPORT.md:150:./docs/etl/ETL_CANON.md:355:Gate → технический + schema check
./docs/etl/CANON_GAP_REPORT.md:339:./docs/etl/CANON_GAP_REPORT.md:245:./docs/etl/CANON_GAP_REPORT.md:151:./docs/etl/ETL_CANON.md:356:DQ → контроль качества
./docs/etl/CANON_GAP_REPORT.md:340:./docs/etl/CANON_GAP_REPORT.md:246:./docs/etl/CANON_GAP_REPORT.md:152:./docs/etl/ETL_CANON.md:363:- запуск без DQ-конфига
./docs/etl/CANON_GAP_REPORT.md:341:./docs/etl/CANON_GAP_REPORT.md:247:./docs/etl/CANON_GAP_REPORT.md:153:./docs/etl/ETL_CANON.md:365:- прямое чтение state_store
./docs/etl/CANON_GAP_REPORT.md:342:./docs/etl/CANON_GAP_REPORT.md:248:./docs/etl/CANON_GAP_REPORT.md:154:./docs/etl/ETL_CANON_TEST.md:15:- проверки DQ-порогов
./docs/etl/CANON_GAP_REPORT.md:343:./docs/etl/CANON_GAP_REPORT.md:249:./docs/etl/CANON_GAP_REPORT.md:155:./docs/etl/ETL_CANON_TEST.md:77:- run_id может быть перезапущен
./docs/etl/CANON_GAP_REPORT.md:344:./docs/etl/CANON_GAP_REPORT.md:250:./docs/etl/CANON_GAP_REPORT.md:156:./docs/etl/ETL_CANON_TEST.md:81:## 6. Gate
./docs/etl/CANON_GAP_REPORT.md:345:./docs/etl/CANON_GAP_REPORT.md:251:./docs/etl/CANON_GAP_REPORT.md:157:./docs/etl/ETL_CANON_TEST.md:83:Gate выполняет технические проверки.
./docs/etl/CANON_GAP_REPORT.md:346:./docs/etl/CANON_GAP_REPORT.md:252:./docs/etl/CANON_GAP_REPORT.md:158:./docs/etl/ETL_CANON_TEST.md:95:DQ-пороги могут быть:
./docs/etl/CANON_GAP_REPORT.md:347:./docs/etl/CANON_GAP_REPORT.md:253:./docs/etl/CANON_GAP_REPORT.md:159:./docs/etl/INGESTION_V1.md:1:INGESTION v1 (SSOT segmented by run_id)
./docs/etl/CANON_GAP_REPORT.md:348:./docs/etl/CANON_GAP_REPORT.md:254:./docs/etl/CANON_GAP_REPORT.md:160:./docs/etl/INGESTION_V1.md:3:Gate rule: ingest only if verdict in PASS/WARN
./docs/etl/CANON_GAP_REPORT.md:349:./docs/etl/CANON_GAP_REPORT.md:255:./docs/etl/CANON_GAP_REPORT.md:161:./docs/etl/INGESTION_V1.md:4:Idempotency: accepted_runs/<run_id>.json exists => exit 0 (already_ingested)
./docs/etl/CANON_GAP_REPORT.md:350:./docs/etl/CANON_GAP_REPORT.md:256:./docs/etl/CANON_GAP_REPORT.md:162:./docs/etl/INGESTION_V1.md:5:Locking: locks/<run_id>.lock created atomically (O_EXCL); if exists => exit 1
./docs/etl/CANON_GAP_REPORT.md:351:./docs/etl/CANON_GAP_REPORT.md:257:./docs/etl/CANON_GAP_REPORT.md:163:./docs/etl/INGESTION_V1.md:6:Validation: every NDJSON line must contain required fields and types; run_id/effective_at/mapping_hash/mapping_version must match stats
./docs/etl/CANON_GAP_REPORT.md:352:./docs/etl/CANON_GAP_REPORT.md:258:./docs/etl/CANON_GAP_REPORT.md:164:./docs/etl/INGESTION_V1.md:7:Commit: write tmp/<run_id>.ndjson.tmp then atomic rename to facts/<supplier>/<parser>/<YYYY-MM>/<run_id>.ndjson
./docs/etl/CANON_GAP_REPORT.md:353:./docs/etl/CANON_GAP_REPORT.md:259:./docs/etl/CANON_GAP_REPORT.md:165:./docs/etl/INGESTION_V1.md:8:After commit: write manifests/<run_id>.json then accepted_runs/<run_id>.json
./docs/etl/CANON_GAP_REPORT.md:354:./docs/etl/CANON_GAP_REPORT.md:260:./docs/etl/CANON_GAP_REPORT.md:166:./docs/etl/archive/ETL_CANON_V1.md:8:Система построена по модели: Extractor → Emitter → Gate → Ingestion → Curated / Offers.  
./docs/spec/golden_set_contract_v1.md:30:## Versioning + manifest
./docs/spec/golden_set_contract_v1.md:31:golden/manifest.json must include:
./scripts/curated/tirehub_curate_v1.py:12:  "emitter_version","run_id","effective_at","sku_candidate_key","raw","parsed","quality_flags","_meta"
./scripts/curated/tirehub_curate_v1.py:59:  ap.add_argument("--manifest",required=True,help="path to SSOT manifest json")
./scripts/curated/tirehub_curate_v1.py:64:  manifest_path=Path(args.manifest)
./scripts/curated/tirehub_curate_v1.py:65:  if not manifest_path.exists(): die(f"manifest not found: {manifest_path}")
./scripts/curated/tirehub_curate_v1.py:67:  manifest=load_json(manifest_path)
./scripts/curated/tirehub_curate_v1.py:68:  run_id=manifest.get("run_id")
./scripts/curated/tirehub_curate_v1.py:69:  paths=(manifest.get("paths") or {})
./scripts/curated/tirehub_curate_v1.py:71:  if not run_id or not seg_path: die("manifest missing run_id or paths.segment")
./scripts/curated/tirehub_curate_v1.py:76:  out_base=Path(args.out_dir)/run_id
./scripts/curated/tirehub_curate_v1.py:134:          "run_id": obj.get("run_id"),
./scripts/curated/tirehub_curate_v1.py:152:  manifest_sha=sha256_file(manifest_path)
./scripts/curated/tirehub_curate_v1.py:156:    "run_id": run_id,
./scripts/curated/tirehub_curate_v1.py:169:      "manifest": str(manifest_path),
./scripts/curated/tirehub_curate_v1.py:170:      "manifest_sha256": manifest_sha,
./scripts/ingestion/centrshin/emit_centrshin_xlsx_diski_v1.py:58:    RUN_ID = args.run_id
./scripts/ingestion/centrshin/emit_centrshin_xlsx_diski_v1.py:77:        "run_id": RUN_ID,
./scripts/ingestion/centrshin/emit_centrshin_xlsx_diski_v1.py:161:                "run_id": RUN_ID,
./scripts/ingestion/centrshin/emit_centrshin_xlsx_shiny_v1.py:58:    RUN_ID = args.run_id
./scripts/ingestion/centrshin/emit_centrshin_xlsx_shiny_v1.py:77:        "run_id": RUN_ID,
./scripts/ingestion/centrshin/emit_centrshin_xlsx_shiny_v1.py:141:                "run_id": RUN_ID,
./scripts/ingestion/emit_generic_ndjson_v1.py:24:    "--run-id", args.run_id,
./scripts/ingestion/kolobox/emit_kolobox_ndjson_v1.py:256:        "run_id": args.run_id,
./scripts/ingestion/kolobox/emit_kolobox_ndjson_v1.py:343:                "run_id": args.run_id,
./scripts/ingestion/kolobox/emit_kolobox_ndjson_v1_FINAL.py:190:    out_dir=Path(args.out_dir)/args.run_id
./scripts/ingestion/kolobox/emit_kolobox_ndjson_v1_FINAL.py:243:                            "run_id":args.run_id,
./scripts/ingestion/kolobox/emit_kolobox_ndjson_v1_FINAL.py:267:                            "run_id":args.run_id,
./scripts/ingestion/kolobox/emit_kolobox_ndjson_v1_FINAL.py:304:                        "run_id":args.run_id,
./scripts/ingestion/kolobox/emit_kolobox_ndjson_v1_FINAL.py:328:                "run_id":args.run_id,
./scripts/ingestion/kolobox/tirehub_gate_v1.py:84:        "run_id","supplier_id","parser_id",
./scripts/ingestion/kolobox/tirehub_gate_v1.py:128:        "run_id": stats["run_id"],
./scripts/ingestion/run_inbox_batch_v1.py:12:def now_run_id() -> str:
./scripts/ingestion/run_inbox_batch_v1.py:53:    run_id: str
./scripts/ingestion/run_inbox_batch_v1.py:61:    def plan(self, file: Path, run_id: str, out_dir: Path) -> Optional[Plan]: return None
./scripts/ingestion/run_inbox_batch_v1.py:96:    def plan(self, file: Path, run_id: str, out_dir: Path) -> Optional[Plan]:
./scripts/ingestion/run_inbox_batch_v1.py:103:        nd = out_dir / f"{tag}.{run_id}.ndjson"
./scripts/ingestion/run_inbox_batch_v1.py:104:        st = out_dir / f"{tag}.{run_id}.stats.json"
./scripts/ingestion/run_inbox_batch_v1.py:105:        lg = out_dir / f"{tag}.{run_id}.log"
./scripts/ingestion/run_inbox_batch_v1.py:112:            run_id=run_id,
./scripts/ingestion/run_inbox_batch_v1.py:124:    def plan(self, file: Path, run_id: str, out_dir: Path) -> Optional[Plan]:
./scripts/ingestion/run_inbox_batch_v1.py:143:                nd = out_dir / f"{tag}.{run_id}.ndjson"
./scripts/ingestion/run_inbox_batch_v1.py:144:                st = out_dir / f"{tag}.{run_id}.stats.json"
./scripts/ingestion/run_inbox_batch_v1.py:145:                lg = out_dir / f"{tag}.{run_id}.log"
./scripts/ingestion/run_inbox_batch_v1.py:152:                    run_id=run_id,
./scripts/ingestion/run_inbox_batch_v1.py:182:        "--run-id", pl.run_id,
./scripts/ingestion/run_inbox_batch_v1.py:211:    ap.add_argument("--run-id", default="", help="override run_id (default: now)")
./scripts/ingestion/run_inbox_batch_v1.py:220:    run_id = args.run_id.strip() or now_run_id()
./scripts/ingestion/run_inbox_batch_v1.py:221:    out_dir = out_root / run_id
./scripts/ingestion/run_inbox_batch_v1.py:232:        "run_id": run_id,
./scripts/ingestion/run_inbox_batch_v1.py:257:        pl = adapter.plan(f, run_id, out_dir)
./scripts/ingestion/run_inbox_batch_v1.py:278:    print("RUN_ID=", run_id)
./scripts/ingestion/run_inbox_batch_v1.py:300:def _add_centrshin_tasks(tasks, supplier_guess: str, file_path: str, run_id: str, out_dir: str):
./scripts/ingestion/tirehub_ingest_v1.py:12:  "emitter_version","run_id","effective_at","sku_candidate_key","raw","parsed","quality_flags","_meta"
./scripts/ingestion/tirehub_ingest_v1.py:90:  if obj["run_id"] != expected["run_id"]:
./scripts/ingestion/tirehub_ingest_v1.py:91:    die(f"RUN_ID_MISMATCH: {obj['run_id']} != {expected['run_id']}", EXIT_FAIL)
./scripts/ingestion/tirehub_ingest_v1.py:111:  ap = argparse.ArgumentParser("tirehub-ingest v1 (SSOT segmented by run_id)")
./scripts/ingestion/tirehub_ingest_v1.py:112:  ap.add_argument("--good", required=True, help="path to out/<run_id>/good.ndjson")
./scripts/ingestion/tirehub_ingest_v1.py:113:  ap.add_argument("--stats", required=True, help="path to out/<run_id>/stats.json")
./scripts/ingestion/tirehub_ingest_v1.py:114:  ap.add_argument("--verdict", required=True, help="path to out/<run_id>/verdict.json (gate)")
./scripts/ingestion/tirehub_ingest_v1.py:135:  for k in ["run_id","supplier_id","parser_id","effective_at","mapping_hash","mapping_version","good_rows","file_readable","structure_ok"]:
./scripts/ingestion/tirehub_ingest_v1.py:140:    "run_id": stats["run_id"],
./scripts/ingestion/tirehub_ingest_v1.py:157:  accepted_dir = ssot_root / "accepted_runs"
./scripts/ingestion/tirehub_ingest_v1.py:158:  marker_path = accepted_dir / f"{expected['run_id']}.json"
./scripts/ingestion/tirehub_ingest_v1.py:162:      "run_id": expected["run_id"],
./scripts/ingestion/tirehub_ingest_v1.py:169:  lock_path = locks_dir / f"{expected['run_id']}.lock"
./scripts/ingestion/tirehub_ingest_v1.py:174:  manifests_dir = ssot_root / "manifests"
./scripts/ingestion/tirehub_ingest_v1.py:176:  tmp_path = tmp_dir / f"{expected['run_id']}.ndjson.tmp"
./scripts/ingestion/tirehub_ingest_v1.py:206:    final_seg_path = seg_dir / f"{expected['run_id']}.ndjson"
./scripts/ingestion/tirehub_ingest_v1.py:211:    # manifest (after segment)
./scripts/ingestion/tirehub_ingest_v1.py:212:    manifest_path = manifests_dir / f"{expected['run_id']}.json"
./scripts/ingestion/tirehub_ingest_v1.py:213:    manifest = {
./scripts/ingestion/tirehub_ingest_v1.py:215:      "run_id": expected["run_id"],
./scripts/ingestion/tirehub_ingest_v1.py:240:    atomic_write_text(manifest_path, compact(manifest))
./scripts/ingestion/tirehub_ingest_v1.py:242:    # marker (strictly after manifest)
./scripts/ingestion/tirehub_ingest_v1.py:244:    marker = {"status":"accepted","run_id": expected["run_id"],"ingested_at": ingested_at,"manifest_ref": str(manifest_path)}
./scripts/ingestion/tirehub_ingest_v1.py:249:      "run_id": expected["run_id"],
./scripts/ingestion/tirehub_ingest_v1.py:252:      "manifest": str(manifest_path),
./scripts/run_kolobox_full_v1.sh:25:echo "run_id:  $RUN_ID"
./scripts/run_kolobox_full_v1.sh:45:echo "=== 2) Gate ==="
./scripts/run_kolobox_full_v1.sh:66:MANIFEST_JSON="ssot/manifests/${RUN_ID}.json"
./scripts/run_kolobox_full_v1.sh:69:  echo "== manifests tail =="
./scripts/run_kolobox_full_v1.sh:70:  ls -la ssot/manifests 2>/dev/null | tail -n 50 || true
./scripts/run_kolobox_full_v1.sh:76:  --manifest "$MANIFEST_JSON" \
./scripts/smoke/run_ssot_v1_4a_seed.sh:24:# seed + bridge write as postgres, return run_id via SELECT (single line)
./scripts/smoke/run_ssot_v1_4a_seed.sh:33:    gen_random_uuid() AS run_id,
./scripts/smoke/run_ssot_v1_4a_seed.sh:93:  INSERT INTO ssot_catalog.smoke_runs_v14a(run_id, snapshot_id, artifact_id, item_id, db_name, notes)
./scripts/smoke/run_ssot_v1_4a_seed.sh:95:    g.run_id, g.snapshot_id, g.artifact_id, g.item_id, current_database(),
./scripts/smoke/run_ssot_v1_4a_seed.sh:98:  RETURNING run_id
./scripts/smoke/run_ssot_v1_4a_seed.sh:100:SELECT run_id::text FROM bridge;
./scripts/smoke/run_ssot_v1_4a_seed.sh:107:SELECT 1 FROM ssot_catalog.smoke_runs_v14a WHERE run_id='${RUN_ID}'::uuid;
./scripts/smoke/run_ssot_v1_4a_seed.sh:110:WHERE r.run_id='${RUN_ID}'::uuid AND cs.status='sealed' AND cs.sealed_at IS NOT NULL;
./scripts/smoke/run_ssot_v1_4a_smoke.sh:12:[[ -n "$RUN_ID" ]] || die "RUN_ID is required. Usage: $0 <db> <run_id>"
./scripts/smoke/run_ssot_v1_4a_smoke.sh:37:  psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off -v run_id="$RUN_ID" -f "$SQL_FILE"
./sql/migrations/20260202_ssot_v1_4a_smoke_bridge.sql:6:  run_id uuid PRIMARY KEY,

## 5. Code anchors (current implementation)

### 5.1 run_id generator (run_inbox_batch_v1.py)
     1	#!/usr/bin/env python3
     2	from __future__ import annotations
     3	import argparse, json, os, re, subprocess, sys, time
     4	from dataclasses import dataclass
     5	from pathlib import Path
     6	from typing import Dict, List, Optional, Tuple, Any
     7	
     8	
     9	
    10	def _norm_supplier_name(s: str) -> str:
    11	    return (s or '').strip().lower()
    12	def now_run_id() -> str:
    13	    return time.strftime("%Y%m%d_%H%M%S", time.localtime())
    14	
    15	def ensure_dir(p: Path) -> None:
    16	    p.mkdir(parents=True, exist_ok=True)
    17	
    18	def norm(s: str) -> str:
    19	    s = (s or "").strip()
    20	    s = re.sub(r"\s+", " ", s)
    21	    return s
    22	
    23	def read_xls_shape_and_row0(path: Path) -> Tuple[int, int, List[str]]:
    24	    import xlrd  # type: ignore
    25	    book = xlrd.open_workbook(str(path))
    26	    sh = book.sheet_by_index(0)
    27	    nrows, ncols = sh.nrows, sh.ncols
    28	    row0 = []
    29	    if nrows > 0:
    30	        for c in range(min(80, ncols)):
    31	            row0.append(norm(str(sh.cell_value(0, c))))
    32	    return nrows, ncols, row0
    33	
    34	def safe_relpath(p: Path) -> str:
    35	    try:
    36	        return str(p.relative_to(Path.cwd()))
    37	    except Exception:
    38	        return str(p)
    39	
    40	def sanitize_tag(s: str) -> str:
    41	    s = s.strip()
    42	    s = s.replace(" ", "_").replace("/", "_").replace("\\", "_").replace("(", "_").replace(")", "_")
    43	    s = re.sub(r"[^0-9A-Za-zА-Яа-я._-]+", "_", s)
    44	    return s
    45	
    46	@dataclass
    47	class Plan:
    48	    supplier_id: str
    49	    file: Path
    50	    emitter: Path
    51	    layout: str
    52	    mapping: Path
    53	    run_id: str
    54	    out_ndjson: Path
    55	    out_stats: Path
    56	    out_log: Path
    57	
    58	class SupplierAdapter:
    59	    supplier_id: str = "UNKNOWN"
    60	    def can_handle(self, supplier_id: str) -> bool: return False
    61	    def plan(self, file: Path, run_id: str, out_dir: Path) -> Optional[Plan]: return None
    62	
    63	class KoloboxAdapter(SupplierAdapter):
    64	    supplier_id = "Kolobox"
    65	    def can_handle(self, supplier_id: str) -> bool:
    66	        return supplier_id.lower() == "kolobox"
    67	
    68	    def _detect_layout(self, file: Path) -> str:
    69	        name = file.name.lower()
    70	        if "груз" in name:
    71	            return "truck"
    72	        if "диск" in name:
    73	            return "diski"
    74	        if "комплект" in name:
    75	            return "komplektatsii"
    76	        if "шин" in name:
    77	            return "shiny"
    78	        # fallback: shiny (самый частый)
    79	        return "shiny"
    80	
    81	    def _detect_mapping(self, file: Path, layout: str) -> Path:
    82	        # kolobox mappings we created:
    83	        # - kolobox_truck_xls_v1.yaml (truck 16 cols)
    84	        # - kolobox_diski_xls_v1.yaml (diski 17 cols)
    85	        # - kolobox_komplektatsii_xls_v1.yaml (komplektatsii 18 cols)
    86	        # - kolobox.yaml (baseline, e.g. shiny 21 cols)
    87	        mp_dir = Path("mappings/suppliers")
    88	        if layout == "truck":
    89	            return mp_dir / "kolobox_truck_xls_v1.yaml"
    90	        if layout == "diski":
    91	            return mp_dir / "kolobox_diski_xls_v1.yaml"
    92	        if layout == "komplektatsii":
    93	            return mp_dir / "kolobox_komplektatsii_xls_v1.yaml"
    94	        return mp_dir / "kolobox.yaml"
    95	
    96	    def plan(self, file: Path, run_id: str, out_dir: Path) -> Optional[Plan]:
    97	        emitter = Path("scripts/ingestion/kolobox/emit_kolobox_ndjson_v1.py")
    98	        if not emitter.exists():
    99	            return None
   100	        layout = self._detect_layout(file)
   101	        mapping = self._detect_mapping(file, layout)
   102	        tag = sanitize_tag(f"kolobox__{file.stem}__{layout}")
   103	        nd = out_dir / f"{tag}.{run_id}.ndjson"
   104	        st = out_dir / f"{tag}.{run_id}.stats.json"
   105	        lg = out_dir / f"{tag}.{run_id}.log"
   106	        return Plan(
   107	            supplier_id="kolobox",
   108	            file=file,
   109	            emitter=emitter,
   110	            layout=layout,
   111	            mapping=mapping,
   112	            run_id=run_id,
   113	            out_ndjson=nd,
   114	            out_stats=st,
   115	            out_log=lg,
   116	        )
   117	
   118	
   119	class CentrshinAdapter(SupplierAdapter):
   120	    supplier_id = "Centrshin"

### 5.2 ingestion core (tirehub_ingest_v1.py) — paths/locks/accepted/manifest
46:def acquire_lock(lock_path: Path):
47:  ensure_dir(lock_path.parent)
49:    fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
52:    die(f"LOCKED: {lock_path}", EXIT_ARGS)
54:    die(f"LOCK_CREATE_FAILED: {lock_path} :: {e}", EXIT_FAIL)
56:def release_lock(lock_path: Path):
58:    if lock_path.exists():
59:      lock_path.unlink()
157:  accepted_dir = ssot_root / "accepted_runs"
168:  locks_dir = ssot_root / "locks"
169:  lock_path = locks_dir / f"{expected['run_id']}.lock"
170:  acquire_lock(lock_path)
172:  tmp_dir = ssot_root / "tmp"
174:  manifests_dir = ssot_root / "manifests"
176:  tmp_path = tmp_dir / f"{expected['run_id']}.ndjson.tmp"
183:    ensure_dir(tmp_dir)
185:    with good_path.open("r", encoding="utf-8", errors="strict") as fin, tmp_path.open("w", encoding="utf-8", newline="\n") as fout:
204:    seg_dir = facts_root / expected["supplier_id"] / expected["parser_id"] / month
205:    ensure_dir(seg_dir)
206:    final_seg_path = seg_dir / f"{expected['run_id']}.ndjson"
209:    tmp_path.replace(final_seg_path)
212:    manifest_path = manifests_dir / f"{expected['run_id']}.json"
222:        "segment": str(final_seg_path),
229:        "segment": sha256_file(final_seg_path),
240:    atomic_write_text(manifest_path, compact(manifest))
244:    marker = {"status":"accepted","run_id": expected["run_id"],"ingested_at": ingested_at,"manifest_ref": str(manifest_path)}
250:      "segment": str(final_seg_path),
252:      "manifest": str(manifest_path),
260:      if tmp_path.exists():
261:        tmp_path.unlink()
264:    release_lock(lock_path)

### 5.3 gate (kolobox tirehub_gate_v1.py) — required fields/schema checks
5:EXIT_PASS = 0
6:EXIT_WARN = 10
7:EXIT_FAIL = 20
9:def die(msg: str, code: int = EXIT_FAIL):
17:        die(f"JSON read/parse failed: {p} :: {e}", EXIT_FAIL)
31:        reasons.append({"level": "WARN", "code": "baseline_missing", "detail": "baseline not configured"})
34:        reasons.append({"level": "WARN", "code": "baseline_missing", "detail": f"baseline file not found: {baseline_path}"})
40:        reasons.append({"level": "WARN", "code": "baseline_mismatch", "detail": f"parser_id mismatch: stats={stats.get('parser_id')}, baseline={baseline.get('parser_id')}"})
50:            reasons.append({"level": "WARN", "code": "metric_missing", "detail": f"{metric_path} not in stats"})
55:                reasons.append({"level": "WARN", "code": "metric_out_of_tolerance", "detail": f"{metric_path}: expected '{expected}', got '{actual}'"})
59:                    reasons.append({"level": "WARN", "code": "metric_out_of_tolerance", "detail": f"{metric_path}: expected {expected}±{tolerance}, got {actual}"})
61:                reasons.append({"level": "WARN", "code": "metric_out_of_tolerance", "detail": f"{metric_path}: non-numeric actual={actual}, expected={expected}"})
64:            reasons.append({"level": "WARN", "code": "baseline_invalid", "detail": f"{metric_path}: unsupported expected type {type(expected)}"})
70:    ap.add_argument("--stats", required=True, help="path to stats.json from emitter")
71:    ap.add_argument("--out", required=False, help="path to verdict.json (will be overwritten)")
72:    ap.add_argument("--baseline", required=False, help="path to baseline.json file")
79:        die(f"stats.json not found: {stats_path}", EXIT_FAIL)
83:    required = [
85:        "file_readable","structure_ok",
90:    for k in required:
92:            die(f"stats.json missing field: {k}", EXIT_FAIL)
96:    # FAIL rules
97:    if not bool(stats["file_readable"]):
98:        reasons.append({"level": "FAIL", "code": "file_not_readable", "detail": "file_readable=false"})
99:    if not bool(stats["structure_ok"]):
100:        reasons.append({"level": "FAIL", "code": "structure_not_ok", "detail": "structure_ok=false"})
102:        reasons.append({"level": "FAIL", "code": "exploded_lines_zero", "detail": "exploded_lines == 0"})
104:    if any(r["level"] == "FAIL" for r in reasons):
105:        verdict = "FAIL"
106:        exit_code = EXIT_FAIL
108:        # WARN rules: baseline check
111:        # other WARN signals
113:            reasons.append({"level": "WARN", "code": "has_bad_rows", "detail": f"bad_rows={stats['bad_rows']}"})
117:            reasons.append({"level": "WARN", "code": "has_negative_price", "detail": f"negative_price={flags_counts.get('negative_price')}"})
119:        if any(r["level"] == "WARN" for r in reasons):
120:            verdict = "WARN"
121:            exit_code = EXIT_WARN
123:            verdict = "PASS"
124:            exit_code = EXIT_PASS
131:        "verdict": verdict,

### 5.4 state_store/state/current — presence check

## 6. CANON vs AS-IS (repo implementation)

Legend: ✅ match | ⚠️ partial | ❌ missing | P0 = must fix first

### 6.1 run_id
- CANON: <supplier><YYYYMMDDTHHMMSSZ><rand6> (UTC, Z, rand6 hex)  => ❌
- AS-IS: now_run_id() = time.strftime("%Y%m%d_%H%M%S", localtime)  => localtime, no supplier, no Z, no rand6
P0: replace run_id generator + enforce format everywhere.

### 6.2 Locking
- CANON: lock per supplier (no parallel runs per supplier) => ❌
- AS-IS: locks/<run_id>.lock => lock per run_id, not per supplier
P0: change lock key to supplier (locks/<supplier>.lock).

### 6.3 Idempotency / accepted_runs
- CANON: accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json (keyed by input sha) => ❌
- AS-IS: accepted_runs/<run_id>.json => keyed by run_id only
P0: implement sha256 of input file + accepted marker by (supplier, sha256).

### 6.4 Inbox model (select file)
- CANON: choose newest unique by mtime; dedup by sha; duplicates -> quarantine/duplicates; other uniques -> archive/obsolete => ❌ (not evidenced in code anchors)
- AS-IS: run_inbox_batch_v1.py plans per file; selection/dedup not shown in provided snippet; no accepted_runs sha check
P0: implement canonical inbox algorithm (sha-based) in orchestrator.

### 6.5 Gate
- CANON: FAIL on file-level issues (readable/format/required columns/header exact), no business logic => ⚠️
- AS-IS: tirehub_gate_v1.py checks required stats fields + file_readable/structure_ok + baseline mismatch warns => baseline logic present (still technical), but header/required columns checks not evidenced here
P1: ensure header/required columns checks exist in gate/emitter stats; confirm no business logic.

### 6.6 DQ
- CANON: mandatory per-supplier DQ config (min_fact_count, max_bad_ratio, allow_empty_snapshot), missing => FAIL CONFIG_DQ_MISSING => ❌
- AS-IS: DQ layer not found in anchors; gate warns has_bad_rows but no DQ thresholds
P0: add DQ step with mandatory config.

### 6.7 Run artifacts layout
- CANON: etl_data/raw_v1/runs/<run_id>/{facts.ndjson,bad.ndjson,stats.json,manifest.json} => ❌
- AS-IS: SSOT segmented facts/<supplier>/<parser>/<YYYY-MM>/<run_id>.ndjson + manifests/<run_id>.json + tmp/<run_id>.ndjson.tmp => different layout
P1: decide mapping: either migrate to new raw_v1 layout or adapt canon to SSOT layout (but canon already утверждён).

### 6.8 State
- CANON: immutable state_store/<supplier>/<run_id>/facts.ndjson + atomic publish state/current/<supplier> => ❌
- AS-IS: state_store/state/current not found (presence check empty)
P0: implement state_store + atomic publish, and retention safety window.

### 6.9 reason_code registry
- CANON: bad.ndjson reason_code from docs/etl/reason_codes.yaml => ❌ (file not evidenced)
- AS-IS: gate uses codes inline (e.g. file_not_readable, structure_not_ok, baseline_missing)
P1: create reason_codes.yaml and map to it (or align naming).

## 7. Fix plan (strict order)

P0:
1) run_id v2 (UTC+Z+rand6+supplier prefix)
2) lock per supplier
3) accepted_runs by (supplier, input_sha256) with sha_prefix2 layout
4) DQ mandatory per-supplier config + enforcement
5) state_store + atomic publish state/current

P1:
6) inbox algorithm (sha dedup + obsolete/archive/quarantine)
7) gate completeness: required columns/header exact evidence
8) align reason_code registry + bad.ndjson contract
9) migrate run artifacts to raw_v1/runs/<run_id>/ contract

