TECH LEAD PLAN 2026-03. Документ фиксирует план стабилизации и развития системы Tirehub ETL после завершения ingestion MVP. План предназначен для инженеров и LLM-агентов. Цель этапа: стабилизировать ingestion платформу, синхронизировать архитектуру, зафиксировать data model и подготовить систему к формированию каталога товаров.

1. CURRENT SYSTEM STATE. Репозиторий содержит рабочую ingestion платформу. Подтверждены компоненты: mail ingestion worker, supplier registry routing, attachment landing, NDJSON emission, schema validation, rulesets, canonical storage, curated слой, serving слой и SQL migrations. Последний ingestion статус зафиксирован в docs/status/2026-03-11_ingestion_mvp.md.

2. ARCHITECTURE SYNCHRONIZATION. В системе существуют три описания архитектуры: ARCHITECTURE_v1_LOCK.md, PROJECT MAP и фактическая структура репозитория. Требуется создать единый документ архитектуры docs/architecture/SYSTEM_ARCHITECTURE_V1.md и синхронизировать его с кодом.

3. DOCUMENTATION NORMALIZATION. Документация должна быть разделена по типам: architecture, contracts, spec, status, operations и archive. Архивные документы из docs/etl/archive должны быть перемещены в docs/archive.

4. REPOSITORY STRUCTURE CLEANUP. Каталоги runtime (var, inputs, out, diag) не должны храниться в репозитории. В репозитории должны оставаться только .gitkeep и описание runtime-архитектуры.

5. ARTIFACT STORAGE POLICY. Бинарные bundle-файлы и диагностические дампы должны быть вынесены из git-репозитория в artifact storage. Репозиторий должен содержать только код, конфигурации и контракты.

6. INGESTION PIPELINE DOCUMENTATION. Создать документ docs/architecture/pipeline.md, описывающий полный ingestion pipeline: IMAP → mail ingestion → evidence → landing → supplier parser → NDJSON → schema validation → rulesets → canonical storage → curated layer → serving layer.

7. DATA MODEL DOCUMENTATION. Создать документ docs/spec/data_model_v1.md, описывающий canonical item, rejected item, supplier entity, product identity и offer структуру.

8. SUPPLIER ADAPTER CONTRACT. Создать документ docs/contracts/SUPPLIER_ADAPTER_CONTRACT.md, определяющий интерфейс адаптеров поставщиков: mapping, parser, NDJSON emit, validation и dispatch.

9. DATA QUALITY POLICY. Создать документ docs/spec/data_quality_rules.md, описывающий правила rulesets: sanity checks, impact weights, signature normalization и token pattern fields.

10. ROUTING EVENT CONTRACT. Зафиксировать структуру событий routing log. Создать docs/contracts/ROUTING_EVENT_SCHEMA.md для стандартизации событий ingestion.

11. INGESTION API CONTRACT. Зафиксировать интерфейс ingestion layer. Создать docs/spec/INGESTION_API.md.

12. SCRIPT ENTRYPOINT. Добавить единый entrypoint pipeline scripts/run_pipeline.sh для запуска полного ETL процесса.

13. DATASET REGISTRY. Создать config/datasets.yaml для регистрации dataset-источников и ingestion outputs.

14. SUPPLIER DIRECTORY. Создать config/suppliers_directory.yaml, содержащий metadata поставщиков: supplier id, feed type, parser, контактные данные.

15. DATA LAYER DEFINITION. Зафиксировать уровни данных: raw → canonical → curated → offers.

16. RELEASE PROCESS. Создать docs/operations/release_process.md, описывающий процедуру релиза ingestion pipeline.

17. REPOSITORY MAP. Создать docs/architecture/repo_map.md с описанием ролей каталогов: config, mappings, rulesets, schemas, scripts и sql.

18. OBSERVABILITY IMPROVEMENT. Расширить ingestion logs и добавить стабильную схему событий routing для анализа pipeline.

19. PLATFORM STABILIZATION PHASE. До завершения этапа стабилизации запрещается добавление новых ingestion источников. Основная задача — синхронизация архитектуры и документации.

20. NEXT DEVELOPMENT PHASE. После стабилизации ingestion платформы следующий этап развития системы: product identity layer, offer aggregation layer и формирование каталога товаров на основе canonical storage.
