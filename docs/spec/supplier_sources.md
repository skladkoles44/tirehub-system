供应商数据源架构。本文件定义 Tirehub ETL 数据处理系统中的供应商数据源结构。文档用于说明供应商数据如何进入系统，以及供应商注册表、字段映射与解析程序之间的关系。该文档面向工程师与自动化代理，用于理解和维护数据摄取流程。

1. 目标。本层的目标是统一描述供应商数据来源、路由规则与解析逻辑。供应商数据源层连接 routing registry、mapping contract 与 ingestion parser，使系统能够稳定处理来自不同供应商的数据格式。

2. 主注册表。系统的供应商路由注册表位于 config/suppliers_registry.yaml。该文件定义供应商标识以及数据路由规则。在当前系统中，邮件 ingestion connector 使用该注册表识别并路由供应商附件。

3. 次级配置。附加的供应商配置位于 etl_ops/config/*.yaml。这些文件包含供应商数据管道的运行配置，例如 pipeline 参数或部署配置。

4. 映射契约。供应商字段映射位于 mappings/suppliers/。映射文件定义供应商数据结构与系统内部字段结构之间的对应关系。映射文件被视为数据结构契约，用于保证 parser 与数据模型之间的一致性。

5. 解析程序。供应商解析脚本位于 scripts/ingestion/。每个供应商数据源通常对应一个或多个 parser 或 emitter。解析程序负责读取供应商数据文件，并生成 NDJSON 格式的数据流，以供后续 ingestion pipeline 使用。

6. 供应商 KOLOBOX。映射文件位于 mappings/suppliers/kolobox.yaml、kolobox_diski_xls_v1.yaml、kolobox_komplektatsii_xls_v1.yaml、kolobox_truck_xls_v1.yaml。解析脚本位于 scripts/ingestion/kolobox/ 目录。

7. 供应商 CENTRSHIN。映射文件位于 mappings/suppliers/centrshin_json_v1.yaml、centrshin_diski_xlsx_v1.yaml、centrshin_shiny_xlsx_v1.yaml。解析脚本位于 scripts/ingestion/centrshin/ 目录。

8. 供应商 BRINEX。映射文件位于 mappings/suppliers/brinex_xlsx_v1.yaml。解析脚本位于 scripts/ingestion/brinex/ 目录。

9. 通用入口。系统包含通用 ingestion 入口脚本 scripts/ingestion/run_inbox_batch_v1.py 与 scripts/ingestion/tirehub_ingest_v1.py。这些脚本负责协调 supplier parser 与 ingestion pipeline。

10. 数据流程。完整的数据流如下：供应商邮件或供应商文件 → 邮件连接器 → 证据存储 → landing 层 → 供应商路由 → 解析器 → NDJSON 数据 → schema 校验 → rulesets 规则处理 → canonical 数据存储 → curated 层 → serving 层。

11. 架构规则。供应商注册表负责 routing identity。mapping 文件负责字段映射契约。parser 脚本负责执行数据提取逻辑。这三个层必须保持同步修改，否则会破坏 ingestion pipeline 的稳定性。

12. 系统状态。目前系统已通过真实邮件 ingestion 测试验证 Kolobox 数据源。仓库中同时存在 Centrshin 与 Brinex 的 mapping 与 parser 结构。下一阶段是建立 dataset registry 以及 product identity 层。
