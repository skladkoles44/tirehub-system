# COMPONENT MAP (LLM)
TIREHUB-SYSTEM COMPONENT MAP. This document is a compact machine-oriented component inventory for LLMs and automation agents.
## 1. PURPOSE
Describe the major system components, their responsibilities, boundaries, and main interfaces.
## 2. TOP-LEVEL COMPONENTS
1. Intake connectors
2. Landing dataset layer
3. Supplier parser layer
4. Canonical offer layer
5. Normalization layer
6. Signature layer
7. Entity matching layer
8. Catalog layer
9. Runtime/ops layer
10. Documentation/contracts layer
## 3. COMPONENT: INTAKE CONNECTORS
Role: accept supplier data from external systems.
Current connector families:
- mail connector
- future API connector
- future FTP/SFTP connector
Main current worker:
- scripts/connectors/mail_ingest_worker_v1.py
Responsibilities:
- connect to external source
- discover new input
- checkpoint progress
- collect attachment/file metadata
- route supplier input toward landing/drop flow
Must not:
- perform product normalization
- perform product identity resolution
- store mutable runtime state in repo
## 4. COMPONENT: LANDING DATASET LAYER
Role: immutable persistence layer for supplier inputs.
Responsibilities:
- persist each supplier input as dataset
- assign stable dataset identity
- store checksum and evidence linkage
- provide routing and lineage anchor
Dataset identity fields:
- dataset_id
- supplier_id
- sha256
- arrival_time
- evidence_ref
Must not:
- rewrite landed content
- merge business-level products
## 5. COMPONENT: SUPPLIER PARSER LAYER
Role: supplier-specific adapters that read heterogeneous supplier files.
Examples:
- Brinex XLSX parser family
- Kolobox XLS/XLSX parser family
- Centrshin JSON/XLSX parser family
Responsibilities:
- read file format
- detect layout/sheets/columns
- map supplier fields into canonical offer structure
Must not:
- decide whether two products are the same
- apply shared catalog identity logic
## 6. COMPONENT: CANONICAL OFFER LAYER
Role: shared structural model for supplier offers before product identity resolution.
Offer semantics:
- one supplier record becomes one canonical offer
- canonical offer is not catalog product
Typical fields:
- supplier_id
- parser_id
- dataset_id
- supplier_sku
- title_raw
- brand_raw
- model_raw
- price
- qty
- attributes_raw
- raw_ref
## 7. COMPONENT: NORMALIZATION LAYER
Role: shared attribute normalization across suppliers.
Responsibilities:
- brand normalization
- model normalization
- size parsing
- unit normalization
- attribute extraction
- dictionary mapping
- confidence and unresolved flags
Must not:
- depend on supplier file format
- silently invent missing facts
## 8. COMPONENT: SIGNATURE LAYER
Role: deterministic fingerprint generation from normalized product attributes.
Responsibilities:
- exact structured signature
- relaxed signature
- candidate recall keys
Uses normalized attributes only.
Must not:
- depend on raw supplier formatting
## 9. COMPONENT: ENTITY MATCHING LAYER
Role: resolve whether normalized offer belongs to existing master item or new item.
Responsibilities:
- candidate search
- conflict rules
- rule-based scoring
- manual review routing
Decisions:
- exact same item
- likely same item
- manual review
- new item
## 10. COMPONENT: CATALOG LAYER
Role: persistent product identity model.
Core entities:
- master item
- supplier offer
Relation:
- one master item may have many supplier offers
- one supplier offer may link to one master item only
## 11. COMPONENT: RUNTIME / OPS LAYER
Role: execution, checkpoints, logs, locks, timers, monitoring.
Contours:
- repo = code/config/docs
- var = runtime state
- data = durable ETL data
- drop = inbound supplier files
## 12. COMPONENT: DOCS / CONTRACTS LAYER
Role: machine and human-readable contracts for system behavior.
Contents:
- project maps
- architecture docs
- parser boundaries
- schemas
- issue-driven plans
## 13. PRIMARY INTERFACES
supplier_input -> intake_connector
intake_connector -> landing_dataset
landing_dataset -> supplier_parser
supplier_parser -> canonical_offer
canonical_offer -> normalization
normalization -> signature
normalization + signature -> entity_matching
entity_matching -> catalog
## 14. CRITICAL BOUNDARIES
- intake does not normalize products
- parser does not match products
- normalization does not parse files
- matching does not mutate normalized facts
- catalog does not store supplier raw file semantics
## 15. CURRENT BUILD PRIORITY
1. path discipline and repo stabilization
2. landing dataset layer
3. canonical offer layer
4. parser boundaries
5. normalization
6. signature
7. entity matching
8. catalog
