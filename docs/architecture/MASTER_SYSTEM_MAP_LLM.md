# MASTER SYSTEM MAP (LLM)
TIREHUB-SYSTEM MASTER SYSTEM MAP. This document is the primary machine-oriented description of the system. It is intended for LLMs and automation agents. It combines project map, architecture boundaries, runtime contours, and execution plan.
## 1. SYSTEM GOAL
Build a supplier-data ingestion platform for auto goods. The system must accept heterogeneous supplier files, convert them into canonical offers, normalize product attributes, compute product signatures, resolve product identity, and build a catalog where one product may have many supplier offers.
## 2. CORE ARCHITECTURE
End-to-end target architecture:
supplier_input
-> intake
-> landing_dataset
-> supplier_parser
-> canonical_offer (supplier offer layer)
-> normalization (product attribute normalization)
-> signature (product fingerprint)
-> entity_matching (product identity resolution)
-> catalog (master product + offers)
Current real system state: intake is implemented and verified in dry-run mode. Landing and ingestion layers exist. Canonical offer, normalization, signature, matching, and catalog layers are planned or partial.
## 3. ARCHITECTURAL PRINCIPLE
The system strictly separates contours:
repo = source code and configuration
var = runtime state
data = durable ETL data
drop = supplier inbound files
Rule: repo != var != data != drop. Runtime state, secrets, checkpoints, logs, and mutable artifacts must not live in the repository.
## 4. REPOSITORY
GitHub repository: skladkoles44/tirehub-system
Repository role: source code, docs, schemas, configs, scripts
Repository must not store runtime state, secrets, or durable ETL data
## 5. REPOSITORY STRUCTURE
Current important directories:
config = configs and registries
scripts/connectors = intake workers
scripts/ingestion = supplier-specific parsing and ingestion logic
docs = contracts, architecture, specs, status
golden = test datasets
runtime-like directories currently inside repo:
inputs
out
tmp
curated_v1
offers_v1
These directories are candidates for migration to VAR or DATA layers.
## 6. PHONE DEVELOPMENT CONTOUR
Primary local contour: Android + Termux
REPO_ROOT = /data/data/com.termux/files/home/tirehub-system
Preferred ETL storage root on phone = /storage/emulated/0/Download/ETL
ETL_VAR_ROOT = runtime state
ETL_DATA_ROOT = durable ETL datasets
ETL_DROP_ROOT = supplier inbound files
All local commands should copy output to device clipboard.
## 7. TEST VPS CONTOUR
Test VPS contour:
REPO_ROOT = /home/Test_etl/repo/tirehub-system
ETL_VAR_ROOT = /home/Test_etl/var/tirehub-system
Worker env file: /home/Test_etl/var/tirehub-system/run/mail_ingest.env
This contour is used for mail intake worker execution and runtime verification.
## 8. PATH DISCIPLINE
All scripts must be runnable from any working directory.
Allowed path resolution methods:
- Path(__file__).resolve()
- shared repo-root helper
- environment variables
Disallowed:
- cwd-dependent runtime logic
- hard reliance on launching from repo root
Current project work already introduced shared repo-root-aware path discipline for key orchestration scripts.
## 9. INTAKE LAYER
The intake layer accepts supplier data from connectors such as mail, API, or FTP.
Current implemented and verified connector: mail intake worker
Main file: scripts/connectors/mail_ingest_worker_v1.py
Current routing config: config/suppliers_registry.yaml
Mail worker responsibilities:
- connect to IMAP
- read watermark/state
- discover new messages by UID
- inspect attachments
- route files to supplier flow
- update checkpoint
Dry-run verification has already confirmed IMAP connectivity, mailbox selection, UID checkpoint logic, and correct handling of messages without attachments.
## 10. LANDING DATASET LAYER
Landing dataset layer is the first immutable persistence layer for supplier inputs.
Target rule:
each supplier input file -> one immutable dataset
Dataset identity should include:
- dataset_id
- supplier_id
- sha256
- arrival_time
- evidence_ref
Landing layer must support:
- immutable storage
- checksum dedup
- routing log
- evidence linkage
- lineage
This is the first critical layer after intake.
## DATASET INVARIANTS
Each supplier file must produce exactly one dataset.
Datasets are immutable.
Dataset identity fields:
- dataset_id
- supplier_id
- sha256
- arrival_time
Dataset content must never be modified after landing.

## 11. SUPPLIER PARSER LAYER
Supplier-specific parsers/adapters transform supplier files into canonical offers.
Pattern:
supplier file -> supplier adapter -> canonical offer
Examples of supplier-specific parsing families:
- Brinex XLSX flows
- Kolobox XLS/XLSX flows
- Centrshin JSON/XLSX flows
Parser responsibilities:
- read supplier-specific file format
- detect sheets/columns/structure
- map fields into canonical offer schema
Parser must not perform product identity resolution.
## 12. CANONICAL OFFER LAYER
Canonical offer is the normalized structural representation of a supplier offer before product identity resolution.
Offer is not catalog product.
One catalog product may later contain many offers.
Canonical offer should contain at least:
- supplier_id
- parser_id
- dataset_id
- supplier_sku if available
- title_raw
- brand_raw
- model_raw if available
- price
- qty
- attributes_raw
- raw_ref
## 13. NORMALIZATION LAYER
Normalization transforms canonical offers into stable normalized product records.
Responsibilities:
- brand normalization
- model normalization
- unit normalization
- size parsing
- attribute extraction
- dictionary mapping
- confidence flags
- unresolved flags
Parsing is supplier-specific. Normalization is global and shared across suppliers.
Normalization must not silently invent missing facts.
## 14. SIGNATURE LAYER
Signature layer generates deterministic product fingerprints from normalized attributes.
Example tire signature fields:
- brand_id or brand_norm
- model_id or model_norm
- width
- profile
- diameter
- load_index
- speed_index
Goal:
- fast exact match
- relaxed candidate recall
- stable comparison key
## 15. ENTITY MATCHING LAYER
Entity matching decides whether incoming normalized offer belongs to:
- existing catalog item
- possible duplicate requiring review
- new catalog item
Matching should start with:
- rule-based matching
- exact structured signatures
- relaxed signatures
- conflict rules
Embeddings or advanced ML are optional later stages, not v1 baseline.
## 16. CATALOG LAYER
Catalog model must separate:
- master product item
- supplier offers
One master product item may have many supplier offers.
Target relation:
master_item <- linked_offers_from_many_suppliers
This is the core marketplace-style pattern.
## 17. INVARIANTS
Core invariants:
- parser does not perform matching
- normalization does not silently invent facts
- same normalized item always produces same signature
- brand/size conflicts block match
- one supplier offer cannot link to two master items
- scripts must not depend on cwd
- repo must not store runtime state
## 18. CURRENT PRIORITY ORDER
Execution order:
1. path discipline and repo stabilization
2. intake registry hardening and safe routing
3. landing dataset layer and lineage
4. canonical offer schema
5. parser framework boundaries
6. normalization layer
7. signature layer
8. entity matching
9. catalog layer
## 19. ACTIVE ISSUE GROUPS
Main issue groups in backlog:
- architecture/refactoring
- connectors/intake
- dataset/ETL core
- supplier parser framework
- catalog/normalization/identity
Current top architectural priorities:
- path discipline
- landing datasets
- canonical offer schema
- parser boundaries
Normalization and matching must come after these foundations.
## 20. CHINESE MARKETPLACE PATTERN
Practical marketplace pattern observed in large Chinese ecosystems:
- supplier-specific adapters/parsers are normal
- unified canonical schema comes after parsing
- normalization is global
- product signature/fingerprint is global
- entity matching is downstream of normalization
- one product may have many supplier offers
This matches the target direction of this system.
## 21. SUMMARY
This system is not only a mail intake worker and ingestion script collection.
The full target system is:
supplier ingestion engine
+ canonical offer layer
+ normalization engine
+ signature/matching engine
+ catalog engine
Short form:
supplier_input
-> landing_dataset
-> canonical_offer
-> normalization
-> signature
-> entity_matching
-> catalog

## 22. QUICK SYSTEM MODEL
supplier files
-> datasets
-> offers
-> normalized products
-> product signatures
-> product identity
-> catalog items
-> supplier offers linked to catalog
