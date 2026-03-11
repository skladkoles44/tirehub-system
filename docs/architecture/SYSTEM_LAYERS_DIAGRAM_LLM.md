# SYSTEM LAYERS DIAGRAM (LLM)
TIREHUB-SYSTEM SYSTEM LAYERS DIAGRAM. This document is a compact machine-oriented layer map for LLMs and automation agents.
## 1. PURPOSE
Describe the system as stacked architectural layers, not as individual files.
## 2. LAYER STACK
Layer 1: External supplier sources
Layer 2: Intake connectors
Layer 3: Landing dataset layer
Layer 4: Supplier parser layer
Layer 5: Canonical offer layer
Layer 6: Normalization layer
Layer 7: Signature and matching layer
Layer 8: Catalog layer
Layer 9: Runtime and operational layer
Layer 10: Docs, contracts, and control layer
## 3. LAYER 1: EXTERNAL SUPPLIER SOURCES
Role:
- source of heterogeneous supplier data
Examples:
- IMAP attachments
- API feeds
- FTP/SFTP files
- manual file drops
Output:
- raw supplier payloads
Rule:
- source systems are outside repository control
## 4. LAYER 2: INTAKE CONNECTORS
Role:
- detect and receive new supplier inputs
Responsibilities:
- connect to source
- discover new data
- checkpoint progress
- route inbound files toward landing
Must not:
- normalize products
- resolve product identity
## 5. LAYER 3: LANDING DATASET LAYER
Role:
- immutable persistence boundary for inbound supplier data
Responsibilities:
- store landed dataset
- assign dataset identity
- attach checksum and evidence
- preserve lineage anchor
Core invariant:
- each supplier file becomes one immutable dataset
## 6. LAYER 4: SUPPLIER PARSER LAYER
Role:
- supplier-specific structural adapters
Responsibilities:
- read supplier format
- handle sheets/columns/layout
- emit canonical offers
Must not:
- perform cross-supplier product matching
## 7. LAYER 5: CANONICAL OFFER LAYER
Role:
- shared structural representation of supplier offers
Meaning:
- offer is supplier-facing
- offer is not catalog product
Typical fields:
- supplier_id
- parser_id
- dataset_id
- supplier_sku
- title_raw
- brand_raw
- price
- qty
- attributes_raw
## 8. LAYER 6: NORMALIZATION LAYER
Role:
- convert canonical offers into stable normalized product facts
Responsibilities:
- normalize brand/model
- parse size/specs
- normalize units and attributes
- add flags and confidence
Core rule:
- normalization is global, shared, and deterministic
## 9. LAYER 7: SIGNATURE AND MATCHING LAYER
Role:
- compute product fingerprint and decide identity relation
Subfunctions:
- exact signature
- relaxed signature
- candidate retrieval
- conflict rules
- match decision
Decisions:
- existing master item
- possible duplicate for review
- new master item
## 10. LAYER 8: CATALOG LAYER
Role:
- persist product identity model
Core entities:
- master item
- supplier offer
Relation:
- one master item may have many linked supplier offers
## 11. LAYER 9: RUNTIME AND OPERATIONAL LAYER
Role:
- execution mechanics and runtime state
Examples:
- checkpoints
- locks
- logs
- timers
- monitoring
Contours:
- var = runtime state
- data = durable ETL data
- drop = inbound supplier files
## 12. LAYER 10: DOCS, CONTRACTS, AND CONTROL LAYER
Role:
- describe and constrain system behavior
Contents:
- master maps
- component maps
- flow diagrams
- schemas
- issue-driven plans
Purpose:
- make system understandable to humans and LLMs
## 13. CROSS-LAYER RULES
- connectors do not normalize
- parsers do not match products
- normalization does not parse files
- matching does not rewrite raw facts
- catalog does not depend on supplier file structure
## 14. CURRENT BUILD ORDER
1. path discipline and repo stabilization
2. intake hardening
3. landing dataset layer
4. canonical offer layer
5. parser boundaries
6. normalization
7. signature and matching
8. catalog
## 15. SHORT FORM
sources
-> connectors
-> datasets
-> parsers
-> offers
-> normalized facts
-> signatures and matching
-> catalog
