# DATA FLOW DIAGRAM (LLM)
TIREHUB-SYSTEM DATA FLOW DIAGRAM. This document is a text-form data flow diagram for LLMs and automation agents.
## 1. PRIMARY FLOW
external_supplier_source
-> intake_connector
-> landed_dataset
-> supplier_parser
-> canonical_offer
-> normalized_product_record
-> product_signature
-> entity_matching
-> catalog_master_item + linked_supplier_offer
## 2. EXTERNAL INPUTS
Possible sources:
- IMAP mailbox attachments
- supplier API feeds
- FTP/SFTP files
- manual supplier file drops
## 3. STEP: INTAKE CONNECTOR
Input:
- external supplier event or file
Output:
- discovered supplier input
- evidence metadata
- checkpoint update
Rules:
- connector determines newness
- connector does not resolve product identity
## 4. STEP: LANDED DATASET
Input:
- supplier file or payload from connector
Output:
- immutable landed dataset
- dataset_id
- sha256
- evidence_ref
Rules:
- dataset is immutable
- dataset content is not rewritten
- each supplier file becomes exactly one dataset
## 5. STEP: SUPPLIER PARSER
Input:
- landed dataset
- supplier-specific parsing rules
Output:
- canonical offer records
Examples:
- one XLSX file may produce many offers
- one multi-sheet supplier file may explode into many canonical rows
Rules:
- parser handles structure only
- parser does not decide product identity
## 6. STEP: CANONICAL OFFER
Input:
- parser output
Output:
- structurally unified supplier offers
Fields may include:
- supplier_id
- parser_id
- dataset_id
- supplier_sku
- raw title
- raw brand
- raw attributes
- price
- qty
Rule:
- canonical offer remains supplier-facing, not catalog-facing
## 7. STEP: NORMALIZATION
Input:
- canonical offer
- dictionaries
- shared normalization logic
Output:
- normalized product attributes
Examples:
- brand normalization
- tire size parsing
- season normalization
- model cleanup
- unresolved flags
Rules:
- no silent invention of facts
- same input must yield same normalized output
## 8. STEP: SIGNATURE
Input:
- normalized product record
Output:
- exact signature
- relaxed signature
- candidate keys
Purpose:
- exact comparison
- candidate retrieval
- stable product fingerprint
## 9. STEP: ENTITY MATCHING
Input:
- normalized record
- signatures
- existing catalog index
Output:
- match decision
- new item decision
- manual review decision
Rules:
- brand conflicts block match
- critical size conflicts block match
- gray zone goes to review
## 10. STEP: CATALOG WRITE
Input:
- entity matching decision
Output:
- master catalog item created or reused
- supplier offer linked to master item
Rules:
- one master item may have many offers
- one offer may link to only one master item
## 11. SECONDARY FLOWS
checkpoint_flow:
intake_connector -> runtime_state -> next intake run
evidence_flow:
connector -> evidence metadata -> dataset linkage
lineage_flow:
dataset -> canonical offer -> normalized record -> catalog link
review_flow:
entity_matching -> manual_review_queue -> reviewed catalog decision
## 12. STORAGE CONTOURS
repo:
- code
- configs
- docs
var:
- checkpoints
- locks
- logs
- runtime worker state
data:
- durable ETL datasets
- SSOT
- curated outputs
drop:
- inbound supplier files before or around landing
## 13. CURRENT IMPLEMENTATION STATUS
Implemented or partially implemented:
- mail intake worker
- supplier routing registry
- ingestion scripts
- path-discipline hardening
Planned or partial:
- landing dataset registry
- canonical offer contract
- normalization layer
- signature layer
- entity matching layer
- catalog layer
## 14. SHORT FORM
supplier source
-> connector
-> dataset
-> offers
-> normalized records
-> signatures
-> matching
-> catalog items
-> linked supplier offers
