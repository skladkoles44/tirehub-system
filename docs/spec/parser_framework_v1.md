# Parser framework v1

## Purpose
This document defines parser framework v1 for supplier feeds.
It formalizes the current ingestion contour that already exists in code, but is still fragmented across supplier-specific scripts.

## Goal
Parser framework v1 must define one standard path for converting supplier input files into canonical GOOD records and then into SSOT facts.

Canonical path:

input file
-> supplier detector / routing
-> supplier emitter
-> good.ndjson + stats.json
-> gate
-> verdict.json
-> tirehub_ingest_v1.py
-> SSOT facts / manifests / accepted marker

## Framework boundaries

### In scope
- supplier-specific input handling
- supplier-specific emitter logic
- common GOOD row contract
- common stats contract
- common gate step
- common ingest step
- parser_id discipline
- supplier_id discipline

### Out of scope
- mail connector logic
- offer schema details
- product identity matching
- serving / catalog logic
- UI / API layer
- downstream normalization rules beyond ingestion contract

## Core roles

### 1. Detector / router
Responsible for deciding which supplier path should handle the input.
Examples already present:
- registry / routing logic
- run_inbox_batch_v1.py dispatch

Detector does not implement parsing logic itself.

### 2. Supplier emitter
Responsible for reading supplier file/layout and producing canonical GOOD output.

Emitter responsibilities:
- parse supplier-specific input format
- detect layout/header/data start
- normalize source fields into canonical GOOD structure
- write good.ndjson
- write stats.json
- expose supplier_id
- expose parser_id
- expose emitter_version
- preserve raw/source trace

Emitter must not write directly into SSOT facts.

### 3. Gate
Responsible for validating run-level expectations before ingest.

Current role:
- reads stats.json
- compares with baseline when provided
- emits verdict.json
- allows PASS or WARN into ingest
- blocks FAIL

Gate must remain supplier-agnostic.

### 4. Ingest
Responsible for validating GOOD rows and publishing immutable SSOT artifacts.

Current role:
- validate GOOD row contract
- enforce parser_id / supplier_id consistency
- enforce DQ config
- write SSOT fact segment
- write manifest
- write accepted marker

Ingest must remain supplier-agnostic.

## Required artifacts per supplier run
Every canonical supplier run must produce:

- good.ndjson
- stats.json
- verdict.json

Optional:
- baseline.json
- bad.ndjson
- diagnostics artifacts

## GOOD row contract
All emitters must produce rows compatible with the common GOOD contract enforced by tirehub_ingest_v1.py.

Current required top-level fields:
- supplier_id
- parser_id
- mapping_version
- mapping_hash
- ndjson_contract_version
- emitter_version
- run_id
- effective_at
- sku_candidate_key
- raw
- parsed
- quality_flags
- _meta

## Stats contract
Every emitter must produce stats.json with at least:
- supplier_id
- parser_id
- run_id
- effective_at
- mapping_hash
- mapping_version
- good_rows
- bad_rows
- source_rows_read

Additional counters and flags are allowed.

## Parser ID rules
parser_id is an infrastructure discriminator, not a business field.

Rules:
- every supplier emitter must write explicit parser_id
- parser_id must identify supplier + layout/format + version
- breaking parsing change requires new parser_id
- parser_id must remain stable for identical logic
- seed/smoke parser_id values remain reserved for smoke/seed contour

## Supplier ID rules
supplier_id must be explicit and stable.
It must not depend on cwd, file path accidents, or human guesses during runtime.

## Canonical separation of responsibilities
Framework v1 separates layers as follows:

- detector/router chooses supplier path
- emitter transforms supplier file into canonical GOOD rows
- gate validates run quality
- ingest publishes SSOT artifacts

Supplier-specific business/layout logic must stay in emitter layer.
Gate and ingest must not contain supplier-specific parsing branches.

## Current repository reality
The repository already contains framework pieces:
- supplier-specific emitters under scripts/ingestion/<supplier>/
- shared ingest in scripts/ingestion/tirehub_ingest_v1.py
- shared/near-shared gate in scripts/ingestion/kolobox/tirehub_gate_v1.py
- dispatch logic in scripts/ingestion/run_inbox_batch_v1.py

The problem is not total absence of framework.
The problem is lack of one written framework contract.

## Canonical framework direction
Parser framework v1 should treat the following as canonical:
- one supplier adapter/emitter per supported input/layout path
- one common GOOD contract
- one common gate contract
- one common ingest contract
- one explicit parser_id lifecycle rule

## Legacy / compatibility status
Legacy scripts, duplicated runners, and old parser variants may remain temporarily as compatibility paths.
They are not the canonical framework target.

Canonical framework target is:
supplier adapter -> emitter -> gate -> ingest

## Non-goals
Parser framework v1 does not yet standardize:
- one Python base class for all emitters
- plugin loading system
- automatic registration by reflection
- full bad.ndjson reason taxonomy
- normalization / matching / catalog stages

These may come later after the contract is fixed.

## Conclusion
Parser framework v1 formalizes the supplier ingestion contour that already exists in practice:
detector/router -> emitter -> gate -> ingest.

Issue #38 should be considered a framework-contract issue first, and only then a code-cleanup issue.
