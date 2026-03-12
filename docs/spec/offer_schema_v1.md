# Offer schema v1

## Purpose
This document defines offer schema v1 as the business-facing supplier offer contract.
It is distinct from ingestion facts and trace records.

## Distinction between fact and offer
Current ingestion records (`good.ndjson` / SSOT facts) contain trace and ingestion metadata:
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

These fields belong to ingestion lineage and auditability.
They are not the canonical business offer contract.

Offer schema v1 is the normalized supplier offer representation used by serving and downstream offer publication.

## Minimal offer schema v1
Required fields:
- supplier_id
- supplier_sku
- internal_sku
- qty
- price_purchase
- currency
- updated_at

## Field meaning
- supplier_id: stable supplier identifier
- supplier_sku: supplier-facing SKU/article/code used as offer key
- internal_sku: internal resolved SKU used by serving
- qty: available quantity, integer or null
- price_purchase: supplier purchase price, number or null
- currency: normalized currency code
- updated_at: normalized offer freshness timestamp

## Identity
Offer identity key:
- supplier_id
- supplier_sku

This matches the current serving upsert conflict rule.

## Product linkage
Each supplier offer links to exactly one internal_sku.
One internal/master product may have many supplier offers.

## Mapping from ingestion fact to offer
Typical transformation:
- supplier_id <- fact supplier_id
- supplier_sku <- supplier-specific source SKU/article
- internal_sku <- normalization / SKU builder output
- qty <- parsed.qty
- price_purchase <- parsed.price
- currency <- normalized currency
- updated_at <- normalized timestamp

## Not part of offer core
The following remain ingestion-only / trace-only fields:
- parser_id
- mapping_version
- mapping_hash
- ndjson_contract_version
- emitter_version
- run_id
- raw
- quality_flags
- _meta

## Current implementation source
Offer publication is already implemented in:
- scripts/serving/apply_to_postgres_v1.py

Current serving table:
- supplier_offers_latest

Current persisted offer core:
- supplier_id
- supplier_sku
- internal_sku
- qty
- price_purchase
- currency
- updated_at

## Conclusion
Offer schema v1 formalizes the business supplier offer contract that sits between ingestion facts and serving/catalog layers.
