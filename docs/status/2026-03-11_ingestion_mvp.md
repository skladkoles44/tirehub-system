# Ingestion MVP status
Date: 2026-03-11

## Implemented

IMAP ingestion worker  
Evidence storage (.eml)  
Landing layer for attachments  
Routing to supplier inbox  
Outcome-based watermark (UID)  
Routing log  
Rotation policy

## Storage layout

var/tirehub-system/

evidence/  
landing/  
logs/routing.log  
logs/archive/  
run/mail_ingest_state.json  

## Retention policy

evidence TTL: 30 days  
landing TTL: 14 days  
routing.log smart rotate (age ≥1 day OR size ≥5MB)  
archive TTL: 30 days  
keep-last archive invariant  

## Automation

cron 03:30

scripts/ops/ingestion_rotation.sh

## Status

Ingestion layer MVP completed and operational.
