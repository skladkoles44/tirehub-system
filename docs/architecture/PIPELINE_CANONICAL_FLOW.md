# Pipeline canonical flow

supplier source
→ parser framework
→ GOOD rows (good.ndjson)
→ tirehub_ingest_v1.py
→ SSOT facts
→ offer schema
→ serving layer
