# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-25)

**Core value:** Cross-domain connections — trace any entity across every dataset in the lake
**Current focus:** v2.0 DuckPGQ Graph Infrastructure Rebuild — COMPLETE

## Current Position

Phase: 10 of 10 (Validation & Hardening) — COMPLETE
Plan: 1 of 1 complete (10-02 merged into 10-01 — full test suite run as part of validation)
Status: All 10 phases complete. 18/18 MCP tools pass. property_history permanently fixed. graph_health diagnostic tool added. entity_xray float parsing fixed. v2.0 SHIPPED.
Last activity: 2026-03-26 — Completed 10-01 (validation, hardening, final test suite)

Progress: ██████████ 100%

## Performance Metrics

**Velocity:**
- Total plans completed: 12 (v2.0) / 8 (v1.0)
- Average duration: ~52 min
- Total execution time: ~10.5 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1. Data Audit | 2/2 | 60 min | 30 min |
| 2. Bug Fixes | 2/2 | 135 min | 68 min |
| 3. Ownership Rebuild | 2/2 | 150 min | 75 min |
| 4. Corporate Web Rebuild | 2/2 | 115 min | 58 min |
| 5. Transaction Expansion | 1/1 | 45 min | 45 min |
| 6. Influence Expansion | 1/1 | 40 min | 40 min |
| 7. Cross-Domain Graph | 2/2 | 60 min | 30 min |
| 8. PageRank Integration | 1/1 | 30 min | 30 min |
| 9. Path-Finding Tools | 1/1 | 45 min | 45 min |
| 10. Validation & Hardening | 1/1 | 45 min | 45 min |

**Recent Trend:**
- Last 5 plans: 08-01 ✓, 09-01 ✓, 10-01 ✓
- Trend: Single-plan phases completing in ~30-45 min

## Accumulated Context

### Decisions

- 13 entity types defined for classification
- oath_hearings (21.6M, 7 entity types) is the single highest-value ungraphed table
- Expanded graphs fit in ~1.1GB CSR (4% of 28GB limit)
- 2 new graphs proposed: Enforcement (P0), Civic (P1)
- resolved_entities cluster_id is the Phase 7 bridge key
- **DuckLake ignores CREATE SECRET — only SET s3_* works** (02-01 discovery)
- **MinIO switched to HTTP-only** (internal Docker network, firewall-protected)
- **Reconnect path is primary startup path** — warm-up always fails, all init must be in reconnect
- **QUALIFY must be inside CTE** — not on outer `SELECT *` (02-02 discovery)
- **docker cp required for container updates** — mcp_server.py baked into image, not mounted
- **Name-based PK for graph_owners** — UPPER(TRIM(owner_name)) replaces registrationid (03-01)
- **PLUTO BBL is float string** — use LEFT(bbl, 10) to extract 10-char BBL (03-01)
- **Stage DuckLake reads as temp tables** — avoids full-table scans during UNION (03-01)
- **Mega-owner filter (>100 buildings)** — prevents O(n^2) in graph_shared_owner self-join (03-01)
- **DuckDB config options lock after startup** — must SET at initial connection, not dynamically (03-01)
- **Memory tuning: 18GB limit, 4 threads, insertion order off** — required for expanded PLUTO data (03-01)
- **Graph Parquet cache must be deleted for SQL rebuild** — restart alone loads stale cache (03-02)
- **graph_corps ROW_NUMBER dedup** — nys_corporations has duplicate rows with different date string formats (03-02)
- **property_history safe date sort** — use `str(x["date"] or "0000-00-00")` for comparison, mixed datetime.date/None in ACRIS. PERMANENTLY FIXED in 10-01 (image rebuilt).
- **Incremental temp table staging for large source scans** — UNION across DuckLake tables OOMs; INSERT INTO temp + DROP is safe (04-01)
- **ACRIS grantees only (party_type=2)** — full ACRIS scan (46M rows) too expensive for corp matching; grantees are the relevant party (04-01)
- **Container memory limit reduced to 20GB** — prevents system-wide OOM on 32GB server (04-01)
- **Working mcp_server.py lives in Docker overlay, not image** — image has old code; docker-cp'd versions accumulate in overlay layers (04-01)
- **OATH respondent column is respondent_last_name** — not respondent_name; corps stored in last_name field (04-01)
- **Campaign contributions in city_government schema** — not financial schema (04-01)
- **TX graph 3-stage incremental staging** — deeds, mortgages, assignments/satisfactions as separate temp tables to avoid 48M-row OOM (05-01)
- **Mega-entity cap 200 + per-doc party cap 20** — banks/title companies with 500+ tx caused OOM on self-join; double cap needed (05-01)
- **GRANTOR/GRANTEE role labels** — replaced SOLD/BOUGHT for accuracy across mortgages/assignments (05-01)
- **NYS expenditure columns differ from plan** — `cand_comm_name` (filer), `flng_ent_name` (payee), `org_amt` (amount), `sched_date` (date), `purpose_code_desc` (purpose) (06-01)
- **doing_business_contributions uses `amnt` not `amount`** — same pattern as cfb_offyear_contributions (06-01)
- **Direct name matching for cross-domain bridges** — resolved_entities (56M rows) too expensive for bridge joins; UPPER(TRIM()) on graph tables yields 597K multi-domain entities with zero DuckLake reads (07-01)
- **DuckPGQ MATCH requires edge variable** — `[:Label]` segfaults; must use `[e:Label]` syntax (07-01)
- **DuckPGQ pagerank() uses labels, not table names** — syntax is `pagerank(graph, vertex_label, edge_label)`, returns `(key_col, pagerank)` (08-01)
- **nyc_housing graph referenced stale registrationid column** — caused all graph creation to fail silently; fixed by removing column from PROPERTIES list (08-01)
- **_safe_float() helper for entity_xray** — wraps float() in try/except for lobby compensation fields like "$$300 to $600 per hour" (10-01)

### Prior Milestone Context (v1.0 Entity Resolution)

- Splink 4.0 → 55.5M name index → 33.1M clusters across 44 tables
- `lake.federal.resolved_entities` — the unifying key for cross-domain graphs

### Deferred Issues

- 11 tables missed by column query truncation — corrections appended to registry
- ~~S3 credential breakage~~ — **FIXED** in 02-01
- ~~duckpgq not loading~~ — **FIXED** in 02-01
- ~~property_history date sort crash~~ — **PERMANENTLY FIXED** in 10-01 (image rebuilt, survives restart)
- ~~graph_corps 312K duplicates~~ — **FIXED** in 02-02, regressed in 03-01 (422K), **RE-FIXED** in 03-02 (105K unique)
- ~~graph_has_violation 13.7K orphans~~ — **FIXED** in 02-02 (0 orphans); 5,209 new orphans from PLUTO expansion (0.048%, accepted)
- ~~entity_xray crashes on "HAMILTON HOUSE 79 LLC"~~ — **FIXED** in 10-01 (_safe_float wrapper)
- MinIO HTTP change needs syncing to local infra/ directory
- neighborhood_portrait returns skeleton-only data (known limitation)
- graph_owns uncapped (172K max buildings per owner) — capped only in graph_shared_owner; cosmetic issue in worst_landlords top results
- Bridge tables (graph_owner_tx_bridge, etc.) and graph_unified_entities not present in graph cache — cross-domain tools use SQL fallbacks

### Blockers/Concerns

None — v2.0 complete.

## Session Continuity

Last session: 2026-03-26
Stopped at: v2.0 DuckPGQ Graph Infrastructure Rebuild COMPLETE. All 10 phases done. 18/18 MCP tools pass.
Resume file: None
