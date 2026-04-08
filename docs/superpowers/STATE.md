# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-25)

**Core value:** Cross-domain connections — trace any entity across every dataset in the lake
**Current focus:** v3.0 UX Innovation & Intelligence — Phase 11 (Entity Master)

## Current Position

Phase: 11 of 21 (Entity Master)
Plan: Not started
Status: Ready to plan
Last activity: 2026-04-08 — Phase 1 of source-code audit shipped. Phantom property graphs fixed (4 of 4). Splink last_name m_probability fix in source (retrain pending). Cascading schema-drift bugs unmasked in network.py — 2 paths now work end-to-end, 3 paths fail with diagnosable errors instead of silent failures.

## 2026-04-08 Phase 1 Audit — final state

**Shipped:**
- 4 phantom property graphs renamed to existing graphs (`nyc_building_network` → `nyc_ownership` ×3, `nyc_housing` → `nyc_ownership`, `nyc_influence_network` → `nyc_influence`, `nyc_transaction_network` → `nyc_transactions`)
- New regression test `test_graph_refs.py` parses `tools/*.py` and asserts every referenced graph has a CREATE statement — prevents future phantoms
- Splink training script: added 3rd EM pass with `block_on('first_name', 'zip')` so EM has variance to estimate `last_name` m_probabilities. New regression test `test_splink_model_integrity.py` enforces this.
- 8+ schema-drift bugs in `tools/network.py` fixed (`b.total_units` → `b.units`, `v.severity` → `v.class`, `b.housenumber || ' ' || b.streetname` → `b.address` at 5 sites)
- LCC table function unavailable in installed duckpgq build → cliques view rewritten to use degree-based heuristic on `graph_shared_owner` directly

**Deployed:** all of the above are on Hetzner MCP server as of this commit.

**Working in production end-to-end (verified by full 5/5 smoke test):**
- ✅ `network(type="ownership", name=<bbl>)` — landlord network traversal returns full portfolio + violations
- ✅ `network(type="political", name=<entity>)` — political network returns money_trail with FEC/NYS/NYC donations
- ✅ `network(type="worst")` — slumlord rankings with violations, evictions, complaints. Top result: 145 buildings, 332k units, 1256 violations, 957 evictions
- ✅ `network(type="cliques")` — most-connected buildings by shared-owner edge count. Top BBL has 144 neighbors
- ✅ `network(type="property")` — ACRIS transaction network. BLACKSTONE LLC: 19 tx / $72M / 18 sells + 1 buy

All 5 of 5 network() views now return real data. Up from 0/5 silently broken before the audit (2 of those failing because of column-name typos that the phantom graphs short-circuited; 3 because of schema drift between query and graph builders).

**Schema enrichments deployed:**
- `graph_buildings` — added `zip` column from hpd_jurisdiction
- `graph_eviction_petitioners` — added `bbl` column with 3-tier fallback (borough+block+lot, direct bbl, NULL graceful)
- `graph_tx_entities` — completely rebuilt as aggregation over graph_tx_edges, exposing tx_count, property_count, as_seller, as_buyer, total_amount (was just entity_name + party_type before)
- Build order swapped: graph_tx_edges builds first (40M rows), then graph_tx_entities aggregates from it
- `nyc_ownership` and `nyc_transactions` property graph PROPERTIES lists updated to expose new columns

**Cache rebuilt manually** via `/tmp/recover_graph_cache.py` after detecting that the in-container graph builder runs on a connection that can't access the lake catalog (separate latent bug, masked for years). Recovery script attaches lake directly and writes parquets to `/data/common-ground/graph-cache/`. graph_tx_shared written as empty placeholder (full self-join over 40M edges takes too long; downstream code degrades gracefully).

**Latent issues found and worked around (out of scope for Phase 1):**
- In-container graph builder uses a connection that lacks the lake catalog attach. Recovery via direct attach worked for now, but the build path needs fixing so future cold starts work without manual intervention.
- `graph_building_flags` lacks the rich signals (litigation_count, harassment_findings, lien_count, dob_violation_count) the worst-landlords scoring expects. Currently zeroed; restoring needs aggregations from `lake.housing.hpd_litigations` etc.
- graph_tx_shared empty placeholder means "connected entities via shared transactions" returns empty. Real data needs the full 40M-edge self-join run as a background job.

**Pending manual deploy steps:**
1. Splink retrain (5+ min wall clock):
   ```
   docker exec common-ground-duckdb-server-1 python /app/scripts/train_splink_model.py
   ```
   Then re-materialize `resolved_entities` Dagster asset to score 55M records with the corrected model.
2. Re-run `test_splink_model_integrity.py::test_model_has_m_probability_on_every_match_level` after retrain — must pass.
3. Compare cluster counts before/after to measure recall improvement from the last_name fix.

**Phase 1 Task 1 (ThreadPoolExecutor wrapper) was retracted** — implementer subagent's TDD step proved it wasn't a serialization bug; it's a per-call timeout enforcer. Audit was wrong. See `memory/project_cg_audit_retraction.md`.

**Phase 1 Task 4 (`ForenameSurnameComparison`)** deferred — improvement not bug; will land in Phase 3 plan.

## 2026-04-08 Notes (source-code audit via opensrc)

Pulled canonical sources for DuckDB, Splink, DuckPGQ, Dagster into `opensrc/`. Investigated 3 deferred issues and resolved all 3:

- **VAN DER BERG name-parsing bug** — FALSE ALARM. Splink's `NameComparison` uses Jaro-Winkler (atomic full-string), not Jaccard (tokenized). Our `models/splink_model_v2.json` + `scripts/train_splink_model.py:48` confirm JW-only. Bug only manifests with `JaccardLevel` or raw DuckDB `jaccard()` — we use neither.
- **hnsw_acorn segfault** — RESOLVED as permanent workaround. Root cause: DuckDB connections not thread-safe (`mcp_server.py:1736` has the smoking gun), HNSW build needs write lock that collides with read queries. Stubbed `_create_hnsw_indexes()` is correct behavior, not TODO. Brute-force `array_cosine_distance` at <5K rows is faster than HNSW would be. Revisit only if entity_names exceeds 50K.
- **DuckPGQ MATCH parameterization** — ARCHITECTURAL, not deferred. `cwida/duckpgq-extension/src/core/functions/table/match.cpp:780-827` parses WHERE clauses as ParsedExpression at compile time; no defer hook exists. Zero parameterized examples in entire DuckPGQ test suite. Our f-string + regex validation in `tools/network.py` is the only safe pattern. Not fixable without upstream work (6+ month item).

## 2026-04-07 Notes

- Dual-catalog issue resolved: `lake.*` schema dropped after batched migration; all tables now in `public.*`
- MCP server container rebuilt without METADATA_SCHEMA references
- 84 lake-only tables flagged for re-ingestion via freshness sensor (sensor cursor reset, 19+ runs completed)
- Repo cruft purged (~710 MB of test_*.duckdb, bobaadr.txt, pad.zip, .tmp_dagster_home_*)
- Lance fully purged (code, deps, filenames, memory); dlt fully removed (pyproject, lockfile, .dlt dir, env vars)
- s3fs dep removed; MinIO/S3 env vars removed (DuckLake uses local NVMe, not S3)
- pm-skills marketplace installed (8 plugins); graphify skill installed; opensrc installed
- graphify knowledge graph built: 1,825 nodes, 2,242 edges, 140 communities; Obsidian vault + HTML at `graphify-out/`

Progress: ██████████░░░░░░░░░░ 50% (20/40 total plans across all milestones)

## Performance Metrics

**Velocity:**
- Total plans completed: 20 (v1.0: 8, v2.0: 12)
- Average duration: ~52 min
- Total execution time: ~17.5 hours

**By Milestone:**

| Milestone | Phases | Plans | Total Time |
|-----------|--------|-------|------------|
| v1.0 Entity Resolution | 8 | 8 | ~7 hours |
| v2.0 Graph Rebuild | 10 | 12 | ~10.5 hours |
| v3.0 UX Innovation | 11 | 0/30 | — |

**Recent Trend:**
- Last 5 plans: 08-01 ✓, 09-01 ✓, 10-01 ✓
- Trend: Single-plan phases completing in ~30-45 min

## Accumulated Context

### Decisions

- v3.0 scope driven by 5-agent innovation audit (platform-architect, ai-strategist, product-strategist, data-scientist, infra-engineer)
- Entity Master is the keystone — almost every Phase 12-21 feature depends on stable entity IDs
- Fuzzy address matching (libpostal) is highest-ROI ER improvement (~30-40% more true matches)
- Semantic layer is pure metadata work, no infrastructure changes needed
- Anomaly detection runs as Dagster asset, NOT in MCP server (memory constraints)
- Security items (API keys in embedder.py, S3 encryption, credential fallbacks) deferred per user preference — open data, not user data
- Product-strategist flagged MCP-only distribution as single point of failure — need owned surfaces

### Deferred Issues

- 14 Gemini API keys hardcoded in embedder.py (security, deferred)
- ~~MinIO S3 traffic unencrypted~~ — **RESOLVED 2026-04-07**: MinIO/S3 fully removed, DuckLake on local NVMe
- Hardcoded credential fallbacks in definitions.py — fail-open pattern
- ~~COMBINED_SPACE name parsing VAN DER BERG → BERG~~ — **RESOLVED 2026-04-08**: false alarm, we use Jaro-Winkler not Jaccard (verified via Splink source audit)
- data_health asset is hollow — profiles only 10 columns, no PII detection
- Zero tests for get_extraction_sql() — 53 SQL queries untested
- Bridge tables not in graph cache — cross-domain tools use SQL fallbacks

### Architectural Limitations (not deferred, permanent)

- **DuckPGQ MATCH pattern parameterization** — WHERE clauses parsed at compile time, no runtime defer hook. String interpolation + regex validation is the only safe pattern. See `cwida/duckpgq-extension/src/core/functions/table/match.cpp:780-827`.
- **hnsw_acorn segfault** — DuckDB connections not thread-safe, HNSW build lock collides with read queries. Brute-force `array_cosine_distance` is the stable pattern for <5K rows. Revisit only if entity_names exceeds 50K.

### Blockers/Concerns

None.

## Session Continuity

Last session: 2026-04-01
Stopped at: v3.0 roadmap created. Ready to plan Phase 11.
Resume file: None
