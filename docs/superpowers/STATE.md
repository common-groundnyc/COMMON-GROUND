# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-25)

**Core value:** Cross-domain connections — trace any entity across every dataset in the lake
**Current focus:** v3.0 UX Innovation & Intelligence — Phase 11 (Entity Master)

## Current Position

Phase: 11 of 21 (Entity Master)
Plan: Not started
Status: Ready to plan
Last activity: 2026-04-08 — Source-code audit via opensrc resolved 3 deferred issues. Lance purged everywhere, dlt fully removed, graphify knowledge graph built

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
