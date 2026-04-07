# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-25)

**Core value:** Cross-domain connections — trace any entity across every dataset in the lake
**Current focus:** v3.0 UX Innovation & Intelligence — Phase 11 (Entity Master)

## Current Position

Phase: 11 of 21 (Entity Master)
Plan: Not started
Status: Ready to plan
Last activity: 2026-04-07 — Lake→public catalog convergence completed (332 tables in unified catalog); MCP server redeployed; CLAUDE.md rewritten against ground truth

## 2026-04-07 Notes

- Dual-catalog issue resolved: `lake.*` schema dropped after batched migration; all tables now in `public.*`
- MCP server container rebuilt without METADATA_SCHEMA references
- 84 lake-only tables flagged for re-ingestion via freshness sensor (sensor cursor reset, 19+ runs completed)
- Repo cruft purged (~710 MB of test_*.duckdb, bobaadr.txt, pad.zip, .tmp_dagster_home_*)
- pm-skills marketplace installed (8 plugins) for PM workflow layered on top of `.planning/`

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
- MinIO S3 traffic unencrypted (s3_use_ssl=False) — open data, low risk
- Hardcoded credential fallbacks in definitions.py — fail-open pattern
- COMBINED_SPACE name parsing fails for multi-word surnames (VAN DER BERG → BERG)
- data_health asset is hollow — profiles only 10 columns, no PII detection
- Zero tests for get_extraction_sql() — 53 SQL queries untested
- Bridge tables not in graph cache — cross-domain tools use SQL fallbacks

### Blockers/Concerns

None.

## Session Continuity

Last session: 2026-04-01
Stopped at: v3.0 roadmap created. Ready to plan Phase 11.
Resume file: None
