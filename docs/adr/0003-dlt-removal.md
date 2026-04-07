# 0003 — dlt removed in favor of direct DuckDB ingestion

**Status:** Accepted (2026-03-26)

## Context

The pipeline originally used `dlt` (data load tool) for ingestion: REST API sources, schema inference, staging, and destination loaders. Benchmarks showed:

- Meltano: ~2k rows/sec (rejected earlier)
- dlt: ~8k rows/sec
- Direct httpx → parquet → DuckLake: ~20k rows/sec (2.5× dlt)

dlt's abstractions (`@dlt.resource`, `@dlt.source`, load packages, staging destinations) added operational complexity, state management in `.dlt/`, and debugging surface area for marginal benefit once the sources stabilized.

## Decision

Remove `dlt` entirely. Sources fetch via `httpx` and write straight to DuckLake as parquet. Dagster asset definitions wrap the raw source functions.

## Consequences

- **+** 2.5× throughput on large Socrata tables
- **+** No more dlt state-management infrastructure (no `.dlt/` volume, no staging destination)
- **+** Simpler debugging — one Python function per source
- **−** Lost dlt's automatic schema evolution (handled manually now)
- **−** One-time migration cost (commit `5dc5f01` era)
