# 0004 — hnsw_acorn over Lance for vector search

**Status:** Accepted (2026-04-07)

## Context

Semantic search needed a filtered vector index. Two candidates:

- **Lance** — Rust-based columnar format with ANN support, used via `lancedb`. Required a second storage layer alongside DuckLake.
- **hnsw_acorn** — DuckDB community extension implementing ACORN-1 (filtered HNSW). Keeps everything inside DuckDB.

Experimental HNSW persistence in DuckDB is unreliable, so the index must be rebuilt from a parquet cache.

## Decision

Use **`hnsw_acorn`** with a parquet-cache rebuild pattern. Embeddings (`MiniLM-L6-v2`, 384-dim, INT8) are materialized to `main.*` tables via ONNX Runtime and indexed at server startup.

`infra/duckdb-server/shared/lance.py` was renamed to `shared/vector_search.py` on 2026-04-07 to remove the misleading name.

## Consequences

- **+** One storage layer (DuckLake), no Lance sidecar
- **+** Filtered HNSW via ACORN-1 (no Lance equivalent at the time)
- **+** Index rebuild from parquet cache is deterministic
- **−** Index is rebuilt on every server start (startup latency)
- **−** Relies on a community extension
