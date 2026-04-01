# Platform Architecture Audit — Common Ground Data Lake

**Auditor:** platform-architect (cg-innovation-audit)
**Date:** 2026-04-01
**Scope:** Dagster orchestration, dlt ingestion, DuckDB/DuckLake storage, data modeling, pipeline patterns

---

## Executive Summary

Common Ground is a well-engineered single-node analytical data lake with 294+ tables, 60M+ rows, ingesting NYC open data from 20+ source types into DuckLake (Postgres catalog + MinIO S3 parquet storage) on a Hetzner server. The pipeline is orchestrated by Dagster with direct DuckDB writes (bypassing dlt for most paths), and served via a FastMCP server with 60+ tools.

**Overall assessment: Strong foundation, several scaling and resilience risks.**

The platform punches well above its weight for a single-server deployment. The main risks are: single-point-of-failure architecture, DuckLake's pre-production maturity, no automated testing, and tight coupling between pipeline and serving layers.

---

## 1. Architecture Overview

### Current Flow
```
Sources (Socrata, Federal APIs, CSV, etc.)
    |
    v
Dagster Orchestrator (Mac Studio / Docker)
    |
    v
httpx fetch -> PyArrow tables -> DuckDB zero-copy write
    |
    v
DuckLake (Postgres catalog @ 178.156.228.119:5432 + MinIO S3 @ :9000)
    |
    v
FastMCP Server (DuckDB read-only) -> Claude / LLM consumers
```

### Key Files
| Component | File | Purpose |
|-----------|------|---------|
| Orchestrator | `src/dagster_pipeline/definitions.py` | 7 jobs, 3 schedules, 2 sensors, multiprocess executor |
| Socrata ingestion | `src/dagster_pipeline/defs/socrata_direct_assets.py` | 287 assets via `_make_socrata_asset()` factory |
| Federal ingestion | `src/dagster_pipeline/defs/federal_direct_assets.py` | ~49 assets across 20 source types |
| Writer | `src/dagster_pipeline/ingestion/writer.py` | 5 write modes: full_replace, streaming_replace, delta_merge, streaming_merge, cursor |
| DuckLake resource | `src/dagster_pipeline/resources/ducklake.py` | Process-level connection cache, S3/catalog config |
| Entity resolution | `src/dagster_pipeline/defs/resolved_entities_asset.py` | Splink 4.0 batch processing, 55M records |
| Materialized views | `src/dagster_pipeline/defs/materialized_view_assets.py` | 5 pre-joined views for MCP tool performance |
| Name index | `src/dagster_pipeline/defs/name_index_asset.py` | UNION ALL across 47 tables, cross-reference filter |
| Data quality | `src/dagster_pipeline/defs/quality_assets.py` | Per-table profiling: row counts, null rates |
| Freshness sensor | `src/dagster_pipeline/defs/freshness_sensor.py` | Hourly Socrata vs DuckLake row count comparison |
| Embeddings | `src/dagster_pipeline/defs/entity_embeddings_asset.py` | Gemini -> Lance vector index, 2.96M names |

---

## 2. Strengths

### 2.1 Ingestion Performance (A)
The pipeline achieves **21.6k rows/sec end-to-end** (benchmarked at 3M rows in 139s). Key techniques:
- **Arrow bypass**: `pa.Table.from_pylist()` -> DuckDB zero-copy insert skips dlt normalization entirely (3M rows in 0.3s normalize)
- **Parallel page fetch**: `dlt.defer()` dispatches all pages concurrently via thread pool
- **50k row pages**: Maximizes Socrata's per-request limit with app token
- **MinIO S3 direct**: 94k rows/sec load vs 30k rows/sec over sshfs (3x)

**Reference:** `CLAUDE.md:96-124` (performance tuning section)

### 2.2 Delta/Incremental Ingestion (A-)
The move from full-replace to cursor-based delta merge is well-implemented:
- `_pipeline_state` table tracks per-dataset cursors (`socrata_direct_assets.py:33-43`)
- Streaming merge uses `MERGE INTO ... WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` (`writer.py:130-149`)
- Schema evolution handled via `ALTER TABLE ADD COLUMN` for new columns (`writer.py:216-219`)
- Socrata colon-prefix normalization (`:id` -> `_id`) done as zero-copy Arrow rename (`writer.py:193-204`)
- `touch_cursor()` preserves existing cursor when no new data found (`writer.py:287-322`)

### 2.3 Job/Schedule Design (A-)
Well-tiered scheduling in `definitions.py`:
- **Daily (6 AM)**: ~200 live Socrata + federal datasets, excluding static/monthly
- **Monthly (1st, 3 AM)**: ~87 annual/quarterly datasets
- **Monthly (1st, noon)**: Entity embeddings rebuild
- Proper exclusion sets built from `STATIC_DATASETS` and `MONTHLY_DATASETS` (`definitions.py:33-41`)
- Concurrency control: `tag_concurrency_limits` with per-schema limit of 1 (`definitions.py:150-156`)

### 2.4 Entity Resolution Pipeline (A)
Splink 4.0 probabilistic entity resolution across 55M records:
- Phonetic-first batching via `double_metaphone` groups related names (SMITH/SMYTH same batch)
- 500K batch size with greedy bin-packing (`resolved_entities_asset.py:61-77`)
- Pre-trained model (`models/splink_model_v2.json`) with 0.9 predict / 0.92 cluster thresholds
- Full cleanup of Splink temp tables after each batch (`resolved_entities_asset.py:161-172`)
- ~51 min for full run

### 2.5 Foundation Layer (A-)
Strong analytical acceleration layer:
- **5 materialized views** (`mv_building_hub`, `mv_acris_deeds`, `mv_zip_stats`, `mv_crime_precinct`, `mv_corp_network`) pre-join expensive multi-table scans
- **H3 spatial index** (res 9, ~100m hexagons) across 23 lat/lng tables
- **Phonetic index** with double_metaphone + soundex for name blocking
- **Row fingerprints** via MurmurHash3 for dedup detection
- `AutomationCondition.eager()` on materialized views auto-refreshes on upstream changes

### 2.6 Freshness Monitoring (B+)
The `data_freshness_sensor` (`freshness_sensor.py`) compares Socrata source counts against DuckLake:
- 5% drift threshold triggers stale status
- Only emits `RunRequest` when source count actually changed (prevents duplicate runs)
- Writes sync status back to `_pipeline_state` for dashboarding

---

## 3. Risks and Weaknesses

### 3.1 DuckLake Maturity Risk (HIGH)
DuckLake was released May 2025 and is **not yet production-ready** per the DuckDB team's own assessment (expected Q2 2026). Key risks:
- **Catalog lock contention**: Global snapshot ID creates writer contention across ALL schemas. The codebase already works around this with aggressive retry config: `ducklake_max_retry_count=200`, `ducklake_retry_wait_ms=200`, `ducklake_retry_backoff=2.0` (`ducklake.py:51-53`). This is a workaround for an architectural limitation.
- **No PRIMARY KEY support**: Cursor updates use DELETE + INSERT pattern (`writer.py:265-283`)
- **Missing features on roadmap**: Schema evolution, time travel, partitioning, compaction, garbage collection are all listed as upcoming
- **No transactional guarantees across tables**: A failed run can leave partial writes

**Recommendation:** Monitor DuckLake releases closely. Have an Iceberg migration path ready. The Parquet storage layer is compatible.

### 3.2 Process-Level Connection Cache (MEDIUM)
`ducklake.py:10-11` uses a global `_conn_cache` that persists across all assets in the same worker process. This creates:
- **State leaks**: A failed asset can leave the connection in a bad state, affecting subsequent assets
- **No connection pooling**: Single connection per process, no max lifetime or health checks beyond `SELECT 1`
- **Memory growth**: DuckDB connections accumulate memory from registered tables and temp objects

**Recommendation:** Replace with a proper connection factory that creates fresh connections per asset execution, or add connection lifetime limits.

### 3.3 No Test Coverage (HIGH)
CLAUDE.md explicitly states: "No tests: The pipeline is the test." (`CLAUDE.md:149`). There are zero unit, integration, or regression tests for:
- Arrow schema normalization logic
- Delta merge correctness
- Cursor state management
- Name extraction SQL
- Freshness sensor logic

The freshness sensor has pure functions (`build_dataset_manifest`, `check_socrata_count`, `compute_sync_status`) that are trivially testable but have no tests.

**Recommendation:** Start with the writer module and freshness sensor — these have the highest blast radius when bugs occur.

### 3.4 Hardcoded Credentials in Fallbacks (MEDIUM)
`definitions.py:136-145` has hardcoded MinIO credentials as default values:
```python
s3_access_key=os.environ.get(..., "minioadmin"),
s3_secret_key=os.environ.get(..., "minioadmin"),
```
Similarly, the catalog URL defaults to a full connection string with password.

**Recommendation:** Remove fallback defaults. Fail fast if credentials are missing.

### 3.5 Asset Factory Pattern Creates Hidden Coupling (LOW-MEDIUM)
`_make_socrata_asset()` generates 287 assets dynamically (`socrata_direct_assets.py:46-142`). The factory closure captures `schema`, `table_name`, `dataset_id`, `domain` but the inner function is named `_asset` — all 287 assets share the same function name in stack traces and profiling.

The factory also imports from `name_index_asset._connect_ducklake` for DuckLake connections (`socrata_direct_assets.py:24`), creating a cross-module dependency from the ingestion layer to the entity resolution layer.

### 3.6 dlt Dependency Is Vestigial (LOW)
The `pyproject.toml` still lists `dlt[duckdb,filesystem]>=1.23.0` and `dagster-dlt>=0.28.0` as dependencies, and the CLAUDE.md documents a dlt-based architecture. However, the actual ingestion path (`socrata_direct_assets.py`, `federal_direct_assets.py`) bypasses dlt entirely, using direct httpx + Arrow + DuckDB writes. The dlt dependency adds ~50MB to the install for unused code.

**Recommendation:** Remove dlt dependencies if truly unused, or document which assets still use it. This also reduces potential dependency conflicts.

---

## 4. Scaling Assessment

### Current Scale
| Metric | Value |
|--------|-------|
| Tables | 294+ (287 Socrata + ~49 federal + foundation) |
| Total rows | 60M+ |
| Daily ingestion | ~200 datasets at 6 AM |
| Monthly ingestion | ~87 datasets on 1st |
| Server | Single Hetzner (178.156.228.119) |
| Storage | MinIO S3 (parquet) + Postgres (catalog) |

### Scaling Limits
1. **Single-writer bottleneck**: DuckLake's global snapshot ID means only one writer can commit at a time. At 287 daily datasets, this creates lock contention (hence the 200-retry workaround).
2. **Memory ceiling**: DuckDB's in-memory processing limits single-query complexity. The `memory_limit='2GB'` setting (`ducklake.py:19`) is conservative but means large joins (85M row ACRIS) must be carefully staged.
3. **Compute locality**: Processing happens on Mac Studio, results written to Hetzner. Doubling the dataset count would linearly increase ingestion time.
4. **No horizontal scaling**: The `multiprocess_executor` with `max_concurrent=4` (`definitions.py:148`) runs all work on one machine.

### 10x Growth Path
To handle 10x data (600M+ rows, 2000+ tables):
1. **Move compute to server**: Run Dagster on Hetzner, eliminate Mac-to-Hetzner latency for writes
2. **Partition large tables**: ACRIS (85M), 311 (37M), parking violations (120M+) need date partitioning
3. **Consider MotherDuck**: Managed DuckDB cloud with pay-per-query pricing ($0.08/GB-month storage, per-second compute billing). Eliminates self-managed MinIO/Postgres
4. **Evaluate Dagster+ Hybrid**: Keep compute on Hetzner but offload orchestration UI/scheduling to Dagster Cloud ($10/month base). Reduces operational burden

---

## 5. Technology Evaluation: 2026 Landscape

### 5.1 DuckLake vs Iceberg vs Delta Lake

| Feature | DuckLake (current) | Apache Iceberg | Delta Lake |
|---------|-------------------|----------------|------------|
| Maturity | Pre-production (Q2 2026 target) | Production (v2+) | Production (v4+) |
| Catalog | SQL database (Postgres/MySQL/SQLite) | REST, Hive, Nessie, Unity | Unity Catalog |
| Time travel | Roadmap | Yes | Yes |
| Schema evolution | Roadmap | Yes | Yes |
| Partitioning | Roadmap | Yes (hidden) | Yes |
| DuckDB support | Native (1st party) | Extension (read/write since v1.4) | Extension (read-only) |
| Parquet compatible | Yes | Yes | Yes |
| Compaction/GC | Roadmap | Yes | Yes |

**Assessment:** DuckLake is the right bet for this project because:
- Native DuckDB integration (zero friction vs extension)
- SQL-based catalog is simpler than REST-based Iceberg catalogs
- Parquet storage is interoperable — migration to Iceberg is possible if needed
- The team already has workarounds for DuckLake's limitations

**Risk mitigation:** The Parquet files on MinIO are format-agnostic. If DuckLake stalls, switching to Iceberg requires only catalog migration, not data migration.

### 5.2 MotherDuck Evaluation

MotherDuck offers managed DuckDB with:
- **Managed DuckLake**: Eliminates self-managed Postgres catalog + MinIO
- **Dual Execution**: Local + cloud compute split
- **Pay-per-second billing**: No idle costs
- **SOC 2 Type II compliant**

**Relevance:** Would eliminate the single-server SPOF and MinIO management overhead. However, it adds cloud dependency and monthly costs. Worth evaluating for the MCP serving layer (read-heavy, query-heavy) while keeping ingestion self-hosted.

### 5.3 Lance Vector Index

The current Lance integration (`entity_embeddings_asset.py`) is well-positioned:
- 2.96M names embedded via Gemini into Lance format
- DuckDB's native Lance extension supports `lance_vector_search`, `lance_fts`, `lance_hybrid_search` as SQL table functions
- January 2026: Lance announced Uber-scale multi-bucket storage and 1.5M IOPS

**Assessment:** Lance is the right choice for this workload. No change recommended.

### 5.4 Data Mesh Relevance

Common Ground's domain structure already aligns with data mesh principles:
- **10 domain schemas**: business, education, housing, health, public_safety, etc.
- **Foundation layer**: Cross-domain views and indexes act as "data products"
- **MCP tools**: Each tool is effectively a data product with a defined contract

What's missing for full data mesh:
- No formal data product ownership or SLAs
- No data catalog/discovery layer beyond `data_health`
- No access control per domain

**Assessment:** Don't adopt full data mesh formalism — the team is too small. But the domain-schema structure is a strong foundation if the project grows to multi-team.

---

## 6. Recommendations (Prioritized)

### P0 — Do Now
1. **Add tests for writer.py and freshness_sensor.py** — These are the highest-risk, most testable modules. The pure functions in `freshness_sensor.py` (lines 46-97) are trivially testable.
2. **Remove hardcoded credential fallbacks** from `definitions.py:136-145`. Fail fast on missing env vars.
3. **Fix `_connect_ducklake` coupling** — The ingestion layer imports from `name_index_asset.py` for connections. Extract connection logic to a shared utility.

### P1 — Next Quarter
4. **Add partition support for large tables** — ACRIS (85M), 311 (37M), parking violations need date-based partitioning when DuckLake adds support.
5. **Evaluate MotherDuck for the MCP serving layer** — The read-heavy MCP server would benefit from managed infrastructure. Keep ingestion self-hosted.
6. **Implement data contracts** — Define expected schemas for each domain. The `data_health` asset profiles tables but doesn't enforce contracts.

### P2 — When Resources Allow
7. **Consider Dagster+ Hybrid** — Offload scheduling/UI to cloud while keeping compute on Hetzner. Reduces operational burden.
8. **Build CI/CD pipeline** — Automated testing on commit, staging environment for schema changes.
9. **Clean up dlt dependency** — Remove if unused, or document which paths still use it.

---

## 7. Architecture Diagram (Target State)

```
Sources (Socrata, Federal, CSV)
    |
    v
Dagster (Hetzner, Docker) -----> Dagster+ Cloud (optional: UI/scheduling)
    |
    v
httpx -> Arrow -> DuckDB
    |
    v
DuckLake (Postgres catalog + MinIO S3)
    |                               |
    v                               v
Foundation Layer               Iceberg Export (future)
(MVs, H3, Phonetic,           (for external consumers)
 Fingerprints, Embeddings)
    |
    v
FastMCP Server (DuckDB read-only + Lance vector search)
    |
    v
Claude / LLM consumers
```

---

## Appendix: File Line References

| Finding | File | Lines |
|---------|------|-------|
| Retry config workaround | `resources/ducklake.py` | 51-53 |
| Global connection cache | `resources/ducklake.py` | 10-11 |
| Hardcoded credentials | `definitions.py` | 136-145 |
| No tests stated | `CLAUDE.md` | 149 |
| Asset factory | `defs/socrata_direct_assets.py` | 46-142 |
| Cross-module import | `defs/socrata_direct_assets.py` | 24 |
| Delta merge with retry | `ingestion/writer.py` | 123-149 |
| Streaming merge + schema evolution | `ingestion/writer.py` | 152-257 |
| Cursor DELETE+INSERT pattern | `ingestion/writer.py` | 260-283 |
| Freshness pure functions | `defs/freshness_sensor.py` | 46-97 |
| Splink batch processing | `defs/resolved_entities_asset.py` | 80-158 |
| MV auto-refresh | `defs/materialized_view_assets.py` | 29 |
| Performance benchmarks | `CLAUDE.md` | 96-124 |
| Schedule tiers | `definitions.py` | 108-124 |
| Concurrency limits | `definitions.py` | 148-158 |
