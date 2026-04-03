# Common Ground Data Pipeline — Dagster + dlt + DuckLake

## What This Is

A data pipeline that ingests 208 NYC Socrata open datasets into DuckLake (Postgres catalog + local NVMe parquet) on a Hetzner server. Orchestrated by Dagster, ingested by dlt.

## Architecture

```
Mac Studio (M1 Ultra) → Socrata APIs → dlt extract/normalize → DuckLake on Hetzner
                                                                  ├── Postgres catalog (178.156.228.119:5432/ducklake)
                                                                  └── Local NVMe (parquet files on server disk)
```

Processing happens locally. Only parquet files + catalog updates go to Hetzner.

## Stack

- **Dagster 1.12.19** — orchestration, scheduling, UI
- **dagster-dlt 0.28+** — `@dlt_assets` decorator, `DagsterDltResource`
- **dlt 1.23+** — extraction, normalization, loading
- **DuckLake** — lakehouse format (DuckDB 1.5.1 + ducklake extension)
- **Postgres** — DuckLake catalog (user: `dagster`, db: `ducklake`)
- **Local NVMe** — direct parquet on disk (server-local filesystem)

## Project Structure

```
dagster-pipeline/
├── .dlt/
│   ├── config.toml          # DuckLake storage, worker counts, file rotation
│   └── secrets.toml          # Catalog creds, MinIO creds, Socrata token (GITIGNORED)
├── src/dagster_pipeline/
│   ├── definitions.py        # Definitions, job, daily schedule, DagsterDltResource
│   ├── sources/
│   │   ├── socrata.py        # @dlt.source with parallelized resources, retry, 404 skip
│   │   └── datasets.py       # All 208 dataset IDs organized by schema (10 schemas)
│   └── defs/
│       └── socrata_assets.py # 10 @dlt_assets functions, SocrataDltTranslator, pool
└── pyproject.toml            # Dependencies (scaffolded via create-dagster)
```

## Key Design Decisions

1. **dlt over ingestr/Bruin**: ingestr's jsonpath_ng parser can't handle Socrata's colon-prefixed system fields (`:id`, `:updated_at`). dlt's REST API source lets us control column naming.

2. **Local NVMe over sshfs**: Local NVMe writes at ~94k rows/sec vs ~30k rows/sec over sshfs. 3x faster.

3. **One @dlt_assets per schema**: Each of the 10 schemas (business, education, housing, etc.) is a single Dagster multi-asset. The `SocrataDltTranslator` maps dlt resources to `schema/table_name` Dagster asset keys.

4. **`pool="socrata_api"`** on all assets: Limits concurrent Socrata API calls to avoid rate limits (2000 req/hour).

5. **`parallelized=True`** on dlt resources: Each page fetch runs in a separate thread via `dlt.defer()`.

6. **DuckLake v0.4 catalog**: Was migrated from v0.3 (created by DuckDB 1.4.4). The old v0.3 metadata was wiped from Postgres and recreated clean by DuckDB 1.5.1.

## Credentials

Managed via SOPS + age encryption. See `.env.secrets.enc` (encrypted, committed to git).

```bash
# Edit secrets (opens in $EDITOR, saves encrypted on close)
sops --input-type dotenv --output-type dotenv .env.secrets.enc

# Decrypt for deployment
sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc > .env.secrets
```

Key names follow dlt's env var convention: `DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG`, `SOURCES__SOCRATA__APP_TOKEN`, etc.

## How to Run

```bash
cd ~/Desktop/dagster-pipeline

# Install
uv sync

# Materialize one schema
DAGSTER_HOME=/tmp/dagster-home uv run dagster asset materialize \
  --select 'health/restaurant_inspections' -m dagster_pipeline.definitions

# Materialize all
DAGSTER_HOME=/tmp/dagster-home uv run dagster asset materialize \
  --select '*' -m dagster_pipeline.definitions

# Launch UI
uv run dagster dev -m dagster_pipeline.definitions
```

## Performance Tuning

### How We Achieved High Throughput

Benchmarked at 3M rows in 139s (~21.6k rows/sec end-to-end). The 4 techniques that matter:

1. **`parallelized=True` + `dlt.defer()`** — Each page fetch runs in its own thread. Instead of fetching pages sequentially, all 60 pages (3M rows / 50k per page) are dispatched concurrently via dlt's thread pool. This is the single biggest speedup.

2. **Large page size: 50,000 rows** — Fewer HTTP round-trips. Socrata supports up to 50k per request with an app token. Without a token, max is 1,000.

3. **PyArrow tables instead of Python dicts** — `pa.Table.from_pylist(rows)` converts JSON to Arrow in-memory. When dlt receives Arrow tables, it skips its Python-dict normalization entirely and goes straight to parquet. Normalize stage: 3M rows in 0.3 seconds.

4. **Local NVMe direct writes** — Load stage hits ~94k rows/sec via local disk vs ~30k rows/sec over sshfs/rclone. 3x improvement.

### Stage Breakdown (3M rows)

| Stage | Speed | Notes |
|-------|-------|-------|
| Extract | ~40-50k rows/sec | Socrata API bound, parallelized with `dlt.defer()` |
| Normalize | 3M in 0.3s | Arrow bypass — essentially free |
| Load | ~94k rows/sec | Local NVMe direct writes |
| **End-to-end** | **~21.6k rows/sec** | Pipeline overhead + stage transitions |

### Worker Config (in .dlt/config.toml)

- `[extract] workers = 10` — parallel resource extraction threads
- `[normalize] workers = 4` — multiprocess normalization (CPU-bound)
- `[load] workers = 20` — parallel write threads
- `file_max_items = 100000` — file rotation for efficient parquet sizing
- `ducklake_max_retry_count = 5` — connection resilience

### Bottleneck

The Socrata API is always the bottleneck. Normalize and load are nearly free thanks to Arrow + local NVMe. To go faster, you'd need to increase page size (Socrata caps at 50k) or run multiple datasets concurrently (which the `workers = 10` extract config already does).

## Hetzner Server (178.156.228.119)

SSH: `ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119`

Note: Root login is disabled. Use `fattie` user with sudo. Firewall restricts SSH to team IPs.

Services (via Cloudflare Tunnel):
- MCP: https://mcp.common-ground.nyc/mcp
- DuckDB UI: https://duckdb.common-ground.nyc
- Dagster UI: https://dagster.common-ground.nyc (Cloudflare Access)

Direct access (firewall-restricted):
- Postgres: 178.156.228.119:5432

## Known Issues

1. **416 assets in Dagster UI** instead of 208: dagster-dlt creates both custom translator specs AND default specs. The 208 "default" group ones are dependency stubs, not real duplicate loads. Cosmetic only.

2. **Some Socrata datasets 404**: ~5 datasets have been removed from Socrata. The source gracefully skips them with a warning log.

3. **All assets use `write_disposition="replace"`**: Full refresh on every run. Incremental loading (`merge`) is the next milestone — needs Socrata system field mapping to avoid the colon-prefix issue.

4. **No tests**: The pipeline is the test — materialization either succeeds or fails. Unit tests for the source are a nice-to-have.

## What Comes Next

1. **Run full initial load** — materialize all 208 datasets
2. **Incremental loading** — switch large datasets (parking violations: 120M+ rows) to `merge` with dlt state tracking
3. **Custom Python assets** — port the 21 non-Socrata sources (BLS, Census, College Scorecard, etc.) from the bruin-pipeline project as dlt Python sources
4. **Dagster schedule** — enable the `socrata_full_load_schedule` (daily at 4 AM, currently STOPPED)
5. **Data quality** — add freshness policies and row count checks

## Related Projects

- `~/Desktop/bruin-pipeline/` — original Bruin pipeline (208 Socrata + 21 custom assets). Socrata asset configs and custom Python scripts live here. The custom assets haven't been ported yet.
- `~/Desktop/dagster-pipeline-old/` — first hand-rolled attempt (no dagster-dlt, no create-dagster scaffold). Replaced by this project.
