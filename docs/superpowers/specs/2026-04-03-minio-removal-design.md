# MinIO → Local Filesystem Migration

**Date:** 2026-04-03
**Status:** Approved

## Problem

DuckLake stores parquet files in MinIO (S3-compatible object storage) on the same Hetzner server. Every parquet file read goes through MinIO's HTTP/S3 API layer, adding ~2-5ms per file access. With hundreds of small parquet files per table, this compounds to seconds of overhead per query. MinIO was originally needed to sync data from a Mac to the server — now that ingestion runs server-side via Docker, MinIO is unnecessary infrastructure adding latency.

## Solution

Export all parquet data from MinIO as raw files to local NVMe, update the DuckLake catalog's root `data_path` from `s3://ducklake/data/` to a local filesystem path, verify reads and writes, then remove MinIO. Expected: 50-60x reduction in per-file I/O overhead.

## Server Inventory

- **Server:** Hetzner dedicated, 30GB RAM, NVMe SSDs
- **OS:** Ubuntu 24.04.3 LTS
- **Disk layout:**
  - `sda` (229GB) — OS, Docker, apps
  - `sdb` (250GB) — Hetzner Cloud Volume, mounted at `/mnt/HC_Volume_104936190`, used for Docker volumes (`lakevol`, `pgdata`)
- **Docker containers:** duckdb-server (5.8GB RAM), postgres (160MB), minio (620MB), cloudflared (27MB)
- **MinIO data:** 194GB at `/data/ducklake/data/` (inside container), physically on `sdb` volume
- **Free disk on sdb:** ~55GB (not enough for in-place copy)

## Architecture

### Before
```
DuckDB → httpfs (S3 API) → MinIO (HTTP server) → xl.meta/part.N → NVMe
          ~2-5ms/file         ~0.5-1ms overhead    erasure format
```

### After
```
DuckDB → local read() → raw parquet → NVMe
          ~0.01-0.1ms/file    direct access
```

## Migration Plan

### Phase 1: Prepare disk
- Resize Hetzner Cloud Volume `sdb` from 250GB → 500GB via Hetzner Cloud console
- Online resize filesystem: `resize2fs /dev/sdb`
- Verify: `df -h /mnt/HC_Volume_104936190` shows ~500GB

### Phase 2: Export data from MinIO
- Install `mc` (MinIO client) on the host or in a container
- Create target directory: `mkdir -p /mnt/data/common-ground/lake/data/`
  - Note: the exact path depends on the Docker volume mount. The duckdb-server container sees this as `/data/common-ground/lake/data/`
- Export: `mc mirror myminio/ducklake/data/ /mnt/data/common-ground/lake/data/`
  - This reads objects via S3 API (raw parquet out) and writes plain files to local disk
  - Streams — low memory footprint (~50-100MB)
  - Estimated time: 10-30 minutes for 194GB on local NVMe
- Verify file count:
  - `mc ls --recursive myminio/ducklake/data/ | wc -l`
  - `find /mnt/data/common-ground/lake/data/ -type f | wc -l`
  - Counts must match

### Phase 3: Switch DuckLake catalog
- Stop duckdb-server: `docker compose stop duckdb-server`
- Update root data_path in Postgres catalog (ONE row):
  ```sql
  psql -h localhost -U dagster -d ducklake -c \
    "UPDATE lake.ducklake_metadata SET value = '/data/common-ground/lake/data/' WHERE key = 'data_path';"
  ```
  - All file references are relative to this root — no other catalog changes needed
- Update `mcp_server.py`:
  - Remove `CREATE SECRET minio_s3` blocks (both primary and reconnect paths)
  - Remove httpfs S3 settings: `enable_http_metadata_cache`, `http_retries`, `http_retry_wait_ms`
  - Keep `httpfs` extension loaded (needed for other HTTP features)
- Start duckdb-server: `docker compose start duckdb-server`

### Phase 4: Verify reads
- Run benchmark suite against MCP:
  - `SELECT COUNT(*) FROM lake.housing.hpd_violations`
  - `SELECT address FROM lake.city_government.pluto WHERE ... LIMIT 1`
  - `building(identifier="1000670001")`
  - `entity(name="BLOOMBERG")`
  - `safety(location="14")`
- Compare to S3 baseline times recorded before migration
- All queries must return same data, faster

### Phase 5: Verify writes
- Materialize one small Dagster asset to confirm DuckLake writes to local path:
  ```bash
  dagster asset materialize --select 'foundation/mv_city_averages'
  ```
- Verify the new parquet file appears in `/data/common-ground/lake/data/foundation/mv_city_averages/`
- Update dlt pipeline destination config:
  - Change `bucket_url` from `s3://ducklake` to `file:///data/common-ground/lake`
  - Or: remove S3 credential env vars and set local `DATA_PATH` in dlt config
  - Test one dlt asset materialization

### Phase 6: Cleanup (after 24h confidence)
- Remove MinIO service from `docker-compose.yml` (and `hetzner-docker-compose.yml`)
- Remove MinIO dependency from duckdb-server service
- Remove MinIO env vars from `.env.secrets.enc`
- Delete MinIO's old data: `rm -rf /mnt/data/common-ground/ducklake/`
- Free ~194GB of disk space
- Optionally resize volume back down (but Hetzner volumes can only grow, not shrink — so keep 500GB)

## Rollback

If anything fails at any point:
```sql
-- Revert catalog to S3
psql -h localhost -U dagster -d ducklake -c \
  "UPDATE lake.ducklake_metadata SET value = 's3://ducklake/data/' WHERE key = 'data_path';"
```
Re-add `CREATE SECRET minio_s3` to `mcp_server.py`, restart. Back to MinIO reads in 30 seconds.

## Risks

### Disk space
- **Mitigation:** Resize Hetzner volume to 500GB before starting
- During migration: ~194GB MinIO + ~150-194GB raw parquet = ~350-390GB (fits in 500GB)
- After cleanup: ~200GB used (raw parquet + Postgres + embeddings + local DuckDB files)

### Memory during export
- `mc mirror` streams objects — uses ~50-100MB RAM
- DuckDB server stays running during export (still reads from MinIO)
- No risk of OOM

### Path format mismatch
- DuckLake catalog stores relative paths: `housing/hpd_violations/ducklake-uuid.parquet`
- The root `data_path` is the only absolute path — one UPDATE changes it
- If paths don't resolve: `OVERRIDE_DATA_PATH` at ATTACH time as fallback
- **Test with one small table first** before committing

### dlt write path
- dlt currently writes via S3 to MinIO
- Need to change dlt config to write to local filesystem
- dlt supports `filesystem` destination with local paths
- If dlt config is wrong, new writes fail but old data is fine

### Docker volume mounts
- duckdb-server container must be able to read `/data/common-ground/lake/data/`
- Currently `lakevol` mounts to `/data/common-ground` — the `lake/data/` subdirectory will be inside this volume
- MinIO's old data at `/data/ducklake/` is ALSO inside `lakevol` — both coexist during migration

### Server downtime
- Phase 3 (switch) requires duckdb-server restart — ~2 minutes of downtime
- Phases 1-2 and 4-6 are non-disruptive

## Success Criteria

- All MCP tool queries return same data as before
- Query latency reduced by 2-10x (per-file overhead eliminated)
- Dagster asset materialization writes to local path
- MinIO container removed from docker-compose
- ~194GB disk reclaimed from MinIO's erasure-coded data
