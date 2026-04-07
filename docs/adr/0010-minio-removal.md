# 0010 — MinIO / Hetzner S3 removed from pipeline

**Status:** Accepted (2026-04-07)

## Context

Early iterations used MinIO and Hetzner S3 as an intermediate staging layer between ingestion and DuckLake. This was a vestige of the Meltano era. Once ingestion wrote directly to local NVMe parquet (see ADR 0003 — dlt removal), the S3 layer became dead weight:

- Extra network hops for every row
- Extra credentials to rotate (`MINIO_*`, `HETZNER_S3_*`)
- Extra container (`minio`) in `docker-compose.yml`
- Extra failure mode (bucket full / auth drift)

## Decision

Remove MinIO and Hetzner S3 entirely. Ingestion writes directly to NVMe-backed parquet under DuckLake's managed path.

Cleanup (session 2026-04-07):
- Dropped MinIO container from `docker-compose.yml`
- Removed `.dlt/` volume mount (orphan from dlt era)
- Purged dlt artifacts from `/opt/common-ground/`
- Removed `MINIO_*` / S3 env vars from `.env.secrets.enc`

## Consequences

- **+** Simpler stack: one storage layer (NVMe + DuckLake)
- **+** Fewer secrets to manage
- **+** Lower latency on writes
- **−** No offsite durability at the storage layer — backups now happen at the postgres/parquet level (see `docs/runbooks/` future entry)
- **−** Single-host failure mode: if the NVMe dies, data is gone until restored from backup
