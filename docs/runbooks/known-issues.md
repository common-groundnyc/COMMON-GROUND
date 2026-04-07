# Known Issues

## `lake.housing.hpd_violations` — orphan parquet references

**Symptom:** Filtered queries against `lake.housing.hpd_violations` fail with:
```
IO Error: Cannot open file ducklake-...parquet: No such file
```
`SELECT COUNT(*)` works; `WHERE zip = '...'` does not.

**Affected endpoints:** `/api/neighborhood`, `/api/buildings/worst`. The dashboard handles this gracefully with em-dash placeholders.

**Root cause:** DuckLake catalog references parquet files that no longer exist on disk for some snapshots.

**Fix:** Compact the lake or re-materialize the table from Socrata.

```bash
# Re-materialize
docker compose exec dagster-code dagster asset materialize \
  --select 'housing/hpd_violations' -m dagster_pipeline.definitions
```
