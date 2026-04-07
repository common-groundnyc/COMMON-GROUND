# `lake.housing.hpd_violations` parquet orphans

**Symptom:**
```
IO Error: Cannot open file ducklake-...parquet: No such file
```

**Scope:** `SELECT COUNT(*)` works. `SELECT ... WHERE zip = '...'` fails. Only affects `hpd_violations`.

**Affected endpoints:** `/api/neighborhood`, `/api/buildings/worst`. The dashboard handles it gracefully with em-dash placeholders.

**Root cause:** DuckLake catalog still references parquet files that have been deleted from disk for some snapshots.

**Fix:** Either compact the lake or re-materialize the table:

```bash
docker compose exec dagster-code dagster asset materialize \
  --select 'housing/hpd_violations' -m dagster_pipeline.definitions
```

**See also:** `docs/runbooks/known-issues.md`.
