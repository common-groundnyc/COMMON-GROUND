# src/dagster_pipeline — Agent Notes

This is the **ingestion tier**. Dagster assets fetch from Socrata, federal sources, and custom scrapers, then write directly to DuckLake. No dlt — removed 2026-03-26.

## Layout

```
src/dagster_pipeline/
├── definitions.py              # Dagster Definitions, jobs, schedules, sensors
├── sources/                    # API clients & extractors (pure functions, no Dagster)
│   ├── socrata_direct.py
│   ├── bls.py, census.py, hud.py, fec.py
│   ├── courtlistener.py, littlesis.py, usaspending.py
│   └── ...
├── defs/                       # Dagster @asset definitions
│   ├── socrata_direct_assets.py    # 287 Socrata assets
│   ├── federal_direct_assets.py    # 22 federal assets
│   ├── name_index_asset.py         # ER preprocessing
│   ├── resolved_entities_asset.py  # Splink clustering
│   ├── entity_master_asset.py      # Canonical entities
│   ├── foundation_assets.py        # h3, phonetic, fingerprints
│   ├── materialized_view_assets.py # 9 MVs
│   ├── spatial_views_asset.py
│   ├── address_lookup_asset.py
│   ├── geo_zip_boundaries_asset.py # NYC MODZCTA polygons
│   ├── quality_assets.py
│   ├── flush_sensor.py             # Inline → parquet flush
│   └── freshness_sensor.py         # Hourly source-vs-lake check
└── resources/                  # Dagster resources (duckdb, http clients)
```

## Conventions

- **One asset per table.** Asset key = `schema/table` (e.g. `housing/hpd_violations`).
- **Sources are pure.** `sources/*.py` exposes plain functions; only `defs/*.py` wraps them as `@asset`.
- **Never bypass Dagster.** No ad-hoc ingestion scripts. All runs must appear in the Dagster UI.
- **Register new assets** by importing them into `definitions.py` and adding to `Definitions(assets=[...])`.
- **DuckLake catalog is attached `AS lake`.** Write tables as `lake.<schema>.<table>`.

## Adding a new Socrata asset

1. Add dataset ID + schema to `src/dagster_pipeline/sources/datasets.py`.
2. Run the generator: the `socrata_direct_assets.py` picks it up automatically.
3. Materialize once locally via Docker to verify:
   ```bash
   docker compose exec dagster-code dagster asset materialize \
     --select 'schema/table_name' -m dagster_pipeline.definitions
   ```

## Run rules

- **Dagster always runs in Docker** — never `dagster dev` locally. See `@docs/runbooks/deploy-dagster.md`.
- **Materialize from inside the container:**
  ```bash
  docker compose exec dagster-code dagster asset materialize \
    --select 'housing/hpd_violations' -m dagster_pipeline.definitions
  ```

## See also

- `@docs/adr/0001-dagster-in-docker.md`
- `@docs/adr/0003-dlt-removal.md`
