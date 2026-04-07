# DuckLake catalog convention (post 2026-04-07)

DuckLake is **attached** with `AS lake` — this is the query-time alias.

Postgres metadata lives in the **default `public` schema**. The historic `METADATA_SCHEMA='lake'` was retired 2026-04-07 (see `docs/adr/0002-ducklake-catalog-convention.md`).

## Rules

- Reference tables as `lake.<schema>.<table>` — always three parts.
- Do **not** create a `lake` schema in postgres. Everything goes in `public`.
- If you see `METADATA_SCHEMA='lake'` in code, it's legacy and should be removed.

## Why agents get confused

"lake" has two historical meanings: the ATTACH alias (still current) and a postgres schema (retired). The ATTACH alias is the only one that matters now.
