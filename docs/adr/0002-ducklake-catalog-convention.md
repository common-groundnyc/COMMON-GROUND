# 0002 — DuckLake attached `AS lake`, metadata in postgres `public`

**Status:** Accepted (2026-04-07, supersedes 2026-03 convention)

## Context

DuckLake was originally attached with `METADATA_SCHEMA='lake'`, creating a dedicated `lake` schema in postgres for catalog metadata. This caused confusion: "lake" referred to both the ATTACH alias and the postgres schema. Commit `0dd2e09` claimed convergence was complete, but the actual code still had the dual meaning.

## Decision

- DuckLake is attached with **`AS lake`** (the alias stays).
- Postgres metadata lives in the **default `public` schema** — `METADATA_SCHEMA='lake'` is retired.
- Tables are still referenced as **`lake.<schema>.<table>`** because that's the ATTACH alias — this is a query-time convention, not a postgres schema.

## Consequences

- **+** Single meaning for "lake": the ATTACH alias
- **+** Simpler postgres: one schema, not two
- **−** Historical snapshots with `METADATA_SCHEMA='lake'` are incompatible and were retired
- **−** Agents must understand the two meanings of "lake" before 2026-04-07
