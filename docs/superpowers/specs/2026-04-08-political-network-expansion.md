# Political Network Expansion — USAspending + LittleSis

**Date:** 2026-04-08
**Status:** Implemented

## Why
A graphify audit (2026-04-08) identified `lake.federal.littlesis_*` and
`lake.federal.usaspending_*` as ingested but never queried by any MCP tool.
Both belong in `network(type="political")` because they describe the same
phenomenon — money + influence flowing into and out of named entities.

## What was added
Two SQL constants in `infra/duckdb-server/tools/network.py`:

- `MONEY_USASPENDING_SQL` — UNION of `usaspending_contracts` and
  `usaspending_grants`, grouped by `(recipient_name, awarding_agency)`,
  ordered by `total_amount DESC`.
- `MONEY_LITTLESIS_SQL` — joins `littlesis_entities` to
  `littlesis_relationships` on both sides (entity1 and entity2), filtered
  to `category IN ('donation','lobbying','transaction','ownership','position')`.

Both are wired into `_political_core()` with the same `safe_query` +
render-block pattern as the five existing sources. Each contributes one
text section ("FEDERAL AWARDS — USASPENDING" and "POWER MAP — LITTLESIS")
and one entry in `data["money_trail"]` (`federal_awards`,
`power_map_relationships`).

## Why these category filters for LittleSis
The full LittleSis taxonomy has 12 categories
(`src/dagster_pipeline/sources/littlesis.py:8-21`). For the political tool we
keep only the five that describe money or formal power: donation, lobbying,
transaction, ownership, position. Family/social/professional/membership
belong in `network(type="all")` or a future `network(type="influence")`.

## Why parameter passed twice
DuckDB doesn't reuse `?` placeholders across UNION branches or across a CTE
+ subquery. Both constants take the search term twice. Call sites:

    safe_query(pool, MONEY_USASPENDING_SQL, [name, name])
    safe_query(pool, MONEY_LITTLESIS_SQL,    [name, name])

## Not in scope (deferred)
- DuckPGQ traversal across LittleSis edges — the `_pay_to_play_core` graph
  is FEC/NYS only today.
- NYS BOE: already wired via `lake.federal.nys_campaign_finance` —
  no change needed (the original audit was wrong about this).
- Subagency drilldown for federal awards — kept aggregate to stay under
  the 8-row LLM render budget.
