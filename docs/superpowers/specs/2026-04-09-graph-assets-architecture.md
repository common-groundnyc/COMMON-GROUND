# Graph assets — migration from in-process build to Dagster

**Date:** 2026-04-09
**Status:** Phase 1 (political) implemented. Phases 2-4 deferred.

## Problem
The MCP server built ~30 graph_* tables at startup from raw lake tables. Each
build block was wrapped in try/except that replaced failures with dummy stubs.
Five of seven property graphs were empty due to column-name mismatches. The
flush_ducklake_sensor was additionally deleting freshly-written parquet files
between upstream writes and downstream reads — causing a race condition that
made cross-asset data flow unreliable.

## Root cause (flush sensor)
flush_ducklake_sensor called ducklake_flush_inlined_data on every table after
every successful run. This was designed for DuckLake 0.4's inlining issue but
had a side effect: parquet files written by one Dagster run were deleted before
the next run could read them. Disabling the sensor resolved all IO errors.

## Target architecture
- New lake.graphs DuckLake schema
- Each graph family is a @dg.multi_asset in graph_assets.py
- Sub-tables materialized independently (one bad SQL doesn't poison siblings)
- Daily graphs_daily_job schedule at 07:00
- MCP server startup reads from lake.graphs.* instead of building

## Phase 1 (this change): Political family
pol_entities (748k), pol_donations (1.6M), pol_contracts (1.5k), pol_lobbying (25.8k)

Schema mismatches fixed:
- contract_awards: vendorname→vendor_name, agencyname→agency_name, currentamount→contract_amount
- nys_lobbyist_registration: lobbyist_name→principal_lobbyist_name, client_name→contractual_client_name

## Phases 2-4 (deferred)
| Phase | Family |
|---|---|
| 2 | graph_ownership (195k owners, 377k buildings) |
| 3 | graph_corporate, graph_contractor, graph_officer, graph_bic, graph_coib, graph_transactions |
| 4 | Delete in-process builder and parquet cache entirely |
