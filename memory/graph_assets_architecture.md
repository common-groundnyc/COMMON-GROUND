# Graph assets — where graph_* tables come from

Graph tables used by network() tools are being migrated from in-process
MCP-server startup builds to Dagster assets in lake.graphs.*.

## Current state (2026-04-09, Phase 1 complete)

| Family | Built by | Persisted in |
|---|---|---|
| Political (pol_entities/donations/contracts/lobbying) | Dagster graph_political | lake.graphs.* |
| All others (ownership, corporate, etc.) | mcp_server.py in-process | /data/common-ground/graph-cache/*.parquet |

## Key finding: flush_ducklake_sensor causes file race
flush_ducklake_sensor (disabled 2026-04-09) deletes freshly-written parquet
files between Dagster runs. Must stay disabled until DuckLake 1.0 ships or
an alternative flush mechanism is implemented.

## How to rebuild political graphs
```
dagster asset materialize --select "group:graphs" -m dagster_pipeline.definitions
```
Schedule: graphs_daily fires at 07:00 UTC daily.
