# 0006 — FastMCP serves REST and Mosaic alongside MCP protocol

**Status:** Accepted (2026-03-26)

## Context

The explore dashboard needed REST endpoints (`/api/neighborhood`, `/api/zips/search`, `/api/buildings/worst`) and a Mosaic data server (`/mosaic/query`). Options:

- **Separate FastAPI service** — duplicate DuckDB connection pools, separate deploy, separate container
- **Sidecar process** — same host, separate port, coordination overhead
- **Extend FastMCP with `@mcp.custom_route`** — reuse the existing FastMCP 3.x process and its DuckDB pool

## Decision

Extend the existing FastMCP server. Register REST handlers via `@mcp.custom_route` in `routes/*.py`, all sharing the same `CursorPool`.

## Consequences

- **+** One process, one DuckDB pool, one deploy
- **+** REST endpoints reuse middleware (citation, freshness, percentile)
- **+** Mosaic data server lives next to the MCP tools it mirrors
- **−** Couples REST availability to MCP server availability
- **−** FastMCP's `@mcp.custom_route` is newer and less documented than FastAPI
