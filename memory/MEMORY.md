# Common Ground Pipeline — Project Memory

This is the **repo-scoped** auto-memory, committed to git. Contrast with `~/.claude/projects/*/memory/` which is private/user-scoped.

Each entry is a pointer to a file with the full detail. Keep this index under 200 lines.

## Gotchas
- [Embedder OOM during startup](embedder_oom.md) — force-CPU env var required
- [DuckLake catalog convention](ducklake_catalog_convention.md) — `AS lake`, postgres `public` schema
- [hpd_violations parquet orphans](hpd_violations_parquet_orphans.md) — filtered queries fail, COUNT works
- [Symlink deploy flow](symlink_deploy_flow.md) — `/opt/common-ground/duckdb-server` → git tree
- [MCP CursorPool contract](mcp_cursor_pool.md) — `execute(pool, sql, params) → (cols, rows)`, always `asyncio.to_thread`

## Tooling
- [Tool parameter Literals](tool_parameter_literals.md) — April 2026 audit: codebase already at gold standard, one parameter fixed
- [Eval harness](eval_harness.md) — how to run the MCP tool-use eval harness before deploys and after PostHog regressions
- [Political network sources](network_political_sources.md) — every table `network(type="political")` reads from
