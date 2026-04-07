# 0007 — sqlglot AST validator for `/mosaic/query`

**Status:** Accepted (2026-04-07)

## Context

The Mosaic data server at `/mosaic/query` accepts arbitrary SQL from the browser and returns JSON. Accepting raw SQL is a SQL injection and data-exfiltration footgun. Options:

- **Regex/string filters** — trivially bypassed
- **Parse + walk + allowlist** — robust but requires a real SQL parser
- **DuckDB's own parser** — no stable AST walker API

## Decision

Use **`sqlglot`** to parse with `dialect="duckdb"`, walk the AST, and enforce:

- Only `SELECT` and `UNION` statements allowed
- All referenced tables must be in the schema allowlist
- Table names starting with `_` are rejected (internal tables)
- Column names in the response come from the real `execute()` result, not client input

## Consequences

- **+** Provable rejection of non-SELECT / non-allowlist queries
- **+** AST walk catches nested subqueries, CTEs, unions
- **+** sqlglot is battle-tested and maintained
- **−** Dialect parity is not perfect — some DuckDB-specific syntax may parse oddly
- **−** Adds sqlglot to the server dependency tree
