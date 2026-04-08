# Tool parameter Literal audit — April 2026

**TL;DR:** The duckdb-server is already at the Pamela Fox / Mastra strict-schema gold standard. An April 2026 audit found **exactly one** enum-like parameter typed as plain `str`: `query_sql.format` in `tools/catalog_tools.py`. Fixed in commit `3759aed` on branch main.

**Why:** Don't re-audit unless a new tool is added. When adding a new tool, any parameter that accepts a fixed set of string values MUST use `Literal[...]` — see the gold-standard list below.

**How to apply:** When reviewing a new tool file, check any parameter named `view`, `mode`, `role`, `type`, `domain`, `format`, `kind`, `category`, `level`, `scope`. If it accepts a fixed set of strings, type it as:

```python
param: Annotated[
    Literal["a", "b", "c"],
    Field(description="..."),
] = "a"
```

FastMCP 3.x emits this as a JSON schema `enum`, which clients validate against before calling the tool. Verified by the schema test at `infra/duckdb-server/tests/test_catalog_tools_schema.py`.

**Gold-standard examples already in the codebase:**
- `building.view` — 7-value Literal
- `network.type` — 11-value Literal (the biggest)
- `query.mode` — 8-value Literal
- `entity.role` — 6-value Literal

**Audit source:** `docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md`
