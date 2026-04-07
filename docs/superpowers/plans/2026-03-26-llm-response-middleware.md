# LLM Response Formatting Middleware — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a FastMCP middleware layer that transforms all DuckDB tool responses into LLM-optimized formats — ordered, relevance-filtered, with explicit truncation — using markdown tables as default and TOON format as an option for large result sets.

**Architecture:** A single `OutputFormatterMiddleware` sits in the FastMCP middleware chain (before `PostHogMiddleware`). It intercepts `ToolResult` objects from `on_call_tool`, detects tabular data, and reformats text content for optimal LLM comprehension. A separate `formatters.py` module holds the formatting functions (markdown-table, TOON, markdown-kv). The existing `format_text_table()` and `make_result()` helpers stay — the middleware operates on their output, not replacing them. Each task includes an **Exa research step** to pull the latest docs/examples before coding.

**Tech Stack:** FastMCP 3.1.1 middleware API, DuckDB, Python 3.12 (TOON emitted manually, no external dependency)

---

## Research Mandate

**Every task starts with targeted Exa web search** to pull the latest documentation, examples, and edge cases before writing any code. The implementer MUST use `mcp__exa__web_search_exa` (or equivalent web search) at the start of each task and cite what they found. This prevents stale assumptions — the FastMCP API, TOON spec, and best practices all evolved rapidly in early 2026.

---

## What Changes

| Before | After |
|--------|-------|
| `format_text_table()` outputs markdown tables inline | Middleware reformats ALL tool responses consistently |
| No truncation notice when rows are cut | Explicit `(showing 20 of 12,456 rows — use sql_query with LIMIT/OFFSET for more)` |
| No column relevance filtering | Low-signal columns (internal IDs, nulls) auto-hidden, originals in `structured_content` |
| Fixed markdown-table format only | Markdown-table default, TOON for large tabular results (>50 rows), markdown-kv for single-record |
| Cell values truncated at 40 chars silently | Truncation indicated with `…` suffix |
| No response size guard | `ResponseLimitingMiddleware` caps total response at 50K chars |

---

## File Structure

### New files

| File | Responsibility |
|------|----------------|
| `infra/duckdb-server/formatters.py` | Pure formatting functions: `format_markdown_table()`, `format_toon()`, `format_markdown_kv()`, `detect_format()`, `filter_columns()` |
| `infra/duckdb-server/response_middleware.py` | `OutputFormatterMiddleware(Middleware)` — intercepts `on_call_tool`, delegates to formatters |
| `infra/duckdb-server/tests/test_formatters.py` | Unit tests for all formatting functions |
| `infra/duckdb-server/tests/test_middleware.py` | Integration tests for middleware response transformation |

### Modified files

| File | Change |
|------|--------|
| `infra/duckdb-server/mcp_server.py` | Import and register `OutputFormatterMiddleware` + `ResponseLimitingMiddleware` (3 lines) |
| `infra/duckdb-server/Dockerfile` | Add `toon-parse` to pip install |

---

## Task 1: Research and implement formatting functions

**Files:**
- Create: `infra/duckdb-server/formatters.py`
- Create: `infra/duckdb-server/tests/test_formatters.py`

- [ ] **Step 1: Research — latest TOON Python library API and markdown-table benchmarks**

Search with Exa:
1. `"toon-parse" python pip 2026` — find the current API, installation, any breaking changes
2. `"toon-format" tabular array python encode` — how to encode list-of-dicts as TOON tabular
3. `"markdown table" LLM accuracy tokens 2026 benchmark` — confirm markdown-table is still the best accuracy-per-token format
4. `fastmcp ToolResult content structured_content 2026` — understand the ToolResult structure your formatter must produce

Document findings as a comment block at the top of `formatters.py`. This serves as a living reference for why format choices were made.

- [ ] **Step 2: Write failing tests for `format_markdown_table()`**

```python
# tests/test_formatters.py
import pytest
from formatters import format_markdown_table, format_toon, format_markdown_kv, detect_format, filter_columns

class TestMarkdownTable:
    def test_basic_table(self):
        cols = ["name", "age", "city"]
        rows = [("Alice", 30, "NYC"), ("Bob", 25, "LA")]
        result = format_markdown_table(cols, rows)
        assert "| name | age | city |" in result
        assert "| Alice | 30 | NYC |" in result

    def test_empty_rows(self):
        assert format_markdown_table(["a"], []) == "(no rows)"

    def test_empty_cols(self):
        assert format_markdown_table([], []) == "(no columns)"

    def test_truncation_notice(self):
        rows = [(i,) for i in range(100)]
        result = format_markdown_table(["id"], rows, max_rows=20)
        assert "20 of 100 rows" in result
        assert "sql_query" in result.lower() or "LIMIT" in result

    def test_cell_truncation_with_ellipsis(self):
        rows = [("x" * 60,)]
        result = format_markdown_table(["val"], rows, max_cell=40)
        assert "…" in result
        assert len(result.split("\n")[2].split("|")[1].strip()) <= 41

    def test_pipe_escaped(self):
        rows = [("a|b",)]
        result = format_markdown_table(["val"], rows)
        assert "a\\|b" in result

    def test_none_as_empty(self):
        rows = [(None,)]
        result = format_markdown_table(["val"], rows)
        lines = result.strip().split("\n")
        assert "|  |" in lines[2] or "| |" in lines[2]
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -m pytest tests/test_formatters.py -v
```

Expected: ImportError (formatters module doesn't exist yet)

- [ ] **Step 4: Implement `format_markdown_table()`**

```python
# formatters.py
"""
Response formatters for LLM-optimized output.

Format selection based on 2026 benchmarks (ImprovingAgents, Mar 2026):
- Markdown table: best accuracy-per-token ratio for tabular data
- Markdown-KV: highest accuracy for single-record lookups
- TOON: lowest token count for large result sets (>50 rows)

References: [populated from Step 1 Exa research]
"""


def format_markdown_table(
    cols: list[str],
    rows: list[tuple],
    max_rows: int = 20,
    max_cell: int = 40,
    total_count: int | None = None,
) -> str:
    """Format as markdown table with truncation notice.

    Args:
        total_count: Override for total row count in truncation notice
                     (from meta, may differ from len(rows) which is capped).
    """
    if not cols:
        return "(no columns)"
    if not rows:
        return "(no rows)"

    display = rows[:max_rows]
    total = total_count if total_count is not None else len(rows)

    def cell(v):
        if v is None:
            return ""
        s = str(v).replace("|", "\\|")
        if len(s) > max_cell:
            return s[: max_cell - 1] + "…"
        return s

    lines = [
        "| " + " | ".join(str(c) for c in cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    lines.extend("| " + " | ".join(cell(v) for v in row) + " |" for row in display)

    if total > max_rows:
        lines.append(
            f"\n(showing {max_rows} of {total:,} rows"
            f" — use `sql_query()` with LIMIT/OFFSET for more)"
        )
    return "\n".join(lines)
```

- [ ] **Step 5: Run tests — verify markdown table tests pass**

```bash
python -m pytest tests/test_formatters.py::TestMarkdownTable -v
```

Expected: All PASS

- [ ] **Step 6: Write failing tests for `format_toon()`**

```python
class TestToon:
    def test_basic_toon(self):
        cols = ["id", "name", "role"]
        rows = [(1, "Alice", "admin"), (2, "Bob", "user")]
        result = format_toon(cols, rows)
        # TOON tabular header declares length and fields
        assert "{id,name,role}" in result or "id,name,role" in result
        assert "Alice" in result
        assert "Bob" in result

    def test_toon_token_savings(self):
        """TOON should use fewer characters than markdown for same data."""
        cols = ["id", "name", "email", "role"]
        rows = [(i, f"User{i}", f"user{i}@example.com", "member") for i in range(50)]
        md = format_markdown_table(cols, rows, max_rows=50)
        toon = format_toon(cols, rows, max_rows=50)
        assert len(toon) < len(md)

    def test_toon_truncation(self):
        rows = [(i,) for i in range(100)]
        result = format_toon(["id"], rows, max_rows=20)
        assert "20 of 100" in result

    def test_toon_empty(self):
        assert format_toon(["a"], []) == "(no rows)"
```

- [ ] **Step 7: Research — TOON Python encode API**

Search with Exa:
1. `"toon-parse" ToonConverter from_records python` — exact API for encoding list-of-dicts
2. `"toon_format" encode tabular python pip` — alternative library API
3. `site:github.com toon-format toon-python encode example` — working code examples

If `toon-parse` API is unclear, fall back to manual TOON generation (the tabular format is simple enough to emit directly):

```
results[N]{col1,col2,col3}:
  val1,val2,val3
  val4,val5,val6
```

- [ ] **Step 8: Implement `format_toon()`**

```python
def format_toon(
    cols: list[str],
    rows: list[tuple],
    max_rows: int = 500,
    max_cell: int = 60,
    total_count: int | None = None,
) -> str:
    """Format as TOON tabular notation — ~40% fewer tokens than JSON."""
    if not cols:
        return "(no columns)"
    if not rows:
        return "(no rows)"

    display = rows[:max_rows]
    total = total_count if total_count is not None else len(rows)

    def cell(v):
        if v is None:
            return ""
        s = str(v)
        if len(s) > max_cell:
            return s[: max_cell - 1] + "…"
        # Escape commas in values
        return s.replace(",", "\\,") if "," in s else s

    header = f"results[{len(display)}]" + "{" + ",".join(cols) + "}:"
    lines = [header]
    lines.extend("  " + ",".join(cell(v) for v in row) for row in display)

    if total > max_rows:
        lines.append(
            f"\n(showing {max_rows} of {total:,} rows"
            f" — use `sql_query()` with LIMIT/OFFSET for more)"
        )
    return "\n".join(lines)
```

**Note:** This emits TOON manually rather than importing `toon-parse`. The tabular format is 5 lines of logic — no need for a dependency. If Step 7 research reveals `toon-parse` adds value (validation, nested support), swap in. Otherwise keep it zero-dependency.

- [ ] **Step 9: Run tests — verify TOON tests pass**

```bash
python -m pytest tests/test_formatters.py::TestToon -v
```

- [ ] **Step 10: Write failing tests for `format_markdown_kv()` and `detect_format()`**

```python
class TestMarkdownKV:
    def test_single_record(self):
        cols = ["name", "age", "city"]
        rows = [("Alice", 30, "NYC")]
        result = format_markdown_kv(cols, rows)
        assert "**name:** Alice" in result
        assert "**age:** 30" in result

    def test_multiple_records_fallback(self):
        """KV format only for 1-3 records; more should raise or return None."""
        cols = ["id"]
        rows = [(i,) for i in range(10)]
        result = format_markdown_kv(cols, rows)
        assert result is None  # signals caller to use table format


class TestDetectFormat:
    def test_single_row(self):
        assert detect_format(1, 5) == "kv"

    def test_small_table(self):
        assert detect_format(10, 5) == "markdown"

    def test_large_table(self):
        assert detect_format(100, 5) == "toon"

    def test_many_columns_stays_markdown(self):
        """Even large row counts stay markdown if few columns."""
        assert detect_format(80, 2) == "markdown"
```

- [ ] **Step 11: Implement `format_markdown_kv()`, `detect_format()`, and `filter_columns()`**

```python
def format_markdown_kv(
    cols: list[str],
    rows: list[tuple],
) -> str | None:
    """Key-value format for 1-3 records. Returns None if too many rows."""
    if not rows or len(rows) > 3:
        return None

    blocks = []
    for row in rows:
        lines = [f"**{col}:** {val}" for col, val in zip(cols, row) if val is not None]
        blocks.append("\n".join(lines))
    return "\n\n---\n\n".join(blocks)


def detect_format(row_count: int, col_count: int) -> str:
    """Choose optimal format based on result shape.

    - 1-3 rows: markdown-kv (highest accuracy)
    - 4-50 rows or <=3 columns: markdown-table (best accuracy/token)
    - 51+ rows with 4+ columns: toon (lowest token cost)
    """
    if row_count <= 3:
        return "kv"
    if row_count <= 50 or col_count <= 3:
        return "markdown"
    return "toon"


def filter_columns(
    cols: list[str],
    rows: list[tuple],
    max_cols: int = 12,
) -> tuple[list[str], list[tuple]]:
    """Drop low-signal columns to reduce token waste.

    Removes columns that are:
    - >90% NULL across all rows
    - Internal IDs matching patterns like *_id, *_key (unless they're the only identifier)
    - Exact duplicates of another column's values

    Keeps original data in structured_content (middleware handles this).
    """
    if len(cols) <= max_cols:
        return cols, rows

    # Score each column
    n = len(rows) if rows else 1
    scores = []
    for i, col in enumerate(cols):
        null_pct = sum(1 for row in rows if row[i] is None) / n
        is_internal = col.lower().endswith(("_id", "_key", "_pk", "_fk")) and i > 0
        score = (1.0 - null_pct) - (0.3 if is_internal else 0)
        scores.append((i, col, score))

    # Keep top max_cols by score, preserving original order
    kept = sorted(scores, key=lambda x: -x[2])[:max_cols]
    kept_indices = sorted(i for i, _, _ in kept)

    new_cols = [cols[i] for i in kept_indices]
    new_rows = [tuple(row[i] for i in kept_indices) for row in rows]
    return new_cols, new_rows
```

- [ ] **Step 12: Run all formatter tests**

```bash
python -m pytest tests/test_formatters.py -v
```

Expected: All PASS

- [ ] **Step 13: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/formatters.py infra/duckdb-server/tests/test_formatters.py
git commit -m "feat: add LLM response formatters — markdown-table, TOON, markdown-kv

Markdown table (default) for best accuracy/token ratio.
TOON tabular notation for large results (40% fewer tokens).
Markdown-KV for single-record lookups (highest accuracy).
Auto-detection based on result shape. Column relevance filtering."
```

---

## Task 2: Research and implement the OutputFormatterMiddleware

**Files:**
- Create: `infra/duckdb-server/response_middleware.py`
- Create: `infra/duckdb-server/tests/test_middleware.py`

- [ ] **Step 1: Research — FastMCP middleware on_call_tool and ToolResult API**

Search with Exa:
1. `fastmcp "on_call_tool" middleware ToolResult transform 2026` — how to intercept and modify ToolResult in middleware
2. `fastmcp ToolResult content structured_content meta` — understand the ToolResult object shape
3. `fastmcp ResponseLimitingMiddleware max_size 2026` — how the built-in size limiter works
4. `site:github.com fastmcp middleware on_call_tool example` — real-world middleware examples

Key questions to answer:
- Is `result.content` a string or list of content blocks?
- Can you replace `result.content` directly or do you need to construct a new ToolResult?
- Does `ResponseLimitingMiddleware` play well with custom middleware?

- [ ] **Step 2: Write failing tests for the middleware**

```python
# tests/test_middleware.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from response_middleware import OutputFormatterMiddleware


class TestOutputFormatterMiddleware:
    @pytest.fixture
    def middleware(self):
        return OutputFormatterMiddleware()

    @pytest.mark.asyncio
    async def test_reformats_tabular_result(self, middleware):
        """Middleware should detect tabular content and reformat."""
        # Mock a ToolResult with structured_content containing rows
        mock_result = MagicMock()
        mock_result.content = "Query returned 5 rows.\n\n| id | name |\n| --- | --- |\n| 1 | Alice |"
        mock_result.structured_content = {
            "rows": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        }
        mock_result.meta = {"total_rows": 2}

        context = MagicMock()
        context.message.name = "sql_query"
        call_next = AsyncMock(return_value=mock_result)

        result = await middleware.on_call_tool(context, call_next)
        # Should still contain the data
        assert "Alice" in str(result.content)

    @pytest.mark.asyncio
    async def test_passthrough_non_tabular(self, middleware):
        """Non-tabular results (strings) pass through unchanged."""
        mock_result = MagicMock()
        mock_result.content = "No results found."
        mock_result.structured_content = None
        mock_result.meta = {}

        context = MagicMock()
        context.message.name = "building_profile"
        call_next = AsyncMock(return_value=mock_result)

        result = await middleware.on_call_tool(context, call_next)
        assert result.content == "No results found."

    @pytest.mark.asyncio
    async def test_kv_format_for_single_row(self, middleware):
        """Single-row results should use markdown-kv format."""
        mock_result = MagicMock()
        mock_result.content = "Found 1 building.\n\n| name | age |\n| --- | --- |\n| Alice | 30 |"
        mock_result.structured_content = {"rows": [{"name": "Alice", "age": 30}]}
        mock_result.meta = {"total_rows": 1}

        context = MagicMock()
        context.message.name = "building_profile"
        call_next = AsyncMock(return_value=mock_result)

        result = await middleware.on_call_tool(context, call_next)
        assert "**name:**" in str(result.content)

    @pytest.mark.asyncio
    async def test_error_passthrough(self, middleware):
        """Errors from tools should pass through without formatting."""
        context = MagicMock()
        context.message.name = "sql_query"
        call_next = AsyncMock(side_effect=Exception("DB error"))

        with pytest.raises(Exception, match="DB error"):
            await middleware.on_call_tool(context, call_next)
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
python -m pytest tests/test_middleware.py -v
```

Expected: ImportError

- [ ] **Step 4: Implement `OutputFormatterMiddleware`**

```python
# response_middleware.py
"""
FastMCP middleware that reformats tool responses for optimal LLM consumption.

Sits in the middleware chain before PostHogMiddleware. Intercepts ToolResult
objects, detects tabular data in structured_content, and reformats the text
content using the best format for the result shape:
- 1-3 rows: markdown-kv (highest accuracy per ImprovingAgents benchmarks)
- 4-50 rows: markdown-table (best accuracy/token ratio)
- 51+ rows: TOON tabular notation (~40% fewer tokens)

The structured_content dict is preserved untouched — only the text content
(what the LLM reads) is reformatted.
"""

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

from formatters import (
    detect_format,
    filter_columns,
    format_markdown_kv,
    format_markdown_table,
    format_toon,
)

# Tools whose output should NOT be reformatted (already prose or special format)
_SKIP_TOOLS = frozenset({
    "list_schemas",
    "search_tools",
    "data_catalog",
    "sql_admin",
})


class OutputFormatterMiddleware(Middleware):
    """Reformat tabular tool responses for LLM comprehension."""

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        result = await call_next(context)

        tool_name = context.message.name

        # Skip tools that don't return tabular data
        if tool_name in _SKIP_TOOLS:
            return result

        # Only reformat if we have structured tabular data
        sc = getattr(result, "structured_content", None)
        if not sc or not isinstance(sc, dict) or "rows" not in sc:
            return result

        rows_data = sc["rows"]
        if not rows_data or not isinstance(rows_data, list):
            return result

        # Extract columns and rows from structured_content
        cols = list(rows_data[0].keys()) if rows_data else []
        rows = [tuple(r.get(c) for c in cols) for r in rows_data]
        total_rows = (getattr(result, "meta", {}) or {}).get("total_rows", len(rows))

        # Filter low-signal columns (only for wide results)
        if len(cols) > 12:
            display_cols, display_rows = filter_columns(cols, rows)
        else:
            display_cols, display_rows = cols, rows

        # Pick format based on result shape
        fmt = detect_format(total_rows, len(display_cols))

        # Extract the summary line (text before the table)
        original_text = str(result.content) if result.content else ""
        summary = original_text.split("\n\n")[0] if "\n\n" in original_text else original_text
        # Strip any old table from summary
        if "| " in summary:
            summary = summary.split("| ")[0].strip()

        # Format
        if fmt == "kv":
            formatted = format_markdown_kv(display_cols, display_rows)
            if formatted is None:
                formatted = format_markdown_table(display_cols, display_rows, total_count=total_rows)
        elif fmt == "toon":
            formatted = format_toon(display_cols, display_rows, total_count=total_rows)
        else:
            formatted = format_markdown_table(display_cols, display_rows, total_count=total_rows)

        # Reassemble: summary + formatted data
        new_content = summary + "\n\n" + formatted if summary else formatted

        # Construct new ToolResult — ToolResult.content is list[ContentBlock],
        # not a plain string. The constructor handles string→ContentBlock conversion.
        return ToolResult(
            content=new_content,
            structured_content=result.structured_content,
            meta=getattr(result, "meta", None),
        )
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
python -m pytest tests/test_middleware.py -v
```

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/response_middleware.py infra/duckdb-server/tests/test_middleware.py
git commit -m "feat: add OutputFormatterMiddleware for LLM-optimized responses

Auto-detects result shape and picks markdown-kv, markdown-table, or TOON.
Filters low-signal columns for wide results. Preserves structured_content."
```

---

## Task 3: Integrate middleware into the MCP server

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (3 lines)
- Modify: `infra/duckdb-server/Dockerfile` (1 line, only if using toon-parse)

- [ ] **Step 1: Research — FastMCP middleware ordering and ResponseLimitingMiddleware**

Search with Exa:
1. `fastmcp add_middleware ordering "first added" 2026` — confirm middleware execution order
2. `fastmcp ResponseLimitingMiddleware import max_size` — exact import path and constructor args
3. `fastmcp middleware chain on_call_tool order` — verify that first-added = outermost (runs first on request, last on response)

Key: `OutputFormatterMiddleware` must run AFTER the tool returns (to reformat output) but BEFORE `PostHogMiddleware` captures analytics (so PostHog sees raw tool output). Middleware is first-added = outermost. Add OutputFormatter BEFORE PostHog so it wraps PostHog — on the response path, OutputFormatter runs last (after PostHog), receiving the raw result to reformat.

- [ ] **Step 2: Add imports and register middleware in `mcp_server.py`**

After line 12 (`from fastmcp.server.middleware import Middleware, MiddlewareContext`), add:

```python
from response_middleware import OutputFormatterMiddleware
from fastmcp.server.middleware.response_limiting import ResponseLimitingMiddleware
```

Before line 9586 (`mcp.add_middleware(PostHogMiddleware())`), add:

```python
mcp.add_middleware(ResponseLimitingMiddleware(max_size=50_000))
mcp.add_middleware(OutputFormatterMiddleware())
```

Final middleware order (first-added = outermost):
1. `ResponseLimitingMiddleware` — outermost safety net, truncates oversized responses
2. `OutputFormatterMiddleware` — reformats tabular output
3. `PostHogMiddleware` — analytics (innermost, sees raw tool output)

- [ ] **Step 3: Update Dockerfile to copy new Python files**

The existing Dockerfile only copies `mcp_server.py`. Add the new files:

```dockerfile
# Change line 17 from:
COPY mcp_server.py .
# To:
COPY *.py .
```

This copies `mcp_server.py`, `formatters.py`, and `response_middleware.py` into the container. No new pip dependencies needed (TOON is emitted manually).

- [ ] **Step 4: Add `__init__.py` for tests directory**

```bash
touch ~/Desktop/dagster-pipeline/infra/duckdb-server/tests/__init__.py
```

- [ ] **Step 5: Run all tests locally**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
pip install fastmcp duckdb posthog pytest pytest-asyncio 2>/dev/null
python -m pytest tests/ -v
```

Expected: All PASS

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py infra/duckdb-server/tests/__init__.py
git commit -m "feat: wire OutputFormatterMiddleware + ResponseLimitingMiddleware

Middleware chain: ResponseLimiting (50K cap) → OutputFormatter → PostHog.
All tool responses now auto-formatted for optimal LLM comprehension."
```

---

## Task 4: Deploy and validate on production

**Files:**
- Modify: `infra/duckdb-server/` (deploy via rsync)

- [ ] **Step 1: Research — FastMCP middleware debugging and common pitfalls**

Search with Exa:
1. `fastmcp middleware "on_call_tool" error "not called"` — common middleware registration bugs
2. `fastmcp middleware ToolResult "content" type string list` — verify content type handling
3. `"ResponseLimitingMiddleware" fastmcp truncation behavior` — what happens when it truncates

- [ ] **Step 2: Test middleware locally with a mock DuckDB**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
from formatters import format_markdown_table, format_toon, detect_format

# Simulate a 100-row query result
cols = ['bbl', 'address', 'violations', 'open_violations']
rows = [(f'{1000000000+i}', f'{i} Main St', i*3, i) for i in range(100)]

fmt = detect_format(100, 4)
print(f'Format chosen: {fmt}')
print()

if fmt == 'toon':
    print(format_toon(cols, rows, max_rows=10))
else:
    print(format_markdown_table(cols, rows, max_rows=10))
"
```

Expected: `Format chosen: toon` with TOON tabular output showing 10 of 100 rows.

- [ ] **Step 3: Deploy to Hetzner**

```bash
cd ~/Desktop/dagster-pipeline/infra
bash deploy.sh
```

Or manually:

```bash
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
    --exclude '__pycache__' --exclude '.git' --exclude 'tests' \
    ~/Desktop/dagster-pipeline/infra/duckdb-server/ \
    fattie@178.156.228.119:/opt/common-ground/duckdb-server/

ssh hetzner "cd /opt/common-ground && sudo docker compose up -d --build duckdb-server"
```

- [ ] **Step 4: Verify via MCP tool calls**

Test 3 scenarios — single record (should be KV), small table (markdown), large result (TOON):

```bash
# Single record — expect markdown-kv
curl -s https://mcp.common-ground.nyc/mcp -H "Content-Type: application/json" -d '{
  "jsonrpc": "2.0", "id": 1, "method": "tools/call",
  "params": {"name": "building_profile", "arguments": {"bbl": "1000670001"}}
}' | python -m json.tool | head -30

# Small table — expect markdown table
# (use list_tables which returns ~20 rows)

# Large result — expect TOON
# (use sql_query with a query returning 100+ rows)
```

- [ ] **Step 5: Check container health and logs**

```bash
ssh hetzner "docker logs common-ground-duckdb-server-1 --tail 20 2>&1"
ssh hetzner "docker ps --filter name=duckdb-server --format '{{.Status}}'"
```

Expected: Container running, no errors in logs.

- [ ] **Step 6: Commit deployment state**

```bash
cd ~/Desktop/dagster-pipeline
git add -A infra/duckdb-server/
git commit -m "deploy: LLM response middleware live on production

Verified: markdown-kv for single records, markdown-table for small results,
TOON for large result sets. ResponseLimitingMiddleware capping at 50K chars."
```

---

## Execution Order & Dependencies

```
Task 1 (formatters.py + tests)      ← Pure functions, no dependencies
  ↓
Task 2 (middleware + tests)          ← Imports from formatters.py
  ↓
Task 3 (wire into mcp_server.py)    ← Imports from response_middleware.py
  ↓
Task 4 (deploy + validate)          ← Needs all code committed
```

Strictly sequential — each task builds on the previous.

---

## Verification Checklist

After all tasks complete:

- [ ] `python -m pytest infra/duckdb-server/tests/ -v` — all tests pass
- [ ] Single-record tool responses use markdown-kv format (`**field:** value`)
- [ ] Small table responses (4-50 rows) use markdown tables with truncation notices
- [ ] Large results (51+ rows) use TOON tabular format
- [ ] Wide results (>12 columns) have low-signal columns filtered
- [ ] `structured_content` dict is unchanged (full data for programmatic consumers)
- [ ] `ResponseLimitingMiddleware` truncates responses over 50K chars
- [ ] PostHog analytics still captures tool calls correctly
- [ ] No import errors in production container logs
