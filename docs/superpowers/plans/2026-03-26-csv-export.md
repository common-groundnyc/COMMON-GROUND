# Branded CSV Export — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `export_csv` tool that generates branded CSV files with Common Ground branding, row IDs, metadata header, and data source attribution — written to the persistent Docker volume so users can download them.

**Architecture:** A new `csv_export.py` module handles CSV generation with branding. The `export_csv` MCP tool accepts either a tool name + arguments (re-runs the tool with no row limit) or raw SQL, generates a branded CSV, writes it to `/data/common-ground/exports/`, and returns the file path + a preview of the first rows. Files are accessible via the DuckDB UI, scp, or the Cloudflare-tunneled nginx. Each task starts with Exa research.

**Tech Stack:** FastMCP 3.1.1, DuckDB COPY TO, Python csv module, Docker volume mount

---

## What the User Gets

When an LLM calls `export_csv`, the user gets:

1. **In the chat**: A preview of the first 10 rows + the file path
2. **On disk**: A branded CSV file at a stable path like `/data/common-ground/exports/worst_landlords_20260326_143000.csv`
3. **Downloadable**: Via DuckDB UI (Cloudflare tunnel) or `scp` from the server

### Branded CSV Format

```
# ────────────────────────────────────────────────
# COMMON GROUND — NYC Open Data Export
# common-ground.nyc
# ────────────────────────────────────────────────
# Query: SELECT owner_name, buildings, violations FROM ...
# Tool: worst_landlords(borough='Brooklyn', top_n=25)
# Generated: 2026-03-26T14:30:00Z
# Rows: 25
# Sources: HPD violations, DOB permits, ACRIS transactions
# ────────────────────────────────────────────────
row_id,owner_name,buildings,total_units,violations,open_violations
1,LP PRESERVATION HTC LLC,1,294,10396,2642
2,RENAISSANCE EQUITY LLC,7,2496,21157,1366
...
# ────────────────────────────────────────────────
# Data: NYC public records (open data, no restrictions)
# Powered by DuckDB + DuckLake | common-ground.nyc
# ────────────────────────────────────────────────
```

---

## File Structure

### New files

| File | Responsibility |
|------|----------------|
| `infra/duckdb-server/csv_export.py` | `generate_branded_csv()` pure function + `write_export()` helper |
| `infra/duckdb-server/tests/test_csv_export.py` | Unit tests for CSV generation |

### Modified files

| File | Change |
|------|--------|
| `infra/duckdb-server/mcp_server.py` | Add `export_csv` tool, add to ALWAYS_VISIBLE |
| `infra/duckdb-server/Dockerfile` | Add `COPY csv_export.py .` (already handled by `COPY *.py .`) |

---

## Task 1: Implement branded CSV generator

**Files:**
- Create: `infra/duckdb-server/csv_export.py`
- Create: `infra/duckdb-server/tests/test_csv_export.py`

- [ ] **Step 1: Research — CSV comment conventions and DuckDB COPY**

Search with Exa:
1. `python csv "comment" header "pandas read_csv" "comment=" compatible` — will pandas/Excel skip `#` comment lines?
2. `duckdb "read_csv" "comment" parameter skip lines` — can DuckDB re-read the branded CSV?

Key: pandas supports `comment='#'` in `read_csv()`. DuckDB's `read_csv` does NOT support a comment parameter — but it can skip lines with `skip=N`. We'll use `#` comment lines which pandas handles natively, and include a `skip_rows` count in the metadata for DuckDB users.

- [ ] **Step 2: Write failing tests**

```python
# tests/test_csv_export.py
import pytest
from csv_export import generate_branded_csv, write_export
from datetime import datetime

class TestGenerateBrandedCSV:
    def test_basic_output(self):
        cols = ["name", "age", "city"]
        rows = [("Alice", 30, "NYC"), ("Bob", 25, "LA")]
        result = generate_branded_csv(cols, rows, tool_name="test_tool", tool_args={"limit": 2})
        lines = result.split("\n")

        # Header comments
        assert lines[0].startswith("# ")
        assert "COMMON GROUND" in result
        assert "common-ground.nyc" in result

        # Tool info in header
        assert "test_tool" in result
        assert "Rows: 2" in result

        # Column header (after comments)
        data_lines = [l for l in lines if not l.startswith("#") and l.strip()]
        assert data_lines[0] == "row_id,name,age,city"

        # Data with row_id
        assert data_lines[1] == "1,Alice,30,NYC"
        assert data_lines[2] == "2,Bob,25,LA"

        # Footer comments
        assert lines[-1].startswith("#") or lines[-2].startswith("#")

    def test_row_id_sequential(self):
        cols = ["x"]
        rows = [(i,) for i in range(100)]
        result = generate_branded_csv(cols, rows)
        data_lines = [l for l in result.split("\n") if not l.startswith("#") and l.strip()]
        assert data_lines[1].startswith("1,")
        assert data_lines[100].startswith("100,")

    def test_escapes_commas_in_values(self):
        cols = ["name"]
        rows = [("Smith, John",)]
        result = generate_branded_csv(cols, rows)
        # CSV should quote values containing commas
        assert '"Smith, John"' in result

    def test_handles_none_values(self):
        cols = ["a", "b"]
        rows = [(1, None)]
        result = generate_branded_csv(cols, rows)
        data_lines = [l for l in result.split("\n") if not l.startswith("#") and l.strip()]
        assert data_lines[1] == "1,1,"

    def test_includes_sql_when_provided(self):
        cols = ["x"]
        rows = [(1,)]
        result = generate_branded_csv(cols, rows, sql="SELECT x FROM t")
        assert "SELECT x FROM t" in result

    def test_includes_sources_when_provided(self):
        cols = ["x"]
        rows = [(1,)]
        result = generate_branded_csv(cols, rows, sources=["HPD", "DOB"])
        assert "HPD" in result and "DOB" in result

    def test_empty_rows(self):
        cols = ["a"]
        rows = []
        result = generate_branded_csv(cols, rows)
        assert "Rows: 0" in result
        data_lines = [l for l in result.split("\n") if not l.startswith("#") and l.strip()]
        assert data_lines[0] == "row_id,a"
        assert len(data_lines) == 1  # header only, no data rows

    def test_timestamp_in_header(self):
        cols = ["x"]
        rows = [(1,)]
        result = generate_branded_csv(cols, rows)
        assert "Generated:" in result
        # Should contain an ISO-ish date
        assert "2026" in result


class TestWriteExport:
    def test_writes_file(self, tmp_path):
        csv_text = "# header\nrow_id,a\n1,hello\n"
        path = write_export(csv_text, "test_export", export_dir=str(tmp_path))
        assert path.endswith(".csv")
        with open(path) as f:
            assert f.read() == csv_text

    def test_filename_includes_timestamp(self, tmp_path):
        csv_text = "data"
        path = write_export(csv_text, "my_query", export_dir=str(tmp_path))
        assert "my_query" in path
        assert "2026" in path

    def test_creates_export_dir(self, tmp_path):
        export_dir = str(tmp_path / "subdir" / "exports")
        csv_text = "data"
        path = write_export(csv_text, "test", export_dir=export_dir)
        assert path.startswith(export_dir)
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -m pytest tests/test_csv_export.py -v
```

Expected: ImportError

- [ ] **Step 4: Implement `csv_export.py`**

```python
# csv_export.py
"""
Branded CSV export for Common Ground NYC data lake.

Generates CSV files with:
- Comment header: branding, query info, timestamp, row count, data sources
- row_id column prepended (1-indexed)
- Comment footer: license + attribution
- Standard CSV body compatible with pandas (comment='#'), Excel, R

Comment lines use '#' prefix — pandas reads with read_csv(comment='#').
DuckDB users: skip the first N lines (count given in header).
"""

import csv
import os
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path


EXPORT_DIR = "/data/common-ground/exports"

HEADER_TEMPLATE = """\
# ────────────────────────────────────────────────
# COMMON GROUND — NYC Open Data Export
# common-ground.nyc
# ────────────────────────────────────────────────
{query_info}\
# Generated: {timestamp}
# Rows: {row_count:,}
{sources_line}\
# Comment lines: {comment_lines} (skip for raw CSV)
# ────────────────────────────────────────────────
"""

FOOTER = """\
# ────────────────────────────────────────────────
# Data: NYC public records (open data, no restrictions)
# Powered by DuckDB + DuckLake | common-ground.nyc
# ────────────────────────────────────────────────
"""


def generate_branded_csv(
    cols: list[str],
    rows: list[tuple],
    tool_name: str = "",
    tool_args: dict | None = None,
    sql: str = "",
    sources: list[str] | None = None,
) -> str:
    """Generate a branded CSV string with Common Ground header/footer.

    Args:
        cols: Column names.
        rows: Row tuples from DuckDB query.
        tool_name: MCP tool that produced the data (for attribution).
        tool_args: Arguments passed to the tool.
        sql: Raw SQL query (if applicable).
        sources: Data source descriptions (e.g., ["HPD violations", "DOB permits"]).

    Returns:
        Complete CSV string with comment header, row_id column, and footer.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build query info lines
    query_lines = ""
    if sql:
        # Truncate long SQL
        sql_display = sql.strip().replace("\n", " ")[:200]
        query_lines += f"# Query: {sql_display}\n"
    if tool_name:
        args_str = ", ".join(f"{k}={v!r}" for k, v in (tool_args or {}).items())
        query_lines += f"# Tool: {tool_name}({args_str})\n"

    sources_line = ""
    if sources:
        sources_line = f"# Sources: {', '.join(sources)}\n"

    # Count comment lines in header (for skip hint)
    # Header has a fixed structure — count # lines
    header_draft = HEADER_TEMPLATE.format(
        query_info=query_lines,
        timestamp=timestamp,
        row_count=len(rows),
        sources_line=sources_line,
        comment_lines=0,  # placeholder
    )
    comment_count = sum(1 for line in header_draft.split("\n") if line.startswith("#"))

    header = HEADER_TEMPLATE.format(
        query_info=query_lines,
        timestamp=timestamp,
        row_count=len(rows),
        sources_line=sources_line,
        comment_lines=comment_count,
    )

    # Build CSV body with row_id
    buf = StringIO()
    buf.write(header)

    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["row_id"] + list(cols))
    for i, row in enumerate(rows, 1):
        writer.writerow([i] + [v if v is not None else "" for v in row])

    buf.write(FOOTER)
    return buf.getvalue()


def write_export(
    csv_text: str,
    name: str,
    export_dir: str = EXPORT_DIR,
) -> str:
    """Write CSV text to a file in the export directory.

    Args:
        csv_text: The full CSV string (with header/footer).
        name: Base name for the file (e.g., "worst_landlords").
        export_dir: Directory to write to.

    Returns:
        Absolute path to the written file.
    """
    Path(export_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    # Sanitize name
    safe_name = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)[:100]
    filename = f"{safe_name}_{timestamp}.csv"
    path = os.path.join(export_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(csv_text)
    return path
```

- [ ] **Step 5: Run tests**

```bash
python -m pytest tests/test_csv_export.py -v
```

Expected: All PASS

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/csv_export.py infra/duckdb-server/tests/test_csv_export.py
git commit -m "feat: add branded CSV export generator

Common Ground branding, row IDs, metadata header, data source attribution.
Compatible with pandas (comment='#'), Excel, and R."
```

---

## Task 2: Add `export_csv` MCP tool

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Research — FastMCP tool that writes files and returns path**

Search with Exa:
1. `fastmcp tool "write file" return path pattern 2026` — best practice for file-generating tools
2. `fastmcp ToolResult "content" "file path" export pattern` — how to report a file path

- [ ] **Step 2: Add `export_csv` tool to mcp_server.py**

Add the import at the top (after the existing imports):

```python
from csv_export import generate_branded_csv, write_export
```

Add the tool after the `suggest_explorations` tool (in the discovery tools section). The tool accepts either raw SQL or re-runs a previous query at full scale:

```python
@mcp.tool(annotations=READONLY, tags={"discovery"})
def export_csv(
    sql: Annotated[str, Field(description="SQL query to export. Use the exact query that produced the data you want. All tables in 'lake' database. Example: SELECT * FROM lake.housing.hpd_violations WHERE bbl = '1000670001'")],
    ctx: Context,
    name: Annotated[str, Field(description="Short name for the export file. Example: 'violations_10003', 'worst_landlords_brooklyn'")] = "export",
    sources: Annotated[str, Field(description="Comma-separated data sources for attribution. Example: 'HPD violations, DOB permits'")] = "",
) -> str:
    """Export query results as a branded CSV file with Common Ground watermark, row IDs, and metadata. Use this when the user wants to download data, save results, get a spreadsheet, or export to CSV. The file is written to the server's export directory. Returns the file path and a preview of the first rows. Maximum 100,000 rows per export."""
    _validate_sql(sql)
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Execute with higher row limit for exports
    with _db_lock:
        try:
            cur = db.execute(sql.strip().rstrip(";"))
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(100_000)
        except duckdb.Error as e:
            raise ToolError(f"Export query failed: {e}. Use data_catalog(keyword) to find table names.")

    if not cols:
        raise ToolError("Query returned no columns. Check your SQL.")

    elapsed = round((time.time() - t0) * 1000)
    source_list = [s.strip() for s in sources.split(",") if s.strip()] if sources else []

    # Generate branded CSV
    csv_text = generate_branded_csv(
        cols, rows,
        sql=sql,
        sources=source_list,
    )

    # Write to export directory
    path = write_export(csv_text, name)

    # Preview first 5 rows
    preview_rows = rows[:5]
    preview_lines = [" | ".join(str(c) for c in cols)]
    preview_lines.append("-" * len(preview_lines[0]))
    for row in preview_rows:
        preview_lines.append(" | ".join(str(v) if v is not None else "" for v in row))

    return (
        f"Exported {len(rows):,} rows to CSV ({elapsed}ms).\n\n"
        f"File: {path}\n"
        f"Size: {len(csv_text):,} bytes\n\n"
        f"Preview (first {len(preview_rows)} rows):\n"
        + "\n".join(preview_lines)
        + (f"\n\n... and {len(rows) - 5:,} more rows" if len(rows) > 5 else "")
    )
```

- [ ] **Step 3: Add `export_csv` to ALWAYS_VISIBLE**

Find the `ALWAYS_VISIBLE` list and add `"export_csv"`:

```python
ALWAYS_VISIBLE = [
    # Data discovery (always needed)
    "sql_query",
    "data_catalog",
    "list_schemas",
    "suggest_explorations",
    "export_csv",           # <-- ADD THIS
    ...
```

- [ ] **Step 4: Update INSTRUCTIONS to mention export**

Find the `INSTRUCTIONS` string. In the ROUTING section, add a new entry:

```
* EXPORT/DOWNLOAD/CSV → export_csv (branded CSV with row IDs and metadata)
```

And in the POWER FEATURES section, add:

```
* export_csv generates branded CSV files with Common Ground watermark, row IDs, and full data attribution
```

- [ ] **Step 5: Verify locally**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
from mcp_server import ALWAYS_VISIBLE, INSTRUCTIONS
assert 'export_csv' in ALWAYS_VISIBLE
assert 'export_csv' in INSTRUCTIONS
print('OK')
"
```

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add export_csv tool for branded CSV downloads

Generates branded CSV with Common Ground watermark, row IDs, metadata
header, and data source attribution. Max 100K rows per export.
Written to persistent Docker volume at /data/common-ground/exports/."
```

---

## Task 3: Deploy and validate

**Files:** Server-side deployment

- [ ] **Step 1: Deploy**

```bash
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
    --exclude '__pycache__' --exclude '.git' --exclude 'tests' --exclude '*.md' \
    ~/Desktop/dagster-pipeline/infra/duckdb-server/ \
    fattie@178.156.228.119:/opt/common-ground/duckdb-server/

ssh hetzner "cd /opt/common-ground && sudo docker compose up -d --build duckdb-server"
```

- [ ] **Step 2: Wait for startup and test export**

```bash
sleep 45

# Test export via MCP
curl -s https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"export_csv","arguments":{"sql":"SELECT boroid, block, lot, streetname, housenumber FROM lake.housing.hpd_jurisdiction LIMIT 50","name":"test_export","sources":"HPD jurisdiction"}}}' 2>&1 | python3 -c "
import sys, json
for line in sys.stdin:
    if line.startswith('data: '):
        d = json.loads(line[6:])
        if 'result' in d:
            for c in d['result'].get('content', []):
                if c.get('type') == 'text':
                    print(c['text'][:500])
"
```

Expected: Shows "Exported 50 rows to CSV", file path, and preview.

- [ ] **Step 3: Verify the file on server**

```bash
ssh hetzner "ls -la /mnt/data/common-ground/exports/ && echo '---' && head -15 /mnt/data/common-ground/exports/test_export_*.csv"
```

Expected: File exists with Common Ground branded header, row_id column, data rows.

- [ ] **Step 4: Verify the branded format**

```bash
ssh hetzner "cat /mnt/data/common-ground/exports/test_export_*.csv | head -20"
```

Expected output should match the branded format:
```
# ────────────────────────────────────────────────
# COMMON GROUND — NYC Open Data Export
# common-ground.nyc
# ────────────────────────────────────────────────
# Query: SELECT boroid, block, lot, streetname, housenumber FROM ...
# Generated: 2026-03-26T...
# Rows: 50
# Sources: HPD jurisdiction
# Comment lines: N (skip for raw CSV)
# ────────────────────────────────────────────────
row_id,boroid,block,lot,streetname,housenumber
1,3,995,19,6 STREET,432
2,2,5534,47,SWINTON AVENUE,1018
...
```

- [ ] **Step 5: Test with a real analytics query**

```bash
curl -s https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"export_csv","arguments":{"sql":"SELECT o.owner_name, COUNT(DISTINCT ow.bbl) AS buildings, COUNT(*) AS violations FROM main.graph_owners o JOIN main.graph_owns ow ON o.owner_id = ow.owner_id JOIN main.graph_violations v ON ow.bbl = v.bbl WHERE o.owner_name IS NOT NULL GROUP BY o.owner_name ORDER BY violations DESC LIMIT 100","name":"worst_landlords_all","sources":"HPD violations, HPD registrations"}}}' 2>&1 | head -3
```

Expected: Exports 100 worst landlords with branded CSV.

- [ ] **Step 6: Commit deployment**

```bash
cd ~/Desktop/dagster-pipeline
git add -A infra/duckdb-server/
git commit -m "deploy: branded CSV export live on production

Verified: export_csv generates branded files on persistent volume.
Accessible via server exports directory."
```

---

## Execution Order

```
Task 1 (csv_export.py + tests)   ← Pure functions, no dependencies
  ↓
Task 2 (export_csv tool)         ← Imports from csv_export.py
  ↓
Task 3 (deploy + validate)       ← After all code
```

Strictly sequential.

---

## Verification Checklist

- [ ] `python -m pytest tests/test_csv_export.py -v` — all tests pass
- [ ] `export_csv` tool callable via MCP
- [ ] Exported CSV has `# COMMON GROUND` header
- [ ] Exported CSV has `row_id` as first column (1-indexed)
- [ ] Exported CSV has query info, timestamp, row count in header
- [ ] Exported CSV has footer with license + attribution
- [ ] File written to `/data/common-ground/exports/` (persists on Docker volume)
- [ ] Preview shows first 5 rows in tool response
- [ ] `export_csv` in ALWAYS_VISIBLE and INSTRUCTIONS
- [ ] Max 100K rows enforced
