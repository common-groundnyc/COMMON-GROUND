# DuckLake Table & Column Comments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate all ~270 DuckLake tables and their key columns with LLM-optimized comments, sourced from Socrata metadata API + Claude-rewritten descriptions, stored in a version-controlled YAML manifest.

**Architecture:** Two-phase generate-then-apply. Phase 1: Python script fetches Socrata table+column metadata via `GET /api/views/{id}.json`, profiles columns via DuckLake SQL, sends batches to Claude API for rewrite into LLM-optimized format, outputs a YAML manifest. Phase 2: Apply script reads manifest and runs `COMMENT ON TABLE/COLUMN` statements against DuckLake. Non-Socrata tables (~40) get hand-written descriptions in the same YAML format.

**Tech Stack:** Python 3.13, `httpx` (Socrata API), `anthropic` SDK (Claude rewrite), `duckdb` + `ducklake` extension (DuckLake connection), `PyYAML` (manifest format), `pytest` (tests)

---

## File Structure

```
scripts/
├── generate_comments.py      # Phase 1: Fetch metadata → Claude rewrite → YAML manifest
├── apply_comments.py          # Phase 2: Read YAML → COMMENT ON statements → DuckLake
└── test_comments.py           # Tests for both scripts

data/
└── catalog_comments.yaml      # The manifest — version-controlled, reviewable
```

**Why these files:**
- `generate_comments.py` — one-time (or periodic) script. Fetches Socrata metadata, profiles columns, calls Claude API to rewrite descriptions, outputs YAML. Idempotent — can be re-run when tables change.
- `apply_comments.py` — reads the YAML manifest and applies `COMMENT ON` statements to DuckLake. Can run locally or via SSH on Hetzner. Idempotent — re-running overwrites existing comments.
- `catalog_comments.yaml` — the single source of truth. Reviewable in PRs, diffable, editable by hand for non-Socrata tables.

---

## Task 1: Socrata Metadata Fetcher

**Files:**
- Create: `scripts/generate_comments.py`
- Create: `scripts/test_comments.py`
- Reference: `src/dagster_pipeline/sources/datasets.py` (SOCRATA_DATASETS dict with all dataset IDs and domain keys)
- Reference: `src/dagster_pipeline/sources/socrata.py:33-37` (DOMAINS mapping: nyc/nys/health/cdc → URLs)

- [ ] **Step 1: Create conftest.py for script imports**

```python
# scripts/conftest.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
```

- [ ] **Step 2: Write test for Socrata metadata fetching**

```python
# scripts/test_comments.py
import pytest
from unittest.mock import patch, MagicMock
from generate_comments import fetch_socrata_metadata, DOMAINS

def test_fetch_socrata_metadata_returns_name_and_description():
    """Verify we extract the right fields from the Socrata API response."""
    mock_response = {
        "name": "Housing Maintenance Code Violations",
        "description": "This dataset contains violations issued by HPD.",
        "category": "Housing & Development",
        "attribution": "Department of Housing Preservation and Development (HPD)",
        "columns": [
            {"fieldName": "violationid", "name": "ViolationID", "description": "Unique ID", "dataTypeName": "number"},
            {"fieldName": "boroid", "name": "BoroID", "description": "Borough code", "dataTypeName": "number"},
        ],
    }
    with patch("generate_comments.httpx.get") as mock_get:
        mock_get.return_value = MagicMock(status_code=200, json=lambda: mock_response)
        result = fetch_socrata_metadata("data.cityofnewyork.us", "wvxf-dwi5")

    assert result["name"] == "Housing Maintenance Code Violations"
    assert result["description"].startswith("This dataset")
    assert len(result["columns"]) == 2
    assert result["columns"][0]["fieldName"] == "violationid"

def test_fetch_socrata_metadata_handles_missing_description():
    """Some datasets have empty descriptions — should return empty string, not crash."""
    mock_response = {"name": "Test", "columns": []}
    with patch("generate_comments.httpx.get") as mock_get:
        mock_get.return_value = MagicMock(status_code=200, json=lambda: mock_response)
        result = fetch_socrata_metadata("data.cityofnewyork.us", "xxxx-xxxx")

    assert result["description"] == ""
    assert result["columns"] == []
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py::test_fetch_socrata_metadata_returns_name_and_description -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'generate_comments'`

- [ ] **Step 4: Implement the Socrata metadata fetcher**

```python
# scripts/generate_comments.py
"""Generate LLM-optimized DuckLake table/column comments from Socrata metadata + Claude AI.

Usage:
    uv run python scripts/generate_comments.py              # Generate manifest
    uv run python scripts/generate_comments.py --fetch-only  # Fetch metadata without Claude rewrite
"""
from __future__ import annotations
import sys
import time
import httpx
import yaml
from pathlib import Path

# Socrata domain mapping (mirrors src/dagster_pipeline/sources/socrata.py)
DOMAINS = {
    "nyc": "data.cityofnewyork.us",
    "nys": "data.ny.gov",
    "health": "health.data.ny.gov",
    "cdc": "data.cdc.gov",
}

MANIFEST_PATH = Path(__file__).parent.parent / "data" / "catalog_comments.yaml"


def fetch_socrata_metadata(domain: str, dataset_id: str) -> dict:
    """Fetch table + column metadata from Socrata's views API."""
    url = f"https://{domain}/api/views/{dataset_id}.json"
    resp = httpx.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Extract update frequency from domain-specific custom_fields
    custom = data.get("metadata", {}).get("custom_fields", {})
    frequency = (
        custom.get("Update", {}).get("Update Frequency")
        or custom.get("Dataset Summary", {}).get("Posting Frequency")
        or None
    )

    columns = []
    for col in data.get("columns", []):
        # Skip Socrata system columns (prefixed with :)
        if col.get("fieldName", "").startswith(":"):
            continue
        columns.append({
            "fieldName": col.get("fieldName", ""),
            "name": col.get("name", ""),
            "description": col.get("description", ""),
            "dataTypeName": col.get("dataTypeName", ""),
        })

    return {
        "name": data.get("name", ""),
        "description": data.get("description", "") or "",
        "category": data.get("category", ""),
        "attribution": data.get("attribution", ""),
        "frequency": frequency,
        "source_url": f"https://{domain}/d/{dataset_id}",
        "columns": columns,
    }


def fetch_all_socrata_metadata(datasets: dict) -> dict:
    """Fetch metadata for all Socrata datasets. Returns {schema: {table: metadata}}."""
    results = {}
    total = sum(len(tables) for tables in datasets.values())
    done = 0

    for schema, tables in datasets.items():
        results[schema] = {}
        for table_name, dataset_id, domain_key in tables:
            domain = DOMAINS.get(domain_key, DOMAINS["nyc"])
            done += 1
            print(f"  [{done}/{total}] {schema}.{table_name} ({dataset_id})...", end=" ", flush=True)
            try:
                meta = fetch_socrata_metadata(domain, dataset_id)
                results[schema][table_name] = meta
                print(f"OK — {len(meta['columns'])} columns", flush=True)
            except Exception as e:
                print(f"FAILED: {e}", flush=True)
                results[schema][table_name] = {
                    "name": table_name.replace("_", " ").title(),
                    "description": "",
                    "category": "",
                    "attribution": "",
                    "frequency": None,
                    "source_url": "",
                    "columns": [],
                    "error": str(e),
                }
            # Rate limiting: ~2 req/sec to be polite
            time.sleep(0.5)

    return results
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py -v
```
Expected: 2 PASS

- [ ] **Step 6: Commit**

```bash
git add scripts/generate_comments.py scripts/test_comments.py scripts/conftest.py
git commit -m "feat: add Socrata metadata fetcher for DuckLake comments"
```

---

## Task 2: Column Profiling from DuckLake

**Files:**
- Modify: `scripts/generate_comments.py`
- Modify: `scripts/test_comments.py`
- Reference: `src/dagster_pipeline/sources/socrata_direct.py:172-196` (DuckLake connection pattern)

- [ ] **Step 1: Write test for column profiling**

```python
# scripts/test_comments.py — add these tests

def test_profile_columns_returns_stats():
    """Verify column profiler returns cardinality, nulls, and sample values."""
    from generate_comments import profile_table_columns
    import duckdb

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE test_t (id INT, status VARCHAR, borough INT)")
    conn.execute("""INSERT INTO test_t VALUES
        (1, 'OPEN', 1), (2, 'CLOSED', 2), (3, 'OPEN', 3),
        (4, NULL, 1), (5, 'PENDING', 2)""")

    profile = profile_table_columns(conn, "memory", "main", "test_t")

    assert "status" in profile
    assert profile["status"]["distinct_count"] == 3  # OPEN, CLOSED, PENDING
    assert profile["status"]["null_pct"] == 20.0
    assert set(profile["status"]["top_values"]) == {"OPEN", "CLOSED", "PENDING"}
    assert profile["borough"]["distinct_count"] == 3
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py::test_profile_columns_returns_stats -v
```
Expected: FAIL — `cannot import name 'profile_table_columns'`

- [ ] **Step 3: Implement column profiler**

Add to `scripts/generate_comments.py`:

```python
import duckdb


def profile_table_columns(
    conn: duckdb.DuckDBPyConnection,
    database: str,
    schema: str,
    table: str,
    max_top_values: int = 10,
) -> dict:
    """Profile columns: cardinality, null %, top values, min/max for numerics."""
    qualified = f"{database}.{schema}.{table}"

    # Get column names and types
    cols_info = conn.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = '{database}'
          AND table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position
    """).fetchall()

    row_count = conn.execute(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]
    if row_count == 0:
        return {c[0]: {"distinct_count": 0, "null_pct": 0, "top_values": [], "data_type": c[1]} for c in cols_info}

    profile = {}
    for col_name, data_type in cols_info:
        # Skip dlt internal columns
        if col_name.startswith("_dlt_"):
            continue

        try:
            stats = conn.execute(f"""
                SELECT
                    COUNT(DISTINCT "{col_name}") AS distinct_count,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE "{col_name}" IS NULL) / COUNT(*), 1) AS null_pct
                FROM {qualified}
            """).fetchone()

            # Get top values for low-cardinality columns (< 50 distinct)
            top_values = []
            if stats[0] <= 50:
                top_rows = conn.execute(f"""
                    SELECT "{col_name}"::VARCHAR AS val, COUNT(*) AS n
                    FROM {qualified}
                    WHERE "{col_name}" IS NOT NULL
                    GROUP BY val ORDER BY n DESC LIMIT {max_top_values}
                """).fetchall()
                top_values = [r[0] for r in top_rows]

            profile[col_name] = {
                "distinct_count": stats[0],
                "null_pct": float(stats[1]) if stats[1] else 0.0,
                "top_values": top_values,
                "data_type": data_type,
            }
        except Exception:
            profile[col_name] = {
                "distinct_count": -1,
                "null_pct": -1,
                "top_values": [],
                "data_type": data_type,
            }

    return profile
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py -v
```
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/generate_comments.py scripts/test_comments.py
git commit -m "feat: add column profiler for DuckLake comment generation"
```

---

## Task 3: Claude API Rewriter

**Files:**
- Modify: `scripts/generate_comments.py`
- Modify: `scripts/test_comments.py`
- Dependency: `anthropic` SDK (already in project deps)

- [ ] **Step 1: Write test for Claude rewriter**

```python
# scripts/test_comments.py — add

def test_build_rewrite_prompt_includes_all_context():
    """Verify the prompt sent to Claude contains metadata, profile, and format instructions."""
    from generate_comments import build_rewrite_prompt

    socrata_meta = {
        "name": "HPD Violations",
        "description": "Housing code violations from HPD.",
        "attribution": "Dept of HPD",
        "frequency": "Daily",
        "source_url": "https://data.cityofnewyork.us/d/wvxf-dwi5",
        "columns": [
            {"fieldName": "boroid", "name": "BoroID", "description": "Borough code", "dataTypeName": "number"},
        ],
    }
    column_profile = {
        "boroid": {"distinct_count": 5, "null_pct": 0.1, "top_values": ["1", "2", "3", "4", "5"], "data_type": "BIGINT"},
    }

    prompt = build_rewrite_prompt("housing", "hpd_violations", socrata_meta, column_profile)

    assert "hpd_violations" in prompt
    assert "housing" in prompt
    assert "Housing code violations" in prompt
    assert "boroid" in prompt
    assert "Borough code" in prompt
    assert "top_values" in prompt or "1, 2, 3" in prompt


def test_parse_rewrite_response_extracts_yaml():
    """Verify we can parse Claude's YAML response into table + column comments."""
    from generate_comments import parse_rewrite_response

    response_text = """
table_comment: "HPD housing code violations issued to NYC buildings. One row per violation. Use to find building safety issues, worst landlords, violation trends by borough."
columns:
  boroid: "Borough: 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island"
  violationid: "Unique violation identifier. Integer PK."
"""
    result = parse_rewrite_response(response_text)
    assert "HPD housing code violations" in result["table_comment"]
    assert "Manhattan" in result["columns"]["boroid"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py::test_build_rewrite_prompt_includes_all_context -v
```
Expected: FAIL — `cannot import name 'build_rewrite_prompt'`

- [ ] **Step 3: Implement rewrite prompt builder and response parser**

Add to `scripts/generate_comments.py`:

```python
import anthropic


REWRITE_SYSTEM_PROMPT = """\
You are a database documentation expert. You write concise, LLM-optimized table and column descriptions for a NYC open data lake (DuckDB/DuckLake).

Your descriptions will be stored as SQL COMMENT ON statements and used by LLM agents for:
1. Table discovery — "which table answers this question?"
2. Text-to-SQL — generating correct SQL queries
3. Schema understanding — knowing what columns mean, how to join tables

Rules:
- Table comment: 1-2 sentences. Include: what it contains, grain (one row per what), key use cases. Max 200 chars.
- Column comment: 10-30 words. Include: business meaning, valid values for enums, join targets, NULL meaning. Max 120 chars.
- Plain English, no JSON/YAML in the comment text itself.
- For coded columns (boroid, status codes), list all valid values.
- For join keys (bbl, registrationid, bin), name the target tables.
- For date columns, mention the format and range.
- Do NOT repeat the column name in the description.
- Do NOT add descriptions for _dlt_* columns.

Output format — valid YAML:
table_comment: "your table description"
columns:
  column_name: "your column description"
  another_column: "another description"

Only include columns where you can write a meaningful description. Skip columns where the name is self-explanatory (e.g., latitude, longitude, zip_code)."""


def build_rewrite_prompt(
    schema: str,
    table: str,
    socrata_meta: dict,
    column_profile: dict,
) -> str:
    """Build the user prompt for Claude to rewrite metadata."""
    lines = [
        f"Schema: {schema}",
        f"Table: {table}",
        f"Source name: {socrata_meta.get('name', '')}",
        f"Source description: {socrata_meta.get('description', '')}",
        f"Agency: {socrata_meta.get('attribution', '')}",
        f"Update frequency: {socrata_meta.get('frequency', 'unknown')}",
        f"Source URL: {socrata_meta.get('source_url', '')}",
        "",
        "Columns (from Socrata metadata + DuckLake profiling):",
    ]

    # Merge Socrata column descriptions with profile stats
    socrata_cols = {c["fieldName"]: c for c in socrata_meta.get("columns", [])}

    for col_name, stats in sorted(column_profile.items()):
        if col_name.startswith("_dlt_"):
            continue
        socrata_col = socrata_cols.get(col_name, {})
        socrata_desc = socrata_col.get("description", "")
        line = f"  - {col_name} ({stats['data_type']})"
        if socrata_desc:
            line += f" — Socrata says: \"{socrata_desc}\""
        line += f" — {stats['distinct_count']} distinct, {stats['null_pct']}% null"
        if stats["top_values"]:
            line += f", top values: {', '.join(str(v) for v in stats['top_values'][:10])}"
        lines.append(line)

    return "\n".join(lines)


def rewrite_with_claude(
    schema: str,
    table: str,
    socrata_meta: dict,
    column_profile: dict,
    client: anthropic.Anthropic | None = None,
) -> dict:
    """Send metadata + profile to Claude and get back optimized descriptions."""
    if client is None:
        client = anthropic.Anthropic()

    prompt = build_rewrite_prompt(schema, table, socrata_meta, column_profile)

    response = client.messages.create(
        model="claude-sonnet-4-5-20250514",
        max_tokens=1024,
        system=REWRITE_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    return parse_rewrite_response(response.content[0].text)


def parse_rewrite_response(text: str) -> dict:
    """Parse Claude's YAML response into {table_comment, columns}."""
    # Strip markdown code fences if present
    text = text.strip()
    if text.startswith("```"):
        text = "\n".join(text.split("\n")[1:])
    if text.endswith("```"):
        text = "\n".join(text.split("\n")[:-1])

    parsed = yaml.safe_load(text)
    return {
        "table_comment": parsed.get("table_comment", ""),
        "columns": parsed.get("columns", {}),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py -v
```
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/generate_comments.py scripts/test_comments.py
git commit -m "feat: add Claude API rewriter for LLM-optimized descriptions"
```

---

## Task 4: YAML Manifest Generator (Main Pipeline)

**Files:**
- Modify: `scripts/generate_comments.py` (add `main()` function + CLI)
- Create: `data/catalog_comments.yaml` (output — generated, then version-controlled)
- Reference: `src/dagster_pipeline/sources/datasets.py` (import SOCRATA_DATASETS)

- [ ] **Step 1: Write test for manifest generation**

```python
# scripts/test_comments.py — add

def test_generate_manifest_entry():
    """Verify a single table produces a valid manifest entry."""
    from generate_comments import build_manifest_entry

    socrata_meta = {
        "name": "HPD Violations",
        "description": "Housing violations from HPD.",
        "attribution": "Dept of HPD",
        "frequency": "Daily",
        "source_url": "https://data.cityofnewyork.us/d/wvxf-dwi5",
        "columns": [],
    }
    rewritten = {
        "table_comment": "HPD housing code violations. One row per violation.",
        "columns": {"boroid": "Borough: 1=MN, 2=BX, 3=BK, 4=QN, 5=SI"},
    }

    entry = build_manifest_entry("housing", "hpd_violations", socrata_meta, rewritten)

    assert entry["schema"] == "housing"
    assert entry["table"] == "hpd_violations"
    assert entry["comment"] == rewritten["table_comment"]
    assert entry["source"] == "Socrata"
    assert entry["source_url"] == socrata_meta["source_url"]
    assert entry["columns"]["boroid"] == rewritten["columns"]["boroid"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py::test_generate_manifest_entry -v
```
Expected: FAIL

- [ ] **Step 3: Implement manifest builder + CLI**

Add to `scripts/generate_comments.py`:

```python
import argparse


def build_manifest_entry(
    schema: str,
    table: str,
    socrata_meta: dict,
    rewritten: dict,
) -> dict:
    """Build a single manifest entry combining source metadata + rewritten comments."""
    return {
        "schema": schema,
        "table": table,
        "comment": rewritten.get("table_comment", ""),
        "source": "Socrata",
        "source_url": socrata_meta.get("source_url", ""),
        "attribution": socrata_meta.get("attribution", ""),
        "update_frequency": socrata_meta.get("frequency", "unknown"),
        "columns": rewritten.get("columns", {}),
    }


def write_manifest(entries: list[dict], path: Path) -> None:
    """Write the manifest to YAML."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(
            entries,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            width=120,
        )
    print(f"Manifest written to {path} ({len(entries)} tables)")


def main():
    parser = argparse.ArgumentParser(description="Generate DuckLake catalog comments")
    parser.add_argument("--fetch-only", action="store_true",
                        help="Fetch Socrata metadata without Claude rewrite")
    parser.add_argument("--output", type=Path, default=MANIFEST_PATH,
                        help="Output YAML manifest path")
    parser.add_argument("--schema", type=str, default=None,
                        help="Only process one schema (for testing)")
    args = parser.parse_args()

    # Import datasets registry
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from dagster_pipeline.sources.datasets import SOCRATA_DATASETS

    datasets = SOCRATA_DATASETS
    if args.schema:
        datasets = {args.schema: datasets[args.schema]}

    # Phase 1: Fetch Socrata metadata
    print(f"Fetching Socrata metadata for {sum(len(v) for v in datasets.values())} datasets...")
    all_metadata = fetch_all_socrata_metadata(datasets)

    if args.fetch_only:
        # Dump raw metadata for inspection
        write_manifest(
            [{"schema": s, "table": t, **meta}
             for s, tables in all_metadata.items()
             for t, meta in tables.items()],
            args.output,
        )
        return

    # Phase 2: Profile columns from DuckLake (requires connection)
    print("Connecting to DuckLake for column profiling...")
    import os
    catalog_url = os.environ.get(
        "DUCKLAKE_CATALOG_URL",
        "ducklake:postgres:dbname=ducklake user=dagster host=178.156.228.119",
    )
    s3_config = {
        "endpoint": os.environ.get("S3_ENDPOINT", "178.156.228.119:9000"),
        "access_key": os.environ.get("MINIO_ROOT_USER", ""),
        "secret_key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
        "use_ssl": False,
    }

    from dagster_pipeline.sources.socrata_direct import get_ducklake_connection
    conn = get_ducklake_connection(catalog_url, s3_config)

    # Phase 3: For each table, profile + rewrite with Claude
    client = anthropic.Anthropic()
    entries = []

    for schema, tables in all_metadata.items():
        for table_name, meta in tables.items():
            print(f"  Profiling + rewriting {schema}.{table_name}...", end=" ", flush=True)
            try:
                profile = profile_table_columns(conn, "lake", schema, table_name)
                rewritten = rewrite_with_claude(schema, table_name, meta, profile, client)
                entry = build_manifest_entry(schema, table_name, meta, rewritten)
                entries.append(entry)
                print(f"OK — {len(rewritten.get('columns', {}))} column comments", flush=True)
            except Exception as e:
                print(f"FAILED: {e}", flush=True)
                # Fallback: use raw Socrata description
                entries.append({
                    "schema": schema,
                    "table": table_name,
                    "comment": meta.get("description", "")[:200],
                    "source": "Socrata",
                    "source_url": meta.get("source_url", ""),
                    "attribution": meta.get("attribution", ""),
                    "update_frequency": meta.get("frequency", "unknown"),
                    "columns": {},
                    "error": str(e),
                })
            time.sleep(1)  # Rate limit Claude API calls

    write_manifest(entries, args.output)
    conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py -v
```
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/generate_comments.py scripts/test_comments.py
git commit -m "feat: add manifest generator CLI with Claude rewrite pipeline"
```

---

## Task 5: Apply Comments Script

**Files:**
- Create: `scripts/apply_comments.py`
- Modify: `scripts/test_comments.py`

- [ ] **Step 1: Write test for comment application**

```python
# scripts/test_comments.py — add

def test_apply_comments_sets_table_and_column_comments():
    """Verify COMMENT ON statements are executed correctly."""
    from apply_comments import apply_manifest_to_db
    import duckdb

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA test_schema")
    conn.execute("CREATE TABLE test_schema.test_table (id INT, status VARCHAR, borough INT)")

    manifest = [{
        "schema": "test_schema",
        "table": "test_table",
        "comment": "Test table for unit testing. One row per test case.",
        "columns": {
            "status": "Current status: OPEN, CLOSED, PENDING",
            "borough": "Borough: 1=MN, 2=BX, 3=BK, 4=QN, 5=SI",
        },
    }]

    stats = apply_manifest_to_db(conn, manifest, database="memory")

    assert stats["tables_commented"] == 1
    assert stats["columns_commented"] == 2

    # Verify table comment
    result = conn.execute(
        "SELECT comment FROM duckdb_tables() WHERE schema_name='test_schema' AND table_name='test_table'"
    ).fetchone()
    assert "unit testing" in result[0]

    # Verify column comment
    result = conn.execute(
        "SELECT comment FROM duckdb_columns() WHERE schema_name='test_schema' AND table_name='test_table' AND column_name='status'"
    ).fetchone()
    assert "OPEN" in result[0]


def test_apply_comments_skips_missing_tables():
    """Tables not in the database should be skipped, not crash."""
    from apply_comments import apply_manifest_to_db
    import duckdb

    conn = duckdb.connect(":memory:")
    manifest = [{
        "schema": "nonexistent",
        "table": "fake_table",
        "comment": "Should be skipped",
        "columns": {},
    }]

    stats = apply_manifest_to_db(conn, manifest, database="memory")
    assert stats["tables_commented"] == 0
    assert stats["tables_skipped"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py::test_apply_comments_sets_table_and_column_comments -v
```
Expected: FAIL — `No module named 'apply_comments'`

- [ ] **Step 3: Implement apply script**

```python
# scripts/apply_comments.py
"""Apply DuckLake table/column comments from YAML manifest.

Usage:
    uv run python scripts/apply_comments.py                         # Apply to production DuckLake
    uv run python scripts/apply_comments.py --dry-run               # Show SQL without executing
    uv run python scripts/apply_comments.py --manifest path/to.yaml  # Custom manifest path
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
import yaml
import duckdb

MANIFEST_PATH = Path(__file__).parent.parent / "data" / "catalog_comments.yaml"


def apply_manifest_to_db(
    conn: duckdb.DuckDBPyConnection,
    manifest: list[dict],
    database: str = "lake",
    dry_run: bool = False,
) -> dict:
    """Apply COMMENT ON statements from manifest. Returns stats."""
    stats = {"tables_commented": 0, "columns_commented": 0, "tables_skipped": 0, "errors": []}

    # In dry-run mode, skip validation — print all SQL
    if dry_run:
        for entry in manifest:
            schema = entry["schema"]
            table = entry["table"]
            comment = entry.get("comment", "")
            qualified = f"{database}.{schema}.{table}"
            if comment:
                escaped = comment.replace("'", "''")
                print(f"COMMENT ON TABLE {qualified} IS '{escaped}';")
                stats["tables_commented"] += 1
            for col_name, col_comment in entry.get("columns", {}).items():
                if col_comment:
                    escaped_col = col_comment.replace("'", "''")
                    print(f'COMMENT ON COLUMN {qualified}."{col_name}" IS \'{escaped_col}\';')
                    stats["columns_commented"] += 1
        return stats

    # Get existing tables for validation
    existing = set()
    rows = conn.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = ?",
        [database],
    ).fetchall()
    existing = {(r[0], r[1]) for r in rows}

    for entry in manifest:
        schema = entry["schema"]
        table = entry["table"]
        comment = entry.get("comment", "")

        if (schema, table) not in existing:
            stats["tables_skipped"] += 1
            continue

        qualified = f"{database}.{schema}.{table}"

        # Apply table comment
        if comment:
            escaped = comment.replace("'", "''")
            sql = f"COMMENT ON TABLE {qualified} IS '{escaped}'"
            try:
                conn.execute(sql)
                stats["tables_commented"] += 1
            except Exception as e:
                stats["errors"].append(f"{qualified}: {e}")

        # Apply column comments
        # Get existing columns for this table
        try:
            col_rows = conn.execute(
                "SELECT column_name FROM duckdb_columns() WHERE database_name = ? AND schema_name = ? AND table_name = ?",
                [database, schema, table],
            ).fetchall()
            existing_cols = {r[0] for r in col_rows}
        except Exception:
            existing_cols = set()

        for col_name, col_comment in entry.get("columns", {}).items():
            if col_name not in existing_cols:
                continue
            if not col_comment:
                continue
            escaped_col = col_comment.replace("'", "''")
            sql = f'COMMENT ON COLUMN {qualified}."{col_name}" IS \'{escaped_col}\''
            try:
                conn.execute(sql)
                stats["columns_commented"] += 1
            except Exception as e:
                stats["errors"].append(f"{qualified}.{col_name}: {e}")

    return stats


def main():
    parser = argparse.ArgumentParser(description="Apply DuckLake comments from YAML manifest")
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    args = parser.parse_args()

    with open(args.manifest) as f:
        manifest = yaml.safe_load(f)

    if args.dry_run:
        # Dry-run doesn't need a DB connection — just prints SQL
        apply_manifest_to_db(None, manifest, dry_run=True)
        return

    # Connect to production DuckLake
    import os
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from dagster_pipeline.sources.socrata_direct import get_ducklake_connection

    catalog_url = os.environ.get(
        "DUCKLAKE_CATALOG_URL",
        "ducklake:postgres:dbname=ducklake user=dagster host=178.156.228.119",
    )
    s3_config = {
        "endpoint": os.environ.get("S3_ENDPOINT", "178.156.228.119:9000"),
        "access_key": os.environ.get("MINIO_ROOT_USER", ""),
        "secret_key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
        "use_ssl": False,
    }

    conn = get_ducklake_connection(catalog_url, s3_config)
    print(f"Applying {len(manifest)} table comments to DuckLake...")

    stats = apply_manifest_to_db(conn, manifest)

    print(f"Done: {stats['tables_commented']} tables, {stats['columns_commented']} columns commented")
    if stats["tables_skipped"]:
        print(f"Skipped {stats['tables_skipped']} tables (not found in database)")
    if stats["errors"]:
        print(f"Errors ({len(stats['errors'])}):")
        for err in stats["errors"]:
            print(f"  - {err}")

    conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Add YAML round-trip test**

```python
# scripts/test_comments.py — add

def test_manifest_yaml_round_trip(tmp_path):
    """Verify generate output can be read by apply script."""
    from generate_comments import write_manifest
    from apply_comments import apply_manifest_to_db
    import duckdb

    entries = [{
        "schema": "test_schema",
        "table": "test_table",
        "comment": "Round-trip test table. Contains apostrophe's and \"quotes\".",
        "source": "Test",
        "source_url": "",
        "columns": {
            "col_a": "First column description",
            "col_b": "Status: 'OPEN', 'CLOSED'",
        },
    }]

    manifest_path = tmp_path / "test_manifest.yaml"
    write_manifest(entries, manifest_path)

    # Read back
    import yaml
    with open(manifest_path) as f:
        loaded = yaml.safe_load(f)

    assert len(loaded) == 1
    assert loaded[0]["comment"] == entries[0]["comment"]
    assert loaded[0]["columns"]["col_b"] == entries[0]["columns"]["col_b"]

    # Apply to in-memory DB
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA test_schema")
    conn.execute("CREATE TABLE test_schema.test_table (col_a INT, col_b VARCHAR)")
    stats = apply_manifest_to_db(conn, loaded, database="memory")
    assert stats["tables_commented"] == 1
    assert stats["columns_commented"] == 2
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest scripts/test_comments.py -v
```
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add scripts/apply_comments.py scripts/test_comments.py
git commit -m "feat: add apply script to push YAML manifest comments to DuckLake"
```

---

## Task 6: Non-Socrata Table Descriptions

**Files:**
- Modify: `data/catalog_comments.yaml` (add hand-written entries for ~40 non-Socrata tables)

These tables come from custom Python sources and have no Socrata metadata to pull from. The descriptions must be hand-written based on the source code docstrings and data content.

- [ ] **Step 1: Add non-Socrata entries to manifest**

Append the following entries to `data/catalog_comments.yaml`. These cover tables in the `federal` schema from custom sources, plus `foundation` schema tables.

Key non-Socrata table groups and their descriptions:

```yaml
# --- Federal: CourtListener (courtlistener_v3.py) ---
- schema: federal
  table: cl_judges
  comment: "Federal judges from CourtListener. One row per judge. Includes education, political affiliation, appointment history."
  source: CourtListener API
  source_url: https://www.courtlistener.com/api/rest/v4/people/
  columns:
    name_full: "Full name of the judge"
    political_affiliation: "Political party affiliation at time of appointment"

- schema: federal
  table: cl_fjc_cases
  comment: "Federal Judicial Center integrated database of federal court cases. One row per case."
  source: CourtListener API
  source_url: https://www.courtlistener.com/api/rest/v4/fjc-integrated-database/

- schema: federal
  table: cl_nypd_cases_sdny
  comment: "NYPD misconduct cases filed in Southern District of New York (SDNY). Includes parties, attorneys, documents."
  source: CourtListener API

- schema: federal
  table: cl_nypd_cases_edny
  comment: "NYPD misconduct cases filed in Eastern District of New York (EDNY). Includes parties, attorneys, documents."
  source: CourtListener API

- schema: federal
  table: cl_financial_disclosures
  comment: "Federal judge financial disclosures. Investments, gifts, debts, positions, spousal income."
  source: CourtListener API

# --- Federal: Census ACS (census.py, census_zcta.py) ---
- schema: federal
  table: acs_demographics
  comment: "American Community Survey 5-year demographics by NYC census tract. Population, race, income, housing."
  source: US Census Bureau API
  source_url: https://api.census.gov/data/2022/acs/acs5
  columns:
    geoid: "Census tract FIPS code. Format: STATE(2)+COUNTY(3)+TRACT(6). Example: 36061000100"
    total_pop: "Total population estimate for the tract"

# (Continue for all ~35 ACS tables with similar format)

- schema: federal
  table: acs_zcta_demographics
  comment: "ACS 5-year demographics by NYC ZIP code (ZCTA). Population, race, income, poverty, housing tenure."
  source: US Census Bureau API
  columns:
    zcta: "5-digit ZIP Code Tabulation Area. NYC ZIPs: 100xx (MN), 104xx (BX), 112xx (BK), 11xxx (QN), 103xx (SI)"

# --- Federal: Other ---
- schema: federal
  table: fec_contributions
  comment: "FEC individual campaign contributions from NYC donors. Bulk download, filtered to NY state."
  source: FEC Bulk Data
  source_url: https://www.fec.gov/data/browse-data/?tab=bulk-data
  columns:
    cmte_id: "FEC committee ID receiving the contribution"
    transaction_amt: "Contribution amount in USD"

- schema: federal
  table: college_scorecard_schools
  comment: "US Department of Education College Scorecard. One row per institution per year. Admissions, costs, outcomes."
  source: College Scorecard API
  source_url: https://collegescorecard.ed.gov/

- schema: federal
  table: epa_echo_facilities
  comment: "EPA ECHO environmental compliance facilities in NYC. Inspections, violations, enforcement actions."
  source: EPA ECHO API
  source_url: https://echo.epa.gov/

- schema: federal
  table: usaspending_contracts
  comment: "Federal contract awards to NYC-area recipients from USASpending.gov."
  source: USASpending API
  source_url: https://api.usaspending.gov/

- schema: federal
  table: usaspending_grants
  comment: "Federal grant awards to NYC-area recipients from USASpending.gov."
  source: USASpending API
  source_url: https://api.usaspending.gov/

- schema: federal
  table: littlesis_entities
  comment: "LittleSis power network entities — people and organizations with political/economic influence."
  source: LittleSis API
  source_url: https://littlesis.org/

- schema: federal
  table: littlesis_relationships
  comment: "LittleSis relationships between power entities — donations, board seats, lobbying, ownership ties."
  source: LittleSis API

- schema: federal
  table: propublica_nonprofits
  comment: "ProPublica Nonprofit Explorer — IRS-registered nonprofits in NY. Revenue, assets, EIN."
  source: ProPublica API
  source_url: https://projects.propublica.org/nonprofits/api

- schema: federal
  table: propublica_990s
  comment: "IRS Form 990 filings for NY nonprofits from ProPublica. Financial details by tax year."
  source: ProPublica API

- schema: federal
  table: nypd_ccrb_complaints
  comment: "NYPD CCRB misconduct complaint database with officer names (50-a repeal). One row per allegation."
  source: NYPD Misconduct GitHub
  source_url: https://github.com/new-york-civil-liberties-union/NYPD-Misconduct-Complaint-Database

- schema: federal
  table: nypd_ccrb_officers_current
  comment: "Current NYPD officers from CCRB active roster. Cross-reference with misconduct complaints."
  source: NYPD Misconduct GitHub

- schema: federal
  table: nys_death_index
  comment: "NYS Death Index 1957-1969. Historical death records with name, age, county, certificate number."
  source: NYS Open Data
  source_url: https://data.ny.gov/

- schema: federal
  table: police_settlements_538
  comment: "Police misconduct settlements from FiveThirtyEight. Payments, case types, departments nationwide."
  source: FiveThirtyEight GitHub
  source_url: https://github.com/fivethirtyeight/data/tree/master/police-settlements

- schema: federal
  table: name_index
  comment: "Cross-dataset name index for entity resolution. One row per unique name+source combination. Join to source tables via source_schema, source_table, source_id."
  source: Internal (entity_resolution.py)
  columns:
    canonical_name: "Standardized uppercase name used for matching"
    source_schema: "Schema of the source table this name came from"
    source_table: "Table name this name came from"

- schema: federal
  table: resolved_entities
  comment: "Splink entity resolution results. Probabilistic matches between names across all lake tables. Match weight indicates confidence."
  source: Internal (entity_resolution.py)
  columns:
    match_weight: "Splink match confidence. >6 = strong match, 2-6 = review, <2 = weak"
    cluster_id: "Entity cluster ID — all records in same cluster are the same person/entity"

# --- Foundation tables ---
- schema: foundation
  table: data_health
  comment: "Data freshness and quality metrics per table. Row counts, last update timestamps, staleness flags."
  source: Internal (quality_assets.py)

- schema: foundation
  table: h3_index
  comment: "H3 hexagonal spatial index. Maps lat/lng from source tables to H3 resolution-9 cells for spatial joins."
  source: Internal (foundation_assets.py)
  columns:
    h3_index: "H3 cell ID at resolution 9 (~174m hexagon)"
    source_table: "Original table this point came from"

- schema: foundation
  table: phonetic_index
  comment: "Double Metaphone phonetic index of names for fuzzy matching. One row per name variant."
  source: Internal (foundation_assets.py)

- schema: foundation
  table: row_fingerprints
  comment: "Content-hash fingerprints per row for deduplication. SHA-256 of key columns."
  source: Internal (foundation_assets.py)

# --- Housing: non-Socrata ---
- schema: housing
  table: fdny_vacate_list
  comment: "FDNY vacate orders — buildings ordered evacuated by Fire Department. Active and historical."
  source: Socrata (data.cityofnewyork.us)
```

- [ ] **Step 2: Validate YAML syntax**

```bash
cd ~/Desktop/dagster-pipeline && uv run python -c "import yaml; yaml.safe_load(open('data/catalog_comments.yaml'))" && echo "YAML valid"
```
Expected: "YAML valid"

- [ ] **Step 3: Commit**

```bash
git add data/catalog_comments.yaml
git commit -m "feat: add hand-written descriptions for non-Socrata tables"
```

---

## Task 7: Enhance MCP Server `describe_table` to Show Comments

**Note:** `data_catalog` already searches `t.comment` via `DATA_CATALOG_SQL` — once comments are populated, keyword search will automatically match on table descriptions. No change needed there.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (describe_table function — search for `def describe_table`)

- [ ] **Step 1: Modify describe_table to include column comments**

In `mcp_server.py`, update the `describe_table` function's SQL query to include the `comment` column:

Change the SQL at line ~3107 from:
```sql
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
```

To:
```sql
SELECT c.column_name, c.data_type, c.is_nullable, dc.comment
FROM information_schema.columns c
LEFT JOIN duckdb_columns() dc
  ON dc.database_name = 'lake'
  AND dc.schema_name = c.table_schema
  AND dc.table_name = c.table_name
  AND dc.column_name = c.column_name
WHERE c.table_catalog = 'lake'
  AND c.table_schema = ?
  AND c.table_name = ?
ORDER BY c.ordinal_position
```

Also add the table-level comment to the summary text returned by the tool. After the `describe_table` function fetches rows, add:

```python
# Get table comment
table_comment = ""
try:
    _, tc_rows = _execute(pool, """
        SELECT comment FROM duckdb_tables()
        WHERE database_name = 'lake' AND schema_name = ? AND table_name = ?
    """, [schema, table])
    if tc_rows and tc_rows[0][0]:
        table_comment = tc_rows[0][0]
except Exception:
    pass
```

And include it in the returned ToolResult summary.

- [ ] **Step 2: Test via MCP tool call**

After deploying, verify:
```
describe_table("housing", "hpd_violations")
```
Should now show column comments alongside column names/types.

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: show table and column comments in describe_table MCP tool"
```

---

## Task 8: Run Generate + Apply on Production

This is the execution task — run the scripts against the real DuckLake.

- [ ] **Step 1: Fetch Socrata metadata (fetch-only mode first)**

```bash
cd ~/Desktop/dagster-pipeline
source <(sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc)
uv run python scripts/generate_comments.py --fetch-only --output data/socrata_raw_metadata.yaml
```

Review `data/socrata_raw_metadata.yaml` to verify metadata quality. Check that descriptions are non-empty for most datasets.

- [ ] **Step 2: Run full generation with Claude rewrite (one schema first)**

```bash
uv run python scripts/generate_comments.py --schema housing --output data/catalog_comments_housing.yaml
```

Review the output. Check that Claude's rewrites are concise, accurate, and include join keys + enum values.

- [ ] **Step 3: Run full generation for all schemas**

```bash
uv run python scripts/generate_comments.py --output data/catalog_comments.yaml
```

This will take ~15-20 minutes (208 Socrata API calls + 208 Claude API calls with rate limiting).

- [ ] **Step 4: Merge non-Socrata entries into manifest**

Manually merge the hand-written non-Socrata entries (from Task 6) into `data/catalog_comments.yaml`.

- [ ] **Step 5: Review the complete manifest**

Open `data/catalog_comments.yaml` and review:
- Table comments are 1-2 sentences, under 200 chars
- Column comments include enum values for coded columns
- Join keys mention target tables
- No hallucinated column names

- [ ] **Step 6: Dry run apply**

```bash
uv run python scripts/apply_comments.py --dry-run
```

Review the generated SQL. Should be ~270 `COMMENT ON TABLE` + ~500-1000 `COMMENT ON COLUMN` statements.

- [ ] **Step 7: Apply to production DuckLake**

```bash
uv run python scripts/apply_comments.py
```

Expected output: "Done: ~270 tables, ~500+ columns commented"

- [ ] **Step 8: Verify via MCP**

Test that comments are visible:
```
data_catalog("violations")  → should now match on table comments
describe_table("housing", "hpd_violations")  → should show column comments
```

- [ ] **Step 9: Commit manifest + deploy MCP server**

```bash
git add data/catalog_comments.yaml
git commit -m "feat: populate DuckLake with LLM-optimized table and column comments"
```

Deploy updated MCP server (with Task 7 changes) to Hetzner.

---

## Summary

| Task | What | Output |
|------|------|--------|
| 1 | Socrata metadata fetcher | `fetch_socrata_metadata()` function |
| 2 | Column profiler | `profile_table_columns()` function |
| 3 | Claude AI rewriter | `rewrite_with_claude()` + prompt template |
| 4 | Manifest generator CLI | `generate_comments.py` main() |
| 5 | Apply comments script | `apply_comments.py` |
| 6 | Non-Socrata descriptions | Hand-written YAML entries |
| 7 | MCP server enhancement | `describe_table` shows comments |
| 8 | Production execution | Comments live in DuckLake |
