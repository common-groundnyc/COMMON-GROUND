# Semantic Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `query(mode="nl")` that translates natural language questions into validated DuckDB SQL using vector-matched table selection, schema context injection, and LLM-powered SQL generation with self-correction.

**Architecture:** When a user submits a natural language question, the system: (1) embeds the question using the existing Gemini embedder, (2) vector-searches `catalog_embeddings` to find the top-5 most relevant tables, (3) fetches full column schemas + COMMENT ON metadata for those tables, (4) composes a prompt with schema context + few-shot examples + the question, (5) calls Gemini Flash to generate SQL, (6) validates the SQL with DuckDB EXPLAIN, (7) on error retries with the error message (max 2 retries), (8) executes and returns results. Zero new dependencies — uses existing embedder, HNSW index, and DuckDB.

**Tech Stack:** DuckDB 1.5.1, FastMCP, Gemini API (existing embedder infrastructure), existing `catalog_embeddings` HNSW index

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `infra/duckdb-server/mcp_server.py` | Modify | Expand `_build_catalog_embeddings()` to embed all 294 table descriptions (not just 11 schemas) |
| `infra/duckdb-server/tools/nl_query.py` | Create | NL-to-SQL pipeline: table selection, prompt composition, SQL generation, validation, self-correction |
| `infra/duckdb-server/tools/query.py` | Modify | Add `mode="nl"` dispatch to the query() super tool |
| `infra/duckdb-server/shared/nl_examples.py` | Create | Curated few-shot SQL examples for prompt engineering |
| `infra/duckdb-server/tests/test_nl_query.py` | Create | Unit tests for table selection, prompt composition, SQL validation |

---

### Task 0: Embed All Table Descriptions into catalog_embeddings

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` — `_build_catalog_embeddings()` function (line ~536)

Currently `_build_catalog_embeddings()` embeds only 11 schema-level descriptions (SCHEMA_DESCRIPTIONS dict) with `table_name='__schema__'`. The vector search in `select_tables()` needs per-table embeddings to distinguish between 294 tables.

- [ ] **Step 1: Read the existing function** at `mcp_server.py:536-551`

- [ ] **Step 2: Expand `_build_catalog_embeddings()` to also embed per-table descriptions**

After the existing schema embedding loop, add a second pass that:
1. Queries `duckdb_tables()` for all tables in the `lake` database
2. For tables WITH a comment, uses the comment as the description
3. For tables WITHOUT a comment, auto-generates: `"{schema} — {table_name} table"` (basic but better than nothing)
4. Embeds all table descriptions and inserts with `table_name` set to the actual table name (not `'__schema__'`)
5. Uses incremental embedding — skip tables already in `catalog_embeddings`

```python
def _build_catalog_embeddings(emb_conn, embed_batch, dims, read_conn=None):
    """Embed schema + table descriptions — schemas always rebuilt, tables incremental."""
    # --- Schema-level (always rebuild, tiny) ---
    schema_rows = []
    for schema_name, description in SCHEMA_DESCRIPTIONS.items():
        schema_rows.append((schema_name, "__schema__", description))

    # --- Table-level (incremental) ---
    table_rows = []
    if read_conn:
        try:
            existing = set()
            try:
                existing_rows = emb_conn.execute(
                    "SELECT schema_name, table_name FROM catalog_embeddings WHERE table_name != '__schema__'"
                ).fetchall()
                existing = {(r[0], r[1]) for r in existing_rows}
            except Exception:
                pass

            lake_tables = read_conn.execute("""
                SELECT schema_name, table_name, comment
                FROM duckdb_tables()
                WHERE database_name = 'lake'
                  AND schema_name NOT IN ('information_schema', 'pg_catalog', 'ducklake', 'public')
                ORDER BY schema_name, table_name
            """).fetchall()

            for schema, table, comment in lake_tables:
                if (schema, table) in existing:
                    continue
                desc = comment if comment else f"{schema} — {table.replace('_', ' ')} table"
                table_rows.append((schema, table, desc))
        except Exception as e:
            print(f"  Warning: table description scan failed: {e}", flush=True)

    # --- Embed schemas (always) ---
    if schema_rows:
        texts = [r[2] for r in schema_rows]
        vecs = embed_batch(texts)
        emb_conn.execute("DELETE FROM catalog_embeddings WHERE table_name = '__schema__'")
        for (s, t, d), vec in zip(schema_rows, vecs):
            emb_conn.execute(
                f"INSERT INTO catalog_embeddings VALUES (?, ?, ?, ?::FLOAT[{dims}])",
                [s, t, d, vec.tolist()],
            )

    # --- Embed new tables (incremental) ---
    if table_rows:
        BATCH = 100
        for i in range(0, len(table_rows), BATCH):
            batch = table_rows[i:i + BATCH]
            texts = [r[2] for r in batch]
            vecs = embed_batch(texts)
            for (s, t, d), vec in zip(batch, vecs):
                emb_conn.execute(
                    f"INSERT INTO catalog_embeddings VALUES (?, ?, ?, ?::FLOAT[{dims}])",
                    [s, t, d, vec.tolist()],
                )

    total = emb_conn.execute("SELECT COUNT(*) FROM catalog_embeddings").fetchone()[0]
    new_tables = len(table_rows)
    print(f"  Catalog embeddings: {total} rows ({len(schema_rows)} schemas, {new_tables} new tables)", flush=True)
```

- [ ] **Step 3: Update the call site** at `mcp_server.py:1702`

The existing call is:
```python
_build_catalog_embeddings(emb_conn, embed_batch_fn, embed_dims)
```

Change to pass `read_conn` (the main DuckLake connection):
```python
_build_catalog_embeddings(emb_conn, embed_batch_fn, embed_dims, read_conn=conn)
```

Where `conn` is the main DuckDB connection available in the lifespan context (the same one used for `_build_description_embeddings`).

- [ ] **Step 4: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(semantic-layer): embed all 294 table descriptions into catalog_embeddings"
```

---

### Task 1: Table Selection via Vector Search

**Files:**
- Create: `infra/duckdb-server/tests/test_nl_query.py`
- Create: `infra/duckdb-server/tools/nl_query.py`

The first step of NL-to-SQL is finding which tables are relevant. The server already has `catalog_embeddings` (schema descriptions embedded as Gemini 256d vectors). We need a function that embeds a question and returns the top-K matching tables with their full column schemas.

- [ ] **Step 1: Write failing tests for table selection**

```python
# infra/duckdb-server/tests/test_nl_query.py
import pytest


def test_build_schema_context_formats_correctly():
    """Schema context should include table name, comment, and columns."""
    from tools.nl_query import build_schema_context

    table_info = {
        "schema": "housing",
        "table": "hpd_violations",
        "comment": "HPD housing violations filed against NYC buildings",
        "columns": [
            {"name": "boroid", "type": "VARCHAR", "comment": "Borough code"},
            {"name": "block", "type": "VARCHAR", "comment": None},
            {"name": "violationid", "type": "BIGINT", "comment": "Unique violation ID"},
        ],
    }
    result = build_schema_context([table_info])
    assert "lake.housing.hpd_violations" in result
    assert "HPD housing violations" in result
    assert "boroid" in result
    assert "VARCHAR" in result
    assert "Borough code" in result


def test_build_schema_context_handles_no_comment():
    """Tables without comments should still render cleanly."""
    from tools.nl_query import build_schema_context

    table_info = {
        "schema": "recreation",
        "table": "parks",
        "comment": None,
        "columns": [
            {"name": "park_name", "type": "VARCHAR", "comment": None},
        ],
    }
    result = build_schema_context([table_info])
    assert "lake.recreation.parks" in result
    assert "park_name" in result


def test_build_schema_context_multiple_tables():
    """Multiple tables should all appear in context."""
    from tools.nl_query import build_schema_context

    tables = [
        {"schema": "housing", "table": "hpd_violations", "comment": "Violations",
         "columns": [{"name": "id", "type": "BIGINT", "comment": None}]},
        {"schema": "housing", "table": "hpd_complaints", "comment": "Complaints",
         "columns": [{"name": "id", "type": "BIGINT", "comment": None}]},
    ]
    result = build_schema_context(tables)
    assert "hpd_violations" in result
    assert "hpd_complaints" in result
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_nl_query.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write table selection and schema context functions**

```python
# infra/duckdb-server/tools/nl_query.py
"""Natural language to SQL pipeline for the Common Ground data lake."""

from __future__ import annotations

import time
from typing import Any


def select_tables(emb_conn, embed_fn, question: str, top_k: int = 5) -> list[str]:
    """Find the most relevant tables for a NL question via vector similarity.

    Uses the existing catalog_embeddings HNSW index.
    Returns list of "schema.table" strings.
    """
    query_vec = embed_fn(question)
    rows = emb_conn.execute(
        "SELECT schema_name, table_name, "
        "array_cosine_distance(embedding, ?::FLOAT[]) AS distance "
        "FROM catalog_embeddings "
        "WHERE table_name != '__schema__' "
        "ORDER BY distance "
        "LIMIT ?",
        [query_vec.tolist(), top_k],
    ).fetchall()
    return [f"{r[0]}.{r[1]}" for r in rows]


def fetch_table_schemas(pool, tables: list[str]) -> list[dict]:
    """Fetch full column schemas + COMMENT ON metadata for selected tables.

    Args:
        pool: CursorPool instance
        tables: list of "schema.table" strings

    Returns:
        list of {schema, table, comment, columns: [{name, type, comment}]}
    """
    results = []
    for qualified in tables:
        parts = qualified.split(".")
        if len(parts) != 2:
            continue
        schema, table = parts
        try:
            with pool.cursor() as cur:
                # Table comment
                tc_row = cur.execute(
                    "SELECT comment FROM duckdb_tables() "
                    "WHERE database_name = 'lake' AND schema_name = ? AND table_name = ?",
                    [schema, table],
                ).fetchone()
                table_comment = tc_row[0] if tc_row and tc_row[0] else None

                # Column schemas
                col_rows = cur.execute(
                    "SELECT c.column_name, c.data_type, dc.comment "
                    "FROM information_schema.columns c "
                    "LEFT JOIN duckdb_columns() dc "
                    "  ON dc.database_name = 'lake' "
                    "  AND dc.schema_name = c.table_schema "
                    "  AND dc.table_name = c.table_name "
                    "  AND dc.column_name = c.column_name "
                    "WHERE c.table_catalog = 'lake' "
                    "  AND c.table_schema = ? AND c.table_name = ? "
                    "ORDER BY c.ordinal_position",
                    [schema, table],
                ).fetchall()

                results.append({
                    "schema": schema,
                    "table": table,
                    "comment": table_comment,
                    "columns": [
                        {"name": r[0], "type": r[1], "comment": r[2]}
                        for r in col_rows
                    ],
                })
        except Exception:
            continue
    return results


def build_schema_context(table_schemas: list[dict]) -> str:
    """Format table schemas into a text context block for the LLM prompt.

    Produces a readable schema description that an LLM can use to write SQL.
    """
    sections = []
    for t in table_schemas:
        qualified = f"lake.{t['schema']}.{t['table']}"
        header = f"### {qualified}"
        if t.get("comment"):
            header += f"\n{t['comment']}"

        col_lines = []
        for c in t.get("columns", []):
            line = f"  - {c['name']} ({c['type']})"
            if c.get("comment"):
                line += f" — {c['comment']}"
            col_lines.append(line)

        sections.append(header + "\n" + "\n".join(col_lines))

    return "\n\n".join(sections)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_nl_query.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/nl_query.py infra/duckdb-server/tests/test_nl_query.py
git commit -m "feat(semantic-layer): add table selection and schema context builder"
```

---

### Task 2: Few-Shot SQL Examples

**Files:**
- Create: `infra/duckdb-server/shared/nl_examples.py`

Curated example queries that teach the LLM how to write correct DuckDB SQL for this lake. These are drawn from existing `EXPLORATION_QUERIES`, `suggest()` tool examples, and the named SQL constants in building/neighborhood tools.

- [ ] **Step 1: Create the examples file**

```python
# infra/duckdb-server/shared/nl_examples.py
"""Few-shot SQL examples for NL-to-SQL prompt engineering.

Each example is a (question, sql) pair demonstrating correct DuckDB SQL
against the Common Ground lake. Covers common patterns: aggregation,
joins, filtering, ILIKE, TRY_CAST, GROUP BY ALL, and lake.schema.table
qualified names.
"""

NL_SQL_EXAMPLES: list[tuple[str, str]] = [
    (
        "What are the worst buildings by open violations?",
        "SELECT ownername, bbl, address, "
        "COUNT(*) FILTER (WHERE currentstatus = 'OPEN') AS open_violations, "
        "COUNT(*) AS total_violations "
        "FROM lake.housing.hpd_violations "
        "GROUP BY ownername, bbl, address "
        "HAVING open_violations > 0 "
        "ORDER BY open_violations DESC "
        "LIMIT 20",
    ),
    (
        "How many 311 noise complaints per ZIP code?",
        "SELECT incident_zip, COUNT(*) AS complaints "
        "FROM lake.social_services.n311_service_requests "
        "WHERE complaint_type ILIKE '%noise%' "
        "AND incident_zip IS NOT NULL "
        "GROUP BY incident_zip "
        "ORDER BY complaints DESC "
        "LIMIT 20",
    ),
    (
        "Which restaurants failed their last inspection?",
        "SELECT DISTINCT dba, building || ' ' || street AS address, "
        "boro, zipcode, grade, inspection_date "
        "FROM lake.health.restaurant_inspections "
        "WHERE grade IN ('C', 'Z', 'P') "
        "ORDER BY inspection_date DESC "
        "LIMIT 20",
    ),
    (
        "Top campaign donors in NYC",
        "SELECT name, SUM(TRY_CAST(amnt AS DOUBLE)) AS total_donated, "
        "COUNT(*) AS num_donations, "
        "LIST(DISTINCT recipname) AS recipients "
        "FROM lake.city_government.campaign_contributions "
        "WHERE TRY_CAST(amnt AS DOUBLE) > 0 "
        "GROUP BY name "
        "ORDER BY total_donated DESC "
        "LIMIT 20",
    ),
    (
        "Crime counts by precinct in Brooklyn",
        "SELECT addr_pct_cd AS precinct, "
        "COUNT(*) AS total_crimes, "
        "COUNT(*) FILTER (WHERE ofns_desc ILIKE '%assault%') AS assaults, "
        "COUNT(*) FILTER (WHERE ofns_desc ILIKE '%robbery%') AS robberies "
        "FROM lake.public_safety.nypd_complaint_data "
        "WHERE boro_nm = 'BROOKLYN' "
        "GROUP BY precinct "
        "ORDER BY total_crimes DESC "
        "LIMIT 20",
    ),
    (
        "Average city employee salary by agency",
        "SELECT agency_name, "
        "ROUND(AVG(TRY_CAST(base_salary AS DOUBLE)), 0) AS avg_salary, "
        "COUNT(*) AS employees "
        "FROM lake.city_government.citywide_payroll "
        "WHERE fiscal_year = '2025' "
        "GROUP BY agency_name "
        "ORDER BY avg_salary DESC "
        "LIMIT 20",
    ),
    (
        "Property sales over $10 million in Manhattan",
        "SELECT p.name AS party, m.doc_type, "
        "TRY_CAST(m.document_amt AS DOUBLE) AS amount, "
        "m.document_date, l.street_name "
        "FROM lake.housing.acris_parties p "
        "JOIN lake.housing.acris_master m ON p.document_id = m.document_id "
        "JOIN lake.housing.acris_legals l ON p.document_id = l.document_id "
        "WHERE m.doc_type IN ('DEED', 'DEED, RP') "
        "AND TRY_CAST(m.document_amt AS DOUBLE) > 10000000 "
        "AND l.borough = '1' "
        "ORDER BY amount DESC "
        "LIMIT 20",
    ),
    (
        "How many evictions happened in each borough last year?",
        "SELECT eviction_zip, COUNT(*) AS evictions "
        "FROM lake.housing.evictions "
        "WHERE executed_date >= '2025-01-01' "
        "GROUP BY eviction_zip "
        "ORDER BY evictions DESC "
        "LIMIT 20",
    ),
]


def format_examples(n: int = 4) -> str:
    """Format the first n examples as a prompt section."""
    lines = []
    for question, sql in NL_SQL_EXAMPLES[:n]:
        lines.append(f"Question: {question}")
        lines.append(f"SQL: {sql}")
        lines.append("")
    return "\n".join(lines)
```

- [ ] **Step 2: Write a test**

```python
# Append to infra/duckdb-server/tests/test_nl_query.py

def test_format_examples_returns_pairs():
    from shared.nl_examples import format_examples, NL_SQL_EXAMPLES
    result = format_examples(3)
    assert "Question:" in result
    assert "SQL:" in result
    assert result.count("Question:") == 3
    assert len(NL_SQL_EXAMPLES) >= 8
```

- [ ] **Step 3: Run tests**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_nl_query.py -v`
Expected: All 4 tests PASS

- [ ] **Step 4: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/shared/nl_examples.py infra/duckdb-server/tests/test_nl_query.py
git commit -m "feat(semantic-layer): add curated few-shot SQL examples"
```

---

### Task 3: Prompt Composition and SQL Generation

**Files:**
- Modify: `infra/duckdb-server/tools/nl_query.py`
- Modify: `infra/duckdb-server/tests/test_nl_query.py`

Build the prompt that combines schema context + few-shot examples + the user's question, and call Gemini to generate SQL.

- [ ] **Step 1: Write failing tests for prompt composition**

```python
# Append to infra/duckdb-server/tests/test_nl_query.py

def test_compose_prompt_includes_all_sections():
    from tools.nl_query import compose_prompt
    result = compose_prompt(
        question="How many violations in Brooklyn?",
        schema_context="### lake.housing.hpd_violations\n  - boroid (VARCHAR)\n  - status (VARCHAR)",
        examples="Question: count by borough\nSQL: SELECT ...",
    )
    assert "How many violations in Brooklyn?" in result
    assert "lake.housing.hpd_violations" in result
    assert "Question: count by borough" in result
    assert "DuckDB" in result  # should mention DuckDB dialect
    assert "lake." in result  # should mention qualified table names


def test_compose_prompt_includes_rules():
    from tools.nl_query import compose_prompt
    result = compose_prompt(
        question="test",
        schema_context="test",
        examples="test",
    )
    # Must include key rules for correct SQL generation
    assert "SELECT" in result  # mention SELECT-only
    assert "LIMIT" in result   # mention LIMIT
    assert "TRY_CAST" in result  # DuckDB-specific
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_nl_query.py::test_compose_prompt_includes_all_sections -v`
Expected: FAIL

- [ ] **Step 3: Implement prompt composition and SQL generation**

```python
# Add to infra/duckdb-server/tools/nl_query.py

import json
import requests

from shared.nl_examples import format_examples


_SYSTEM_PROMPT = """You are a SQL expert for the Common Ground NYC open data lake.
The database is DuckDB. All tables use the format: lake.schema.table_name

Rules:
- Write ONLY a single SELECT statement. No DDL, no INSERT, no UPDATE, no DELETE.
- Always use lake.schema.table_name (e.g., lake.housing.hpd_violations)
- Use TRY_CAST() instead of CAST() for type conversions (handles nulls gracefully)
- Use ILIKE for case-insensitive string matching
- Always include LIMIT (default 20, max 100)
- Use GROUP BY ALL when grouping (DuckDB shortcut)
- For counts with conditions, use COUNT(*) FILTER (WHERE ...)
- Return ONLY the SQL query, no explanation, no markdown, no backticks."""


def compose_prompt(question: str, schema_context: str, examples: str) -> str:
    """Compose the full prompt for SQL generation.

    Combines system rules, schema context, few-shot examples, and the question.
    """
    return f"""{_SYSTEM_PROMPT}

## Available Tables

{schema_context}

## Examples

{examples}

## Question

{question}

SQL:"""


def generate_sql(
    question: str,
    schema_context: str,
    api_key: str,
    model: str = "gemini-2.5-flash",
) -> str:
    """Call Gemini to generate SQL from a natural language question.

    Args:
        question: The user's natural language question
        schema_context: Formatted table schemas from build_schema_context()
        api_key: Gemini API key
        model: Gemini model name

    Returns:
        Generated SQL string (cleaned of markdown fences)
    """
    examples = format_examples(4)
    prompt = compose_prompt(question, schema_context, examples)

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    resp = requests.post(
        f"{url}?key={api_key}",
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.0, "maxOutputTokens": 1024},
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3].strip()
    if text.lower().startswith("sql\n"):
        text = text[4:].strip()

    return text
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_nl_query.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/nl_query.py infra/duckdb-server/tests/test_nl_query.py
git commit -m "feat(semantic-layer): add prompt composition and Gemini SQL generation"
```

---

### Task 4: SQL Validation and Self-Correction

**Files:**
- Modify: `infra/duckdb-server/tools/nl_query.py`
- Modify: `infra/duckdb-server/tests/test_nl_query.py`

Validate generated SQL with DuckDB EXPLAIN before execution. On error, retry with the error message fed back to the LLM.

- [ ] **Step 1: Write failing tests**

```python
# Append to infra/duckdb-server/tests/test_nl_query.py

def test_validate_generated_sql_accepts_valid():
    from tools.nl_query import validate_generated_sql
    # Valid SQL should return True
    assert validate_generated_sql("SELECT 1") is True


def test_validate_generated_sql_rejects_ddl():
    from tools.nl_query import validate_generated_sql
    assert validate_generated_sql("DROP TABLE foo") is False


def test_validate_generated_sql_rejects_empty():
    from tools.nl_query import validate_generated_sql
    assert validate_generated_sql("") is False
    assert validate_generated_sql("   ") is False


def test_compose_correction_prompt_includes_error():
    from tools.nl_query import compose_correction_prompt
    result = compose_correction_prompt(
        original_question="How many violations?",
        bad_sql="SELECT * FROM lake.housing.violations",
        error="Table 'violations' does not exist",
        schema_context="### lake.housing.hpd_violations\n  - id (BIGINT)",
    )
    assert "violations" in result
    assert "does not exist" in result
    assert "hpd_violations" in result
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_nl_query.py::test_validate_generated_sql_accepts_valid -v`
Expected: FAIL

- [ ] **Step 3: Implement validation and self-correction**

```python
# Add to infra/duckdb-server/tools/nl_query.py

import re
from shared.validation import validate_sql


def validate_generated_sql(sql: str) -> bool:
    """Check if generated SQL is safe and syntactically reasonable.

    Returns True if the SQL passes basic validation, False otherwise.
    Does NOT execute the SQL — that's done separately with EXPLAIN.
    """
    if not sql or not sql.strip():
        return False
    stripped = sql.strip().rstrip(";")
    # Must start with SELECT or WITH
    if not re.match(r"^\s*(SELECT|WITH)\b", stripped, re.IGNORECASE):
        return False
    try:
        validate_sql(stripped)
        return True
    except Exception:
        return False


def try_explain(pool, sql: str) -> str | None:
    """Try to EXPLAIN the SQL without executing it.

    Returns None if valid, or the error message if invalid.
    """
    try:
        with pool.cursor() as cur:
            cur.execute(f"EXPLAIN {sql.strip().rstrip(';')}")
        return None
    except Exception as e:
        return str(e)


def compose_correction_prompt(
    original_question: str,
    bad_sql: str,
    error: str,
    schema_context: str,
) -> str:
    """Compose a prompt for correcting a failed SQL query."""
    return f"""{_SYSTEM_PROMPT}

## Available Tables

{schema_context}

## Previous Attempt

Question: {original_question}
Generated SQL: {bad_sql}
Error: {error}

Fix the SQL to avoid this error. Return ONLY the corrected SQL query.

SQL:"""
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_nl_query.py -v`
Expected: All 10 tests PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/nl_query.py infra/duckdb-server/tests/test_nl_query.py
git commit -m "feat(semantic-layer): add SQL validation and self-correction prompting"
```

---

### Task 5: End-to-End NL Query Pipeline

**Files:**
- Modify: `infra/duckdb-server/tools/nl_query.py`

Wire everything together into a single `nl_query()` function that the query tool will call.

- [ ] **Step 1: Implement the end-to-end pipeline**

```python
# Add to infra/duckdb-server/tools/nl_query.py

from fastmcp.tools.tool import ToolResult
from shared.formatting import make_result


def nl_query(
    question: str,
    pool,
    emb_conn,
    embed_fn,
    api_key: str,
    format: str = "text",
    ctx=None,
) -> ToolResult:
    """Full NL-to-SQL pipeline: table selection → SQL generation → validation → execution.

    Args:
        question: Natural language question
        pool: CursorPool for DuckDB queries
        emb_conn: Embeddings DuckDB connection (for catalog_embeddings)
        embed_fn: Function to embed text (Gemini)
        api_key: Gemini API key for SQL generation
        format: Output format (text, xlsx, csv)

    Returns:
        ToolResult with query results
    """
    t0 = time.time()

    # Step 1: Find relevant tables via vector similarity
    relevant_tables = select_tables(emb_conn, embed_fn, question, top_k=5)
    if not relevant_tables:
        return ToolResult(
            content="Could not find relevant tables for your question. "
            "Try rephrasing or use mode='catalog' to explore available data."
        )

    # Step 2: Fetch full schemas for selected tables
    table_schemas = fetch_table_schemas(pool, relevant_tables)
    if not table_schemas:
        return ToolResult(content="Could not load schema information for matched tables.")

    schema_context = build_schema_context(table_schemas)

    # Step 3: Generate SQL
    sql = generate_sql(question, schema_context, api_key)

    # Step 4: Validate
    if not validate_generated_sql(sql):
        return ToolResult(
            content=f"Generated SQL failed validation. Try rephrasing your question.\n\nGenerated: {sql}"
        )

    # Step 5: EXPLAIN check + self-correction (max 2 retries)
    explain_error = try_explain(pool, sql)
    retries = 0
    while explain_error and retries < 2:
        retries += 1
        correction_prompt = compose_correction_prompt(
            question, sql, explain_error, schema_context,
        )
        # Re-generate with error context
        sql = generate_sql(
            correction_prompt, schema_context="", api_key=api_key,
        )
        if not validate_generated_sql(sql):
            break
        explain_error = try_explain(pool, sql)

    if explain_error:
        return ToolResult(
            content=f"Could not generate valid SQL after {retries + 1} attempts.\n\n"
            f"Last SQL: {sql}\nError: {explain_error}\n\n"
            "Try rephrasing, or use mode='sql' to write SQL directly."
        )

    # Step 6: Execute
    elapsed_gen = round((time.time() - t0) * 1000)

    # Delegate to existing _sql handler for execution + export
    from tools.query import _sql as execute_sql
    result = execute_sql(pool, sql, format, ctx)

    # Annotate with generation metadata
    gen_note = (
        f"\n\n---\n*Generated from: \"{question}\"*\n"
        f"*Tables used: {', '.join(relevant_tables)}*\n"
        f"*Generation: {elapsed_gen}ms, {retries} retries*\n"
        f"```sql\n{sql}\n```"
    )

    if isinstance(result, ToolResult):
        return ToolResult(
            content=result.content + gen_note,
            structured_content=result.structured_content,
            meta={**(result.meta or {}), "nl_generation_ms": elapsed_gen,
                  "nl_retries": retries, "nl_tables": relevant_tables,
                  "nl_sql": sql},
        )
    # String result (from export paths)
    return result + gen_note
```

- [ ] **Step 2: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/nl_query.py
git commit -m "feat(semantic-layer): wire end-to-end NL-to-SQL pipeline"
```

---

### Task 6: Wire into query() Super Tool

**Files:**
- Modify: `infra/duckdb-server/tools/query.py`

Add `mode="nl"` to the query() tool's mode dispatch and parameter definition.

- [ ] **Step 1: Update the mode type to include "nl"**

In `infra/duckdb-server/tools/query.py`, find the `mode` parameter definition (around line 474) and add `"nl"`:

```python
    mode: Annotated[
        Literal["sql", "nl", "catalog", "schemas", "tables", "describe", "health", "admin"],
        Field(
            default="sql",
            description="'sql' executes read-only SQL against the lake. 'nl' translates a natural language question into SQL, validates it, and executes (e.g., 'how many violations in Brooklyn?'). 'catalog' searches table names and descriptions by keyword. 'schemas' lists all schemas. 'tables' lists tables in a schema. 'describe' shows columns and types for a table. 'health' shows lake freshness and row counts. 'admin' executes DDL (CREATE OR REPLACE VIEW only).",
        )
    ] = "sql",
```

- [ ] **Step 2: Add the nl dispatch case**

In the query() function body, after the `mode == "sql"` check (around line 494), add:

```python
    elif mode == "nl":
        from tools.nl_query import nl_query
        emb_conn = ctx.lifespan_context.get("emb_conn")
        embed_fn = ctx.lifespan_context.get("embed_fn")
        # Use first available Gemini key for SQL generation
        import embedder
        api_key = embedder._GEMINI_KEYS[0] if embedder._GEMINI_KEYS else ""
        if not embed_fn or not emb_conn:
            raise ToolError("Embeddings not available. NL query requires the embedding pipeline.")
        if not api_key:
            raise ToolError("No Gemini API key available for NL query generation.")
        return nl_query(input, pool, emb_conn, embed_fn, api_key, format, ctx=ctx)
```

- [ ] **Step 3: Update the tool docstring**

Update the query() docstring to mention NL mode:

```python
    """Execute SQL queries, discover schemas, search the catalog, and export data from the NYC data lake.
    294 tables across 14 schemas. Use mode='nl' for natural language questions (e.g., 'how many violations in Brooklyn?').
    Use mode='sql' for raw SQL. Use mode='catalog' to search for tables by keyword."""
```

- [ ] **Step 4: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/query.py
git commit -m "feat(semantic-layer): add mode='nl' to query() super tool"
```

---

### Task 7: Deploy and Test

**Files:** None (deployment + manual testing)

- [ ] **Step 1: Deploy updated files to Hetzner**

```bash
CONTAINER=$(ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 'docker ps --format "{{.Names}}" | grep duckdb-server')
scp -i ~/.ssh/id_ed25519_hetzner \
  infra/duckdb-server/tools/query.py \
  infra/duckdb-server/tools/nl_query.py \
  infra/duckdb-server/shared/nl_examples.py \
  fattie@178.156.228.119:/tmp/nl-update/

ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "
  docker cp /tmp/nl-update/query.py $CONTAINER:/app/tools/query.py
  docker cp /tmp/nl-update/nl_query.py $CONTAINER:/app/tools/nl_query.py
  docker cp /tmp/nl-update/nl_examples.py $CONTAINER:/app/shared/nl_examples.py
  docker restart $CONTAINER
"
```

Wait 90 seconds for server to become healthy.

- [ ] **Step 2: Test 5 natural language queries via MCP**

Test each of these via the `mcp__duckdb__query` tool with `mode="nl"`:

1. `"How many housing violations are there in each borough?"` — should hit hpd_violations, GROUP BY borough
2. `"Top 10 highest paid city employees"` — should hit citywide_payroll, ORDER BY salary DESC
3. `"Which ZIP codes have the most noise complaints?"` — should hit 311 requests, ILIKE noise
4. `"How many evictions happened in 2025?"` — should hit evictions, date filter
5. `"What restaurants got a C grade in Manhattan?"` — should hit restaurant_inspections, grade filter

For each, verify:
- Correct tables were selected
- SQL is valid DuckDB syntax
- Results are returned (not an error)
- The generation metadata shows tables used and SQL generated

- [ ] **Step 3: Commit test results as verification**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add -A
git commit -m "feat(semantic-layer): Phase 13 complete — NL-to-SQL via query(mode='nl')"
```
