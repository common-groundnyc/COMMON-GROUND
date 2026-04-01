"""Natural language to SQL pipeline for the Common Ground data lake."""

from __future__ import annotations

import json
import re
import time
import urllib.request
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
    """Fetch full column schemas + COMMENT ON metadata for selected tables."""
    results = []
    for qualified in tables:
        parts = qualified.split(".")
        if len(parts) != 2:
            continue
        schema, table = parts
        try:
            with pool.cursor() as cur:
                tc_row = cur.execute(
                    "SELECT comment FROM duckdb_tables() "
                    "WHERE database_name = 'lake' AND schema_name = ? AND table_name = ?",
                    [schema, table],
                ).fetchone()
                table_comment = tc_row[0] if tc_row and tc_row[0] else None

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
    """Format table schemas into a text context block for the LLM prompt."""
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


# ---------------------------------------------------------------------------
# Prompt composition
# ---------------------------------------------------------------------------

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
    """Compose the full prompt for SQL generation."""
    return f"""{_SYSTEM_PROMPT}

## Available Tables

{schema_context}

## Examples

{examples}

## Question

{question}

SQL:"""


def generate_sql(question: str, schema_context: str, api_key: str, model: str = "gemini-2.5-flash") -> str:
    """Call Gemini to generate SQL from a natural language question."""
    from shared.nl_examples import format_examples

    examples = format_examples(4)
    prompt = compose_prompt(question, schema_context, examples)

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 1024},
    }).encode()

    req = urllib.request.Request(
        f"{url}?key={api_key}",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())

    text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3].strip()
    if text.lower().startswith("sql\n"):
        text = text[4:].strip()

    return text


# ---------------------------------------------------------------------------
# SQL validation + self-correction
# ---------------------------------------------------------------------------

def validate_generated_sql(sql: str) -> bool:
    """Check if generated SQL is safe and syntactically reasonable."""
    if not sql or not sql.strip():
        return False
    stripped = sql.strip().rstrip(";")
    if not re.match(r"^\s*(SELECT|WITH)\b", stripped, re.IGNORECASE):
        return False
    try:
        from shared.validation import validate_sql
    except ImportError:
        # fastmcp not available in this environment — skip deep validation
        return True
    try:
        validate_sql(stripped)
    except Exception:
        return False
    return True


def try_explain(pool, sql: str) -> str | None:
    """Try to EXPLAIN the SQL without executing. Returns None if valid, error string if not."""
    try:
        with pool.cursor() as cur:
            cur.execute(f"EXPLAIN {sql.strip().rstrip(';')}")
        return None
    except Exception as e:
        return str(e)


def compose_correction_prompt(original_question: str, bad_sql: str, error: str, schema_context: str) -> str:
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
