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
