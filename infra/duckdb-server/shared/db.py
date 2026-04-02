"""Database helpers — CursorPool re-export, execute/safe_query wrappers, placeholders."""

import datetime
import decimal
import os
import re
import threading
import time

import duckdb
from fastmcp.exceptions import ToolError

from cursor_pool import CursorPool  # re-export
from sql_utils import sanitize_error
from shared.types import MAX_QUERY_ROWS, RECONNECT_ERRORS


def _normalize_cell(v: object) -> object:
    """Coerce DuckDB-native types to JSON-safe Python primitives.

    Prevents downstream issues like datetime.date vs str comparisons,
    Decimal vs float mismatches, and UUID objects in sort keys.
    """
    if v is None:
        return None
    if isinstance(v, datetime.date):
        return v.isoformat()  # date/datetime → "2026-04-01" / "2026-04-01T12:00:00"
    if isinstance(v, datetime.timedelta):
        return str(v)
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.hex()
    return v


def _normalize_rows(rows: list[tuple]) -> list[tuple]:
    """Apply _normalize_cell to every value in every row."""
    return [tuple(_normalize_cell(v) for v in row) for row in rows]

__all__ = ["CursorPool", "execute", "safe_query", "fill_placeholders", "build_catalog"]

# ---------------------------------------------------------------------------
# Module-level reconnect state (mirrors mcp_server.py globals)
# ---------------------------------------------------------------------------

_reconnect_lock = threading.Lock()
_last_reconnect: float = 0


def execute(pool: CursorPool, sql: str, params: list | None = None, *, schema_descriptions: dict | None = None) -> tuple[list, list]:
    """Execute SQL via *pool*, return (cols, rows). Handles S3/DuckLake reconnect."""
    global _last_reconnect
    with pool.cursor() as cur:
        try:
            result = cur.execute(sql, params or [])
            cols = [d[0] for d in result.description] if result.description else []
            rows = _normalize_rows(result.fetchmany(MAX_QUERY_ROWS))
            return cols, rows
        except duckdb.Error as e:
            err = str(e)

            # S3/DuckLake stale connection — auto-reconnect (max once per 60s)
            if any(sig in err for sig in RECONNECT_ERRORS):
                with _reconnect_lock:
                    if time.time() - _last_reconnect > 60:
                        _last_reconnect = time.time()
                        try:
                            print("Auto-reconnect: DuckLake S3 error, re-attaching catalog...", flush=True)
                            with pool.cursor() as rc:
                                rc.execute("DETACH lake")
                                pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "''")
                                rc.execute(f"""
                                    ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
                                    AS lake (METADATA_SCHEMA 'lake')
                                """)
                            print("Auto-reconnect: DuckLake re-attached successfully", flush=True)
                            # Retry the original query
                            with pool.cursor() as retry_cur:
                                result = retry_cur.execute(sql, params or [])
                                cols = [d[0] for d in result.description] if result.description else []
                                rows = _normalize_rows(result.fetchmany(MAX_QUERY_ROWS))
                                return cols, rows
                        except Exception as reconnect_err:
                            print(f"Auto-reconnect failed: {reconnect_err}", flush=True)

            # Improved error hints
            hint = ""
            schemas = schema_descriptions or {}
            if "does not exist" in err.lower():
                if "schema" in err.lower():
                    schema_match = re.search(r'schema "(\w+)"', err, re.IGNORECASE)
                    if schema_match:
                        wrong_schema = schema_match.group(1)
                        real_schemas = list(schemas.keys())
                        suggestions = [s for s in real_schemas if wrong_schema.lower() in s.lower() or s.lower() in wrong_schema.lower()]
                        if suggestions:
                            hint = f" Schema '{wrong_schema}' doesn't exist. Did you mean: {', '.join(suggestions)}? Use list_schemas() to see all schemas."
                        else:
                            hint = f" Schema '{wrong_schema}' doesn't exist. Available schemas: {', '.join(real_schemas[:6])}... Use list_schemas() for the full list."
                    else:
                        hint = " Use list_schemas() to see available schemas, then data_catalog(keyword) to find tables."
                elif "table" in err.lower():
                    hint = " Use data_catalog(keyword) to find table names, or list_tables(schema) to browse a schema."
                elif "column" in err.lower() or "not found" in err.lower():
                    hint = " Use describe_table(schema, table) to see exact column names before querying."
                else:
                    hint = " Use data_catalog(keyword) to find table names, or list_tables(schema) to browse a schema."
            elif "not found" in err.lower():
                hint = " Use describe_table(schema, table) to check column names."
            elif "permission" in err.lower() or "read-only" in err.lower():
                hint = " Only SELECT queries are allowed. Use sql_query() for reads."
            elif any(sig in err for sig in RECONNECT_ERRORS):
                hint = " Data temporarily unavailable — try again in a moment."
            raise ToolError(f"SQL error: {sanitize_error(str(e))}{hint}")


def safe_query(pool: CursorPool, sql: str, params: list | None = None) -> tuple[list, list]:
    """Execute SQL, return (cols, rows) or ([], []) if table doesn't exist."""
    try:
        return execute(pool, sql, params)
    except ToolError:
        return [], []


def parallel_queries(
    pool: CursorPool,
    queries: list[tuple[str, str, list | None]],
) -> dict[str, tuple[list, list]]:
    """Run multiple (name, sql, params) queries concurrently, return {name: (cols, rows)}."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results: dict[str, tuple[list, list]] = {}

    def _run(name: str, sql: str, params: list | None) -> tuple[str, tuple[list, list]]:
        return name, safe_query(pool, sql, params)

    with ThreadPoolExecutor(max_workers=min(len(queries), 20)) as ex:
        futures = {
            ex.submit(_run, name, sql, params): name
            for name, sql, params in queries
        }
        for fut in as_completed(futures):
            name, result = fut.result()
            results[name] = result

    return results


def build_catalog(conn) -> dict:
    """Build schema -> table -> {row_count, column_count} catalog from DuckLake."""
    catalog: dict = {}
    try:
        cur = conn.execute("""
            SELECT schema_name, table_name, estimated_size, column_count
            FROM duckdb_tables()
            WHERE database_name = 'lake'
              AND schema_name NOT IN ('information_schema', 'pg_catalog')
            ORDER BY schema_name, table_name
        """)
        for schema, table, est_size, col_count in cur.fetchall():
            catalog.setdefault(schema, {})[table] = {
                "row_count": est_size or 0,
                "column_count": col_count or 0,
            }
    except Exception as e:
        print(f"Warning: catalog cache build failed: {e}", flush=True)
    return catalog


def fill_placeholders(sql_template: str, bbls: list[str]) -> str:
    """Replace {placeholders} with ?,?,? for the BBL list."""
    ph = ",".join(["?"] * len(bbls))
    return sql_template.replace("{placeholders}", ph)
