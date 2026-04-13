"""Database helpers — CursorPool re-export, execute/safe_query wrappers, placeholders."""

import datetime
import decimal
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

import duckdb
from fastmcp.exceptions import ToolError

QUERY_TIMEOUT_S = 60

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

__all__ = ["CursorPool", "execute", "safe_query", "fill_placeholders", "build_catalog",
           "set_catalog_cache"]

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_reconnect_lock = threading.Lock()
_last_reconnect: float = 0

# Catalog cache: set by mcp_server.py at startup via set_catalog_cache()
# schema -> table -> {row_count, column_count}
_catalog: dict[str, dict] = {}
# Flat lookup: all "schema.table" names lowercase
_all_tables: list[str] = []
# Column cache: "schema.table" -> [col_name, ...]
_column_cache: dict[str, list[str]] = {}


def set_catalog_cache(catalog: dict, conn=None) -> None:
    """Called once at startup by mcp_server.py to populate the fuzzy-match index."""
    global _catalog, _all_tables, _column_cache
    _catalog = catalog
    _all_tables = [
        f"{schema}.{table}"
        for schema, tables in catalog.items()
        for table in tables
    ]
    # Cache column names per table for column-level correction
    if conn is not None:
        try:
            rows = conn.execute("""
                SELECT schema_name, table_name, column_name
                FROM duckdb_columns()
                WHERE database_name = 'lake'
                  AND schema_name NOT IN ('information_schema', 'pg_catalog')
                  AND table_name NOT LIKE 'v!_%' ESCAPE '!'
                ORDER BY schema_name, table_name, column_index
            """).fetchall()
            for schema, table, col in rows:
                key = f"{schema}.{table}"
                _column_cache.setdefault(key, []).append(col)
        except Exception as e:
            print(f"Warning: column cache build failed: {e}", flush=True)
    print(f"SQL corrector ready: {len(_all_tables)} tables, "
          f"{sum(len(v) for v in _column_cache.values())} columns indexed", flush=True)


def _fuzzy_match(bad_name: str, candidates: list[str], threshold: float = 0.5) -> str | None:
    """Find the best fuzzy match for bad_name among candidates.

    Uses substring matching + Levenshtein-like scoring without external deps.
    Returns the best match or None if nothing is close enough.
    """
    bad = bad_name.lower()
    best_score = 0.0
    best_match = None

    for candidate in candidates:
        cand = candidate.lower()
        # Exact match
        if bad == cand:
            return candidate
        # Substring containment (strong signal)
        if bad in cand or cand in bad:
            # Score by length similarity
            score = min(len(bad), len(cand)) / max(len(bad), len(cand))
            score = score * 0.9 + 0.1  # boost substring matches
        else:
            # Token overlap (handles word reordering, extra/missing words)
            bad_tokens = set(bad.replace("_", " ").split())
            cand_tokens = set(cand.replace("_", " ").split())
            if not bad_tokens or not cand_tokens:
                continue
            overlap = len(bad_tokens & cand_tokens)
            score = overlap / max(len(bad_tokens), len(cand_tokens))

        if score > best_score:
            best_score = score
            best_match = candidate

    return best_match if best_score >= threshold else None


def _try_fix_sql(sql: str, error_msg: str) -> str | None:
    """Attempt to fix a SQL query based on the error message.

    Returns corrected SQL or None if unfixable.
    """
    err = error_msg.lower()

    # --- Table not found ---
    # Pattern: Table with name X does not exist
    table_match = re.search(
        r'table with name "?(\w+)"? does not exist',
        error_msg, re.IGNORECASE,
    )
    if table_match:
        bad_table = table_match.group(1)
        # Try to find which schema it was used with
        schema_match = re.search(
            rf'lake\.(\w+)\.{re.escape(bad_table)}',
            sql, re.IGNORECASE,
        )
        if schema_match:
            schema = schema_match.group(1)
            # Match against tables in that schema
            schema_tables = list(_catalog.get(schema, {}).keys())
            fix = _fuzzy_match(bad_table, schema_tables)
            if fix:
                old = f"lake.{schema}.{bad_table}"
                new = f"lake.{schema}.{fix}"
                fixed = re.sub(re.escape(old), new, sql, flags=re.IGNORECASE)
                print(f"SQL auto-correct: {old} → {new}", flush=True)
                return fixed
        else:
            # No schema prefix — search all tables
            all_table_names = [t.split(".")[-1] for t in _all_tables]
            fix = _fuzzy_match(bad_table, all_table_names)
            if fix:
                fixed = re.sub(
                    rf'\b{re.escape(bad_table)}\b', fix, sql, flags=re.IGNORECASE,
                )
                print(f"SQL auto-correct table: {bad_table} → {fix}", flush=True)
                return fixed

    # --- Column not found ---
    # Pattern: Referenced column "X" not found
    col_match = re.search(
        r'(?:column|referenced column) "?(\w+)"? not found',
        error_msg, re.IGNORECASE,
    )
    if col_match:
        bad_col = col_match.group(1)
        # Find which tables are in the query and check their columns
        for schema, tables in _catalog.items():
            for table in tables:
                full = f"lake.{schema}.{table}"
                if full.lower() in sql.lower() or table.lower() in sql.lower():
                    cols = _column_cache.get(f"{schema}.{table}", [])
                    if cols:
                        fix = _fuzzy_match(bad_col, cols)
                        if fix:
                            fixed = re.sub(
                                rf'\b{re.escape(bad_col)}\b', fix, sql,
                                count=0, flags=re.IGNORECASE,
                            )
                            print(f"SQL auto-correct column: {bad_col} → {fix} "
                                  f"(in {schema}.{table})", flush=True)
                            return fixed

    # --- Schema not found ---
    schema_match = re.search(
        r'schema "?(\w+)"? does not exist',
        error_msg, re.IGNORECASE,
    )
    if schema_match:
        bad_schema = schema_match.group(1)
        real_schemas = list(_catalog.keys())
        fix = _fuzzy_match(bad_schema, real_schemas)
        if fix:
            fixed = re.sub(
                rf'lake\.{re.escape(bad_schema)}\.',
                f"lake.{fix}.",
                sql, flags=re.IGNORECASE,
            )
            print(f"SQL auto-correct schema: {bad_schema} → {fix}", flush=True)
            return fixed

    return None


def execute(
    pool: CursorPool,
    sql: str,
    params: list | None = None,
    *,
    schema_descriptions: dict | None = None,
    _retried: bool = False,
) -> tuple[list, list]:
    """Execute SQL via *pool*, return (cols, rows).

    Handles: S3/DuckLake reconnect, fuzzy SQL correction, row normalization.
    """
    global _last_reconnect
    with pool.cursor() as cur:
        try:
            with ThreadPoolExecutor(1) as _qpool:
                try:
                    future = _qpool.submit(cur.execute, sql, params or [])
                    result = future.result(timeout=QUERY_TIMEOUT_S)
                except FuturesTimeoutError:
                    raise ToolError(
                        f"Query timed out after {QUERY_TIMEOUT_S}s. "
                        "Try a simpler query or add filters."
                    )
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
                                    AS lake (AUTOMATIC_MIGRATION TRUE)
                                """)
                            print("Auto-reconnect: DuckLake re-attached successfully", flush=True)
                            with pool.cursor() as retry_cur:
                                result = retry_cur.execute(sql, params or [])
                                cols = [d[0] for d in result.description] if result.description else []
                                rows = _normalize_rows(result.fetchmany(MAX_QUERY_ROWS))
                                return cols, rows
                        except Exception as reconnect_err:
                            print(f"Auto-reconnect failed: {reconnect_err}", flush=True)

            # --- Fuzzy SQL correction (one retry) ---
            if not _retried and ("does not exist" in err or "not found" in err):
                fixed_sql = _try_fix_sql(sql, err)
                if fixed_sql and fixed_sql != sql:
                    return execute(pool, fixed_sql, params,
                                   schema_descriptions=schema_descriptions,
                                   _retried=True)

            # Error hints for the LLM
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

    def _run(name: str, sql: str, params: list | None) -> tuple[str, tuple[list, list], float]:
        t = time.time()
        result = safe_query(pool, sql, params)
        elapsed = (time.time() - t) * 1000
        return name, result, elapsed

    with ThreadPoolExecutor(max_workers=min(len(queries), 16)) as ex:
        futures = {
            ex.submit(_run, name, sql, params): name
            for name, sql, params in queries
        }
        timings = []
        for fut in as_completed(futures):
            name, result, elapsed = fut.result()
            results[name] = result
            timings.append((name, elapsed))

    # Log per-query timings (sorted slowest first)
    timings.sort(key=lambda x: -x[1])
    parts = [f"{n}={ms:.0f}ms" for n, ms in timings]
    print(f"[parallel] {', '.join(parts)}", flush=True)

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
