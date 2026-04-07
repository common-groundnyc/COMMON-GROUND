"""Mosaic data server endpoint at /mosaic/query.

Validates queries via sqlglot AST walking before execution.
Read-only, allowlisted-schema queries only. Returns rows with real column names.
"""
import asyncio
import logging
from typing import Any

import sqlglot
import sqlglot.expressions as exp
from starlette.requests import Request
from starlette.responses import JSONResponse

from shared.db import execute

logger = logging.getLogger(__name__)

ALLOWED_SCHEMAS = frozenset({
    "housing",
    "public_safety",
    "social_services",
    "health",
    "foundation",
    "city_government",
    "education",
    "transportation",
    "environment",
})

MAX_QUERY_LEN = 10_000

_ALLOWED_STATEMENTS = (exp.Select, exp.Union, exp.Intersect, exp.Except)


class QueryRejected(Exception):
    """Raised when a query fails the allowlist check."""


def _validate_query(sql: str) -> None:
    """Parse and validate the SQL. Raises QueryRejected on any violation."""
    if not sql or not sql.strip():
        raise QueryRejected("empty query")
    if len(sql) > MAX_QUERY_LEN:
        raise QueryRejected("query too long")

    try:
        statements = sqlglot.parse(sql, dialect="duckdb")
    except sqlglot.errors.ParseError as exc:
        raise QueryRejected(f"parse error: {exc}") from exc

    if not statements:
        raise QueryRejected("no statement parsed")

    for stmt in statements:
        if stmt is None:
            raise QueryRejected("empty statement in batch")

        if not isinstance(stmt, _ALLOWED_STATEMENTS):
            raise QueryRejected(f"statement type not allowed: {type(stmt).__name__}")

        for node in stmt.walk():
            if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Drop,
                                  exp.Create, exp.Alter, exp.TruncateTable, exp.Merge,
                                  exp.Pragma, exp.Set, exp.Use, exp.Command, exp.Transaction)):
                raise QueryRejected(f"forbidden expression: {type(node).__name__}")

        for table in stmt.find_all(exp.Table):
            catalog_arg = table.args.get("catalog")
            schema_arg = table.args.get("db")
            catalog = catalog_arg.name.lower() if catalog_arg else ""
            schema = schema_arg.name.lower() if schema_arg else ""
            name = table.name or ""

            if catalog != "lake":
                raise QueryRejected(
                    f"table reference must be fully qualified as lake.<schema>.<table>: {table.sql()}"
                )
            if schema not in ALLOWED_SCHEMAS:
                raise QueryRejected(f"schema not allowed: {schema}")
            if name.startswith("_"):
                raise QueryRejected(f"internal table not allowed: {name}")


def is_query_allowed(sql: str) -> bool:
    """Backwards-compatible wrapper used by tests."""
    try:
        _validate_query(sql)
        return True
    except QueryRejected:
        return False


def _serialize_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return value


def _column_names(sql: str, count: int) -> list[str]:
    """Best-effort projection name extraction. Falls back to col0..colN."""
    try:
        statements = sqlglot.parse(sql, dialect="duckdb")
        if not statements or not isinstance(
            statements[0], (exp.Select, exp.Union, exp.Intersect, exp.Except)
        ):
            raise ValueError
        select = statements[0]
        while isinstance(select, (exp.Union, exp.Intersect, exp.Except)):
            select = select.this
        names: list[str] = []
        for projection in select.expressions:
            alias = projection.alias_or_name
            names.append(alias if alias else f"col{len(names)}")
        if len(names) != count:
            raise ValueError
        return names
    except Exception:
        return [f"col{i}" for i in range(count)]


def _run_mosaic_query(payload: dict[str, Any]) -> dict[str, Any]:
    """Execute a Mosaic query payload against DuckDB.

    Returns rows as dicts keyed by their actual column names.
    """
    sql: str = payload.get("sql", "")
    rows = execute(sql)
    if not rows:
        return {"data": []}

    columns = _column_names(sql, len(rows[0]))
    data = [
        {columns[idx]: _serialize_value(value) for idx, value in enumerate(row)}
        for row in rows
    ]
    return {"data": data}


async def mosaic_query_endpoint(request: Request) -> JSONResponse:
    """POST /mosaic/query -> Mosaic data server compatible endpoint."""
    try:
        payload = await request.json()
    except Exception as exc:
        logger.warning("invalid mosaic JSON body: %s", exc)
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    sql = payload.get("sql", "")
    try:
        _validate_query(sql)
    except QueryRejected as exc:
        return JSONResponse(
            {"error": f"Query rejected: {exc}"},
            status_code=403,
        )

    try:
        result = await asyncio.to_thread(_run_mosaic_query, payload)
    except Exception:
        logger.exception("mosaic query failed")
        return JSONResponse({"error": "Internal error"}, status_code=500)

    return JSONResponse(result)
