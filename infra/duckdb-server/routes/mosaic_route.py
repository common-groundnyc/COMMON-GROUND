"""Mosaic data server endpoint at /mosaic/query.

The browser-side Mosaic Coordinator sends SQL queries via POST.
We validate against an allowlist and execute via the shared DuckDB pool.
Read-only, non-destructive queries only.
"""
import asyncio
import logging
import re
from typing import Any

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

DISALLOWED_KEYWORDS = (
    "drop ", "delete ", "update ", "insert ", "attach ", "create ",
    "alter ", "truncate ", "copy ", "load ", "install ",
)

MAX_QUERY_LEN = 10_000

_TABLE_REF = re.compile(r"\blake\.([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)", re.IGNORECASE)


def is_query_allowed(sql: str) -> bool:
    """Allowlist-based query validation. Returns True if safe to run."""
    if not sql or len(sql) > MAX_QUERY_LEN:
        return False
    lowered = sql.lower()
    for kw in DISALLOWED_KEYWORDS:
        if kw in lowered:
            return False
    refs = _TABLE_REF.findall(sql)
    if not refs:
        return True
    for schema, table in refs:
        if schema.lower() not in ALLOWED_SCHEMAS:
            return False
        if table.startswith("_"):
            return False
    return True


def _serialize_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _run_mosaic_query(payload: dict) -> dict:
    """Execute a Mosaic query payload against DuckDB."""
    sql = payload.get("sql", "")
    rows = execute(sql)
    if not rows:
        return {"data": []}
    data = [
        {f"col{idx}": _serialize_value(value) for idx, value in enumerate(row)}
        for row in rows
    ]
    return {"data": data}


async def mosaic_query_endpoint(request: Request) -> JSONResponse:
    """POST /mosaic/query -> Mosaic data server compatible endpoint."""
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    sql = payload.get("sql", "")
    if not is_query_allowed(sql):
        return JSONResponse(
            {"error": "Query rejected by allowlist (schema, length, or keyword)"},
            status_code=403,
        )

    try:
        result = await asyncio.to_thread(_run_mosaic_query, payload)
    except Exception:
        logger.exception("mosaic query failed")
        return JSONResponse({"error": "Internal error"}, status_code=500)

    return JSONResponse(result, headers={"Access-Control-Allow-Origin": "*"})
