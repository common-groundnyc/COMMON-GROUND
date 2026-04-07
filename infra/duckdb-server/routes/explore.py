"""REST endpoints for the /explore dashboard.

These are Starlette handlers registered via @mcp.custom_route in mcp_server.py.
They wrap the existing query builders + DuckDB connection pool.
"""
import asyncio
import logging
import re
from datetime import date, datetime
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

from shared.db import execute
from shared.explore_queries import (
    build_zip_overview_query,
    build_zip_search_query,
    build_worst_buildings_query,
)

ZIP_RE = re.compile(r"^\d{5}$")
MAX_DAYS = 3650  # 10 years
DEFAULT_DAYS = 365
DEFAULT_LIMIT = 10
MAX_LIMIT = 100

CACHE_HEADER = {"Cache-Control": "public, max-age=300, s-maxage=300"}


def _clamp_days(raw: str | None) -> int:
    try:
        n = int(raw or DEFAULT_DAYS)
    except ValueError:
        return DEFAULT_DAYS
    return max(1, min(n, MAX_DAYS))


def _clamp_limit(raw: str | None) -> int:
    try:
        n = int(raw or DEFAULT_LIMIT)
    except ValueError:
        return DEFAULT_LIMIT
    return max(1, min(n, MAX_LIMIT))


def _serialize(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


async def neighborhood_endpoint(request: Request) -> JSONResponse:
    """GET /api/neighborhood/{zip} → stat-card payload for the dashboard."""
    zip_code = request.path_params["zip_code"]
    if not ZIP_RE.match(zip_code):
        return JSONResponse({"error": "Invalid zip code (must be 5 digits)"}, status_code=400)

    days = _clamp_days(request.query_params.get("days"))
    sql, params = build_zip_overview_query(zip_code=zip_code, days=days)
    try:
        rows = await asyncio.to_thread(execute, sql, params)
    except Exception:
        logger.exception("explore endpoint failed: zip=%s", zip_code)
        return JSONResponse({"error": "Internal error"}, status_code=500)
    if not rows:
        return JSONResponse({"error": "No data"}, status_code=404)

    violations, complaints, crimes, restaurants_a_pct = rows[0]
    body = {
        "zip": zip_code,
        "days": days,
        "stats": {
            "violations_count": violations or 0,
            "complaints_count": complaints or 0,
            "crimes_count": crimes or 0,
            "restaurants_a_pct": float(restaurants_a_pct or 0),
        },
        "sources": ["HPD", "311", "NYPD", "DOHMH"],
    }
    return JSONResponse(body, headers=CACHE_HEADER)


async def zips_search_endpoint(request: Request) -> JSONResponse:
    """GET /api/zips/search?q=112 → autocomplete for ZIP / neighborhood name."""
    q = request.query_params.get("q", "").strip()
    if not q:
        return JSONResponse({"error": "Missing query parameter 'q'"}, status_code=400)

    sql, params = build_zip_search_query(prefix=q)
    try:
        rows = await asyncio.to_thread(execute, sql, params)
    except Exception:
        logger.exception("explore endpoint failed: q=%s", q)
        return JSONResponse({"error": "Internal error"}, status_code=500)
    body = {
        "query": q,
        "results": [
            {"zip": _serialize(z), "label": _serialize(label), "borough": _serialize(borough)}
            for (z, label, borough) in rows
        ],
    }
    return JSONResponse(body, headers=CACHE_HEADER)


async def worst_buildings_endpoint(request: Request) -> JSONResponse:
    """GET /api/buildings/worst?zip=11201&days=365&limit=10 → ranked buildings."""
    zip_code = request.query_params.get("zip", "").strip()
    if not ZIP_RE.match(zip_code):
        return JSONResponse({"error": "Invalid zip parameter (must be 5 digits)"}, status_code=400)

    days = _clamp_days(request.query_params.get("days"))
    limit = _clamp_limit(request.query_params.get("limit"))

    sql, params = build_worst_buildings_query(zip_code=zip_code, days=days, limit=limit)
    try:
        rows = await asyncio.to_thread(execute, sql, params)
    except Exception:
        logger.exception("explore endpoint failed: zip=%s", zip_code)
        return JSONResponse({"error": "Internal error"}, status_code=500)

    body = {
        "zip": zip_code,
        "days": days,
        "buildings": [
            {
                "bbl": _serialize(bbl),
                "address": _serialize(address),
                "violation_count": violation_count,
                "class_c_count": class_c_count,
                "last_violation_date": _serialize(last_violation_date),
            }
            for (bbl, address, violation_count, class_c_count, last_violation_date) in rows
        ],
        "sources": ["HPD"],
    }
    return JSONResponse(body, headers=CACHE_HEADER)
