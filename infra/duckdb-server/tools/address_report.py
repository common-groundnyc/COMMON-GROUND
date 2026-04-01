"""address_report() — 360-degree dossier for any NYC address."""

import re
from typing import Annotated

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import execute, parallel_queries, safe_query
from tools._address_format import assemble_report
from tools._address_queries import build_queries


# ---------------------------------------------------------------------------
# Public tool
# ---------------------------------------------------------------------------


def address_report(
    address: Annotated[
        str,
        Field(
            description="NYC street address, e.g. '305 Linden Blvd, Brooklyn' or '350 5th Ave, Manhattan'",
            examples=["305 Linden Blvd, Brooklyn", "350 5th Ave, Manhattan", "123 Main St, Bronx"],
        ),
    ],
    ctx: Context = None,
) -> ToolResult:
    """Complete 360-degree report for any NYC address. Returns building profile,
    violations with percentile rankings, neighborhood demographics, crime stats,
    school quality, health indicators, environmental data, civic representation,
    nearby services, and fun facts. Every metric ranked against the city.
    Use this as the first lookup for any address. For deeper investigation,
    use the drill-deeper suggestions at the end of the report."""
    if not address or not address.strip():
        raise ToolError("Please provide a NYC street address, e.g. '305 Linden Blvd, Brooklyn'")

    pool = ctx.lifespan_context["pool"]

    # Step 1: detect BBL (10-digit number) or resolve address
    stripped = address.strip()
    if re.match(r'^\d{10}$', stripped):
        ctx_data = _resolve_context_by_bbl(pool, stripped)
    else:
        ctx_data = _resolve_context(pool, address)
    bbl = ctx_data["bbl"]

    # Step 2: build and execute all queries in parallel
    queries = build_queries(
        bbl=bbl,
        zipcode=ctx_data.get("zip", ""),
        borough=ctx_data.get("borough", ""),
        precinct=ctx_data.get("precinct", ""),
        cd=ctx_data.get("community_district", ""),
        boro_code=bbl[0] if bbl else "",
        address=ctx_data.get("address", address),
        latitude=ctx_data.get("latitude"),
        longitude=ctx_data.get("longitude"),
    )
    results = parallel_queries(pool, queries)

    # Step 3: assemble the formatted report
    report = assemble_report(ctx_data, results)

    # Step 4: structured content for programmatic access
    structured = {
        "bbl": bbl,
        "address": ctx_data.get("address", address),
        "zipcode": ctx_data.get("zip", ""),
        "borough": ctx_data.get("borough", ""),
        "sections": {k: v for k, v in results.items() if v != ([], [])},
    }

    return ToolResult(content=report, structured_content=structured)


# ---------------------------------------------------------------------------
# Context resolution
# ---------------------------------------------------------------------------


def _resolve_context_by_bbl(pool, bbl: str) -> dict:
    """Resolve a raw 10-digit BBL directly — skip address resolution."""
    cols, rows = execute(pool, """
        SELECT
            borocode || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') AS bbl,
            address, borough, zipcode AS zip, zonedist1 AS zoning,
            bldgclass,
            TRY_CAST(numfloors AS INT) AS numfloors,
            TRY_CAST(unitsres AS INT) AS unitsres,
            TRY_CAST(unitstotal AS INT) AS unitstotal,
            TRY_CAST(yearbuilt AS INT) AS yearbuilt,
            TRY_CAST(assesstot AS BIGINT) AS assesstot,
            TRY_CAST(assessland AS BIGINT) AS assessland,
            ownername,
            cd AS community_district, council AS council_district,
            tract2010 AS census_tract,
            TRY_CAST(lotarea AS INT) AS lotarea,
            TRY_CAST(bldgarea AS INT) AS bldgarea,
            TRY_CAST(latitude AS DOUBLE) AS latitude,
            TRY_CAST(longitude AS DOUBLE) AS longitude
        FROM lake.city_government.pluto
        WHERE borocode || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') = ?
        LIMIT 1
    """, [bbl])

    if not rows:
        raise ToolError(f"BBL {bbl} not found in PLUTO. Check the 10-digit BBL and try again.")

    data = dict(zip(cols, rows[0]))
    data["bbl"] = bbl

    _, pct_rows = safe_query(pool, """
        SELECT TRY_CAST(REGEXP_EXTRACT(incident_address, '(\\d+)\\s+PCT', 1) AS INT) AS pct
        FROM lake.social_services.n311_service_requests
        WHERE incident_zip = ? AND pct IS NOT NULL
        GROUP BY pct ORDER BY COUNT(*) DESC LIMIT 1
    """, [data.get("zip", "")])

    if not pct_rows:
        _, pct_rows = safe_query(pool, """
            SELECT DISTINCT addr_pct_cd AS pct
            FROM lake.public_safety.nypd_complaints_ytd
            WHERE TRY_CAST(SUBSTR(?, 1, 1) AS INT) IS NOT NULL
            LIMIT 1
        """, [bbl])

    data["precinct"] = str(pct_rows[0][0]) if pct_rows and pct_rows[0][0] else ""
    return data


def _resolve_context(pool, address: str) -> dict:
    """Resolve address -> BBL, then derive ZIP, borough, precinct, community district."""
    from tools.building import _normalize_address, _resolve_bbl

    bbl = _resolve_bbl(pool, address)

    # Get PLUTO data for the resolved BBL
    cols, rows = execute(pool, """
        SELECT
            borocode || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') AS bbl,
            address, borough, zipcode AS zip, zonedist1 AS zoning,
            bldgclass,
            TRY_CAST(numfloors AS INT) AS numfloors,
            TRY_CAST(unitsres AS INT) AS unitsres,
            TRY_CAST(unitstotal AS INT) AS unitstotal,
            TRY_CAST(yearbuilt AS INT) AS yearbuilt,
            TRY_CAST(assesstot AS BIGINT) AS assesstot,
            TRY_CAST(assessland AS BIGINT) AS assessland,
            ownername,
            cd AS community_district, council AS council_district,
            tract2010 AS census_tract,
            TRY_CAST(lotarea AS INT) AS lotarea,
            TRY_CAST(bldgarea AS INT) AS bldgarea,
            TRY_CAST(latitude AS DOUBLE) AS latitude,
            TRY_CAST(longitude AS DOUBLE) AS longitude
        FROM lake.city_government.pluto
        WHERE borocode || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') = ?
        LIMIT 1
    """, [bbl])

    if not rows:
        raise ToolError(f"BBL {bbl} not found in PLUTO. Try a different address.")

    data = dict(zip(cols, rows[0]))
    data["bbl"] = bbl

    # Derive precinct from 311 data (most common precinct for this ZIP)
    _, pct_rows = safe_query(pool, """
        SELECT TRY_CAST(REGEXP_EXTRACT(incident_address, '(\\d+)\\s+PCT', 1) AS INT) AS pct
        FROM lake.social_services.n311_service_requests
        WHERE incident_zip = ? AND pct IS NOT NULL
        GROUP BY pct ORDER BY COUNT(*) DESC LIMIT 1
    """, [data.get("zip", "")])

    # Fallback: try community district to precinct mapping
    if not pct_rows:
        _, pct_rows = safe_query(pool, """
            SELECT DISTINCT addr_pct_cd AS pct
            FROM lake.public_safety.nypd_complaints_ytd
            WHERE TRY_CAST(SUBSTR(?, 1, 1) AS INT) IS NOT NULL
            LIMIT 1
        """, [bbl])

    data["precinct"] = str(pct_rows[0][0]) if pct_rows and pct_rows[0][0] else ""

    return data
