"""transit() super tool — parking violations, MTA ridership, traffic volume,
and street infrastructure (potholes, pavement, pedestrian ramps).

Views:
  full           → transportation overview for an area
  parking        → parking violations by plate or location
  ridership      → MTA subway + bus + ferry ridership trends
  traffic        → traffic volume by location
  infrastructure → potholes, pavement quality, pedestrian ramps
"""

import re
from typing import Annotated, Literal

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import execute
from shared.formatting import make_result, format_text_table
from shared.types import MAX_LLM_ROWS, ZIP_PATTERN, COORDS_PATTERN

# ---------------------------------------------------------------------------
# Input patterns
# ---------------------------------------------------------------------------

_ZIP_PATTERN = ZIP_PATTERN
_COORDS_PATTERN = COORDS_PATTERN
_PLATE_PATTERN = re.compile(r"^[A-Z0-9]{4,10}$", re.IGNORECASE)


def _parse_location(location: str) -> dict:
    """Classify location input into type + value.

    Returns dict with 'type' key:
      'zip'     → {'type': 'zip', 'zip': '10003'}
      'coords'  → {'type': 'coords', 'lat': 40.71, 'lng': -74.00}
      'plate'   → {'type': 'plate', 'plate': 'ABC1234'}
      'station' → {'type': 'station', 'name': 'Times Square'}
    """
    loc = location.strip()

    if _ZIP_PATTERN.match(loc):
        return {"type": "zip", "zip": loc}

    m = _COORDS_PATTERN.match(loc)
    if m:
        return {"type": "coords", "lat": float(m.group(1)), "lng": float(m.group(2))}

    # Plate detection: all alphanumeric, 4-10 chars, has both letters and digits
    upper = loc.upper().replace(" ", "").replace("-", "")
    if _PLATE_PATTERN.match(upper) and re.search(r"[A-Z]", upper) and re.search(r"\d", upper):
        return {"type": "plate", "plate": upper}

    # Default: station or address name
    return {"type": "station", "name": loc}


# ---------------------------------------------------------------------------
# SQL constants — parking
# ---------------------------------------------------------------------------

PARKING_BY_PLATE_SQL = """
SELECT plate_id, registration_state, plate_type,
       violation_description, issue_date, fine_amount,
       violation_county, street_name, house_number
FROM lake.transportation.parking_violations
WHERE UPPER(REPLACE(plate_id, ' ', '')) ILIKE ?
ORDER BY TRY_CAST(issue_date AS DATE) DESC NULLS LAST
LIMIT 200
"""

PARKING_BY_ZIP_SQL = """
SELECT violation_description, COUNT(*) AS cnt,
       ROUND(AVG(TRY_CAST(fine_amount AS DOUBLE)), 2) AS avg_fine,
       MAX(issue_date) AS latest
FROM lake.transportation.parking_violations
WHERE violation_county IS NOT NULL
  AND street_name IS NOT NULL
  AND TRY_CAST(issue_date AS DATE) >= CURRENT_DATE - INTERVAL '365' DAY
GROUP BY violation_description
ORDER BY cnt DESC
LIMIT 20
"""

PARKING_BY_ZIP_FILTERED_SQL = """
SELECT violation_description, COUNT(*) AS cnt,
       ROUND(AVG(TRY_CAST(fine_amount AS DOUBLE)), 2) AS avg_fine,
       MAX(issue_date) AS latest
FROM lake.transportation.parking_violations
WHERE violation_county IS NOT NULL
  AND street_name IS NOT NULL
  AND TRY_CAST(issue_date AS DATE) >= CURRENT_DATE - INTERVAL '365' DAY
  AND CAST(violation_precinct AS VARCHAR) IN (
      SELECT DISTINCT CAST(precinct AS VARCHAR)
      FROM lake.public_safety.nypd_complaints
      WHERE CAST(addr_pct_cd AS VARCHAR) IS NOT NULL
        AND zip_code = ?
      LIMIT 10
  )
GROUP BY violation_description
ORDER BY cnt DESC
LIMIT 20
"""

PARKING_BY_ADDRESS_SQL = """
SELECT plate_id, violation_description, issue_date, fine_amount,
       street_name, house_number, violation_county
FROM lake.transportation.parking_violations
WHERE UPPER(street_name) ILIKE ?
ORDER BY TRY_CAST(issue_date AS DATE) DESC NULLS LAST
LIMIT 100
"""

PARKING_SUMMARY_SQL = """
SELECT COUNT(*) AS total_violations,
       COUNT(DISTINCT plate_id) AS unique_plates,
       ROUND(SUM(TRY_CAST(fine_amount AS DOUBLE)), 2) AS total_fines,
       ROUND(AVG(TRY_CAST(fine_amount AS DOUBLE)), 2) AS avg_fine,
       MIN(issue_date) AS earliest,
       MAX(issue_date) AS latest
FROM lake.transportation.parking_violations
WHERE UPPER(REPLACE(plate_id, ' ', '')) ILIKE ?
"""

# ---------------------------------------------------------------------------
# SQL constants — ridership
# ---------------------------------------------------------------------------

MTA_RIDERSHIP_SQL = """
SELECT date, subways_total_estimated_ridership AS subway_riders,
       subways_pct_of_comparable_pre_pandemic_day AS subway_pct_pre_covid,
       buses_total_estimated_ridership AS bus_riders,
       buses_pct_of_comparable_pre_pandemic_day AS bus_pct_pre_covid,
       lirr_total_estimated_ridership AS lirr_riders,
       metro_north_total_estimated_ridership AS metro_north_riders,
       staten_island_railway_total_estimated_ridership AS sir_riders
FROM lake.transportation.mta_daily_ridership
ORDER BY TRY_CAST(date AS DATE) DESC NULLS LAST
LIMIT 30
"""

MTA_RIDERSHIP_MONTHLY_SQL = """
SELECT DATE_TRUNC('month', TRY_CAST(date AS DATE)) AS month,
       ROUND(AVG(TRY_CAST(subways_total_estimated_ridership AS DOUBLE))) AS avg_subway_daily,
       ROUND(AVG(TRY_CAST(buses_total_estimated_ridership AS DOUBLE))) AS avg_bus_daily,
       COUNT(*) AS days_reported
FROM lake.transportation.mta_daily_ridership
WHERE TRY_CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '365' DAY
GROUP BY DATE_TRUNC('month', TRY_CAST(date AS DATE))
ORDER BY month DESC
"""

MTA_ENTRANCE_BY_STATION_SQL = """
SELECT station_name, line_name, entrance_type,
       entry, exit_only, ada, ada_notes,
       entrance_latitude, entrance_longitude
FROM lake.transportation.mta_entrances
WHERE UPPER(station_name) ILIKE ?
ORDER BY line_name
"""

FERRY_RIDERSHIP_SQL = """
SELECT route, date, total_ridership, weekday_or_weekend
FROM lake.transportation.ferry_ridership
ORDER BY TRY_CAST(date AS DATE) DESC NULLS LAST
LIMIT 50
"""

FERRY_RIDERSHIP_BY_ROUTE_SQL = """
SELECT route,
       COUNT(*) AS days_reported,
       ROUND(AVG(TRY_CAST(total_ridership AS DOUBLE))) AS avg_daily_riders,
       MAX(TRY_CAST(total_ridership AS DOUBLE)) AS peak_riders,
       MAX(date) AS latest_date
FROM lake.transportation.ferry_ridership
WHERE TRY_CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '365' DAY
GROUP BY route
ORDER BY avg_daily_riders DESC
"""

# ---------------------------------------------------------------------------
# SQL constants — traffic
# ---------------------------------------------------------------------------

TRAFFIC_VOLUME_SQL = """
SELECT street_name, from_street, to_street, direction,
       TRY_CAST(yr AS INTEGER) AS year,
       TRY_CAST(vol AS DOUBLE) AS volume,
       segment_id
FROM lake.transportation.traffic_volume
ORDER BY TRY_CAST(yr AS INTEGER) DESC NULLS LAST,
         TRY_CAST(vol AS DOUBLE) DESC NULLS LAST
LIMIT 50
"""

TRAFFIC_VOLUME_BY_STREET_SQL = """
SELECT street_name, from_street, to_street, direction,
       TRY_CAST(yr AS INTEGER) AS year,
       TRY_CAST(vol AS DOUBLE) AS volume,
       segment_id
FROM lake.transportation.traffic_volume
WHERE UPPER(street_name) ILIKE ?
ORDER BY TRY_CAST(yr AS INTEGER) DESC NULLS LAST,
         TRY_CAST(vol AS DOUBLE) DESC NULLS LAST
LIMIT 50
"""

# ---------------------------------------------------------------------------
# SQL constants — infrastructure
# ---------------------------------------------------------------------------

POTHOLE_ORDERS_BY_ZIP_SQL = """
SELECT zip, street, from_street, to_street,
       status, repair_date, response_days,
       boro
FROM lake.transportation.pothole_orders
WHERE CAST(zip AS VARCHAR) = ?
ORDER BY TRY_CAST(repair_date AS DATE) DESC NULLS LAST
LIMIT 100
"""

POTHOLE_ORDERS_BY_ADDRESS_SQL = """
SELECT zip, street, from_street, to_street,
       status, repair_date, response_days,
       boro
FROM lake.transportation.pothole_orders
WHERE UPPER(street) ILIKE ?
ORDER BY TRY_CAST(repair_date AS DATE) DESC NULLS LAST
LIMIT 100
"""

POTHOLE_SUMMARY_BY_ZIP_SQL = """
SELECT CAST(zip AS VARCHAR) AS zip,
       COUNT(*) AS total_orders,
       COUNT(*) FILTER (WHERE UPPER(status) = 'CLOSED') AS closed,
       COUNT(*) FILTER (WHERE UPPER(status) != 'CLOSED') AS open_or_pending,
       ROUND(AVG(TRY_CAST(response_days AS DOUBLE)), 1) AS avg_response_days,
       MAX(repair_date) AS latest_repair
FROM lake.transportation.pothole_orders
WHERE CAST(zip AS VARCHAR) = ?
GROUP BY CAST(zip AS VARCHAR)
"""

PAVEMENT_RATING_BY_ZIP_SQL = """
SELECT street, from_street, to_street, boro,
       rating_word, rating_date, segment_id
FROM lake.transportation.pavement_rating
WHERE CAST(zip_code AS VARCHAR) = ?
ORDER BY TRY_CAST(rating_date AS DATE) DESC NULLS LAST
LIMIT 100
"""

PAVEMENT_SUMMARY_BY_ZIP_SQL = """
SELECT rating_word, COUNT(*) AS segments
FROM lake.transportation.pavement_rating
WHERE CAST(zip_code AS VARCHAR) = ?
GROUP BY rating_word
ORDER BY segments DESC
"""

PEDESTRIAN_RAMPS_BY_ZIP_SQL = """
SELECT location, boro, status, type, date_installed
FROM lake.transportation.pedestrian_ramps
WHERE CAST(zip_code AS VARCHAR) = ?
ORDER BY TRY_CAST(date_installed AS DATE) DESC NULLS LAST
LIMIT 100
"""

PEDESTRIAN_RAMPS_SUMMARY_SQL = """
SELECT status, COUNT(*) AS cnt
FROM lake.transportation.pedestrian_ramps
WHERE CAST(zip_code AS VARCHAR) = ?
GROUP BY status
ORDER BY cnt DESC
"""

ALT_FUEL_STATIONS_BY_ZIP_SQL = """
SELECT station_name, street_address, city, state, zip,
       fuel_type_code, ev_network, ev_connector_types,
       latitude, longitude
FROM lake.federal.nrel_alt_fuel_stations
WHERE CAST(zip AS VARCHAR) = ?
ORDER BY station_name
LIMIT 50
"""

ALT_FUEL_STATIONS_NEARBY_SQL = """
SELECT station_name, street_address, city, state, zip,
       fuel_type_code, ev_network, ev_connector_types,
       latitude, longitude
FROM lake.federal.nrel_alt_fuel_stations
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
ORDER BY POW(TRY_CAST(latitude AS DOUBLE) - ?, 2)
       + POW(TRY_CAST(longitude AS DOUBLE) - ?, 2)
LIMIT 20
"""


# ---------------------------------------------------------------------------
# View handlers
# ---------------------------------------------------------------------------


def _view_parking(pool, loc: dict) -> ToolResult:
    """Parking violations by plate number or location."""
    if loc["type"] == "plate":
        plate = f"%{loc['plate']}%"
        cols_s, rows_s = execute(pool, PARKING_SUMMARY_SQL, [plate])
        cols, rows = execute(pool, PARKING_BY_PLATE_SQL, [plate])
        summary_line = ""
        if rows_s and rows_s[0][0]:
            r = dict(zip(cols_s, rows_s[0]))
            summary_line = (
                f"Plate match: {r.get('total_violations', 0)} violations, "
                f"{r.get('unique_plates', 0)} plate variants, "
                f"${r.get('total_fines', 0):,.0f} total fines "
                f"(avg ${r.get('avg_fine', 0):,.0f})"
            )
        return make_result(
            summary_line or f"Parking violations for plate '{loc['plate']}'",
            cols, rows,
        )

    if loc["type"] == "zip":
        cols, rows = execute(pool, PARKING_BY_ZIP_FILTERED_SQL, [loc["zip"]])
        if not rows:
            cols, rows = execute(pool, PARKING_BY_ZIP_SQL)
        return make_result(
            f"Top parking violations near ZIP {loc['zip']} (last 12 months)",
            cols, rows,
        )

    # Address / station name → search by street
    name = loc.get("name", "")
    search = f"%{name.upper()}%"
    cols, rows = execute(pool, PARKING_BY_ADDRESS_SQL, [search])
    return make_result(
        f"Recent parking violations on '{name}'",
        cols, rows,
    )


def _view_ridership(pool, loc: dict) -> ToolResult:
    """MTA ridership trends — station-specific or system-wide."""
    sections: list[str] = []
    all_cols: list = []
    all_rows: list = []

    if loc["type"] == "station":
        # Station entrance lookup
        search = f"%{loc['name'].upper()}%"
        cols_e, rows_e = execute(pool, MTA_ENTRANCE_BY_STATION_SQL, [search])
        if rows_e:
            sections.append(f"**Station entrances matching '{loc['name']}'**")
            sections.append(format_text_table(cols_e, rows_e))
            all_cols = cols_e
            all_rows = rows_e

    # System-wide daily ridership (last 30 days)
    cols_d, rows_d = execute(pool, MTA_RIDERSHIP_SQL)
    if rows_d:
        sections.append("**MTA system-wide ridership (last 30 days)**")
        sections.append(format_text_table(cols_d, rows_d))
        if not all_cols:
            all_cols, all_rows = cols_d, rows_d

    # Monthly averages
    cols_m, rows_m = execute(pool, MTA_RIDERSHIP_MONTHLY_SQL)
    if rows_m:
        sections.append("**Monthly average daily ridership (last 12 months)**")
        sections.append(format_text_table(cols_m, rows_m))

    # Ferry ridership summary
    cols_f, rows_f = execute(pool, FERRY_RIDERSHIP_BY_ROUTE_SQL)
    if rows_f:
        sections.append("**NYC Ferry ridership by route (last 12 months)**")
        sections.append(format_text_table(cols_f, rows_f))

    text = "\n\n".join(sections) if sections else "No ridership data found."
    structured = (
        {"rows": [dict(zip(all_cols, row)) for row in all_rows[:100]]}
        if all_cols and all_rows
        else None
    )
    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"total_rows": len(all_rows)},
    )


def _view_traffic(pool, loc: dict) -> ToolResult:
    """Traffic volume data by street or overall."""
    if loc["type"] in ("station", "zip"):
        name = loc.get("name") or loc.get("zip", "")
        search = f"%{name.upper()}%"
        cols, rows = execute(pool, TRAFFIC_VOLUME_BY_STREET_SQL, [search])
        if rows:
            return make_result(
                f"Traffic volume data matching '{name}'",
                cols, rows,
            )

    # Fallback: top traffic volumes
    cols, rows = execute(pool, TRAFFIC_VOLUME_SQL)
    return make_result("Top traffic volume readings (most recent)", cols, rows)


def _view_infrastructure(pool, loc: dict) -> ToolResult:
    """Potholes, pavement quality, pedestrian ramps, EV charging."""
    if loc["type"] not in ("zip", "station", "coords"):
        raise ToolError(
            "Infrastructure view needs a ZIP code, address, or coordinates. "
            "License plates are only valid for the 'parking' view."
        )

    sections: list[str] = []
    primary_cols: list = []
    primary_rows: list = []

    # Resolve ZIP — for station/address, we can't easily map to ZIP,
    # so we'll search by street name where possible
    zip_code = loc.get("zip")

    if zip_code:
        # Pothole summary
        cols_ps, rows_ps = execute(pool, POTHOLE_SUMMARY_BY_ZIP_SQL, [zip_code])
        if rows_ps and rows_ps[0][0]:
            r = dict(zip(cols_ps, rows_ps[0]))
            sections.append(
                f"**Potholes in {zip_code}**: {r.get('total_orders', 0)} orders "
                f"({r.get('closed', 0)} closed, {r.get('open_or_pending', 0)} open/pending), "
                f"avg {r.get('avg_response_days', '?')} days to repair"
            )

        # Pothole details
        cols_p, rows_p = execute(pool, POTHOLE_ORDERS_BY_ZIP_SQL, [zip_code])
        if rows_p:
            sections.append("**Recent pothole orders**")
            sections.append(format_text_table(cols_p, rows_p))
            if not primary_cols:
                primary_cols, primary_rows = cols_p, rows_p

        # Pavement rating summary
        cols_pav_s, rows_pav_s = execute(pool, PAVEMENT_SUMMARY_BY_ZIP_SQL, [zip_code])
        if rows_pav_s:
            sections.append("**Pavement quality distribution**")
            sections.append(format_text_table(cols_pav_s, rows_pav_s))

        # Pavement details
        cols_pav, rows_pav = execute(pool, PAVEMENT_RATING_BY_ZIP_SQL, [zip_code])
        if rows_pav:
            sections.append("**Pavement ratings**")
            sections.append(format_text_table(cols_pav, rows_pav))
            if not primary_cols:
                primary_cols, primary_rows = cols_pav, rows_pav

        # Pedestrian ramps summary
        cols_pr_s, rows_pr_s = execute(pool, PEDESTRIAN_RAMPS_SUMMARY_SQL, [zip_code])
        if rows_pr_s:
            sections.append("**Pedestrian ramp status**")
            sections.append(format_text_table(cols_pr_s, rows_pr_s))

        # Pedestrian ramps details
        cols_pr, rows_pr = execute(pool, PEDESTRIAN_RAMPS_BY_ZIP_SQL, [zip_code])
        if rows_pr:
            sections.append("**Pedestrian ramps**")
            sections.append(format_text_table(cols_pr, rows_pr))

        # Alt fuel / EV stations
        cols_ev, rows_ev = execute(pool, ALT_FUEL_STATIONS_BY_ZIP_SQL, [zip_code])
        if rows_ev:
            sections.append(f"**EV / alternative fuel stations in {zip_code}**")
            sections.append(format_text_table(cols_ev, rows_ev))

    elif loc["type"] == "coords":
        # Alt fuel stations near coordinates
        cols_ev, rows_ev = execute(
            pool, ALT_FUEL_STATIONS_NEARBY_SQL,
            [loc["lat"], loc["lng"]],
        )
        if rows_ev:
            sections.append("**Nearest EV / alternative fuel stations**")
            sections.append(format_text_table(cols_ev, rows_ev))
            primary_cols, primary_rows = cols_ev, rows_ev

    else:
        # Station/address — search potholes by street name
        name = loc.get("name", "")
        search = f"%{name.upper()}%"
        cols_p, rows_p = execute(pool, POTHOLE_ORDERS_BY_ADDRESS_SQL, [search])
        if rows_p:
            sections.append(f"**Pothole orders on '{name}'**")
            sections.append(format_text_table(cols_p, rows_p))
            primary_cols, primary_rows = cols_p, rows_p

    if not sections:
        return make_result(
            "No infrastructure data found for this location. Try a ZIP code.",
            [], [],
        )

    text = "\n\n".join(sections)
    structured = (
        {"rows": [dict(zip(primary_cols, row)) for row in primary_rows[:100]]}
        if primary_cols and primary_rows
        else None
    )
    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"total_rows": len(primary_rows)},
    )


def _view_full(pool, loc: dict) -> ToolResult:
    """Full transportation overview — combines parking + ridership + infrastructure."""
    sections: list[str] = []
    primary_cols: list = []
    primary_rows: list = []

    # --- Parking summary ---
    if loc["type"] == "zip":
        cols_pk, rows_pk = execute(pool, PARKING_BY_ZIP_FILTERED_SQL, [loc["zip"]])
        if not rows_pk:
            cols_pk, rows_pk = execute(pool, PARKING_BY_ZIP_SQL)
        if rows_pk:
            sections.append(f"**Top parking violations near {loc['zip']} (12 months)**")
            sections.append(format_text_table(cols_pk, rows_pk, max_rows=10))
            primary_cols, primary_rows = cols_pk, rows_pk

    # --- Ridership snapshot ---
    cols_r, rows_r = execute(pool, MTA_RIDERSHIP_SQL)
    if rows_r:
        sections.append("**MTA ridership (last 30 days)**")
        sections.append(format_text_table(cols_r, rows_r, max_rows=5))

    # --- Ferry ---
    cols_f, rows_f = execute(pool, FERRY_RIDERSHIP_BY_ROUTE_SQL)
    if rows_f:
        sections.append("**NYC Ferry ridership by route**")
        sections.append(format_text_table(cols_f, rows_f, max_rows=5))

    # --- Infrastructure (if ZIP) ---
    if loc["type"] == "zip":
        zip_code = loc["zip"]

        cols_ps, rows_ps = execute(pool, POTHOLE_SUMMARY_BY_ZIP_SQL, [zip_code])
        if rows_ps and rows_ps[0][0]:
            r = dict(zip(cols_ps, rows_ps[0]))
            sections.append(
                f"**Potholes in {zip_code}**: {r.get('total_orders', 0)} orders, "
                f"avg {r.get('avg_response_days', '?')} days to repair"
            )

        cols_pav, rows_pav = execute(pool, PAVEMENT_SUMMARY_BY_ZIP_SQL, [zip_code])
        if rows_pav:
            sections.append("**Pavement quality**")
            sections.append(format_text_table(cols_pav, rows_pav))

        cols_ev, rows_ev = execute(pool, ALT_FUEL_STATIONS_BY_ZIP_SQL, [zip_code])
        if rows_ev:
            sections.append(f"**EV / alt fuel stations ({len(rows_ev)} found)**")
            sections.append(format_text_table(cols_ev, rows_ev, max_rows=5))

    # --- Traffic volume ---
    if loc["type"] in ("station",):
        search = f"%{loc['name'].upper()}%"
        cols_t, rows_t = execute(pool, TRAFFIC_VOLUME_BY_STREET_SQL, [search])
        if rows_t:
            sections.append(f"**Traffic volume on '{loc['name']}'**")
            sections.append(format_text_table(cols_t, rows_t, max_rows=5))

    if not sections:
        return make_result(
            "No transportation data found. Try a NYC ZIP code like '10003'.",
            [], [],
        )

    text = "\n\n".join(sections)
    structured = (
        {"rows": [dict(zip(primary_cols, row)) for row in primary_rows[:100]]}
        if primary_cols and primary_rows
        else None
    )
    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"total_rows": len(primary_rows)},
    )


# ---------------------------------------------------------------------------
# View dispatch
# ---------------------------------------------------------------------------

_VIEW_DISPATCH: dict[str, callable] = {
    "full": _view_full,
    "parking": _view_parking,
    "ridership": _view_ridership,
    "traffic": _view_traffic,
    "infrastructure": _view_infrastructure,
}


# ---------------------------------------------------------------------------
# Main super tool
# ---------------------------------------------------------------------------


def transit(
    location: Annotated[str, Field(
        description="ZIP code, address, station name, license plate, or lat/lng coordinates, e.g. '10003', 'Times Square', 'ABC1234', '40.7128,-74.0060'",
        examples=["10003", "Times Square", "ABC1234", "Grand Central", "40.7128,-74.0060"],
    )],
    view: Annotated[
        Literal["full", "parking", "ridership", "traffic", "infrastructure"],
        Field(
            default="full",
            description="'full' returns transportation overview for the area. 'parking' returns parking violations by plate or location. 'ridership' returns MTA subway, bus, and ferry ridership trends. 'traffic' returns traffic volume and speed data. 'infrastructure' returns pothole orders, pavement quality, and pedestrian ramp data.",
        )
    ] = "full",
    ctx: Context = None,
) -> ToolResult:
    """Parking tickets, MTA ridership, traffic volume, and street infrastructure for any NYC location. Returns violation counts, ridership trends, and infrastructure condition.

    GUIDELINES: Show the complete transit report. Use tables for ticket data and ridership stats.
    Present the FULL response to the user. Do not omit any section.

    LIMITATIONS: Not for crash data (use safety). Not for building-level data (use building).

    RETURNS: Parking violations, ridership trends, traffic volumes, and infrastructure data."""
    pool = ctx.lifespan_context["pool"]

    loc = _parse_location(location)

    handler = _VIEW_DISPATCH.get(view)
    if not handler:
        raise ToolError(
            f"Unknown view '{view}'. Choose from: full, parking, ridership, traffic, infrastructure"
        )

    directive = "PRESENTATION: Show this complete transit report. Use tables for ticket data and ridership stats. Do not omit any section.\n\n"
    result = handler(pool, loc)
    return ToolResult(
        content=directive + (result.content if isinstance(result.content, str) else "\n".join(str(c) for c in result.content) if result.content else ""),
        structured_content=result.structured_content,
        meta=result.meta,
    )
