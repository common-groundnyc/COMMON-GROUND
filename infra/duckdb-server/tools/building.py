"""building() super tool — absorbs building_profile, address_lookup, owner_violations,
building_story, building_context, block_timeline, nyc_twins, similar_buildings,
enforcement_web, property_history, and flipper_detector into one dispatch."""

import datetime
import re
import time
from typing import Annotated, Literal

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import execute, safe_query, fill_placeholders, parallel_queries
from shared.formatting import make_result, format_text_table
from shared.types import MAX_LLM_ROWS, BBL_PATTERN
from percentiles import lookup_percentiles, format_percentile

# ---------------------------------------------------------------------------
# Input patterns
# ---------------------------------------------------------------------------

_BBL_PATTERN = BBL_PATTERN  # alias for backward compat

# ---------------------------------------------------------------------------
# Address normalization helpers
# ---------------------------------------------------------------------------

_ORDINAL_MAP = {
    "FIRST": "1", "SECOND": "2", "THIRD": "3", "FOURTH": "4", "FIFTH": "5",
    "SIXTH": "6", "SEVENTH": "7", "EIGHTH": "8", "NINTH": "9", "TENTH": "10",
    "ELEVENTH": "11", "TWELFTH": "12",
}

_ABBREV_MAP = {
    "AVE": "AVENUE", "ST": "STREET", "BLVD": "BOULEVARD", "BL": "BOULEVARD",
    "DR": "DRIVE", "PL": "PLACE", "RD": "ROAD", "CT": "COURT", "LN": "LANE",
    "PKY": "PARKWAY", "PKWY": "PARKWAY", "TER": "TERRACE", "HWY": "HIGHWAY",
    "E": "EAST", "W": "WEST", "N": "NORTH", "S": "SOUTH",
}

_BOROUGH_MAP = {
    "MANHATTAN": "1", "MN": "1", "NEW YORK": "1", "NY": "1",
    "BRONX": "2", "BX": "2", "THE BRONX": "2",
    "BROOKLYN": "3", "BK": "3", "BKLYN": "3", "KINGS": "3",
    "QUEENS": "4", "QN": "4", "QNS": "4",
    "STATEN ISLAND": "5", "SI": "5", "RICHMOND": "5", "SHAOLIN": "5",
}


def _normalize_address_for_pad(raw: str) -> str:
    """Normalize a street address to match PAD (Property Address Directory) format.

    Returns the full normalized address (with house number, no borough/city/state/zip).
    Always strips ordinal suffixes (5TH -> 5) since PAD is consistent on this.
    """
    s = raw.strip().upper()
    # Strip trailing state/zip (e.g. ", NY 10118")
    s = re.sub(r',?\s*NY\s*\d*\s*$', '', s)
    # Strip borough from end (only after comma to avoid stripping from street names)
    for boro in sorted(_BOROUGH_MAP.keys(), key=len, reverse=True):
        s = re.sub(rf',\s*{re.escape(boro)}\s*$', '', s)
    s = s.strip().rstrip(',').strip()
    # Strip apartment/unit/floor/suite suffixes
    s = re.sub(r'\s*(APT|UNIT|FL|FLOOR|STE|SUITE|RM|ROOM|#)\s*\S+\s*$', '', s)
    # Strip ordinal suffixes: 5TH -> 5, 3RD -> 3
    s = re.sub(r'\b(\d+)(?:ST|ND|RD|TH)\b', r'\1', s)
    # Expand spelled-out ordinals: FIFTH -> 5
    for word, num in _ORDINAL_MAP.items():
        s = re.sub(rf'\b{word}\b', num, s)
    # Expand abbreviations: AVE -> AVENUE
    for abbr, full in _ABBREV_MAP.items():
        s = re.sub(rf'\b{abbr}\b', full, s)
    return s


# Backward-compat alias — address_report.py imports _normalize_address
_normalize_address = _normalize_address_for_pad


def _extract_borough(raw: str) -> str | None:
    """Extract a borough code ('1'-'5') from an address string, or None."""
    s = raw.strip().upper()
    for boro in sorted(_BOROUGH_MAP.keys(), key=len, reverse=True):
        if re.search(rf'\b{re.escape(boro)}\b', s):
            return _BOROUGH_MAP[boro]
    return None


def _extract_house_number(raw: str) -> int | None:
    """Extract the leading house number from an address string, or None.

    Handles hyphenated Queens-style addresses (45-17 21st St -> 45).
    """
    m = re.match(r'^\s*(\d+)', raw.strip())
    if m:
        return int(m.group(1))
    return None

# ---------------------------------------------------------------------------
# BBL resolution
# ---------------------------------------------------------------------------


def _resolve_bbl(pool, identifier: str) -> str:
    """Resolve an address string to a 10-digit BBL via PAD lookup.

    Two-tier search against foundation.address_lookup:
    1. Exact house number + street prefix match (with borough filter if available)
    2. Nearest lot on same street (fallback)
    """
    street_norm = _normalize_address_for_pad(identifier)
    boro = _extract_borough(identifier)
    house_num = _extract_house_number(identifier)

    # Extract street part (everything after house number)
    street_part = re.sub(r'^\d+[\-\d]*\s*', '', street_norm).strip()

    if not street_part:
        raise ToolError(
            f"Could not parse street name from '{identifier}'. "
            "Try a format like '350 5th Ave, Manhattan'."
        )

    # Tier 1: exact house number + street prefix
    if house_num is not None:
        boro_clause = "AND boro_code = ?" if boro else ""

        _cols, _rows = execute(pool, f"""
            SELECT bbl FROM lake.foundation.address_lookup
            WHERE house_number <= ? AND house_number_high >= ?
              AND street_std LIKE ?
              AND (addr_type IS NULL OR addr_type IN ('', 'V'))
              {boro_clause}
            LIMIT 1
        """, [house_num, house_num, street_part + "%"] + ([boro] if boro else []))
        if _rows:
            return str(_rows[0][0])

        # Tier 1b: exact house_number match (no range)
        _cols, _rows = execute(pool, f"""
            SELECT bbl FROM lake.foundation.address_lookup
            WHERE house_number = ?
              AND street_std LIKE ?
              AND (addr_type IS NULL OR addr_type IN ('', 'V'))
              {boro_clause}
            LIMIT 1
        """, [house_num, street_part + "%"] + ([boro] if boro else []))
        if _rows:
            return str(_rows[0][0])

    # Tier 2: nearest lot on the same street
    if house_num is not None and street_part:
        boro_clause = "AND boro_code = ?" if boro else ""
        _cols, _rows = execute(pool, f"""
            SELECT bbl FROM lake.foundation.address_lookup
            WHERE street_std LIKE ?
              AND (addr_type IS NULL OR addr_type IN ('', 'V'))
              {boro_clause}
            ORDER BY ABS(house_number - ?)
            LIMIT 1
        """, [street_part + "%"] + ([boro] if boro else []) + [house_num])
        if _rows:
            return str(_rows[0][0])

    raise ToolError(
        f"No building found for address '{identifier}'. "
        "Try a simpler address like '350 5th Ave' or include borough."
    )

# ---------------------------------------------------------------------------
# SQL constants — building profile
# ---------------------------------------------------------------------------

BUILDING_PROFILE_MV_SQL = """
SELECT bbl, address, zipcode AS zip, stories, total_units,
       mgmt_program AS managementprogram, ownername, yearbuilt, bldgclass, zoning,
       total_violations, open_violations, class_c_violations,
       latest_violation AS latest_violation_date,
       total_complaints, open_complaints,
       latest_complaint AS latest_complaint_date,
       dob_violations AS total_dob_violations,
       latest_dob AS latest_dob_date
FROM lake.foundation.mv_building_hub
WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
"""

BUILDING_PROFILE_SQL = """
WITH building AS (
    SELECT boroid, block, lot, buildingid, bin, streetname, housenumber,
           legalstories, legalclassa, legalclassb, managementprogram, zip,
           (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
    FROM lake.housing.hpd_jurisdiction
    WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
    LIMIT 1
),
violation_counts AS (
    SELECT
        COUNT(*) AS total_violations,
        COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_violations,
        MAX(TRY_CAST(novissueddate AS DATE)) AS latest_violation_date
    FROM lake.housing.hpd_violations
    WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?::VARCHAR
),
complaint_counts AS (
    SELECT
        COUNT(DISTINCT complaint_id) AS total_complaints,
        COUNT(DISTINCT complaint_id) FILTER (WHERE complaint_status = 'OPEN') AS open_complaints,
        MAX(TRY_CAST(received_date AS DATE)) AS latest_complaint_date
    FROM lake.housing.hpd_complaints
    WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?::VARCHAR
),
dob_counts AS (
    SELECT
        COUNT(*) AS total_dob_violations,
        MAX(TRY_CAST(issue_date AS DATE)) AS latest_dob_date
    FROM lake.housing.dob_ecb_violations
    WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
)
SELECT
    b.bbl, b.housenumber || ' ' || b.streetname AS address, b.zip,
    b.legalstories AS stories,
    (COALESCE(TRY_CAST(b.legalclassa AS INTEGER), 0) + COALESCE(TRY_CAST(b.legalclassb AS INTEGER), 0)) AS total_units,
    b.managementprogram,
    v.total_violations, v.open_violations, v.latest_violation_date,
    c.total_complaints, c.open_complaints, c.latest_complaint_date,
    d.total_dob_violations, d.latest_dob_date
FROM building b
CROSS JOIN violation_counts v
CROSS JOIN complaint_counts c
CROSS JOIN dob_counts d
"""

OWNER_VIOLATIONS_SQL = """
WITH hpd AS (
    SELECT 'HPD' AS source, violationid AS violation_id,
           class, novdescription AS description,
           violationstatus AS status, novissueddate AS issue_date,
           currentstatus AS current_status, currentstatusdate AS status_date
    FROM lake.housing.hpd_violations
    WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
    ORDER BY TRY_CAST(novissueddate AS DATE) DESC NULLS LAST
    LIMIT 200
),
dob AS (
    SELECT 'DOB/ECB' AS source, ecb_violation_number AS violation_id,
           severity AS class, violation_description AS description,
           ecb_violation_status AS status, issue_date,
           certification_status AS current_status, hearing_date AS status_date
    FROM lake.housing.dob_ecb_violations
    WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
    ORDER BY TRY_CAST(issue_date AS DATE) DESC NULLS LAST
    LIMIT 200
),
combined AS (
    SELECT *, TRY_CAST(issue_date AS DATE) AS sort_date FROM hpd
    UNION ALL
    SELECT *, TRY_CAST(issue_date AS DATE) AS sort_date FROM dob
)
SELECT source, violation_id, class, description, status, issue_date, current_status, status_date
FROM combined
ORDER BY sort_date DESC NULLS LAST
"""

# ---------------------------------------------------------------------------
# SQL constants — building story
# ---------------------------------------------------------------------------

STORY_PLUTO_SQL = """
SELECT yearbuilt, numfloors, unitsres, unitstotal, bldgclass, bldgarea, lotarea,
       address, ownername, yearalter1, yearalter2,
       zonedist1, assesstot, borough, zipcode
FROM lake.city_government.pluto
WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
LIMIT 1
"""

STORY_VIOLATIONS_SQL = """
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_ct,
       MIN(TRY_CAST(novissueddate AS DATE)) AS first_violation,
       MAX(TRY_CAST(novissueddate AS DATE)) AS latest_violation
FROM lake.housing.hpd_violations
WHERE boroid = ? AND block = ? AND lot = ?
"""

STORY_COMPLAINTS_SQL = """
SELECT majorcategory, COUNT(*) AS cnt
FROM lake.housing.hpd_complaints
WHERE boroid = ? AND block = ? AND lot = ?
GROUP BY majorcategory ORDER BY cnt DESC LIMIT 8
"""

STORY_311_SQL = """
SELECT complaint_type, COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ? AND bbl = ?
GROUP BY complaint_type ORDER BY cnt DESC LIMIT 8
"""

STORY_PERMITS_SQL = """
SELECT job_type, COUNT(*) AS cnt,
       MIN(TRY_CAST(filing_date AS DATE)) AS earliest,
       MAX(TRY_CAST(filing_date AS DATE)) AS latest
FROM lake.housing.dob_permit_issuance
WHERE borough = ? AND block = ? AND lot = ?
GROUP BY job_type ORDER BY cnt DESC LIMIT 5
"""

STORY_NEIGHBORS_SQL = """
SELECT
    COUNT(*) AS total_buildings,
    AVG(TRY_CAST(yearbuilt AS INT)) FILTER (WHERE TRY_CAST(yearbuilt AS INT) > 1800) AS avg_year,
    AVG(TRY_CAST(numfloors AS DOUBLE)) AS avg_floors,
    AVG(TRY_CAST(unitsres AS DOUBLE)) AS avg_units
FROM lake.city_government.pluto
WHERE zipcode = ? AND TRY_CAST(yearbuilt AS INT) > 1800
"""

# ---------------------------------------------------------------------------
# SQL constants — building context
# ---------------------------------------------------------------------------

CONTEXT_ERA_BUILDINGS_SQL = """
SELECT COUNT(*) AS total_same_year,
       COUNT(DISTINCT zipcode) AS zips_with_same_year
FROM lake.city_government.pluto
WHERE TRY_CAST(yearbuilt AS INT) = ?
"""

CONTEXT_ERA_BOROUGH_SQL = """
SELECT borough,
       COUNT(*) AS cnt
FROM lake.city_government.pluto
WHERE TRY_CAST(yearbuilt AS INT) = ?
GROUP BY borough ORDER BY cnt DESC
"""

CONTEXT_DECADE_SQL = """
SELECT
    COUNT(*) AS total_decade,
    AVG(TRY_CAST(numfloors AS DOUBLE)) AS avg_floors,
    COUNT(DISTINCT bldgclass) AS class_variety
FROM lake.city_government.pluto
WHERE TRY_CAST(yearbuilt AS INT) BETWEEN ? AND ?
"""

CONTEXT_ZIP_ERA_SQL = """
SELECT
    COUNT(*) AS buildings_in_zip,
    COUNT(*) FILTER (WHERE TRY_CAST(yearbuilt AS INT) = ?) AS same_year_in_zip,
    MIN(TRY_CAST(yearbuilt AS INT)) FILTER (WHERE TRY_CAST(yearbuilt AS INT) > 1800) AS oldest_in_zip,
    MAX(TRY_CAST(yearbuilt AS INT)) AS newest_in_zip
FROM lake.city_government.pluto
WHERE zipcode = ?
"""

# ---------------------------------------------------------------------------
# SQL constants — block timeline
# ---------------------------------------------------------------------------

BLOCK_PLUTO_SQL = """
SELECT LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') AS bbl,
       address, borough, zipcode, TRY_CAST(yearbuilt AS INT) AS yearbuilt,
       TRY_CAST(numfloors AS DOUBLE) AS numfloors, bldgclass
FROM lake.city_government.pluto
WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
LIMIT 1
"""

BLOCK_NEIGHBORS_SQL = """
SELECT LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') AS bbl,
       address, TRY_CAST(yearbuilt AS INT) AS yearbuilt,
       TRY_CAST(numfloors AS DOUBLE) AS numfloors, bldgclass,
       TRY_CAST(unitsres AS INT) AS unitsres
FROM lake.city_government.pluto
WHERE SUBSTRING(LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0'), 1, 6) = ?
  AND TRY_CAST(yearbuilt AS INT) > 1800
ORDER BY address
LIMIT 50
"""

BLOCK_PERMITS_SQL = """
SELECT EXTRACT(YEAR FROM TRY_CAST(issuance_date AS DATE)) AS yr,
       job_type, permit_type, COUNT(*) AS cnt
FROM lake.housing.dob_permit_issuance
WHERE borough = ? AND block = ?
  AND TRY_CAST(issuance_date AS DATE) >= CURRENT_DATE - INTERVAL '10 years'
GROUP BY yr, job_type, permit_type
ORDER BY yr DESC, cnt DESC
"""

BLOCK_RESTAURANTS_SQL = """
WITH inspections AS (
    SELECT camis, dba, cuisine_description, grade, grade_date,
           building || ' ' || street AS addr,
           ROW_NUMBER() OVER (PARTITION BY camis ORDER BY grade_date DESC) AS rn
    FROM lake.health.restaurant_inspections
    WHERE zipcode = ? AND grade IN ('A','B','C')
)
SELECT camis, dba, cuisine_description, grade, grade_date, addr
FROM inspections WHERE rn = 1
ORDER BY grade_date DESC
LIMIT 30
"""

BLOCK_311_SQL = """
SELECT
    EXTRACT(YEAR FROM TRY_CAST(created_date AS DATE)) AS yr,
    complaint_type,
    COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ? AND bbl LIKE ?
GROUP BY yr, complaint_type
ORDER BY yr DESC, cnt DESC
"""

BLOCK_HPD_SQL = """
SELECT
    EXTRACT(YEAR FROM TRY_CAST(receiveddate AS DATE)) AS yr,
    majorcategory,
    COUNT(*) AS cnt
FROM lake.housing.hpd_complaints
WHERE boroid = ? AND block = ?
GROUP BY yr, majorcategory
ORDER BY yr DESC, cnt DESC
"""

# ---------------------------------------------------------------------------
# SQL constants — twins
# ---------------------------------------------------------------------------

TWINS_FIND_SQL = """
WITH target AS (
    SELECT bbl, yearbuilt, numfloors, unitsres, bldgclass, bldgarea, lotarea,
           address, borough, zipcode, assesstot
    FROM lake.city_government.pluto
    WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
    LIMIT 1
)
SELECT
    LPAD(TRY_CAST(TRY_CAST(p.bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') AS twin_bbl,
    p.address, p.borough, p.zipcode,
    TRY_CAST(p.yearbuilt AS INT) AS yearbuilt,
    TRY_CAST(p.numfloors AS DOUBLE) AS numfloors,
    TRY_CAST(p.unitsres AS INT) AS unitsres,
    p.bldgclass,
    TRY_CAST(p.assesstot AS DOUBLE) AS assesstot,
    TRY_CAST(p.bldgarea AS INT) AS bldgarea,
    ABS(TRY_CAST(p.yearbuilt AS INT) - TRY_CAST(t.yearbuilt AS INT)) AS year_diff,
    ABS(TRY_CAST(p.numfloors AS DOUBLE) - TRY_CAST(t.numfloors AS DOUBLE)) AS floor_diff,
    ABS(TRY_CAST(p.unitsres AS INT) - TRY_CAST(t.unitsres AS INT)) AS unit_diff
FROM lake.city_government.pluto p
CROSS JOIN target t
WHERE p.bldgclass = t.bldgclass
  AND LPAD(TRY_CAST(TRY_CAST(p.bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') != ?
  AND p.borough != t.borough
  AND TRY_CAST(p.yearbuilt AS INT) BETWEEN TRY_CAST(t.yearbuilt AS INT) - 5 AND TRY_CAST(t.yearbuilt AS INT) + 5
  AND TRY_CAST(p.numfloors AS DOUBLE) BETWEEN TRY_CAST(t.numfloors AS DOUBLE) - 2 AND TRY_CAST(t.numfloors AS DOUBLE) + 2
  AND TRY_CAST(p.yearbuilt AS INT) > 1800
ORDER BY (year_diff + floor_diff * 2 + unit_diff * 0.5) ASC
LIMIT 5
"""

TWINS_VIOLATIONS_SQL = """
SELECT
    (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl,
    COUNT(*) AS total_violations,
    COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_violations
FROM lake.housing.hpd_violations
WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) IN ({placeholders})
GROUP BY 1
"""

TWINS_COUNT_SQL = """
SELECT COUNT(*) AS total_similar
FROM lake.city_government.pluto p
WHERE bldgclass = ?
  AND TRY_CAST(yearbuilt AS INT) BETWEEN ? AND ?
  AND TRY_CAST(yearbuilt AS INT) > 1800
"""

# ---------------------------------------------------------------------------
# Reference data — eras, milestones, famous buildings
# ---------------------------------------------------------------------------

_NYC_ERAS = {
    range(1800, 1860): ("Antebellum New York", "when Manhattan was still farmland above 14th Street"),
    range(1860, 1900): ("the Gilded Age", "as immigrants poured through Castle Garden and the Brooklyn Bridge opened"),
    range(1900, 1920): ("the Progressive Era", "when tenement reform reshaped housing and the subway arrived"),
    range(1920, 1930): ("the Roaring Twenties", "during Prohibition, jazz clubs, and the Harlem Renaissance"),
    range(1930, 1946): ("the Depression & War years", "when WPA workers built parks and public housing across the city"),
    range(1946, 1960): ("the postwar boom", "as GIs returned home and Robert Moses reshaped the boroughs"),
    range(1960, 1980): ("the urban crisis", "through fiscal collapse, the '77 blackout, and the South Bronx fires"),
    range(1980, 2000): ("the renewal era", "as crack receded, crime fell, and neighborhoods rebuilt"),
    range(2000, 2010): ("the post-9/11 era", "through rebuilding, rezoning, and the 2008 financial crisis"),
    range(2010, 2020): ("the Bloomberg-de Blasio years", "amid record construction, Hurricane Sandy, and rising rents"),
    range(2020, 2030): ("the pandemic era", "through COVID, remote work, and a transformed city"),
}

_NYC_MILESTONES = [
    (1883, "Brooklyn Bridge opened"),
    (1886, "Statue of Liberty dedicated"),
    (1904, "First subway line opened"),
    (1929, "Stock market crash"),
    (1931, "Empire State Building completed"),
    (1939, "World's Fair in Flushing"),
    (1964, "World's Fair returns"),
    (1969, "Stonewall uprising"),
    (1977, "Blackout"),
    (2001, "September 11th"),
    (2012, "Hurricane Sandy"),
    (2020, "COVID-19 pandemic"),
]

_FAMOUS_BUILDINGS = [
    (1883, "Brooklyn Bridge"),
    (1885, "The Dakota"),
    (1902, "Flatiron Building"),
    (1903, "Williamsburg Bridge"),
    (1904, "New York Times Tower (original)"),
    (1910, "Penn Station (original)"),
    (1913, "Woolworth Building"),
    (1913, "Grand Central Terminal"),
    (1927, "Holland Tunnel"),
    (1930, "Chrysler Building"),
    (1931, "Empire State Building"),
    (1931, "George Washington Bridge"),
    (1932, "Radio City Music Hall"),
    (1937, "Lincoln Tunnel"),
    (1939, "Rockefeller Center"),
    (1950, "United Nations HQ"),
    (1959, "Guggenheim Museum"),
    (1962, "Lincoln Center"),
    (1964, "Verrazano-Narrows Bridge"),
    (1966, "Madison Square Garden (current)"),
    (1973, "World Trade Center (original)"),
    (1977, "Citicorp Center (now 601 Lex)"),
    (2004, "Time Warner Center"),
    (2014, "One World Trade Center"),
    (2019, "Hudson Yards"),
]


# ---------------------------------------------------------------------------
# View implementations
# ---------------------------------------------------------------------------


def _view_full(pool, bbl: str) -> ToolResult:
    """Full building profile with violations, enrichments, and percentile."""
    t_start = time.time()

    # --- Step 1: BBL resolution (sequential — everything depends on this) ---
    try:
        cols, rows = execute(pool, BUILDING_PROFILE_MV_SQL, [bbl])
    except Exception:
        cols, rows = execute(pool, BUILDING_PROFILE_SQL, [bbl, bbl, bbl, bbl])

    t_bbl = time.time()
    print(f"[building:full] BBL resolve: {(t_bbl - t_start)*1000:.0f}ms", flush=True)

    if not rows:
        raise ToolError(f"No building found for BBL {bbl}")

    row = dict(zip(cols, rows[0]))

    # Determine whether PLUTO backfill is needed before launching parallel queries
    needs_pluto = not row.get("address") or not row.get("zip")

    borough = bbl[0]
    block = bbl[1:6].lstrip("0") or "0"
    lot = bbl[6:10].lstrip("0") or "0"

    # --- Step 2: All independent enrichment queries in parallel ---
    enrichment_queries: list[tuple[str, str, list | None]] = [
        (
            "landmark",
            """
            SELECT lm_name, lm_type, hist_distr, status, desdate
            FROM lake.housing.designated_buildings
            WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
            LIMIT 1
            """,
            [bbl],
        ),
        (
            "tax",
            """
            SELECT exmp_code, exname, year, curexmptot, benftstart
            FROM lake.housing.tax_exemptions
            WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
            ORDER BY year DESC
            LIMIT 5
            """,
            [bbl],
        ),
        # NOTE: property_valuation (10.5M rows, ~25s full scan) removed from parallel
        # batch. Key fields (zoning, owner, yearbuilt) already come from mv_building_hub.
        # TODO: add foundation.mv_valuation materialized view for fast point lookups.
        (
            "sro",
            """
            SELECT dobbuildingclass, legalclassa, legalclassb, managementprogram
            FROM lake.housing.sro_buildings
            WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
            LIMIT 1
            """,
            [bbl],
        ),
        (
            "facade",
            """
            SELECT current_status, filing_status, cycle, exterior_wall_type_sx
            FROM lake.housing.dob_safety_facades
            WHERE borough = ? AND block = ? AND lot = ?
            ORDER BY TRY_CAST(submitted_on AS DATE) DESC NULLS LAST
            LIMIT 1
            """,
            [borough, block, lot],
        ),
    ]

    if needs_pluto:
        enrichment_queries.append((
            "pluto",
            """
            SELECT address, zipcode, ownername, yearbuilt, bldgclass, zonedist1
            FROM lake.city_government.pluto
            WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
            LIMIT 1
            """,
            [bbl],
        ))

    t_pre_parallel = time.time()
    print(f"[building:full] Pre-parallel setup: {(t_pre_parallel - t_bbl)*1000:.0f}ms, {len(enrichment_queries)} queries", flush=True)
    results = parallel_queries(pool, enrichment_queries)
    t_post_parallel = time.time()
    print(f"[building:full] Parallel queries: {(t_post_parallel - t_pre_parallel)*1000:.0f}ms", flush=True)

    # Unpack parallel results
    landmark_rows = results.get("landmark", ([], []))[1]
    tax_rows = results.get("tax", ([], []))[1]
    val_rows = results.get("valuation", ([], []))[1]
    sro_rows = results.get("sro", ([], []))[1]
    facade_rows = results.get("facade", ([], []))[1]

    # Backfill address/zip/owner from PLUTO when MV returns NULLs
    if needs_pluto:
        pluto_rows = results.get("pluto", ([], []))[1]
        if pluto_rows:
            pr = pluto_rows[0]
            if not row.get("address") and pr[0]:
                row["address"] = pr[0]
            if not row.get("zip") and pr[1]:
                row["zip"] = pr[1]
            if not row.get("ownername") and pr[2]:
                row["ownername"] = pr[2]
            if not row.get("yearbuilt") and pr[3]:
                row["yearbuilt"] = pr[3]
            if not row.get("bldgclass") and pr[4]:
                row["bldgclass"] = pr[4]
            if not row.get("zoning") and pr[5]:
                row["zoning"] = pr[5]

    # Violation percentile — use pre-computed pctile_buildings (fast point lookup,
    # replaces the full city-wide GROUP BY scan that ran on every call)
    pctile_data = lookup_percentiles(pool, "building", bbl)
    if pctile_data.get("violation_pctile") is not None:
        row["violation_percentile"] = round(pctile_data["violation_pctile"] * 100, 1)

    summary = (
        "PRESENTATION: Show this complete building profile to the user. "
        "Use interactive charts for violation counts and percentile ranking. "
        "Do not omit any field — every metric is independently valuable.\n\n"
        f"BBL {row['bbl']}: {row['address']}, {row['zip']}\n"
        f"  {row['stories']} stories, {row['total_units']} units"
        f" ({row['managementprogram'] or 'N/A'})\n"
        f"  HPD Violations: {row['total_violations']} total,"
        f" {row['open_violations']} open"
        f" (latest: {row['latest_violation_date'] or 'N/A'})\n"
        f"  HPD Complaints: {row['total_complaints']} total,"
        f" {row['open_complaints']} open"
        f" (latest: {row['latest_complaint_date'] or 'N/A'})\n"
        f"  DOB/ECB Violations: {row['total_dob_violations']}"
        f" (latest: {row['latest_dob_date'] or 'N/A'})"
    )

    if landmark_rows:
        r = landmark_rows[0]
        summary += f"\n  Landmark: {r[0] or '?'} ({r[1] or '?'}) | {r[2] or 'N/A'} | {r[3] or '?'} | designated {r[4] or '?'}"
        row["landmark"] = {"name": r[0], "type": r[1], "district": r[2], "status": r[3], "date": r[4]}

    if tax_rows:
        exemptions = [f"{r[1] or r[0]} ({r[2] or '?'})" for r in tax_rows[:3]]
        summary += f"\n  Tax Exemptions: {', '.join(exemptions)}"
        row["tax_exemptions"] = [{"code": r[0], "name": r[1], "year": r[2], "amount": r[3], "start": r[4]} for r in tax_rows]

    if val_rows:
        r = val_rows[0]
        mkt = f"${float(r[1] or 0):,.0f}" if r[1] else "?"
        summary += f"\n  Valuation ({r[0] or '?'}): Market {mkt} | Zoning {r[4] or '?'} | Owner: {r[5] or '?'} | Built {r[7] or '?'}"
        row["valuation"] = {"year": r[0], "market_total": r[1], "actual_total": r[2],
                            "taxable_total": r[3], "zoning": r[4], "owner": r[5],
                            "bldg_class": r[6], "year_built": r[7], "units": r[8], "gross_sqft": r[9]}

    if sro_rows:
        summary += f"\n  SRO: Yes — Class A: {sro_rows[0][1] or 0}, Class B: {sro_rows[0][2] or 0}"
        row["is_sro"] = True

    if facade_rows:
        r = facade_rows[0]
        summary += f"\n  Facade (FISP): {r[0] or '?'} | Cycle {r[2] or '?'} | Wall: {r[3] or '?'}"
        row["facade"] = {"status": r[0], "filing": r[1], "cycle": r[2], "wall_type": r[3]}

    if "violation_percentile" in row:
        summary += f"\n  Violation percentile: {row['violation_percentile']}% (city-wide, by building)"

    return ToolResult(
        content=summary,
        structured_content={"building": row},
        meta={"bbl": bbl},
    )


def _view_story(pool, bbl: str) -> ToolResult:
    """Narrative history of a building."""
    t0 = time.time()

    cols, rows = safe_query(pool, STORY_PLUTO_SQL, [bbl])
    if not rows:
        raise ToolError(f"No building found for BBL {bbl} in PLUTO.")

    p = dict(zip(cols, rows[0]))
    year = int(float(p.get("yearbuilt") or 0))
    floors = int(float(p.get("numfloors") or 0))
    units = int(float(p.get("unitsres") or 0))
    bldg_class = p.get("bldgclass") or "Unknown"
    address = p.get("address") or "Unknown"
    owner = p.get("ownername") or "Unknown"
    borough = p.get("borough") or ""
    zipcode = p.get("zipcode") or ""
    landmark = p.get("landmark")
    lot_area = int(float(p.get("lotarea") or 0))
    bldg_area = int(float(p.get("bldgarea") or 0))
    assessed = float(p.get("assesstot") or 0)
    alter1 = int(float(p.get("yearalter1") or 0))
    alter2 = int(float(p.get("yearalter2") or 0))
    zone = p.get("zonedist1") or ""

    boro_digit = bbl[0]
    block = str(int(bbl[1:6]))
    lot = str(int(bbl[6:10]))

    current_year = datetime.date.today().year
    age = current_year - year if year > 1800 else None
    era_name, era_desc = "an unknown era", ""
    for yr_range, (name, desc) in _NYC_ERAS.items():
        if year in yr_range:
            era_name, era_desc = name, desc
            break

    witnessed = [(y, e) for y, e in _NYC_MILESTONES if y >= year] if year > 1800 else []
    families_est = (units * age // 5) if age and units else None

    lines = []
    lines.append(f"THE LIFE OF {address}, {borough}")
    lines.append("=" * 60)

    if age and year > 1800:
        lines.append(f"\nBuilt in {year}, during {era_name} — {era_desc}.")
        lines.append(f"Your building is {age} years old.")
        if families_est:
            lines.append(f"Approximately {families_est:,} families have called it home.")
    else:
        lines.append(f"\nYear built: {year if year > 0 else 'Unknown'}")

    lines.append(f"\nTHE FACTS")
    lines.append(f"  Floors: {floors}")
    lines.append(f"  Residential units: {units}")
    lines.append(f"  Building class: {bldg_class}")
    lines.append(f"  Lot area: {lot_area:,} sq ft")
    lines.append(f"  Building area: {bldg_area:,} sq ft")
    lines.append(f"  Zoning: {zone}")
    lines.append(f"  Assessed value: ${assessed:,.0f}")
    lines.append(f"  Owner: {owner}")

    if landmark:
        lines.append(f"  Landmark: {landmark}")
    if alter1 and alter1 > 1800:
        lines.append(f"  Major alteration: {alter1}")
    if alter2 and alter2 > 1800:
        lines.append(f"  Second alteration: {alter2}")

    # All independent queries in one parallel batch
    story_batch = [
        ("violations", STORY_VIOLATIONS_SQL, [boro_digit, block, lot]),
        ("complaints", STORY_COMPLAINTS_SQL, [boro_digit, block, lot]),
        ("permits", STORY_PERMITS_SQL, [boro_digit, block, lot]),
    ]
    if zipcode:
        story_batch.append(("s311", STORY_311_SQL, [zipcode, bbl]))
        story_batch.append(("neighbors", STORY_NEIGHBORS_SQL, [zipcode]))
    story_results = parallel_queries(pool, story_batch)

    cols_v, rows_v = story_results.get("violations", ([], []))
    cols_c, rows_c = story_results.get("complaints", ([], []))
    cols_p, rows_p = story_results.get("permits", ([], []))
    cols_311, rows_311 = story_results.get("s311", ([], []))
    cols_n, rows_n = story_results.get("neighbors", ([], []))

    # Violations
    if rows_v:
        v = dict(zip(cols_v, rows_v[0]))
        total_v = int(v.get("total") or 0)
        open_v = int(v.get("open_ct") or 0)
        first_v = v.get("first_violation")
        latest_v = v.get("latest_violation")
        if total_v:
            lines.append(f"\nVIOLATION HISTORY")
            lines.append(f"  {total_v:,} total violations on record ({open_v:,} currently open)")
            if first_v:
                lines.append(f"  First recorded: {first_v}")
            if latest_v:
                lines.append(f"  Most recent: {latest_v}")

    # Complaint character
    if rows_c:
        lines.append(f"\nWHAT RESIDENTS TALK ABOUT")
        for row in rows_c[:6]:
            r = dict(zip(cols_c, row))
            cat = r.get("majorcategory") or "Other"
            cnt = int(r.get("cnt") or 0)
            lines.append(f"  {cat}: {cnt:,} complaints")

    # 311
    if zipcode and rows_311:
        lines.append(f"\n311 CALLS (since 2020)")
        for row in rows_311[:5]:
            r = dict(zip(cols_311, row))
            lines.append(f"  {r.get('complaint_type')}: {int(r.get('cnt') or 0):,}")

    # Construction history
    if rows_p:
        lines.append(f"\nCONSTRUCTION HISTORY")
        for row in rows_p:
            r = dict(zip(cols_p, row))
            lines.append(f"  {r.get('job_type')}: {int(r.get('cnt') or 0)} permits ({r.get('earliest')} — {r.get('latest')})")

    # Milestones
    if witnessed:
        lines.append(f"\nWHAT THIS BUILDING HAS WITNESSED")
        for yr, event in witnessed:
            lines.append(f"  {yr}: {event}")

    # Neighborhood comparison
    if zipcode and rows_n:
        n = dict(zip(cols_n, rows_n[0]))
        total_n = int(n.get("total_buildings") or 0)
        avg_yr = int(float(n.get("avg_year") or 0))
        avg_fl = round(float(n.get("avg_floors") or 0), 1)
        if total_n:
            lines.append(f"\nIN THE NEIGHBORHOOD (ZIP {zipcode})")
            lines.append(f"  {total_n:,} buildings in your ZIP code")
            if avg_yr > 1800 and year > 1800:
                diff = year - avg_yr
                if diff < -5:
                    lines.append(f"  Your building is {abs(diff)} years older than average (built {avg_yr})")
                elif diff > 5:
                    lines.append(f"  Your building is {diff} years younger than average (built {avg_yr})")
                else:
                    lines.append(f"  Your building is typical age for the area (avg built {avg_yr})")
            if floors:
                if floors > avg_fl + 2:
                    lines.append(f"  Taller than neighbors ({floors} floors vs avg {avg_fl})")
                elif floors < avg_fl - 2:
                    lines.append(f"  Shorter than neighbors ({floors} floors vs avg {avg_fl})")
                else:
                    lines.append(f"  Similar height to neighbors ({floors} floors, avg {avg_fl})")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "bbl": bbl, "address": address, "borough": borough,
        "year_built": year, "age": age, "era": era_name,
        "floors": floors, "units": units, "building_class": bldg_class,
        "assessed_value": assessed, "owner": owner,
        "landmark": landmark,
        "families_estimate": families_est, "milestones_witnessed": len(witnessed),
    }

    content = "\n".join(lines)
    analysis_directive = (
        "ANALYSIS: Before showing this building story, write 1-2 sentences summarizing "
        "the key takeaway — cite specific numbers (violations, age, owner, complaints). "
        "Be direct, no preamble.\n\n"
    )

    return ToolResult(
        content=analysis_directive + content,
        structured_content=structured,
        meta={"bbl": bbl, "query_time_ms": elapsed},
    )


def _view_block(pool, bbl: str) -> ToolResult:
    """Block timeline — all buildings, permits, restaurants, complaints."""
    t0 = time.time()

    cols_b, rows_b = safe_query(pool, BLOCK_PLUTO_SQL, [bbl])
    if not rows_b:
        raise ToolError(f"No building found for BBL {bbl}.")

    bldg = dict(zip(cols_b, rows_b[0]))
    address = bldg.get("address") or "Unknown"
    borough = bldg.get("borough") or ""
    zipcode = bldg.get("zipcode") or ""
    boro_digit = bbl[0]
    block = str(int(bbl[1:6]))
    block_prefix = bbl[:6]

    lines = []
    lines.append(f"YOUR BLOCK THROUGH TIME")
    lines.append(f"Block {block}, {borough} (from {address})")
    lines.append("=" * 60)

    # All independent block queries in one parallel batch
    block_batch = [
        ("neighbors", BLOCK_NEIGHBORS_SQL, [block_prefix]),
        ("permits", BLOCK_PERMITS_SQL, [boro_digit, block]),
        ("hpd", BLOCK_HPD_SQL, [boro_digit, block]),
    ]
    if zipcode:
        block_batch.append(("restaurants", BLOCK_RESTAURANTS_SQL, [zipcode]))
        bbl_prefix = bbl[:6] + "%"
        block_batch.append(("s311", BLOCK_311_SQL, [zipcode, bbl_prefix]))
    block_results = parallel_queries(pool, block_batch)

    cols_n, rows_n = block_results.get("neighbors", ([], []))
    cols_p, rows_p = block_results.get("permits", ([], []))
    cols_h, rows_h = block_results.get("hpd", ([], []))
    cols_r, rows_r = block_results.get("restaurants", ([], []))
    cols_311, rows_311 = block_results.get("s311", ([], []))

    # All buildings on the block
    if rows_n:
        buildings = [dict(zip(cols_n, r)) for r in rows_n]
        years = [b["yearbuilt"] for b in buildings if b.get("yearbuilt")]
        lines.append(f"\nBUILDINGS ON YOUR BLOCK: {len(buildings)}")
        if years:
            lines.append(f"  Oldest: {min(years)} · Newest: {max(years)} · Span: {max(years) - min(years)} years")
        decades = {}
        for b in buildings:
            y = b.get("yearbuilt")
            if y:
                decade = (y // 10) * 10
                decades[decade] = decades.get(decade, 0) + 1
        if decades:
            lines.append(f"  Built by decade:")
            for d in sorted(decades):
                lines.append(f"    {d}s: {decades[d]} building{'s' if decades[d] > 1 else ''}")

        lines.append(f"\n  Notable addresses:")
        for b in buildings[:10]:
            y = b.get("yearbuilt") or "?"
            fl = int(float(b.get("numfloors") or 0))
            cls = b.get("bldgclass") or ""
            marker = " ← YOUR BUILDING" if b.get("bbl") == bbl else ""
            lines.append(f"    {b.get('address')}: {y}, {fl}fl, {cls}{marker}")

    # Construction permits (last 10 years)
    if rows_p:
        lines.append(f"\nCONSTRUCTION ACTIVITY (last 10 years)")
        by_year = {}
        for row in rows_p:
            r = dict(zip(cols_p, row))
            yr = int(r.get("yr") or 0)
            if yr > 0:
                if yr not in by_year:
                    by_year[yr] = []
                by_year[yr].append((r.get("job_type") or "?", int(r.get("cnt") or 0)))

        for yr in sorted(by_year, reverse=True)[:8]:
            total = sum(c for _, c in by_year[yr])
            types = ", ".join(f"{t}: {c}" for t, c in by_year[yr][:3])
            lines.append(f"  {yr}: {total} permits ({types})")

        years_list = sorted(by_year.keys())
        if len(years_list) >= 3:
            recent = sum(sum(c for _, c in by_year[y]) for y in years_list[-3:]) / 3
            earlier = sum(sum(c for _, c in by_year[y]) for y in years_list[:3]) / 3
            if earlier > 0:
                change = (recent - earlier) / earlier * 100
                if change > 20:
                    lines.append(f"\n  Trend: construction activity up {change:.0f}% vs. early period")
                elif change < -20:
                    lines.append(f"\n  Trend: construction activity down {abs(change):.0f}% vs. early period")

    # Restaurant scene
    if zipcode and rows_r:
        restaurants = [dict(zip(cols_r, r)) for r in rows_r]
        cuisines = {}
        for rest in restaurants:
            c = rest.get("cuisine_description") or "Other"
            cuisines[c] = cuisines.get(c, 0) + 1
        lines.append(f"\nRESTAURANT SCENE (ZIP {zipcode})")
        lines.append(f"  {len(restaurants)} restaurants with recent grades")
        top_cuisines = sorted(cuisines.items(), key=lambda x: -x[1])[:5]
        lines.append(f"  Top cuisines: {', '.join(f'{c} ({n})' for c, n in top_cuisines)}")

    # 311 trends
    if zipcode and rows_311:
        lines.append(f"\n311 COMPLAINTS ON YOUR BLOCK (since 2020)")
        by_year_311 = {}
        for row in rows_311:
            r = dict(zip(cols_311, row))
            yr = int(r.get("yr") or 0)
            if yr > 0:
                if yr not in by_year_311:
                    by_year_311[yr] = {}
                by_year_311[yr][r.get("complaint_type")] = int(r.get("cnt") or 0)
        for yr in sorted(by_year_311, reverse=True)[:4]:
            total = sum(by_year_311[yr].values())
            top = sorted(by_year_311[yr].items(), key=lambda x: -x[1])[:3]
            top_str = ", ".join(f"{t}: {c}" for t, c in top)
            lines.append(f"  {yr}: {total} calls ({top_str})")

    # HPD complaint trends
    if rows_h:
        lines.append(f"\nHPD COMPLAINTS ON YOUR BLOCK")
        by_year_hpd = {}
        for row in rows_h:
            r = dict(zip(cols_h, row))
            yr = int(r.get("yr") or 0)
            if yr >= 2018:
                if yr not in by_year_hpd:
                    by_year_hpd[yr] = {}
                by_year_hpd[yr][r.get("majorcategory")] = int(r.get("cnt") or 0)
        for yr in sorted(by_year_hpd, reverse=True)[:5]:
            total = sum(by_year_hpd[yr].values())
            top = sorted(by_year_hpd[yr].items(), key=lambda x: -x[1])[:2]
            top_str = ", ".join(f"{t}: {c}" for t, c in top)
            lines.append(f"  {yr}: {total} complaints ({top_str})")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "bbl": bbl, "address": address, "borough": borough,
        "block": block, "buildings_on_block": len(rows_n) if rows_n else 0,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"bbl": bbl, "query_time_ms": elapsed},
    )


def _view_similar(pool, bbl: str, ctx=None) -> ToolResult:
    """Find similar buildings via vector similarity + PLUTO twins."""
    t0 = time.time()

    # Try hnsw_acorn vector search first
    emb_conn = ctx.lifespan_context.get("emb_conn") if hasattr(ctx, 'lifespan_context') else None
    try:
        if not emb_conn:
            raise Exception("No embedding DB")
        target_rows = emb_conn.execute(
            "SELECT features FROM building_vectors WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?", [bbl]
        ).fetchall()

        if target_rows:
            target_vec = target_rows[0][0]
            limit = 10
            knn_result = emb_conn.execute(
                "SELECT bv.bbl, gb.housenumber, gb.streetname, gb.zip, "
                "gb.stories, gb.units, gb.year_built, gb.borough, "
                "array_distance(bv.features, ?::FLOAT[]) AS distance "
                "FROM building_vectors bv "
                "LEFT JOIN main.graph_buildings gb ON gb.bbl = bv.bbl "
                "WHERE bv.bbl != ? "
                "ORDER BY distance LIMIT ?",
                [list(target_vec), bbl, limit],
            )
            result = knn_result.fetchall()
            cols = [d[0] for d in knn_result.description]

            elapsed = round((time.time() - t0) * 1000)

            if result:
                lines = [f"BUILDINGS SIMILAR TO {bbl}", "=" * 60, ""]
                records = []
                for i, row in enumerate(result, 1):
                    r = dict(zip(cols, row))
                    sim_bbl = r.get("bbl") or ""
                    housenumber = r.get("housenumber") or ""
                    streetname = r.get("streetname") or ""
                    zip_code = r.get("zip") or ""
                    stories = r.get("stories") or "?"
                    units = r.get("units") or "?"
                    year_built = r.get("year_built") or "?"
                    boro = r.get("borough") or ""
                    addr = f"{housenumber} {streetname}".strip() or "Unknown address"
                    addr_full = f"{addr}, {boro} {zip_code}".strip(", ")
                    lines.append(f"{i}. BBL {sim_bbl} — {addr_full}")
                    lines.append(f"   {stories} stories | {units} units | built {year_built}")
                    records.append({
                        "bbl": sim_bbl, "address": addr_full,
                        "stories": stories, "units": units,
                        "year_built": year_built, "borough": boro,
                    })
                lines.append(f"\n({elapsed}ms)")
                return ToolResult(
                    content="\n".join(lines),
                    structured_content={"target_bbl": bbl, "similar_buildings": records},
                    meta={"bbl": bbl, "query_time_ms": elapsed, "count": len(records)},
                )
    except Exception:
        pass

    # Fallback: PLUTO twins (nyc_twins logic)
    cols_t, rows_t = safe_query(pool, STORY_PLUTO_SQL, [bbl])
    if not rows_t:
        raise ToolError(f"No building found for BBL {bbl} in PLUTO.")

    target = dict(zip(cols_t, rows_t[0]))
    year = int(float(target.get("yearbuilt") or 0))
    floors = int(float(target.get("numfloors") or 0))
    units = int(float(target.get("unitsres") or 0))
    bldg_class = target.get("bldgclass") or "Unknown"
    address = target.get("address") or "Unknown"
    borough = target.get("borough") or ""
    assessed = float(target.get("assesstot") or 0)

    if year < 1800:
        raise ToolError(f"Building has no valid year built ({year}). Cannot find twins.")

    twins_results = parallel_queries(pool, [
        ("twins", TWINS_FIND_SQL, [bbl, bbl]),
        ("count", TWINS_COUNT_SQL, [bldg_class, year - 5, year + 5]),
    ])
    cols_tw, rows_tw = twins_results.get("twins", ([], []))
    cols_ct, rows_ct = twins_results.get("count", ([], []))
    total_similar = 0
    if rows_ct:
        total_similar = int(dict(zip(cols_ct, rows_ct[0])).get("total_similar") or 0)

    lines = []
    lines.append(f"BUILDINGS LIKE YOURS")
    lines.append("=" * 60)
    lines.append(f"\nYOUR BUILDING: {address}, {borough}")
    lines.append(f"  Built {year} · {floors} floors · {units} units · Class {bldg_class}")
    lines.append(f"  Assessed: ${assessed:,.0f}")

    if total_similar:
        lines.append(f"\nThere are {total_similar:,} buildings in NYC with the same class ({bldg_class})")
        lines.append(f"built within 5 years of yours ({year-5}–{year+5}).")

    if not rows_tw:
        lines.append(f"\nNo close twins found in other boroughs with matching class and era.")
    else:
        twin_bbls = [dict(zip(cols_tw, r))["twin_bbl"] for r in rows_tw]
        all_bbls = [bbl] + twin_bbls
        viol_sql = fill_placeholders(TWINS_VIOLATIONS_SQL, all_bbls)
        cols_v, rows_v = safe_query(pool, viol_sql, all_bbls)
        viol_map = {}
        if rows_v:
            for row in rows_v:
                v = dict(zip(cols_v, row))
                viol_map[v["bbl"]] = (int(v["total_violations"]), int(v["open_violations"]))

        my_viols = viol_map.get(bbl, (0, 0))
        lines.append(f"\n  Your violations: {my_viols[0]:,} total, {my_viols[1]:,} open")
        lines.append(f"\nTWINS IN OTHER BOROUGHS")
        lines.append("-" * 40)

        for row in rows_tw:
            tw = dict(zip(cols_tw, row))
            tw_bbl = tw["twin_bbl"]
            tw_viols = viol_map.get(tw_bbl, (0, 0))
            tw_assessed = float(tw.get("assesstot") or 0)
            val_diff = ""
            if assessed > 0 and tw_assessed > 0:
                ratio = tw_assessed / assessed
                if ratio > 1.2:
                    val_diff = f" ({ratio:.1f}x your assessed value)"
                elif ratio < 0.8:
                    val_diff = f" ({ratio:.1f}x your assessed value)"

            lines.append(f"\n  {tw.get('address')}, {tw.get('borough')} (ZIP {tw.get('zipcode')})")
            lines.append(f"    Built {tw.get('yearbuilt')} · {int(float(tw.get('numfloors') or 0))} floors · {tw.get('unitsres')} units")
            lines.append(f"    Assessed: ${tw_assessed:,.0f}{val_diff}")
            lines.append(f"    Violations: {tw_viols[0]:,} total, {tw_viols[1]:,} open")

            if tw_viols[0] > my_viols[0] * 1.5 and my_viols[0] > 0:
                lines.append(f"    ↑ {tw_viols[0] / my_viols[0]:.1f}x more violations than yours")
            elif my_viols[0] > tw_viols[0] * 1.5 and tw_viols[0] > 0:
                lines.append(f"    ↓ {my_viols[0] / tw_viols[0]:.1f}x fewer violations than yours")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "bbl": bbl, "address": address, "borough": borough,
        "year_built": year, "building_class": bldg_class,
        "total_similar_citywide": total_similar,
        "twins_found": len(rows_tw) if rows_tw else 0,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"bbl": bbl, "query_time_ms": elapsed},
    )


def _view_enforcement(pool, bbl: str) -> ToolResult:
    """Multi-agency enforcement web for a property."""
    t0 = time.time()
    borough = bbl[0]
    block = bbl[1:6].lstrip("0") or "0"
    lot = bbl[6:10].lstrip("0") or "0"

    # Resolve BIN once — needed by queries 7 and 9
    _bin_cols, _bin_rows = safe_query(pool, """
        SELECT bin FROM lake.housing.hpd_jurisdiction
        WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
        LIMIT 1
    """, [bbl])
    bin_number = _bin_rows[0][0] if _bin_rows else None

    # Run all 10 queries in parallel
    results = parallel_queries(pool, [
        # 1. HPD violations
        ("hpd", """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_v,
                COUNT(*) FILTER (WHERE UPPER(class) = 'C') AS class_c,
                COUNT(*) FILTER (WHERE UPPER(class) = 'B') AS class_b,
                COUNT(*) FILTER (WHERE UPPER(class) = 'A') AS class_a,
                MAX(TRY_CAST(novissueddate AS DATE)) AS latest,
                MIN(TRY_CAST(novissueddate AS DATE)) AS earliest
            FROM lake.housing.hpd_violations
            WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        """, [bbl]),
        # 2. DOB ECB violations
        ("dob", """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE UPPER(violation_type) LIKE '%HAZARD%'
                    OR UPPER(severity) = 'HAZARDOUS') AS hazardous,
                SUM(TRY_CAST(balance_due AS DOUBLE)) AS total_penalties_due,
                SUM(TRY_CAST(penality_imposed AS DOUBLE)) AS total_paid,
                MAX(TRY_CAST(issue_date AS DATE)) AS latest,
                MIN(TRY_CAST(issue_date AS DATE)) AS earliest
            FROM lake.housing.dob_ecb_violations
            WHERE boro = ? AND block = ? AND lot = ?
        """, [borough, block, lot]),
        # 3. FDNY violations
        ("fdny", """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE UPPER(action) NOT IN ('CLOSED', 'DISMISSED')) AS open_v,
                MAX(TRY_CAST(vio_date AS DATE)) AS latest,
                MIN(TRY_CAST(vio_date AS DATE)) AS earliest
            FROM lake.housing.fdny_violations
            WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        """, [bbl]),
        # 4. OATH hearings
        ("oath", """
            SELECT
                issuing_agency,
                COUNT(*) AS total,
                SUM(TRY_CAST(total_violation_amount AS DOUBLE)) AS total_penalty,
                SUM(TRY_CAST(paid_amount AS DOUBLE)) AS total_paid,
                SUM(TRY_CAST(balance_due AS DOUBLE)) AS amount_due,
                COUNT(*) FILTER (WHERE UPPER(hearing_status) = 'DEFAULT') AS defaults,
                MAX(TRY_CAST(hearing_date AS DATE)) AS latest
            FROM lake.city_government.oath_hearings
            WHERE violation_location_block_no = ? AND violation_location_lot_no = ?
                AND violation_location_borough IS NOT NULL
            GROUP BY issuing_agency
            ORDER BY total_penalty DESC NULLS LAST
        """, [block, lot]),
        # 5. Restaurant inspections
        ("rest", """
            SELECT dba, cuisine_description, grade,
                   inspection_date, violation_code, violation_description,
                   critical_flag, score
            FROM lake.health.restaurant_inspections
            WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
            ORDER BY inspection_date DESC
            LIMIT 20
        """, [bbl]),
        # 6. HPD complaints summary
        ("complaints", """
            SELECT
                COUNT(DISTINCT complaint_id) AS total_complaints,
                COUNT(DISTINCT complaint_id) FILTER (WHERE UPPER(complaint_status) = 'OPEN') AS open_complaints,
                MAX(TRY_CAST(received_date AS DATE)) AS latest
            FROM lake.housing.hpd_complaints
            WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        """, [bbl]),
        # 7. DOB complaints (needs BIN)
        ("dob_comp", """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE UPPER(status) = 'ACTIVE') AS active,
                COUNT(DISTINCT complaint_category) AS categories,
                MAX(TRY_CAST(date_entered AS DATE)) AS latest,
                MIN(TRY_CAST(date_entered AS DATE)) AS earliest
            FROM lake.housing.dob_complaints
            WHERE bin = ?
        """, [bin_number]),
        # 8. DOB safety facades
        ("facades", """
            SELECT current_status, filing_status, cycle,
                   owner_name, exterior_wall_type_sx, comments
            FROM lake.housing.dob_safety_facades
            WHERE borough = ? AND block = ? AND lot = ?
            ORDER BY TRY_CAST(submitted_on AS DATE) DESC NULLS LAST
            LIMIT 5
        """, [borough, block, lot]),
        # 9. DOB safety boilers (needs BIN)
        ("boilers", """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE UPPER(defects_exist) = 'YES') AS with_defects,
                MAX(TRY_CAST(inspection_date AS DATE)) AS latest_inspection,
                owner_first_name || ' ' || owner_last_name AS owner
            FROM lake.housing.dob_safety_boiler
            WHERE bin_number = ?
            GROUP BY owner
        """, [bin_number]),
        # 10. SRO buildings
        ("sro", """
            SELECT buildingid, managementprogram, legalclassa, legalclassb,
                   dobbuildingclass, legalstories
            FROM lake.housing.sro_buildings
            WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
            LIMIT 1
        """, [bbl]),
    ])

    # Unpack results
    hpd_cols, hpd_rows = results["hpd"]
    dob_cols, dob_rows = results["dob"]
    fdny_cols, fdny_rows = results["fdny"]
    oath_cols, oath_rows = results["oath"]
    rest_cols, rest_rows = results["rest"]
    complaint_cols, complaint_rows = results["complaints"]
    dob_comp_cols, dob_comp_rows = results["dob_comp"]
    facade_cols, facade_rows = results["facades"]
    boiler_cols, boiler_rows = results["boilers"]
    sro_cols, sro_rows = results["sro"]

    hpd = dict(zip(hpd_cols, hpd_rows[0])) if hpd_rows else {}
    dob = dict(zip(dob_cols, dob_rows[0])) if dob_rows else {}
    fdny = dict(zip(fdny_cols, fdny_rows[0])) if fdny_rows else {}
    complaints = dict(zip(complaint_cols, complaint_rows[0])) if complaint_rows else {}
    dob_comp = dict(zip(dob_comp_cols, dob_comp_rows[0])) if dob_comp_rows else {}
    is_sro = len(sro_rows) > 0

    elapsed = int((time.time() - t0) * 1000)

    # Calculate enforcement score
    score = 0
    score += (hpd.get("class_c") or 0) * 3
    score += (hpd.get("class_b") or 0) * 1
    score += (hpd.get("open_v") or 0) * 2
    score += (dob.get("hazardous") or 0) * 5
    score += (dob.get("total") or 0)
    score += (fdny.get("total") or 0) * 2
    score += sum(r[5] for r in oath_rows) * 3  # defaults
    score += (complaints.get("open_complaints") or 0)
    score += (dob_comp.get("active") or 0) * 2
    score += len(facade_rows) * 2
    score += sum(1 for r in boiler_rows if r[1] and r[1] > 0) * 3
    score += 5 if is_sro else 0

    severity = "LOW" if score < 10 else "MODERATE" if score < 50 else "HIGH" if score < 200 else "CRITICAL"

    lines = [f"ENFORCEMENT WEB — BBL {bbl}"]
    lines.append(f"Severity: {severity} (score: {score})\n")

    if hpd.get("total"):
        lines.append(f"HPD VIOLATIONS: {hpd['total']} total "
                     f"({hpd.get('open_v', 0)} open, {hpd.get('class_c', 0)} Class C, "
                     f"{hpd.get('class_b', 0)} Class B, {hpd.get('class_a', 0)} Class A)")
        lines.append(f"  Range: {hpd.get('earliest', '?')} — {hpd.get('latest', '?')}")

    if complaints.get("total_complaints"):
        lines.append(f"HPD COMPLAINTS: {complaints['total_complaints']} total "
                     f"({complaints.get('open_complaints', 0)} open)")

    if dob.get("total"):
        penalties_str = f"${dob['total_penalties_due']:,.0f}" if dob.get("total_penalties_due") else "$0"
        paid_str = f"${dob['total_paid']:,.0f}" if dob.get("total_paid") else "$0"
        lines.append(f"DOB ECB VIOLATIONS: {dob['total']} total "
                     f"({dob.get('hazardous', 0)} hazardous)")
        lines.append(f"  Penalties due: {penalties_str} | Paid: {paid_str}")
        lines.append(f"  Range: {dob.get('earliest', '?')} — {dob.get('latest', '?')}")

    if fdny.get("total"):
        lines.append(f"FDNY VIOLATIONS: {fdny['total']} total "
                     f"({fdny.get('open_v', 0)} open/failed)")
        lines.append(f"  Range: {fdny.get('earliest', '?')} — {fdny.get('latest', '?')}")

    if oath_rows:
        lines.append(f"\nOATH HEARINGS BY AGENCY:")
        for r in oath_rows:
            penalty_str = f"${r[2]:,.0f}" if r[2] else "$0"
            due_str = f"${r[4]:,.0f}" if r[4] else "$0"
            lines.append(f"  {r[0] or 'Unknown'}: {r[1]} hearings | "
                         f"Penalties: {penalty_str} | Due: {due_str} | "
                         f"Defaults: {r[5]} | Latest: {r[6] or '?'}")

    if rest_rows:
        restaurants = {}
        for r in rest_rows:
            name = r[0] or "Unknown"
            if name not in restaurants:
                restaurants[name] = {"cuisine": r[1], "grade": r[2], "violations": []}
            if r[4]:
                restaurants[name]["violations"].append(r[5] or r[4])
        lines.append(f"\nRESTAURANT INSPECTIONS:")
        for name, info in restaurants.items():
            lines.append(f"  {name} ({info['cuisine']}) — Grade: {info['grade'] or 'Pending'}")
            for v in info["violations"][:3]:
                lines.append(f"    - {v[:80]}")

    if dob_comp.get("total"):
        lines.append(f"\nDOB COMPLAINTS: {dob_comp['total']} total "
                     f"({dob_comp.get('active', 0)} active, {dob_comp.get('categories', 0)} categories)")
        lines.append(f"  Range: {dob_comp.get('earliest', '?')} — {dob_comp.get('latest', '?')}")

    if facade_rows:
        lines.append(f"\nFACADE SAFETY (FISP): {len(facade_rows)} filings")
        for r in facade_rows:
            lines.append(f"  Cycle {r[2] or '?'} | {r[0] or '?'} | Filing: {r[1] or '?'} | Wall: {r[4] or '?'}")
            if r[5]:
                lines.append(f"    {r[5][:100]}")

    if boiler_rows:
        total_boilers = sum(r[0] or 0 for r in boiler_rows)
        defect_count = sum(r[1] or 0 for r in boiler_rows)
        lines.append(f"\nBOILER SAFETY: {total_boilers} inspections ({defect_count} with defects)")
        for r in boiler_rows[:3]:
            lines.append(f"  Owner: {r[3] or '?'} | {r[0]} inspections, {r[1] or 0} defects | Latest: {r[2] or '?'}")

    if is_sro:
        r = sro_rows[0]
        lines.append(f"\nSRO BUILDING: Yes — {r[4] or '?'} class, {r[5] or '?'} stories, "
                     f"Class A: {r[2] or 0}, Class B: {r[3] or 0}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "bbl": bbl, "severity": severity, "score": score,
            "hpd_violations": hpd, "dob_ecb_violations": dob,
            "fdny_violations": fdny, "hpd_complaints": complaints,
            "oath_hearings": [dict(zip(oath_cols, r)) for r in oath_rows],
            "restaurant_inspections": [dict(zip(rest_cols, r)) for r in rest_rows],
            "dob_complaints": dob_comp,
            "facade_safety": [dict(zip(facade_cols, r)) for r in facade_rows],
            "boiler_safety": [dict(zip(boiler_cols, r)) for r in boiler_rows],
            "is_sro": is_sro,
        },
        meta={"severity": severity, "score": score, "query_time_ms": elapsed},
    )


def _view_history(pool, bbl: str) -> ToolResult:
    """ACRIS transaction chain since 1966."""
    t0 = time.time()
    borough = bbl[0]
    block = bbl[1:6].lstrip("0") or "0"
    lot = bbl[6:10].lstrip("0") or "0"

    # Try pre-joined MV first
    _mv_ok = False
    try:
        mv_cols, mv_rows = execute(pool, """
            SELECT document_id, doc_type, amount, doc_date, party_name AS name,
                   role, address_1, city, state
            FROM lake.foundation.mv_acris_deeds
            WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
            ORDER BY doc_date DESC
            LIMIT 500
        """, [bbl])
        if mv_rows:
            cols = ["doc_date", "doc_type", "amount", "recorded_date", "role", "name", "address_1", "city", "state", "document_id"]
            rows = []
            for r in mv_rows:
                mv = dict(zip(mv_cols, r))
                rows.append((
                    mv.get("doc_date"), mv.get("doc_type"), mv.get("amount"),
                    None, mv.get("role"), mv.get("name"), mv.get("address_1"),
                    mv.get("city"), mv.get("state"), mv.get("document_id"),
                ))
            _mv_ok = True
    except Exception:
        pass

    if not _mv_ok:
        cols, rows = execute(pool, """
            WITH docs AS (
                SELECT DISTINCT m.document_id, m.doc_type,
                       TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                       TRY_CAST(m.document_date AS DATE) AS doc_date,
                       TRY_CAST(m.recorded_datetime AS DATE) AS recorded_date
                FROM lake.housing.acris_legals l
                JOIN lake.housing.acris_master m ON l.document_id = m.document_id
                WHERE l.borough = ? AND l.block = ? AND l.lot = ?
            ),
            parties AS (
                SELECT p.document_id,
                       p.name,
                       CASE WHEN p.party_type = '1' THEN 'GRANTOR'
                            WHEN p.party_type = '2' THEN 'GRANTEE'
                            ELSE 'OTHER' END AS role,
                       p.address_1,
                       p.city,
                       p.state,
                       p.zip
                FROM lake.housing.acris_parties p
                WHERE p.document_id IN (SELECT document_id FROM docs)
                  AND p.name IS NOT NULL AND TRIM(p.name) != ''
            )
            SELECT d.doc_date, d.doc_type, d.amount, d.recorded_date,
                   p.role, p.name, p.address_1, p.city, p.state,
                   d.document_id
            FROM docs d
            LEFT JOIN parties p ON d.document_id = p.document_id
            ORDER BY d.doc_date DESC NULLS LAST, d.document_id, p.role
        """, [borough, block, lot])

    elapsed = int((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content=f"BBL {bbl}: no ACRIS transaction records found.")

    # Group by document
    doc_map = {}
    for r in rows:
        did = r[9]
        if did not in doc_map:
            doc_map[did] = {
                "date": r[0], "type": r[1], "amount": r[2],
                "recorded": r[3], "parties": [],
            }
        if r[5]:
            doc_map[did]["parties"].append({
                "role": r[4], "name": r[5],
                "address": f"{r[6] or ''} {r[7] or ''} {r[8] or ''}".strip(),
            })

    deeds = [d for d in doc_map.values() if d["type"] in ("DEED", "DEEDO", "DEED, RP")]
    mortgages = [d for d in doc_map.values() if d["type"] in ("MTGE", "M&CON", "ASPM")]
    satisfactions = [d for d in doc_map.values() if d["type"] in ("SAT", "SATIS", "PSAT")]
    other_docs = [d for d in doc_map.values()
                  if d["type"] not in ("DEED", "DEEDO", "DEED, RP", "MTGE", "M&CON", "ASPM", "SAT", "SATIS", "PSAT")]

    lines = [f"PROPERTY HISTORY — BBL {bbl}"]
    lines.append(f"Total documents: {len(doc_map)} ({len(deeds)} deeds, "
                 f"{len(mortgages)} mortgages, {len(satisfactions)} satisfactions, "
                 f"{len(other_docs)} other)\n")

    if deeds:
        lines.append("--- OWNERSHIP TRANSFERS (DEEDS) ---")
        for d in sorted(deeds, key=lambda x: str(x["date"] or ""), reverse=True):
            amt_str = f"${d['amount']:,.0f}" if d['amount'] else "N/A"
            date_str = str(d["date"]) if d["date"] else "?"
            sellers = [p for p in d["parties"] if p["role"] == "GRANTOR"]
            buyers = [p for p in d["parties"] if p["role"] == "GRANTEE"]
            lines.append(f"  {date_str} | {d['type']} | {amt_str}")
            for s in sellers[:3]:
                lines.append(f"    SELLER: {s['name']}" + (f" ({s['address']})" if s["address"] else ""))
            for b in buyers[:3]:
                lines.append(f"    BUYER:  {b['name']}" + (f" ({b['address']})" if b["address"] else ""))
        lines.append("")

    if mortgages:
        lines.append(f"--- MORTGAGES ({len(mortgages)}) ---")
        for d in sorted(mortgages, key=lambda x: str(x["date"] or ""), reverse=True)[:10]:
            amt_str = f"${d['amount']:,.0f}" if d['amount'] else "N/A"
            date_str = str(d["date"]) if d["date"] else "?"
            lenders = [p["name"] for p in d["parties"] if p["role"] == "GRANTEE"]
            borrowers = [p["name"] for p in d["parties"] if p["role"] == "GRANTOR"]
            lender_str = ", ".join(lenders[:2]) if lenders else "?"
            borrower_str = ", ".join(borrowers[:2]) if borrowers else "?"
            lines.append(f"  {date_str} | {amt_str} | Lender: {lender_str} | Borrower: {borrower_str}")
        if len(mortgages) > 10:
            lines.append(f"  ... and {len(mortgages) - 10} more mortgages")
        lines.append("")

    deed_prices = [(d["date"], d["amount"]) for d in sorted(deeds, key=lambda x: str(x["date"] or ""))
                   if d["amount"] and d["amount"] > 10000]
    if len(deed_prices) >= 2:
        lines.append("--- PRICE HISTORY ---")
        for i, (date, price) in enumerate(deed_prices):
            delta = ""
            if i > 0 and deed_prices[i - 1][1]:
                change = price - deed_prices[i - 1][1]
                pct = (change / deed_prices[i - 1][1]) * 100
                delta = f"  ({'+' if change >= 0 else ''}{pct:.0f}%)"
            lines.append(f"  {date or '?'} — ${price:,.0f}{delta}")
        lines.append("")

    lines.append(f"({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "bbl": bbl,
            "total_documents": len(doc_map),
            "deeds": len(deeds),
            "mortgages": len(mortgages),
            "satisfactions": len(satisfactions),
            "transactions": [
                {
                    "date": str(d["date"]) if d["date"] else None,
                    "type": d["type"],
                    "amount": d["amount"],
                    "parties": d["parties"],
                }
                for d in sorted(doc_map.values(), key=lambda x: str(x["date"] or ""), reverse=True)
            ],
        },
        meta={"total_documents": len(doc_map), "query_time_ms": elapsed},
    )


def _view_flippers(pool, bbl: str) -> ToolResult:
    """Detect buy-and-flip activity at this property or on its block."""
    t0 = time.time()

    # Use the BBL's borough for filtering
    boro_code = bbl[0]
    months = 24
    min_profit_pct = 20
    mv_boro_filter = f"AND bbl LIKE '{boro_code}%'"
    borough_filter = f"AND l.borough = '{boro_code}'"

    # Try MV first
    _mv_ok = False
    try:
        cols, rows = execute(pool, f"""
            WITH deed_sales AS (
                SELECT bbl, amount AS price, doc_date AS sale_date, document_id
                FROM lake.foundation.mv_acris_deeds
                WHERE role = 'BUYER' AND amount > 50000
                  AND doc_date >= '2015-01-01'
                  {mv_boro_filter}
            ),
            ranked AS (
                SELECT *,
                       LAG(price) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_price,
                       LAG(sale_date) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_date,
                       LAG(document_id) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_doc_id
                FROM deed_sales
            ),
            flips AS (
                SELECT bbl,
                       prev_date AS buy_date, sale_date AS sell_date,
                       prev_price AS buy_price, price AS sell_price,
                       price - prev_price AS profit,
                       ((price - prev_price) / prev_price * 100)::INT AS profit_pct,
                       DATEDIFF('month', prev_date, sale_date) AS months_held,
                       prev_doc_id AS buy_doc_id, document_id AS sell_doc_id
                FROM ranked
                WHERE prev_price IS NOT NULL
                  AND prev_price > 50000
                  AND price > prev_price
                  AND ((price - prev_price) / prev_price * 100) >= {min_profit_pct}
                  AND DATEDIFF('month', prev_date, sale_date) <= {months}
                  AND DATEDIFF('month', prev_date, sale_date) > 0
            )
            SELECT f.bbl, f.buy_date, f.sell_date, f.buy_price, f.sell_price,
                   f.profit, f.profit_pct, f.months_held,
                   (SELECT FIRST(d.party_name) FROM lake.foundation.mv_acris_deeds d
                    WHERE d.document_id = f.buy_doc_id AND d.role = 'BUYER'
                    AND d.party_name IS NOT NULL LIMIT 1) AS buyer_name,
                   (SELECT FIRST(d.party_name) FROM lake.foundation.mv_acris_deeds d
                    WHERE d.document_id = f.sell_doc_id AND d.role = 'SELLER'
                    AND d.party_name IS NOT NULL LIMIT 1) AS flipper_name,
                   EXISTS (
                       SELECT 1 FROM lake.housing.tax_lien_sales t
                       WHERE (t.borough || LPAD(t.block::VARCHAR, 5, '0') || LPAD(t.lot::VARCHAR, 4, '0')) = f.bbl
                   ) AS had_tax_lien,
                   EXISTS (
                       SELECT 1 FROM lake.housing.dob_permit_issuance d
                       WHERE (d.borough || LPAD(d.block::VARCHAR, 5, '0') || LPAD(d.lot::VARCHAR, 4, '0')) = f.bbl
                         AND TRY_CAST(d.issuance_date AS DATE) BETWEEN f.buy_date AND f.sell_date
                   ) AS had_renovation
            FROM flips f
            WHERE f.bbl = '{bbl}'
            ORDER BY f.profit DESC
            LIMIT 50
        """)
        _mv_ok = True
    except Exception:
        pass

    if not _mv_ok:
        cols, rows = execute(pool, f"""
            WITH deed_sales AS (
                SELECT
                    (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                    TRY_CAST(m.document_amt AS DOUBLE) AS price,
                    TRY_CAST(m.document_date AS DATE) AS sale_date,
                    m.document_id,
                    l.borough
                FROM lake.housing.acris_master m
                JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
                WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
                  AND m.document_amt IS NOT NULL
                  AND TRY_CAST(m.document_amt AS DOUBLE) > 50000
                  AND TRY_CAST(m.document_date AS DATE) >= '2015-01-01'
                  AND l.borough IS NOT NULL
                  {borough_filter}
            ),
            ranked AS (
                SELECT *,
                       LAG(price) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_price,
                       LAG(sale_date) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_date,
                       LAG(document_id) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_doc_id
                FROM deed_sales
            ),
            flips AS (
                SELECT bbl, borough,
                       prev_date AS buy_date, sale_date AS sell_date,
                       prev_price AS buy_price, price AS sell_price,
                       price - prev_price AS profit,
                       ((price - prev_price) / prev_price * 100)::INT AS profit_pct,
                       DATEDIFF('month', prev_date, sale_date) AS months_held,
                       prev_doc_id AS buy_doc_id, document_id AS sell_doc_id
                FROM ranked
                WHERE prev_price IS NOT NULL
                  AND prev_price > 50000
                  AND price > prev_price
                  AND ((price - prev_price) / prev_price * 100) >= {min_profit_pct}
                  AND DATEDIFF('month', prev_date, sale_date) <= {months}
                  AND DATEDIFF('month', prev_date, sale_date) > 0
            )
            SELECT f.bbl, f.buy_date, f.sell_date, f.buy_price, f.sell_price,
                   f.profit, f.profit_pct, f.months_held,
                   (SELECT FIRST(p.name) FROM lake.housing.acris_parties p
                    WHERE p.document_id = f.buy_doc_id AND p.party_type = '2'
                    AND p.name IS NOT NULL LIMIT 1) AS buyer_name,
                   (SELECT FIRST(p.name) FROM lake.housing.acris_parties p
                    WHERE p.document_id = f.sell_doc_id AND p.party_type = '1'
                    AND p.name IS NOT NULL LIMIT 1) AS flipper_name,
                   EXISTS (
                       SELECT 1 FROM lake.housing.tax_lien_sales t
                       WHERE (t.borough || LPAD(t.block::VARCHAR, 5, '0') || LPAD(t.lot::VARCHAR, 4, '0')) = f.bbl
                   ) AS had_tax_lien,
                   EXISTS (
                       SELECT 1 FROM lake.housing.dob_permit_issuance d
                       WHERE (d.borough || LPAD(d.block::VARCHAR, 5, '0') || LPAD(d.lot::VARCHAR, 4, '0')) = f.bbl
                         AND TRY_CAST(d.issuance_date AS DATE) BETWEEN f.buy_date AND f.sell_date
                   ) AS had_renovation
            FROM flips f
            WHERE f.bbl = '{bbl}'
            ORDER BY f.profit DESC
            LIMIT 50
        """)

    elapsed = int((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(
            content=f"BBL {bbl}: no flips detected (>={min_profit_pct}% profit within {months} months since 2015).",
            meta={"bbl": bbl, "query_time_ms": elapsed},
        )

    lines = [f"FLIPPER DETECTOR — BBL {bbl} — {len(rows)} flips found"]
    lines.append(f"Criteria: >={min_profit_pct}% profit within {months} months (since 2015)\n")

    total_profit = sum(r[5] or 0 for r in rows)
    lines.append(f"Total profit across {len(rows)} flips: ${total_profit:,.0f}\n")

    lines.append(f"{'BBL':<12} {'Buy':<12} {'Sell':<12} {'Buy$':<14} {'Sell$':<14} "
                 f"{'Profit':<14} {'%':<6} {'Mo':<4} {'Reno':<5} {'Lien':<5} Flipper")
    lines.append("-" * 140)

    for r in rows:
        buy_str = f"${r[3]:,.0f}" if r[3] else "?"
        sell_str = f"${r[4]:,.0f}" if r[4] else "?"
        profit_str = f"${r[5]:,.0f}" if r[5] else "?"
        reno = "YES" if r[11] else ""
        lien = "YES" if r[10] else ""
        flipper = (r[9] or r[8] or "?")[:25]
        lines.append(f"{r[0]:<12} {str(r[1] or '?'):<12} {str(r[2] or '?'):<12} "
                     f"{buy_str:<14} {sell_str:<14} {profit_str:<14} "
                     f"{r[6] or 0:<6} {r[7] or 0:<4} {reno:<5} {lien:<5} {flipper}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "bbl": bbl,
            "total_profit": total_profit,
            "flips": [
                {
                    "bbl": r[0], "buy_date": str(r[1]), "sell_date": str(r[2]),
                    "buy_price": r[3], "sell_price": r[4],
                    "profit": r[5], "profit_pct": r[6], "months_held": r[7],
                    "buyer_name": r[8], "flipper_name": r[9],
                    "had_tax_lien": r[10], "had_renovation": r[11],
                }
                for r in rows
            ],
        },
        meta={"bbl": bbl, "flip_count": len(rows), "total_profit": total_profit, "query_time_ms": elapsed},
    )


def _view_building_context(pool, bbl: str) -> ToolResult:
    """What was happening when a building was born — era, contemporaries, citywide stats."""
    t0 = time.time()

    cols_b, rows_b = safe_query(pool, STORY_PLUTO_SQL, [bbl])
    if not rows_b:
        raise ToolError(f"No building found for BBL {bbl} in PLUTO.")

    p = dict(zip(cols_b, rows_b[0]))
    year = int(float(p.get("yearbuilt") or 0))
    address = p.get("address") or "Unknown"
    borough = p.get("borough") or ""
    zipcode = p.get("zipcode") or ""

    if year < 1800:
        raise ToolError("Building has no valid construction year. Cannot provide context.")

    era_name, era_desc = "an unknown era", ""
    for yr_range, (name, desc) in _NYC_ERAS.items():
        if year in yr_range:
            era_name, era_desc = name, desc
            break

    lines = []
    lines.append(f"THE YEAR YOUR BUILDING WAS BORN: {year}")
    lines.append(f"{address}, {borough}")
    lines.append("=" * 60)

    lines.append(f"\n{era_name.upper()}")
    lines.append(f"Your building was constructed {era_desc}.")

    # Famous contemporaries
    contemporaries = [(y, name) for y, name in _FAMOUS_BUILDINGS if abs(y - year) <= 3]
    exact_match = [(y, name) for y, name in _FAMOUS_BUILDINGS if y == year]

    if exact_match:
        lines.append(f"\nFAMOUS CONTEMPORARIES (same year)")
        for y, name in exact_match:
            lines.append(f"  {name} ({y})")
    if contemporaries and not exact_match:
        lines.append(f"\nFAMOUS CONTEMPORARIES (within 3 years)")
        for y, name in contemporaries[:5]:
            lines.append(f"  {name} ({y})")

    if not contemporaries:
        closest = min(_FAMOUS_BUILDINGS, key=lambda x: abs(x[0] - year))
        diff = abs(closest[0] - year)
        direction = "after" if closest[0] > year else "before"
        lines.append(f"\nNEAREST FAMOUS CONTEMPORARY")
        lines.append(f"  {closest[1]} ({closest[0]}) — built {diff} years {direction}")

    # How many buildings that year
    cols_c, rows_c = safe_query(pool, CONTEXT_ERA_BUILDINGS_SQL, [year])
    if rows_c:
        c = dict(zip(cols_c, rows_c[0]))
        total = int(c.get("total_same_year") or 0)
        zips = int(c.get("zips_with_same_year") or 0)
        lines.append(f"\nCITYWIDE IN {year}")
        lines.append(f"  {total:,} buildings were constructed across {zips} ZIP codes")

    # Borough breakdown
    cols_boro, rows_boro = safe_query(pool, CONTEXT_ERA_BOROUGH_SQL, [year])
    if rows_boro:
        lines.append(f"  By borough:")
        boro_names = {"MN": "Manhattan", "BK": "Brooklyn", "BX": "Bronx", "QN": "Queens", "SI": "Staten Island"}
        for row in rows_boro:
            r = dict(zip(cols_boro, row))
            bname = boro_names.get(r.get("borough"), r.get("borough") or "?")
            lines.append(f"    {bname}: {int(r.get('cnt') or 0):,}")

    # The decade
    decade_start = (year // 10) * 10
    decade_end = decade_start + 9
    cols_d, rows_d = safe_query(pool, CONTEXT_DECADE_SQL, [decade_start, decade_end])
    if rows_d:
        d = dict(zip(cols_d, rows_d[0]))
        total_decade = int(d.get("total_decade") or 0)
        avg_fl = round(float(d.get("avg_floors") or 0), 1)
        lines.append(f"\nTHE {decade_start}s")
        lines.append(f"  {total_decade:,} buildings constructed citywide that decade")
        lines.append(f"  Average height: {avg_fl} floors")

    # Neighborhood at the time
    if zipcode:
        cols_z, rows_z = safe_query(pool, CONTEXT_ZIP_ERA_SQL, [year, zipcode])
        if rows_z:
            z = dict(zip(cols_z, rows_z[0]))
            total_zip = int(z.get("buildings_in_zip") or 0)
            same_yr = int(z.get("same_year_in_zip") or 0)
            oldest = int(z.get("oldest_in_zip") or 0)
            newest = int(z.get("newest_in_zip") or 0)
            lines.append(f"\nYOUR NEIGHBORHOOD (ZIP {zipcode})")
            lines.append(f"  {total_zip:,} buildings exist in your ZIP today")
            if same_yr > 1:
                lines.append(f"  {same_yr} of them were built the same year as yours")
            elif same_yr == 1:
                lines.append(f"  Yours is the only one built in {year}")
            if oldest > 1800:
                lines.append(f"  Oldest building in ZIP: {oldest} ({year - oldest} years before yours)")
            if newest:
                lines.append(f"  Newest building in ZIP: {newest}")

    # Perspective
    current_year = datetime.date.today().year
    age = current_year - year
    lines.append(f"\nPERSPECTIVE")
    lines.append(f"  Your building has been standing for {age} years.")
    milestones = [(y, e) for y, e in _NYC_MILESTONES if y >= year]
    lines.append(f"  It has witnessed {len(milestones)} defining moments in NYC history.")
    if year <= 1931:
        lines.append(f"  It predates the Empire State Building.")
    if year <= 1904:
        lines.append(f"  It predates the NYC subway system.")
    if year <= 1883:
        lines.append(f"  It predates the Brooklyn Bridge.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "bbl": bbl, "address": address, "borough": borough,
        "year_built": year, "era": era_name,
        "same_year_citywide": int(dict(zip(cols_c, rows_c[0])).get("total_same_year") or 0) if rows_c else 0,
        "famous_contemporaries": [name for _, name in contemporaries[:5]],
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"bbl": bbl, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# View dispatch
# ---------------------------------------------------------------------------

_VIEW_DISPATCH = {
    "full": _view_full,
    "story": _view_story,
    "block": _view_block,
    "similar": _view_similar,
    "enforcement": _view_enforcement,
    "history": _view_history,
    "flippers": _view_flippers,
}


# ---------------------------------------------------------------------------
# Main super tool
# ---------------------------------------------------------------------------


def building(
    identifier: Annotated[str, Field(
        description="Street address or 10-digit BBL, e.g. '350 5th Ave, Manhattan' or '1000670001'",
        examples=["350 5th Ave, Manhattan", "1000670001", "123 Main St, Brooklyn", "2039720033"],
    )],
    view: Annotated[
        Literal["full", "story", "block", "similar", "enforcement", "history", "flippers"],
        Field(
            default="full",
            description="'full' returns profile + violations + enforcement + landlord. 'story' returns narrative history with complaint arcs. 'block' returns all buildings on the tax block. 'similar' finds twin buildings via vectors. 'enforcement' returns multi-agency timeline. 'history' returns ACRIS transactions since 1966. 'flippers' detects buy-and-flip activity.",
        )
    ] = "full",
    ctx: Context = None,
) -> ToolResult:
    """Look up any NYC building by address or BBL. Returns violations,
    enforcement actions, landlord info, and property history.

    GUIDELINES: Use for any question about a specific building, address,
    or property. Present the FULL response — every field contains data
    the user needs. Use interactive tables for violation lists and charts
    for percentile comparisons.

    LIMITATIONS: Do NOT use for person lookups (use entity), neighborhood
    questions without a specific address (use neighborhood), or ownership
    network traversal (use network).

    RETURNS: Building profile with violations, enrichments (landmark, tax,
    SRO, facade), and citywide percentile ranking. Default view='full'
    returns the complete profile. Other views: story, block, similar,
    enforcement, history, flippers."""
    pool = ctx.lifespan_context["pool"]
    t0_building = time.time()

    # Auto-detect: 10-digit numeric = BBL, otherwise resolve address
    bbl_input = identifier.strip()
    if _BBL_PATTERN.match(bbl_input):
        bbl = bbl_input
    else:
        bbl = _resolve_bbl(pool, bbl_input)

    print(f"[building] Address resolve: {(time.time() - t0_building)*1000:.0f}ms → BBL {bbl}", flush=True)

    # Special case: building_context is dispatched as "story" view variant
    # but we keep it as a separate internal — the "story" view maps to building_story
    if view == "story":
        return _view_story(pool, bbl)

    handler = _VIEW_DISPATCH.get(view)
    if not handler:
        raise ToolError(f"Unknown view '{view}'. Choose from: full, story, block, similar, enforcement, history, flippers")

    # Pass ctx for views that need it (e.g. _view_similar)
    import inspect
    sig = inspect.signature(handler)
    if "ctx" in sig.parameters:
        return handler(pool, bbl, ctx=ctx)
    return handler(pool, bbl)
