"""SQL query builders for the /api/explore REST endpoints.

Each builder returns (sql, params_tuple). Pure functions, no I/O.
Parameterized to prevent SQL injection. Use with shared.db.execute().
"""
from typing import Tuple


def build_zip_overview_query(zip_code: str, days: int) -> Tuple[str, tuple]:
    """Build the SQL for the neighborhood stat-card query.

    Returns one row with: violations_count, complaints_count, crimes_count,
    restaurants_a_pct, all filtered to the ZIP and the trailing N days.
    """
    sql = """
    WITH viol AS (
        SELECT COUNT(*) AS n
        FROM lake.housing.hpd_violations
        WHERE zip = ?
          AND TRY_CAST(inspectiondate AS DATE) >= CURRENT_DATE - INTERVAL (?) DAY
    ),
    cmpl AS (
        SELECT COUNT(*) AS n
        FROM lake.social_services.n311_service_requests
        WHERE incident_zip = ?
          AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL (?) DAY
    ),
    crime AS (
        SELECT COUNT(*) AS n
        FROM lake.public_safety.nypd_complaints
        WHERE addr_pct_cd IS NOT NULL
          AND CAST(? AS VARCHAR) IN (
              SELECT DISTINCT incident_zip FROM lake.social_services.n311_service_requests
              WHERE police_precinct = CAST(addr_pct_cd AS VARCHAR)
          )
          AND TRY_CAST(cmplnt_fr_dt AS DATE) >= CURRENT_DATE - INTERVAL (?) DAY
    ),
    rest AS (
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE grade = 'A') AS a_grade
        FROM lake.health.restaurant_inspections
        WHERE zipcode = ?
          AND TRY_CAST(inspection_date AS DATE) >= CURRENT_DATE - INTERVAL (?) DAY
    )
    SELECT
        (SELECT n FROM viol) AS violations_count,
        (SELECT n FROM cmpl) AS complaints_count,
        (SELECT n FROM crime) AS crimes_count,
        (SELECT CASE WHEN total > 0 THEN ROUND(100.0 * a_grade / total, 1) ELSE 0 END
         FROM rest) AS restaurants_a_pct
    """
    params = (zip_code, days, zip_code, days, zip_code, days, zip_code, days)
    return sql, params


def build_zip_search_query(prefix: str) -> Tuple[str, tuple]:
    """Search ZIPs by prefix or name. Returns up to 10 matches."""
    sql = """
    SELECT modzcta AS zip, label, borough
    FROM lake.foundation.geo_zip_boundaries
    WHERE modzcta LIKE ? OR LOWER(label) LIKE LOWER(?)
    ORDER BY modzcta
    LIMIT 10
    """
    return sql, (f"{prefix}%", f"%{prefix}%")


def build_worst_buildings_query(zip_code: str, days: int, limit: int) -> Tuple[str, tuple]:
    """Worst buildings in a ZIP by HPD violation count."""
    clamped = max(1, min(limit, 100))
    sql = f"""
    SELECT
        bbl,
        COALESCE(housenumber || ' ' || streetname, 'Unknown') AS address,
        COUNT(*) AS violation_count,
        COUNT(*) FILTER (WHERE class = 'C') AS class_c_count,
        MAX(TRY_CAST(inspectiondate AS DATE)) AS last_violation_date
    FROM lake.housing.hpd_violations
    WHERE zip = ?
      AND TRY_CAST(inspectiondate AS DATE) >= CURRENT_DATE - INTERVAL (?) DAY
    GROUP BY bbl, housenumber, streetname
    ORDER BY violation_count DESC
    LIMIT {clamped}
    """
    return sql, (zip_code, days)
