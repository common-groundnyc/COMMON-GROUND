"""H3 hex-based spatial query builders.

Replaces ZIP/precinct/community district crosswalk hacks with
H3 k-ring aggregation. Resolution 9 (~100m) for block-level NYC analysis.
"""

from sql_utils import validate_identifier as _validate_identifier

H3_RES = 9


def h3_kring_sql(lat: float, lng: float, radius_rings: int = 2) -> tuple[str, list]:
    """SQL + params to get H3 cells within radius of a point."""
    sql = f"""
        SELECT UNNEST(
            h3_grid_disk(h3_latlng_to_cell(?, ?, {H3_RES}), ?)
        ) AS h3_cell
    """
    return sql, [lat, lng, radius_rings]


def h3_aggregate_sql(
    source_table: str,
    filter_tables: list[str],
    lat: float,
    lng: float,
    radius_rings: int = 3,
) -> tuple[str, list]:
    """SQL + params to aggregate counts within a hex k-ring."""
    _validate_identifier(source_table)
    kring_sql, kring_params = h3_kring_sql(lat, lng, radius_rings)
    placeholders = ", ".join("?" for _ in filter_tables)
    sql = f"""
        WITH target_cells AS (
            {kring_sql}
        )
        SELECT
            source_table,
            COUNT(*) AS row_count,
            COUNT(DISTINCT h3_res9) AS cell_count
        FROM {source_table}
        WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
          AND source_table IN ({placeholders})
        GROUP BY source_table
        ORDER BY row_count DESC
    """
    return sql, kring_params + filter_tables


def h3_heatmap_sql(
    source_table: str,
    filter_table: str,
    lat: float,
    lng: float,
    radius_rings: int = 5,
) -> tuple[str, list]:
    """SQL + params for per-cell heatmap counts."""
    _validate_identifier(source_table)
    kring_sql, kring_params = h3_kring_sql(lat, lng, radius_rings)
    sql = f"""
        WITH target_cells AS (
            {kring_sql}
        )
        SELECT
            h3_res9 AS h3_cell,
            h3_cell_to_lat(h3_res9) AS cell_lat,
            h3_cell_to_lng(h3_res9) AS cell_lng,
            COUNT(*) AS count
        FROM {source_table}
        WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
          AND source_table = ?
        GROUP BY h3_res9
        ORDER BY count DESC
    """
    return sql, kring_params + [filter_table]


def h3_zip_centroid_sql(zipcode: str) -> tuple[str, list]:
    """SQL + params to get H3 centroid for a ZIP code."""
    sql = f"""
        SELECT h3_latlng_to_cell(
            AVG(TRY_CAST(latitude AS DOUBLE)),
            AVG(TRY_CAST(longitude AS DOUBLE)),
            {H3_RES}
        ) AS center_cell,
        AVG(TRY_CAST(latitude AS DOUBLE)) AS center_lat,
        AVG(TRY_CAST(longitude AS DOUBLE)) AS center_lng
        FROM lake.city_government.pluto
        WHERE zipcode = ?
          AND TRY_CAST(latitude AS DOUBLE) BETWEEN 40.4 AND 41.0
          AND TRY_CAST(longitude AS DOUBLE) BETWEEN -74.3 AND -73.6
    """
    return sql, [zipcode]


def h3_neighborhood_stats_sql(lat: float, lng: float, radius_rings: int = 8) -> tuple[str, list]:
    """SQL + params for multi-dimension neighborhood stats."""
    kring_sql, kring_params = h3_kring_sql(lat, lng, radius_rings)
    sql = f"""
        WITH target_cells AS (
            {kring_sql}
        ),
        h3_stats AS (
            SELECT source_table, COUNT(*) AS cnt
            FROM lake.foundation.h3_index
            WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
            GROUP BY source_table
        )
        SELECT
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_complaints_historic'), 0)
                + COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_complaints_ytd'), 0)
                AS total_crimes,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_arrests_historic'), 0)
                + COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_arrests_ytd'), 0)
                AS total_arrests,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'health.restaurant_inspections'), 0) AS restaurants,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'social_services.n311_service_requests'), 0) AS n311_calls,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.shootings'), 0) AS shootings,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'environment.street_trees'), 0) AS street_trees
        FROM h3_stats
    """
    return sql, kring_params
