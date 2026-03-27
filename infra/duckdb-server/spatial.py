"""H3 hex-based spatial query builders.

Replaces ZIP/precinct/community district crosswalk hacks with
H3 k-ring aggregation. Resolution 9 (~100m) for block-level NYC analysis.
"""

H3_RES = 9


def h3_kring_sql(lat: float, lng: float, radius_rings: int = 2) -> str:
    """SQL to get all H3 cells within radius_rings of a point."""
    return f"""
        SELECT UNNEST(
            h3_grid_disk(h3_latlng_to_cell({lat}, {lng}, {H3_RES}), {radius_rings})
        ) AS h3_cell
    """


def h3_aggregate_sql(
    source_table: str,
    filter_tables: list[str],
    lat: float,
    lng: float,
    radius_rings: int = 3,
) -> str:
    """SQL to aggregate counts from h3_index within a hex k-ring."""
    table_filter = ", ".join(f"'{t}'" for t in filter_tables)
    return f"""
        WITH target_cells AS (
            {h3_kring_sql(lat, lng, radius_rings)}
        )
        SELECT
            source_table,
            COUNT(*) AS row_count,
            COUNT(DISTINCT h3_res9) AS cell_count
        FROM {source_table}
        WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
          AND source_table IN ({table_filter})
        GROUP BY source_table
        ORDER BY row_count DESC
    """


def h3_heatmap_sql(
    source_table: str,
    filter_table: str,
    lat: float,
    lng: float,
    radius_rings: int = 5,
) -> str:
    """SQL to generate per-cell counts for heatmap visualization."""
    return f"""
        WITH target_cells AS (
            {h3_kring_sql(lat, lng, radius_rings)}
        )
        SELECT
            h3_res9 AS h3_cell,
            h3_cell_to_lat(h3_res9) AS cell_lat,
            h3_cell_to_lng(h3_res9) AS cell_lng,
            COUNT(*) AS count
        FROM {source_table}
        WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
          AND source_table = '{filter_table}'
        GROUP BY h3_res9
        ORDER BY count DESC
    """


def h3_zip_centroid_sql(zipcode: str) -> str:
    """SQL to get the H3 centroid cell for a ZIP code using PLUTO data."""
    return f"""
        SELECT h3_latlng_to_cell(
            AVG(TRY_CAST(latitude AS DOUBLE)),
            AVG(TRY_CAST(longitude AS DOUBLE)),
            {H3_RES}
        ) AS center_cell,
        AVG(TRY_CAST(latitude AS DOUBLE)) AS center_lat,
        AVG(TRY_CAST(longitude AS DOUBLE)) AS center_lng
        FROM lake.city_government.pluto
        WHERE zipcode = '{zipcode}'
          AND TRY_CAST(latitude AS DOUBLE) BETWEEN 40.4 AND 41.0
          AND TRY_CAST(longitude AS DOUBLE) BETWEEN -74.3 AND -73.6
    """


def h3_neighborhood_stats_sql(lat: float, lng: float, radius_rings: int = 8) -> str:
    """SQL to get multi-dimension neighborhood stats via H3 aggregation."""
    return f"""
        WITH target_cells AS (
            {h3_kring_sql(lat, lng, radius_rings)}
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
