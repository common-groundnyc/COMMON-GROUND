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
