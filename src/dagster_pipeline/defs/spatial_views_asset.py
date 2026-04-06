"""Spatial views asset — materialized tables with ST_Point geometry for MCP spatial queries.

Creates lake.spatial.* tables used by area_snapshot, safety_report, and nearby tools.
Each table has a `geom` GEOMETRY column built from source lat/lng VARCHAR columns.
"""
import logging
import time

from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)

SPATIAL_TABLES = [
    {
        "name": "nypd_crimes",
        "source": "public_safety.nypd_complaints_historic",
        "columns": "law_cat_cd, ofns_desc, rpt_dt, addr_pct_cd",
        "lat": "latitude",
        "lng": "longitude",
    },
    {
        "name": "nypd_crimes_ytd",
        "source": "public_safety.nypd_complaints_ytd",
        "columns": "law_cat_cd, ofns_desc, rpt_dt, addr_pct_cd",
        "lat": "latitude",
        "lng": "longitude",
    },
    {
        "name": "restaurant_inspections",
        "source": "health.restaurant_inspections",
        "columns": "camis, dba, cuisine_description, grade, score, inspection_date, zipcode",
        "lat": "latitude",
        "lng": "longitude",
    },
    {
        "name": "subway_stops",
        "source": "transportation.mta_entrances",
        "columns": "stop_name AS name, line",
        "lat": "entrance_latitude",
        "lng": "entrance_longitude",
        "dedupe": "GROUP BY stop_name, line, entrance_latitude, entrance_longitude",
    },
    {
        "name": "n311_complaints",
        "source": "social_services.n311_service_requests",
        "columns": "agency, problem_formerly_complaint_type, created_date, incident_zip",
        "lat": "latitude",
        "lng": "longitude",
    },
    {
        "name": "rat_inspections",
        "source": "health.rodent_inspections",
        "columns": '"Result", zip_code',
        "lat": "latitude",
        "lng": "longitude",
    },
    {
        "name": "street_trees",
        "source": "environment.street_trees",
        "columns": "health, spc_latin, zipcode",
        "lat": "latitude",
        "lng": "longitude",
    },
]


@asset(
    key=AssetKey(["foundation", "spatial_views"]),
    group_name="foundation",
    deps=[
        AssetKey(["public_safety", "nypd_complaints_historic"]),
        AssetKey(["public_safety", "nypd_complaints_ytd"]),
        AssetKey(["health", "restaurant_inspections"]),
        AssetKey(["transportation", "mta_entrances"]),
        AssetKey(["social_services", "n311_service_requests"]),
        AssetKey(["health", "rodent_inspections"]),
        AssetKey(["environment", "street_trees"]),
    ],
    op_tags={"schema": "foundation"},
)
def spatial_views(context) -> MaterializeResult:
    """Materialize spatial tables with ST_Point geometry for MCP radius queries."""
    conn = _connect_ducklake()
    t_start = time.time()

    try:
        conn.execute("INSTALL spatial; LOAD spatial")
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.spatial")

        total_rows = 0
        table_stats = {}

        for spec in SPATIAL_TABLES:
            name = spec["name"]
            source = spec["source"]
            columns = spec["columns"]
            lat = spec["lat"]
            lng = spec["lng"]
            dedupe = spec.get("dedupe", "")

            try:
                sql = f"""
                    CREATE OR REPLACE TABLE lake.spatial.{name} AS
                    SELECT {columns},
                           ST_Point(
                               TRY_CAST({lng} AS DOUBLE),
                               TRY_CAST({lat} AS DOUBLE)
                           ) AS geom
                    FROM lake.{source}
                    WHERE TRY_CAST({lat} AS DOUBLE) IS NOT NULL
                      AND TRY_CAST({lng} AS DOUBLE) IS NOT NULL
                      AND TRY_CAST({lat} AS DOUBLE) BETWEEN 40.4 AND 41.0
                      AND TRY_CAST({lng} AS DOUBLE) BETWEEN -74.3 AND -73.6
                    {dedupe}
                """
                conn.execute(sql)
                row_count = conn.execute(
                    f"SELECT COUNT(*) FROM lake.spatial.{name}"
                ).fetchone()[0]
                total_rows += row_count
                table_stats[name] = row_count
                context.log.info("spatial.%s: %d rows from %s", name, row_count, source)
            except Exception as e:
                context.log.warning("Failed to create spatial.%s: %s", name, e)
                conn.execute(f"CREATE OR REPLACE TABLE lake.spatial.{name} AS SELECT NULL::GEOMETRY AS geom WHERE FALSE")
                table_stats[name] = 0

        elapsed = time.time() - t_start
        context.log.info("Spatial views: %d tables, %d total rows in %.1fs",
                         len(table_stats), total_rows, elapsed)

        return MaterializeResult(
            metadata={
                "total_rows": MetadataValue.int(total_rows),
                "tables": MetadataValue.json(table_stats),
                "elapsed_sec": MetadataValue.float(round(elapsed, 1)),
            }
        )
    finally:
        conn.close()
