"""Dagster asset for NYC ZIP boundary polygons (MODZCTA).

Source: NYC DOHMH Modified ZIP Code Tabulation Areas
        https://data.cityofnewyork.us/Health/Modified-Zip-Code-Tabulation-Areas-MODZCTA-/pri4-ifjk

Output table: lake.foundation.geo_zip_boundaries
Columns: modzcta, label, borough, centroid_lon, centroid_lat,
         geom_wkb (WKB MultiPolygon), geom_geojson (raw GeoJSON for /explore map).
"""
from __future__ import annotations

import json
import logging
import urllib.request
from typing import Any

import dagster as dg

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)

MODZCTA_URL = "https://data.cityofnewyork.us/resource/pri4-ifjk.geojson?$limit=500"


def _fetch_geojson(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=60) as resp:  # noqa: S310
        return json.loads(resp.read())


@dg.asset(
    key=dg.AssetKey(["foundation", "geo_zip_boundaries"]),
    group_name="foundation",
    description="NYC ZIP boundary polygons (MODZCTA) for the /explore dashboard map layer.",
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.on_cron("0 0 1 1 *"),
)
def geo_zip_boundaries(context) -> dg.MaterializeResult:
    """Download MODZCTA GeoJSON and write to lake.foundation.geo_zip_boundaries."""
    context.log.info("Fetching MODZCTA GeoJSON from %s", MODZCTA_URL)
    geojson = _fetch_geojson(MODZCTA_URL)
    features = geojson.get("features", [])
    if not features:
        raise RuntimeError("MODZCTA GeoJSON returned no features")
    context.log.info("Received %d features", len(features))

    rows: list[dict[str, Any]] = []
    for feature in features:
        props = feature.get("properties") or {}
        modzcta = str(props.get("modzcta") or "").strip()
        if len(modzcta) != 5:
            continue
        rows.append(
            {
                "modzcta": modzcta,
                "label": props.get("label") or "",
                "borough": props.get("borough") or "",
                "geometry": json.dumps(feature["geometry"]),
            }
        )
    if not rows:
        raise RuntimeError("No valid MODZCTA rows parsed from GeoJSON")

    context.log.info("Prepared %d ZIP rows", len(rows))

    conn = _connect_ducklake()
    try:
        conn.execute("INSTALL spatial; LOAD spatial")
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.geo_zip_boundaries")
        conn.execute(
            """
            CREATE TABLE lake.foundation.geo_zip_boundaries (
                modzcta VARCHAR,
                label VARCHAR,
                borough VARCHAR,
                centroid_lon DOUBLE,
                centroid_lat DOUBLE,
                geom_wkb BLOB,
                geom_geojson VARCHAR
            )
            """
        )

        for row in rows:
            result = conn.execute(
                """
                SELECT
                    ST_AsWKB(ST_GeomFromGeoJSON(?)) AS wkb,
                    ST_X(ST_Centroid(ST_GeomFromGeoJSON(?))) AS lon,
                    ST_Y(ST_Centroid(ST_GeomFromGeoJSON(?))) AS lat
                """,
                [row["geometry"], row["geometry"], row["geometry"]],
            ).fetchone()
            wkb, lon, lat = result
            conn.execute(
                "INSERT INTO lake.foundation.geo_zip_boundaries VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    row["modzcta"],
                    row["label"],
                    row["borough"],
                    lon,
                    lat,
                    wkb,
                    row["geometry"],
                ],
            )

        count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.geo_zip_boundaries"
        ).fetchone()[0]
    finally:
        conn.close()

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "source_url": dg.MetadataValue.url(MODZCTA_URL),
        }
    )
