"""Tests for H3 spatial query builders."""
import sys
sys.path.insert(0, "/Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server")

from spatial import h3_kring_sql, h3_aggregate_sql, h3_heatmap_sql


def test_kring_sql():
    sql = h3_kring_sql(lat=40.7128, lng=-74.006, radius_rings=2)
    assert "h3_grid_disk" in sql
    assert "h3_latlng_to_cell" in sql
    assert "40.7128" in sql


def test_aggregate_sql():
    sql = h3_aggregate_sql(
        source_table="lake.foundation.h3_index",
        filter_tables=["public_safety.nypd_complaints_ytd"],
        lat=40.7128, lng=-74.006, radius_rings=3,
    )
    assert "h3_grid_disk" in sql
    assert "GROUP BY" in sql
    assert "public_safety.nypd_complaints_ytd" in sql


def test_heatmap_sql():
    sql = h3_heatmap_sql(
        source_table="lake.foundation.h3_index",
        filter_table="public_safety.nypd_complaints_ytd",
        lat=40.7128, lng=-74.006, radius_rings=3,
    )
    assert "h3_cell_to_lat" in sql
    assert "h3_cell_to_lng" in sql
    assert "COUNT(*) AS count" in sql
