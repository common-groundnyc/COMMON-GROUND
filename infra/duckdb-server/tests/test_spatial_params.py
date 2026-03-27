from spatial import (
    h3_kring_sql,
    h3_aggregate_sql,
    h3_heatmap_sql,
    h3_zip_centroid_sql,
    h3_neighborhood_stats_sql,
)


def test_h3_kring_returns_tuple():
    sql, params = h3_kring_sql(40.7128, -74.0060, radius_rings=2)
    assert isinstance(params, list)
    assert 40.7128 in params
    assert "'40.7128'" not in sql


def test_h3_heatmap_returns_tuple():
    sql, params = h3_heatmap_sql(
        source_table="lake.foundation.h3_index",
        filter_table="public_safety.nypd_complaints_historic",
        lat=40.7128, lng=-74.0060,
    )
    assert "?" in sql
    assert "public_safety.nypd_complaints_historic" in params
    assert "'public_safety.nypd_complaints_historic'" not in sql


def test_h3_zip_centroid_returns_tuple():
    sql, params = h3_zip_centroid_sql("10003")
    assert "?" in sql
    assert "10003" in params
    assert "'10003'" not in sql


def test_h3_neighborhood_stats_returns_tuple():
    sql, params = h3_neighborhood_stats_sql(40.7128, -74.0060, radius_rings=8)
    assert isinstance(params, list)
    assert 40.7128 in params


def test_h3_aggregate_returns_tuple():
    sql, params = h3_aggregate_sql(
        source_table="lake.foundation.h3_index",
        filter_tables=["public_safety.nypd_complaints_historic"],
        lat=40.7128, lng=-74.0060, radius_rings=3,
    )
    assert isinstance(params, list)
    assert len(params) > 0
