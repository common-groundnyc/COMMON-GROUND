from dagster_pipeline.sources.datasets import LAT_LNG_TABLES, NAME_TABLES


def test_lat_lng_tables_populated():
    assert len(LAT_LNG_TABLES) >= 15
    for schema, table, lat_col, lng_col in LAT_LNG_TABLES:
        assert schema and table and lat_col and lng_col


def test_name_tables_match_registry():
    assert len(NAME_TABLES) >= 40
    for schema, table in NAME_TABLES:
        assert schema and table
