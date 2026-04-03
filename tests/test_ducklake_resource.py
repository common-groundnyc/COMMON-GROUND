import duckdb
import pytest
from dagster_pipeline.resources.ducklake import DuckLakeResource


def test_resource_instantiation():
    resource = DuckLakeResource(
        catalog_url="ducklake:postgres://user:pass@localhost/db",
    )
    assert resource.catalog_url == "ducklake:postgres://user:pass@localhost/db"
    assert resource.memory_limit == "2GB"
    assert resource.threads == 4


def test_get_connection_method_exists():
    resource = DuckLakeResource(
        catalog_url="ducklake:postgres://user:pass@localhost/db",
    )
    assert callable(resource.get_connection)


def test_basic_duckdb_query():
    conn = duckdb.connect(":memory:")
    result = conn.execute("SELECT 42 AS answer").fetchone()
    assert result[0] == 42
    conn.close()


def test_resource_custom_settings():
    resource = DuckLakeResource(
        catalog_url="ducklake:postgres://user:pass@localhost/db",
        memory_limit="4GB",
        threads=8,
    )
    assert resource.memory_limit == "4GB"
    assert resource.threads == 8


@pytest.mark.skipif(
    not __import__("os").environ.get("DUCKLAKE_CATALOG_URL"),
    reason="Integration env vars not set",
)
def test_real_connection():
    import os

    resource = DuckLakeResource(
        catalog_url=os.environ["DUCKLAKE_CATALOG_URL"],
    )
    conn = resource.get_connection()
    result = conn.execute("SELECT 1").fetchone()
    assert result[0] == 1
    conn.close()
