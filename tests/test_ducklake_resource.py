import os
import pytest
import duckdb
from dagster_pipeline.resources.ducklake import DuckLakeResource


def test_resource_instantiation():
    resource = DuckLakeResource(
        catalog_url="ducklake:postgres://user:pass@localhost/db",
        s3_endpoint="localhost:9000",
        s3_access_key="testkey",
        s3_secret_key="testsecret",
    )
    assert resource.catalog_url == "ducklake:postgres://user:pass@localhost/db"
    assert resource.s3_endpoint == "localhost:9000"
    assert resource.s3_access_key == "testkey"
    assert resource.s3_secret_key == "testsecret"
    assert resource.s3_use_ssl is False
    assert resource.memory_limit == "2GB"
    assert resource.threads == 4


def test_get_connection_method_exists():
    resource = DuckLakeResource(
        catalog_url="ducklake:postgres://user:pass@localhost/db",
        s3_endpoint="localhost:9000",
        s3_access_key="testkey",
        s3_secret_key="testsecret",
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
        s3_endpoint="minio.example.com:9000",
        s3_access_key="mykey",
        s3_secret_key="mysecret",
        s3_use_ssl=True,
        memory_limit="4GB",
        threads=8,
    )
    assert resource.s3_use_ssl is True
    assert resource.memory_limit == "4GB"
    assert resource.threads == 8


_INTEGRATION_VARS = ("DUCKLAKE_CATALOG_URL", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")
_have_integration_env = all(os.environ.get(v) for v in _INTEGRATION_VARS)


@pytest.mark.skipif(not _have_integration_env, reason="Integration env vars not set")
def test_real_connection():
    resource = DuckLakeResource(
        catalog_url=os.environ["DUCKLAKE_CATALOG_URL"],
        s3_endpoint=os.environ["S3_ENDPOINT"],
        s3_access_key=os.environ["S3_ACCESS_KEY"],
        s3_secret_key=os.environ["S3_SECRET_KEY"],
        s3_use_ssl=os.environ.get("S3_USE_SSL", "false").lower() == "true",
    )
    conn = resource.get_connection()
    result = conn.execute("SELECT 1").fetchone()
    assert result[0] == 1
    conn.close()
