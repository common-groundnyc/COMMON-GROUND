"""DuckLake connection resource — single connection per process.
Follows millpond pattern: crash-and-restart, no connection pooling.
"""
import dagster as dg
import duckdb


class DuckLakeResource(dg.ConfigurableResource):
    catalog_url: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_use_ssl: bool = False
    memory_limit: str = "2GB"
    threads: int = 4

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        conn.execute("INSTALL curl_httpfs FROM community")
        conn.execute("LOAD curl_httpfs")
        conn.execute("LOAD ducklake")
        conn.execute("LOAD postgres")

        conn.execute(f"SET s3_endpoint='{self.s3_endpoint}'")
        conn.execute(f"SET s3_access_key_id='{self.s3_access_key}'")
        conn.execute(f"SET s3_secret_access_key='{self.s3_secret_key}'")
        conn.execute(f"SET s3_use_ssl={'true' if self.s3_use_ssl else 'false'}")
        conn.execute("SET s3_url_style='path'")
        conn.execute("SET preserve_insertion_order=false")
        conn.execute("SET http_timeout=300000")
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        conn.execute(f"SET threads={self.threads}")

        conn.execute(f"ATTACH '{self.catalog_url}' AS lake (METADATA_SCHEMA 'lake')")
        return conn
