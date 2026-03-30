"""DuckLake connection resource — cached per process.
One connection, reused across all assets in the same process.
With in_process_executor: single connection for the entire run.
With multiprocess_executor: one connection per worker process.
"""
import dagster as dg
import duckdb

# Process-level connection cache
_conn_cache: duckdb.DuckDBPyConnection | None = None


class DuckLakeResource(dg.ConfigurableResource):
    catalog_url: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_use_ssl: bool = False
    s3_skip_cert_verify: bool = True
    memory_limit: str = "2GB"
    threads: int = 4

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        global _conn_cache
        if _conn_cache is not None:
            try:
                _conn_cache.execute("SELECT 1")
                return _conn_cache
            except Exception:
                _conn_cache = None

        conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        conn.execute("LOAD ducklake")
        conn.execute("LOAD postgres")
        try:
            conn.execute("INSTALL curl_httpfs; LOAD curl_httpfs")
        except Exception:
            conn.execute("INSTALL httpfs; LOAD httpfs")

        conn.execute(f"SET s3_endpoint='{self.s3_endpoint}'")
        conn.execute(f"SET s3_access_key_id='{self.s3_access_key}'")
        conn.execute(f"SET s3_secret_access_key='{self.s3_secret_key}'")
        conn.execute(f"SET s3_use_ssl={'true' if self.s3_use_ssl else 'false'}")
        conn.execute("SET s3_url_style='path'")
        conn.execute("SET preserve_insertion_order=false")
        conn.execute("SET http_timeout=300000")
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        conn.execute(f"SET threads={self.threads}")

        # DuckLake catalog retry — snapshot ID is global, all writers contend
        # even across different schemas (confirmed architectural in issue #512)
        conn.execute("SET ducklake_max_retry_count=200")
        conn.execute("SET ducklake_retry_wait_ms=200")
        conn.execute("SET ducklake_retry_backoff=2.0")

        if self.s3_skip_cert_verify:
            for opt in ["enable_server_cert_verification",
                        "enable_curl_server_cert_verification"]:
                try:
                    conn.execute(f"SET {opt}=false")
                except Exception:
                    pass

        url = self._build_attach_url()
        conn.execute(f"ATTACH '{url}' AS lake (METADATA_SCHEMA 'lake')")

        _conn_cache = conn
        return conn

    def _build_attach_url(self) -> str:
        url = self.catalog_url
        if not (url.startswith("postgres://") or url.startswith("postgresql://")):
            return url if url.startswith("ducklake:") else f"ducklake:{url}"

        rest = url.split("://", 1)[1]
        at_idx = rest.rfind("@")
        creds = rest[:at_idx]
        host_part = rest[at_idx + 1:]
        colon_idx = creds.index(":")
        user = creds[:colon_idx]
        password = creds[colon_idx + 1:]
        query = ""
        if "?" in host_part:
            host_part, query = host_part.split("?", 1)
        host_port, dbname = host_part.split("/", 1)
        host = host_port
        port = "5432"
        if ":" in host_port:
            host, port = host_port.rsplit(":", 1)
        parts = [f"dbname={dbname}", f"user={user}", f"password={password}",
                 f"host={host}", f"port={port}"]
        if query:
            parts.extend(query.split("&"))
        return "ducklake:postgres:" + " ".join(parts)
