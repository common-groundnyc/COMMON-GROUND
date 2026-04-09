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
    memory_limit: str = "8GB"
    threads: int = 4
    temp_directory: str = "/tmp/duckdb_spill"

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        global _conn_cache
        if _conn_cache is not None:
            try:
                _conn_cache.execute("SELECT 1")
                return _conn_cache
            except Exception:
                _conn_cache = None

        import os
        os.makedirs(self.temp_directory, exist_ok=True)

        conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        conn.execute("LOAD ducklake")
        conn.execute("LOAD postgres")

        conn.execute("SET preserve_insertion_order=false")
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        conn.execute(f"SET threads={self.threads}")
        # Spill to disk instead of OOM-killing the container on big joins
        conn.execute(f"SET temp_directory='{self.temp_directory}'")
        conn.execute("SET max_temp_directory_size='100GB'")

        # DuckLake catalog retry — snapshot ID is global, all writers contend
        # even across different schemas (confirmed architectural in issue #512)
        conn.execute("SET ducklake_max_retry_count=200")
        conn.execute("SET ducklake_retry_wait_ms=200")
        conn.execute("SET ducklake_retry_backoff=2.0")

        url = self._build_attach_url()
        # Default schema (no METADATA_SCHEMA) — single catalog convergence after
        # the lake/public split was discovered 2026-04-07.
        conn.execute(f"ATTACH '{url}' AS lake")

        # Disable data inlining — all writes go directly to Parquet.
        # The default threshold (10 rows) inlines small tables as blobs in the
        # Postgres catalog. This caused no cross-process issues (Postgres catalog
        # is shared), but the now-deleted flush_ducklake_sensor that flushed
        # inlined data triggered implicit CHECKPOINTs on conn.close() which
        # deleted freshly-written parquet files. Setting to 0 eliminates the
        # entire problem class. See docs/superpowers/specs/2026-04-09-graph-assets-architecture.md.
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")

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
