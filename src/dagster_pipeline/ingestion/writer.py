"""DuckLake write operations — full replace, streaming replace, and delta merge.

Uses DuckDB zero-copy Arrow scan (1.2M rows/sec insert).
Retry with exponential backoff for Postgres catalog lock contention.
"""
import logging
import time
import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)

MAX_RETRIES = 10
BASE_WAIT = 0.5  # seconds
MAX_WAIT = 30.0


def _retry(fn, description: str):
    """Retry a function with exponential backoff on DuckDB/IO errors."""
    for attempt in range(MAX_RETRIES):
        try:
            return fn()
        except (duckdb.IOException, duckdb.CatalogException, duckdb.InternalException,
                duckdb.TransactionException) as e:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = min(BASE_WAIT * (2 ** attempt), MAX_WAIT)
            logger.warning("%s: attempt %d/%d failed (%s), retrying in %.1fs",
                           description, attempt + 1, MAX_RETRIES, e, wait)
            time.sleep(wait)


def write_full_replace(conn: duckdb.DuckDBPyConnection,
                       schema: str, table_name: str, table: pa.Table) -> int:
    """Drop and recreate table from Arrow. For small-to-medium tables."""
    fqn = f"lake.{schema}.{table_name}"
    t0 = time.monotonic()

    def _do_write():
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
        conn.execute(f"DROP TABLE IF EXISTS {fqn}")
        conn.register("_arrow_batch", table)
        try:
            conn.execute(f"CREATE TABLE {fqn} AS SELECT * FROM _arrow_batch")
        finally:
            conn.unregister("_arrow_batch")

    _retry(_do_write, fqn)

    elapsed = time.monotonic() - t0
    rps = int(table.num_rows / elapsed) if elapsed > 0 else 0
    mb = table.nbytes / (1024 * 1024)
    logger.info("%s: %d rows (%.1f MB) in %.1fs — %d rows/s (full replace)",
                fqn, table.num_rows, mb, elapsed, rps)
    return table.num_rows


def write_streaming_replace(conn: duckdb.DuckDBPyConnection,
                            schema: str, table_name: str,
                            page_iter, total_rows: int = 0,
                            drop_first: bool = True) -> int:
    """Stream Arrow pages directly to DuckLake. Zero accumulation.

    drop_first=True (initial load): DROP TABLE, CREATE from first page.
    drop_first=False (delta append): INSERT BY NAME into existing table.
    New columns in later pages handled via ALTER TABLE ADD COLUMN.
    ~50MB RAM per page regardless of dataset size.
    """
    fqn = f"lake.{schema}.{table_name}"
    t0 = time.monotonic()
    written = 0
    page_num = 0
    mode = "replace" if drop_first else "append"

    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    if drop_first:
        conn.execute(f"DROP TABLE IF EXISTS {fqn}")

    # Check if table exists (for append mode)
    table_exists = not drop_first
    if not drop_first:
        try:
            conn.execute(f"DESCRIBE {fqn}")
        except Exception:
            table_exists = False

    for page in page_iter:
        if page is None or page.num_rows == 0:
            continue

        def _do_page(p=page, create=(not table_exists and page_num == 0)):
            conn.register("_page", p)
            try:
                if create:
                    conn.execute(f"CREATE TABLE {fqn} AS SELECT * FROM _page")
                else:
                    existing = {r[0] for r in conn.execute(f"DESCRIBE {fqn}").fetchall()}
                    for col in p.schema.names:
                        if col not in existing:
                            conn.execute(f'ALTER TABLE {fqn} ADD COLUMN "{col}" VARCHAR')
                    conn.execute(f"INSERT INTO {fqn} BY NAME SELECT * FROM _page")
            finally:
                conn.unregister("_page")

        _retry(_do_page, f"{fqn}[page {page_num}]")
        written += page.num_rows
        page_num += 1

        if page_num % 20 == 0:
            elapsed = time.monotonic() - t0
            rps = int(written / elapsed) if elapsed > 0 else 0
            pct = f" ({written*100//total_rows}%)" if total_rows else ""
            logger.info("%s: %d rows so far — %d rows/s%s",
                        fqn, written, rps, pct)

    elapsed = time.monotonic() - t0
    rps = int(written / elapsed) if elapsed > 0 else 0
    logger.info("%s: %d rows in %.1fs — %d rows/s (streaming %s, %d pages)",
                fqn, written, elapsed, rps, mode, page_num)
    return written


def write_delta_merge(conn: duckdb.DuckDBPyConnection,
                      schema: str, table_name: str, table: pa.Table,
                      merge_key: str = "_id") -> int:
    """MERGE INTO for incremental upsert by key. Single Arrow table."""
    fqn = f"lake.{schema}.{table_name}"
    t0 = time.monotonic()

    def _do_merge():
        conn.register("_arrow_batch", table)
        try:
            conn.execute(f"""
                MERGE INTO {fqn} AS target
                USING _arrow_batch AS source
                ON target."{merge_key}" = source."{merge_key}"
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        finally:
            conn.unregister("_arrow_batch")

    _retry(_do_merge, fqn)

    elapsed = time.monotonic() - t0
    rps = int(table.num_rows / elapsed) if elapsed > 0 else 0
    logger.info("%s: merged %d rows in %.1fs — %d rows/s",
                fqn, table.num_rows, elapsed, rps)
    return table.num_rows


def write_streaming_merge(conn: duckdb.DuckDBPyConnection,
                          schema: str, table_name: str,
                          page_iter, merge_key: str = "_id",
                          total_rows: int = 0) -> int:
    """Stream MERGE INTO page-by-page. Zero accumulation.

    Each page is MERGE'd into the existing table on merge_key:
      - Matching rows → UPDATE SET *
      - New rows → INSERT *
    Schema evolution via ALTER TABLE ADD COLUMN for new columns.
    ~50MB RAM per page regardless of dataset size.
    """
    fqn = f"lake.{schema}.{table_name}"
    t0 = time.monotonic()
    written = 0
    page_num = 0

    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")

    # Check if table exists — if not, first page creates it
    table_exists = True
    try:
        conn.execute(f"DESCRIBE {fqn}")
    except Exception:
        table_exists = False

    # Get target column names if table exists (for colon-prefix normalization)
    target_cols = None
    if table_exists:
        try:
            target_cols = {r[0] for r in conn.execute(f"DESCRIBE {fqn}").fetchall()}
        except Exception:
            pass

    for page in page_iter:
        if page is None or page.num_rows == 0:
            continue

        # Normalize Socrata colon-prefixed columns (:id → _id, :updated_at → _updated_at)
        # Zero-copy metadata-only rename in Arrow
        rename_map = {}
        for col in page.schema.names:
            if col.startswith(":"):
                clean = "_" + col[1:]
                # Only rename if target uses the clean name (or table doesn't exist yet)
                if target_cols is None or clean in target_cols:
                    rename_map[col] = clean
        if rename_map:
            new_names = [rename_map.get(c, c) for c in page.schema.names]
            page = page.rename_columns(new_names)
            # Update merge_key if it was renamed
            if merge_key in rename_map:
                merge_key = rename_map[merge_key]

        # Check if merge_key exists in this page — fall back to INSERT if not
        page_has_key = merge_key in page.schema.names

        def _do_page(p=page, create=(not table_exists and page_num == 0)):
            conn.register("_page", p)
            try:
                if create:
                    conn.execute(f"CREATE TABLE {fqn} AS SELECT * FROM _page")
                elif page_has_key:
                    # Schema evolution: add missing columns
                    existing = {r[0] for r in conn.execute(f"DESCRIBE {fqn}").fetchall()}
                    for col in p.schema.names:
                        if col not in existing:
                            conn.execute(f'ALTER TABLE {fqn} ADD COLUMN "{col}" VARCHAR')
                    conn.execute(f"""
                        MERGE INTO {fqn} AS target
                        USING _page AS source
                        ON target."{merge_key}" = source."{merge_key}"
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """)
                else:
                    # No merge key in source — fall back to INSERT BY NAME
                    existing = {r[0] for r in conn.execute(f"DESCRIBE {fqn}").fetchall()}
                    for col in p.schema.names:
                        if col not in existing:
                            conn.execute(f'ALTER TABLE {fqn} ADD COLUMN "{col}" VARCHAR')
                    # Drop NOT NULL on columns not in source (e.g. _id from old dlt loads)
                    source_cols = set(p.schema.names)
                    for col in existing - source_cols:
                        try:
                            conn.execute(f'ALTER TABLE {fqn} ALTER COLUMN "{col}" DROP NOT NULL')
                        except Exception:
                            pass  # column may already be nullable
                    conn.execute(f"INSERT INTO {fqn} BY NAME SELECT * FROM _page")
            finally:
                conn.unregister("_page")

        _retry(_do_page, f"{fqn}[merge page {page_num}]")
        written += page.num_rows
        page_num += 1

        if page_num % 20 == 0:
            elapsed = time.monotonic() - t0
            rps = int(written / elapsed) if elapsed > 0 else 0
            pct = f" ({written*100//total_rows}%)" if total_rows else ""
            logger.info("%s: merged %d rows so far — %d rows/s%s",
                        fqn, written, rps, pct)

    elapsed = time.monotonic() - t0
    rps = int(written / elapsed) if elapsed > 0 else 0
    logger.info("%s: merged %d rows in %.1fs — %d rows/s (streaming merge, %d pages)",
                fqn, written, elapsed, rps, page_num)
    return written


def update_cursor(conn: duckdb.DuckDBPyConnection,
                  dataset_name: str, row_count: int) -> None:
    """Update pipeline state with latest cursor.
    DuckLake doesn't support PRIMARY KEY — use DELETE + INSERT.
    """
    def _do_cursor():
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lake._pipeline_state (
                dataset_name VARCHAR,
                last_updated_at TIMESTAMP DEFAULT current_timestamp,
                row_count BIGINT,
                last_run_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        conn.execute(
            "DELETE FROM lake._pipeline_state WHERE dataset_name = ?",
            [dataset_name],
        )
        conn.execute("""
            INSERT INTO lake._pipeline_state
                (dataset_name, last_updated_at, row_count, last_run_at)
            VALUES (?, current_timestamp, ?, current_timestamp)
        """, [dataset_name, row_count])

    _retry(_do_cursor, f"cursor:{dataset_name}")


def touch_cursor(conn: duckdb.DuckDBPyConnection, dataset_name: str) -> None:
    """Update only last_run_at, preserving the existing data cursor and row count.

    Use when an asset checks for new data but finds none (delta_skip).
    Proves the asset ran successfully — data is confirmed current.
    If no row exists yet, inserts one with current_timestamp as cursor.
    """
    def _do_touch():
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lake._pipeline_state (
                dataset_name VARCHAR,
                last_updated_at TIMESTAMP DEFAULT current_timestamp,
                row_count BIGINT,
                last_run_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        # Read existing cursor values before deleting
        existing = conn.execute(
            "SELECT last_updated_at, row_count FROM lake._pipeline_state WHERE dataset_name = ? LIMIT 1",
            [dataset_name],
        ).fetchone()

        old_cursor = existing[0] if existing else None
        old_rows = existing[1] if existing else 0

        conn.execute(
            "DELETE FROM lake._pipeline_state WHERE dataset_name = ?",
            [dataset_name],
        )
        conn.execute("""
            INSERT INTO lake._pipeline_state
                (dataset_name, last_updated_at, row_count, last_run_at)
            VALUES (?, ?, ?, current_timestamp)
        """, [dataset_name, old_cursor, old_rows])

    _retry(_do_touch, f"touch:{dataset_name}")
