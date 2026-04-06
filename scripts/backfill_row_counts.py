"""One-time backfill: update _pipeline_state.row_count from actual duckdb_tables() sizes.

Fixes the historical drift where row_count stored the last-delta-written count
instead of the actual table size. Run once after deploying the writer.py fix.

Usage (from dagster-pipeline root):
    uv run python scripts/backfill_row_counts.py
"""
import duckdb
import os
import sys


def _parse_catalog_url(url: str) -> str:
    """Convert postgres:// URL to libpq format for DuckLake ATTACH."""
    if not (url.startswith("postgres://") or url.startswith("postgresql://")):
        return url if url.startswith("ducklake:") else f"ducklake:{url}"
    rest = url.split("://", 1)[1]
    at_idx = rest.rfind("@")
    creds, host_part = rest[:at_idx], rest[at_idx + 1:]
    colon_idx = creds.index(":")
    user, password = creds[:colon_idx], creds[colon_idx + 1:]
    query = ""
    if "?" in host_part:
        host_part, query = host_part.split("?", 1)
    host_port, dbname = host_part.split("/", 1)
    host, port = (host_port.rsplit(":", 1) if ":" in host_port else (host_port, "5432"))
    parts = [f"dbname={dbname}", f"user={user}", f"password={password}",
             f"host={host}", f"port={port}"]
    if query:
        parts.extend(query.split("&"))
    return "ducklake:postgres:" + " ".join(parts)


def main():
    conn = duckdb.connect(":memory:")
    conn.execute("LOAD ducklake")
    conn.execute("LOAD postgres")

    catalog_url = os.environ.get(
        "DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG",
        "ducklake:postgres:dbname=ducklake user=dagster password=dagster host=localhost",
    )
    attach_url = _parse_catalog_url(catalog_url)
    conn.execute(f"ATTACH '{attach_url}' AS lake (METADATA_SCHEMA 'lake')")

    # Get actual table sizes
    actual = conn.execute("""
        SELECT schema_name || '.' || table_name AS fqn, COALESCE(estimated_size, 0)
        FROM duckdb_tables()
        WHERE database_name = 'lake'
          AND schema_name NOT IN ('information_schema', 'pg_catalog')
    """).fetchall()
    actual_map = {row[0]: row[1] for row in actual}

    # Get current pipeline state (preserving cursors)
    state = conn.execute(
        "SELECT dataset_name, row_count, last_updated_at, last_run_at "
        "FROM lake._pipeline_state"
    ).fetchall()
    state_map = {row[0]: (row[1], row[2], row[3]) for row in state}

    updated = 0
    for dataset_name, actual_rows in actual_map.items():
        entry = state_map.get(dataset_name)

        if entry is None:
            # Not in _pipeline_state — skip (derived tables like name_tokens)
            continue

        state_rows, last_updated_at, last_run_at = entry

        if actual_rows != state_rows:
            conn.execute(
                "DELETE FROM lake._pipeline_state WHERE dataset_name = ?",
                [dataset_name],
            )
            # Preserve last_updated_at (the delta cursor) — only update row_count
            conn.execute("""
                INSERT INTO lake._pipeline_state
                    (dataset_name, last_updated_at, row_count, last_run_at)
                VALUES (?, ?, ?, ?)
            """, [dataset_name, last_updated_at, actual_rows, last_run_at])
            drift = actual_rows - (state_rows or 0)
            print(f"  {dataset_name}: {state_rows} → {actual_rows} (drift={drift:+d})")
            updated += 1

    print(f"\nBackfill complete: {updated} datasets corrected out of {len(state_map)} tracked")
    conn.close()


if __name__ == "__main__":
    main()
