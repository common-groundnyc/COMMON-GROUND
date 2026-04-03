"""
One-time migration: read dlt incremental cursors from _dlt_pipeline_state
across 12 DuckLake schemas and write them to lake._pipeline_state.
"""

import base64
import json
import time
import zlib

import duckdb

SCHEMAS = [
    "business",
    "city_government",
    "education",
    "environment",
    "financial",
    "health",
    "housing",
    "public_safety",
    "recreation",
    "social_services",
    "transportation",
    "federal",
]

EPOCH_CURSOR = "1970-01-01T00:00:00.000Z"


def connect():
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake")
    conn.execute("SET http_timeout=300000")
    conn.execute(
        "ATTACH 'ducklake:postgres:dbname=ducklake user=dagster "
        "password=/8XhCyQVOtxBTwqtScx5xmO5Lj6wHpN9 host=178.156.228.119 port=5432 sslmode=require' "
        "AS lake (METADATA_SCHEMA 'lake')"
    )
    return conn


def decompress_state(state_raw: str) -> dict:
    return json.loads(zlib.decompress(base64.b64decode(state_raw)))


def extract_cursors(schema: str, state: dict) -> list[tuple[str, str]]:
    """Return list of (dataset_name, last_value) from state JSON."""
    cursors = []
    sources = state.get("sources", {})
    for source_name, source_data in sources.items():
        resources = source_data.get("resources", {})
        for resource_name, resource_data in resources.items():
            incremental = resource_data.get("incremental", {})
            for field_name, field_data in incremental.items():
                last_value = field_data.get("last_value")
                if last_value and last_value != EPOCH_CURSOR:
                    dataset_name = f"{schema}.{resource_name}"
                    cursors.append((dataset_name, last_value))
    return cursors


def fetch_schema_cursors_with_conn(conn, schema: str) -> list[tuple[str, str]]:
    """Query _dlt_pipeline_state for a schema using an existing connection."""
    rows = conn.execute(
        f"""
        SELECT pipeline_name, state
        FROM lake.{schema}._dlt_pipeline_state
        WHERE (pipeline_name, version) IN (
            SELECT pipeline_name, MAX(version)
            FROM lake.{schema}._dlt_pipeline_state
            GROUP BY pipeline_name
        )
        """
    ).fetchall()

    cursors = []
    for pipeline_name, state_raw in rows:
        try:
            state = decompress_state(state_raw)
            found = extract_cursors(schema, state)
            cursors.extend(found)
        except Exception as e:
            print(f"  [{schema}] failed to decompress state for {pipeline_name}: {e}")

    return cursors


def create_target_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lake._pipeline_state (
            dataset_name VARCHAR,
            last_updated_at TIMESTAMP,
            row_count BIGINT,
            last_run_at TIMESTAMP
        )
    """)


def main():
    all_cursors: list[tuple[str, str]] = []
    max_retries = 3

    for schema in SCHEMAS:
        print(f"Reading [{schema}]...")
        cursors = []
        for attempt in range(1, max_retries + 1):
            try:
                conn = connect()
                cursors = fetch_schema_cursors_with_conn(conn, schema)
                conn.close()
                break
            except Exception as e:
                print(f"  [{schema}] attempt {attempt} failed — {e}")
                if attempt < max_retries:
                    time.sleep(2)
                else:
                    print(f"  [{schema}] giving up after {max_retries} attempts")
        print(f"  found {len(cursors)} cursors")
        all_cursors.extend(cursors)

    print(f"\nTotal cursors extracted: {len(all_cursors)}")

    if not all_cursors:
        print("Nothing to insert.")
        return

    print("Connecting to DuckLake for write phase...")
    conn = connect()
    print("Connected.\n")

    create_target_table(conn)

    print(f"Inserting {len(all_cursors)} rows into lake._pipeline_state...")

    # Clear existing rows and do a clean bulk insert
    conn.execute("DELETE FROM lake._pipeline_state")

    inserted = 0
    for dataset_name, last_value in all_cursors:
        try:
            conn.execute(
                """
                INSERT INTO lake._pipeline_state
                    (dataset_name, last_updated_at, row_count, last_run_at)
                VALUES (?, ?::TIMESTAMP, 0, current_timestamp)
                """,
                [dataset_name, last_value],
            )
            inserted += 1
        except Exception as e:
            print(f"  failed to insert {dataset_name}: {e}")

    print(f"Inserted/updated: {inserted}")

    # Verify
    count = conn.execute("SELECT COUNT(*) FROM lake._pipeline_state").fetchone()[0]
    print(f"\nVerification — rows in lake._pipeline_state: {count}")

    sample = conn.execute("""
        SELECT dataset_name, last_updated_at
        FROM lake._pipeline_state
        ORDER BY last_updated_at DESC
        LIMIT 10
    """).fetchall()

    print("\nTop 10 most recent cursors:")
    for row in sample:
        print(f"  {row[0]:50s}  {row[1]}")


if __name__ == "__main__":
    main()
