"""
One-time migration: read dlt incremental cursors from _dlt_pipeline_state
across 12 DuckLake schemas and write them to lake._pipeline_state.

Runs inside the common-ground-duckdb-server-1 container where
minio:9000 (HTTPS, self-signed cert) and postgres:5432 are accessible.
"""

import base64
import json
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
    conn.execute("INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs")
    conn.execute("SET s3_region='us-east-1'")
    conn.execute("SET s3_endpoint='minio:9000'")
    conn.execute("SET s3_access_key_id='minioadmin'")
    conn.execute("SET s3_secret_access_key='mn_cg_2026_qW5xRtK3pL'")
    conn.execute("SET s3_use_ssl=true")
    conn.execute("SET enable_server_cert_verification=false")
    conn.execute("SET enable_curl_server_cert_verification=false")
    conn.execute("SET s3_url_style='path'")
    conn.execute("SET http_timeout=300000")
    conn.execute(
        "ATTACH 'ducklake:postgres:dbname=ducklake user=dagster "
        "password=/8XhCyQVOtxBTwqtScx5xmO5Lj6wHpN9 host=postgres port=5432' "
        "AS lake (METADATA_SCHEMA 'lake')"
    )
    return conn


def decompress_state(state_raw):
    return json.loads(zlib.decompress(base64.b64decode(state_raw)))


def extract_cursors(schema, state):
    cursors = []
    sources = state.get("sources", {})
    for source_name, source_data in sources.items():
        resources = source_data.get("resources", {})
        for resource_name, resource_data in resources.items():
            incremental = resource_data.get("incremental", {})
            for field_name, field_data in incremental.items():
                last_value = field_data.get("last_value")
                if last_value and last_value != EPOCH_CURSOR:
                    dataset_name = "{}.{}".format(schema, resource_name)
                    cursors.append((dataset_name, last_value))
    return cursors


def fetch_all_cursors(conn):
    all_cursors = []
    for schema in SCHEMAS:
        print("Reading [{}]...".format(schema), flush=True)
        try:
            rows = conn.execute(
                """
                SELECT pipeline_name, state
                FROM lake.{schema}._dlt_pipeline_state
                WHERE (pipeline_name, version) IN (
                    SELECT pipeline_name, MAX(version)
                    FROM lake.{schema}._dlt_pipeline_state
                    GROUP BY pipeline_name
                )
                """.format(schema=schema)
            ).fetchall()
        except Exception as e:
            print("  [{}] SKIP — {}".format(schema, str(e)[:120]), flush=True)
            continue

        schema_cursors = []
        for pipeline_name, state_raw in rows:
            try:
                state = decompress_state(state_raw)
                found = extract_cursors(schema, state)
                schema_cursors.extend(found)
            except Exception as e:
                print("  decompress failed for {}: {}".format(pipeline_name, e), flush=True)

        print("  found {} cursors".format(len(schema_cursors)), flush=True)
        all_cursors.extend(schema_cursors)

    return all_cursors


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
    print("Connecting to DuckLake...", flush=True)
    conn = connect()
    print("Connected.\n", flush=True)

    all_cursors = fetch_all_cursors(conn)

    print("\nTotal cursors extracted: {}".format(len(all_cursors)), flush=True)

    if not all_cursors:
        print("Nothing to insert.", flush=True)
        return

    create_target_table(conn)
    print("Creating/ensuring lake._pipeline_state table...", flush=True)

    conn.execute("DELETE FROM lake._pipeline_state")
    print("Inserting {} rows...".format(len(all_cursors)), flush=True)

    inserted = 0
    for dataset_name, last_value in all_cursors:
        try:
            conn.execute(
                "INSERT INTO lake._pipeline_state (dataset_name, last_updated_at, row_count, last_run_at) "
                "VALUES (?, ?::TIMESTAMP, 0, current_timestamp)",
                [dataset_name, last_value],
            )
            inserted += 1
        except Exception as e:
            print("  failed to insert {}: {}".format(dataset_name, e), flush=True)

    print("Inserted: {}".format(inserted), flush=True)

    count = conn.execute("SELECT COUNT(*) FROM lake._pipeline_state").fetchone()[0]
    print("\nVerification — rows in lake._pipeline_state: {}".format(count), flush=True)

    sample = conn.execute("""
        SELECT dataset_name, last_updated_at
        FROM lake._pipeline_state
        ORDER BY last_updated_at DESC
        LIMIT 10
    """).fetchall()

    print("\nTop 10 most recent cursors:", flush=True)
    for row in sample:
        print("  {:<50}  {}".format(row[0], row[1]), flush=True)


if __name__ == "__main__":
    main()
