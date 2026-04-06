"""Ingest 2 new Socrata datasets directly into DuckLake via SQL.

Bypasses dlt (which needs Docker for DuckLake extension loading) by
fetching from Socrata API and inserting via DuckDB connection.

Usage: cd ~/Desktop/dagster-pipeline && uv run python scripts/ingest_new_datasets.py
"""
import sys
sys.path.insert(0, "src")

import logging
import time
import httpx
import orjson
from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOCRATA_TOKEN = "KECcMSAGIwfqQKOpYjsX5EVVO"
PAGE_SIZE = 50_000

DATASETS = [
    ("city_government", "civil_litigation", "pjgc-h7uv", "data.cityofnewyork.us"),
    ("city_government", "settlement_payments", "d7pv-3qwq", "data.cityofnewyork.us"),
]


def fetch_all_rows(domain, dataset_id):
    """Fetch all rows from Socrata API with pagination."""
    all_rows = []
    offset = 0
    while True:
        url = f"https://{domain}/resource/{dataset_id}.json"
        params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"}
        headers = {"X-App-Token": SOCRATA_TOKEN, "Accept": "application/json"}
        resp = httpx.get(url, params=params, headers=headers, timeout=300)
        resp.raise_for_status()
        rows = orjson.loads(resp.content)
        if not rows:
            break
        all_rows.extend(rows)
        logger.info("  Fetched %d rows (total: %d)", len(rows), len(all_rows))
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return all_rows


def create_table_from_rows(conn, schema, table_name, rows):
    """Create a DuckLake table from JSON rows."""
    if not rows:
        logger.warning("No rows for %s.%s — skipping", schema, table_name)
        return 0

    # Register as a DuckDB table from JSON
    import json
    json_str = json.dumps(rows)
    conn.execute(f"CREATE OR REPLACE TEMP TABLE _staging AS SELECT * FROM read_json_auto('{json_str}')")

    # Write to lake
    full_table = f"lake.{schema}.{table_name}"
    conn.execute(f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM _staging")
    count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
    conn.execute("DROP TABLE IF EXISTS _staging")
    return count


def main():
    t0 = time.time()
    conn = _connect_ducklake()

    for schema, table_name, dataset_id, domain in DATASETS:
        logger.info("=== %s.%s (Socrata: %s) ===", schema, table_name, dataset_id)
        t1 = time.time()

        rows = fetch_all_rows(domain, dataset_id)
        logger.info("  Fetched %d total rows in %.1fs", len(rows), time.time() - t1)

        if not rows:
            continue

        # Write rows as JSON file for DuckDB to read
        import tempfile, json, os
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(rows, f)
            tmp_path = f.name

        try:
            full_table = f"lake.{schema}.{table_name}"
            conn.execute(f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM read_json_auto('{tmp_path}')")
            count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
            logger.info("  Wrote %d rows to %s (%.1fs)", count, full_table, time.time() - t1)
        finally:
            os.unlink(tmp_path)

    conn.close()
    logger.info("Done in %.1fs", time.time() - t0)


if __name__ == "__main__":
    main()
