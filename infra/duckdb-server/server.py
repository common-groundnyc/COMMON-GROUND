import duckdb
import os
import time

DUCKLAKE_PATH = "/data/common-ground"

conn = duckdb.connect()

conn.execute("INSTALL ducklake; INSTALL postgres;")
conn.execute("LOAD ducklake; LOAD postgres;")

pg_pass = os.environ["DAGSTER_PG_PASSWORD"].replace("'", "''")

conn.execute(f"""
    ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres' AS lake
    (DATA_PATH '{DUCKLAKE_PATH}/data/', METADATA_SCHEMA 'lake')
""")

for schema in ("nyc", "census", "manual"):
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    except Exception:
        pass

print("DuckLake catalog attached (Postgres metadata, local disk data)", flush=True)

try:
    conn.execute("INSTALL httpserver FROM community; LOAD httpserver;")
    api_user = os.environ.get("DUCKDB_API_USER", "")
    api_pass = os.environ.get("DUCKDB_API_PASS", "")
    if api_user and api_pass:
        conn.execute(f"SELECT httpserve_start('0.0.0.0', 9999, '{api_user}:{api_pass}')")
        print("HTTP API running on :9999", flush=True)
    else:
        print("HTTP API disabled (DUCKDB_API_USER/DUCKDB_API_PASS not set)", flush=True)
except Exception as e:
    print(f"httpserver extension not available: {e}", flush=True)

print("DuckDB HTTP API server ready (MCP server runs separately on :4213)", flush=True)

print("DuckDB server running", flush=True)

while True:
    time.sleep(60)
