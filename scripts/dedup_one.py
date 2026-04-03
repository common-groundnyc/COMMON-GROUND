"""Dedup a single DuckLake table. Usage: python3 dedup_one.py schema.table"""
import duckdb, time, sys, os

fqn = sys.argv[1]
print(f"Dedup: {fqn}", flush=True)

conn = duckdb.connect("/data/server.duckdb")
conn.execute("LOAD ducklake")
conn.execute("SET memory_limit = '1GB'")
conn.execute("SET temp_directory = '/tmp/duckdb_dedup'")
conn.execute("SET max_temp_directory_size = '50GB'")
conn.execute("SET threads = 1")
conn.execute("SET preserve_insertion_order = false")

pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "dagster")
conn.execute(f"""
    ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
    AS lake (METADATA_SCHEMA 'lake')
""")
conn.execute("USE lake")

before = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
print(f"  Before: {before:,}", flush=True)

t0 = time.time()
conn.execute(f"CREATE OR REPLACE TABLE {fqn} AS SELECT DISTINCT * FROM {fqn}")
elapsed = time.time() - t0

after = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
print(f"  After:  {after:,} (removed {before - after:,} in {elapsed:.1f}s)", flush=True)
conn.close()
