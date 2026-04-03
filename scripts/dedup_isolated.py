"""Dedup remaining tables. Run in a separate container."""
import duckdb, time, os

conn = duckdb.connect("/data/dedup_temp.duckdb", config={"allow_unsigned_extensions": "true"})
conn.execute("INSTALL ducklake; LOAD ducklake")
conn.execute("SET memory_limit = '4GB'")
conn.execute("SET temp_directory = '/tmp/duckdb_dedup'")
conn.execute("SET max_temp_directory_size = '50GB'")
conn.execute("SET threads = 2")
conn.execute("SET preserve_insertion_order = false")
pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "dagster").replace("'", "''")
conn.execute(f"""
    ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
    AS lake (METADATA_SCHEMA 'lake')
""")
conn.execute("USE lake")
print("Connected", flush=True)

tables = [
    "city_government.nys_campaign_contributions",
    "city_government.nys_campaign_finance",
    "transportation.street_permits",
    "housing.dob_complaints",
]

for fqn in tables:
    before = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
    print(f"DEDUP {fqn}: {before:,} rows...", flush=True)
    t0 = time.time()
    conn.execute(f"CREATE OR REPLACE TABLE {fqn} AS SELECT DISTINCT * FROM {fqn}")
    after = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
    print(f"  DONE in {time.time()-t0:.1f}s: {before:,} -> {after:,} (removed {before-after:,})", flush=True)

conn.close()
os.unlink("/data/dedup_temp.duckdb")
print("All done.")
