"""Dedup all doubled tables in DuckLake by replacing with SELECT DISTINCT *."""
import duckdb, time, os

conn = duckdb.connect("/data/server.duckdb")

conn.execute("LOAD ducklake")

conn.execute("SET memory_limit = '4GB'")
conn.execute("SET temp_directory = '/tmp/duckdb_dedup'")
conn.execute("SET max_temp_directory_size = '50GB'")

pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "dagster")
conn.execute(f"""
    ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
    AS lake (METADATA_SCHEMA 'lake')
""")
conn.execute("USE lake")
print("Connected to DuckLake", flush=True)

# Verify access
test = conn.execute("SELECT COUNT(*) FROM business.nys_entity_addresses").fetchone()[0]
print(f"Test: business.nys_entity_addresses = {test:,} rows", flush=True)

TABLES = [
    # ed_flu_visits already done
    "business.nys_corp_all_filings",
    "business.nys_entity_addresses",
    "business.nys_corp_name_history",
    "business.nys_corporations",
    "city_government.nys_campaign_expenditures",
    "city_government.nys_campaign_contributions",
    "city_government.nys_campaign_finance",
    "transportation.street_permits",
    "housing.dob_complaints",
]

for fqn in TABLES:
    before = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
    print(f"DEDUP {fqn}: {before:,} rows...", flush=True)
    t0 = time.time()
    conn.execute(f"CREATE OR REPLACE TABLE {fqn} AS SELECT DISTINCT * FROM {fqn}")
    elapsed = time.time() - t0
    after = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
    removed = before - after
    print(f"  DONE in {elapsed:.1f}s: {before:,} -> {after:,} (removed {removed:,})", flush=True)

print("\nAll done.")
conn.close()
