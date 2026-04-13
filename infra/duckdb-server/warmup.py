"""Disposable warm-up: trigger DuckLake compactor crash so the main server survives."""
import os, sys
import duckdb

pg_pass = os.environ.get('DAGSTER_PG_PASSWORD', '').replace("'", "''")
conn = duckdb.connect()
for ext in ['ducklake', 'postgres', 'httpfs']:
    conn.execute(f'LOAD {ext}')

conn.execute(f"""
    ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
    AS lake (AUTOMATIC_MIGRATION TRUE)
""")

try:
    conn.execute('SELECT 1 FROM lake.information_schema.tables LIMIT 1')
    print('Warm-up: compactor OK')
except Exception as e:
    print(f'Warm-up: compactor crashed (expected): {e}')
    sys.exit(0)  # exit clean — the crash happened, main server should be fine now
