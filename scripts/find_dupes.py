"""Find all DuckLake tables with duplicate rows."""
import duckdb

conn = duckdb.connect("/data/server.duckdb")
conn.execute("ATTACH 'dbname=ducklake user=dagster password=dagster host=postgres port=5432' AS lake (TYPE ducklake)")
conn.execute("USE lake")

rows = conn.execute("""
    SELECT t.table_schema, t.table_name
    FROM information_schema.tables t
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_catalog = 'lake'
      AND t.table_schema NOT IN ('information_schema', 'pg_catalog', 'lake', 'foundation')
      AND t.table_name NOT LIKE '_dlt_%'
      AND t.table_name NOT LIKE '_pipeline%'
    ORDER BY t.table_schema, t.table_name
""").fetchall()

dupes = []
for schema, table in rows:
    try:
        actual = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        distinct = conn.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {schema}.{table})").fetchone()[0]
        if actual > distinct:
            ratio = round(actual / distinct, 2) if distinct > 0 else 999
            dupes.append((schema, table, actual, distinct, actual - distinct, ratio))
    except Exception as e:
        print(f"SKIP {schema}.{table}: {e}")

print(f"\nTables with duplicates: {len(dupes)}")
for d in sorted(dupes, key=lambda x: -x[4]):
    print(f"  {d[0]}.{d[1]}: {d[2]:,} actual, {d[3]:,} distinct, +{d[4]:,} dupes ({d[5]}x)")

conn.close()
