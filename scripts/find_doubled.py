"""Compare actual row counts vs pipeline_state to find doubled tables.
Runs inside the duckdb-server container where lake is already attached."""
import duckdb, os

DB = os.environ.get("DUCKDB_PATH", "/data/server.duckdb")
conn = duckdb.connect(DB)
conn.execute("ATTACH 'dbname=ducklake user=dagster password=dagster host=postgres port=5432' AS lake (TYPE ducklake)")
conn.execute("USE lake")

# Get expected row counts from pipeline state
expected = {}
for r in conn.execute("SELECT dataset_name, row_count FROM lake.lake._pipeline_state").fetchall():
    expected[r[0]] = r[1]

# Get all base tables
tables = conn.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_catalog = 'lake'
      AND table_schema NOT IN ('information_schema', 'pg_catalog', 'lake', 'foundation', 'ducklake', 'public')
      AND table_name NOT LIKE '_dlt_%'
      AND table_name NOT LIKE '_pipeline%'
""").fetchall()

doubled = []
for schema, table in tables:
    key = f"{schema}.{table}"
    exp = expected.get(key)
    if not exp:
        continue
    try:
        actual = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
        ratio = round(actual / exp, 2) if exp > 0 else 0
        if ratio > 1.5:  # more than 50% over expected
            doubled.append((key, actual, exp, ratio))
    except Exception as e:
        print(f"SKIP {key}: {e}")

doubled.sort(key=lambda x: -(x[1] - x[2]))
print(f"Tables with >1.5x expected rows: {len(doubled)}\n")
total_excess = 0
for key, actual, exp, ratio in doubled:
    excess = actual - exp
    total_excess += excess
    print(f"  {key}: {actual:>12,} actual vs {exp:>12,} expected  ({ratio}x, +{excess:,})")

print(f"\nTotal excess rows: {total_excess:,}")
conn.close()
