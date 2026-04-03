"""Investigate NYPD Sergeant ANGELYNIAUK, VITALIY — pull all records from the lake."""

import duckdb
import os
import json

conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
conn.execute("INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;")

pg = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "''")
conn.execute(
    f"ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg} host=postgres' "
    f"AS lake (METADATA_SCHEMA 'lake')"
)

SEARCH = "%ANGELYNIAUK%"
SEP = "=" * 80


def run_query(label: str, sql: str, params: list | None = None) -> None:
    """Run a query and print all results."""
    print(f"\n{SEP}")
    print(f"  {label}")
    print(SEP)
    try:
        if params:
            result = conn.execute(sql, params).fetchdf()
        else:
            result = conn.execute(sql).fetchdf()
        if result.empty:
            print("  (no records found)")
        else:
            print(result.to_string(index=False, max_rows=500, max_colwidth=120))
            print(f"\n  [{len(result)} row(s)]")
    except Exception as e:
        print(f"  ERROR: {e}")


def describe_table(schema: str, table: str) -> list[str]:
    """Return column names for a table."""
    try:
        df = conn.execute(f"DESCRIBE lake.{schema}.{table}").fetchdf()
        return df["column_name"].tolist()
    except Exception:
        return []


# ── 1. Discover relevant tables ──────────────────────────────────────────────

print("\n" + SEP)
print("  DISCOVERING TABLES")
print(SEP)

# List all tables in public_safety, city_government, federal schemas
for schema in ["public_safety", "city_government", "federal"]:
    try:
        tables = conn.execute(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_catalog = 'lake'"
        ).fetchdf()
        print(f"\n  Schema: {schema}")
        for t in tables["table_name"].tolist():
            print(f"    - {t}")
    except Exception as e:
        print(f"  Schema {schema}: ERROR {e}")


# ── 2. CCRB Officer Profile ──────────────────────────────────────────────────

# Find CCRB tables
ccrb_tables = []
try:
    df = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public_safety' AND table_catalog = 'lake' "
        "AND table_name LIKE '%ccrb%'"
    ).fetchdf()
    ccrb_tables = df["table_name"].tolist()
except Exception:
    pass

print(f"\n\nCCRB tables found: {ccrb_tables}")

for t in ccrb_tables:
    cols = describe_table("public_safety", t)
    print(f"\n  {t} columns: {cols}")

# Query each CCRB table for the officer
for t in ccrb_tables:
    cols = describe_table("public_safety", t)
    # Find name-like columns
    name_cols = [c for c in cols if any(k in c.lower() for k in ["name", "officer", "last", "first"])]
    for nc in name_cols:
        run_query(
            f"CCRB: {t} (searching {nc})",
            f"SELECT * FROM lake.public_safety.{t} WHERE UPPER(CAST({nc} AS VARCHAR)) LIKE ?",
            [SEARCH],
        )


# ── 3. Citywide Payroll ─────────────────────────────────────────────────────

payroll_cols = describe_table("city_government", "citywide_payroll")
print(f"\n\ncitywide_payroll columns: {payroll_cols}")

# Find name column
name_cols_payroll = [c for c in payroll_cols if any(k in c.lower() for k in ["name", "last", "first", "employee"])]
print(f"  Name-like columns: {name_cols_payroll}")

for nc in name_cols_payroll:
    run_query(
        f"PAYROLL: citywide_payroll (searching {nc})",
        f"SELECT * FROM lake.city_government.citywide_payroll WHERE UPPER(CAST({nc} AS VARCHAR)) LIKE ?",
        [SEARCH],
    )


# ── 4. OATH Hearings ────────────────────────────────────────────────────────

oath_cols = describe_table("city_government", "oath_hearings")
print(f"\n\noath_hearings columns: {oath_cols}")

name_cols_oath = [c for c in oath_cols if any(k in c.lower() for k in ["name", "respondent", "last", "first"])]
print(f"  Name-like columns: {name_cols_oath}")

for nc in name_cols_oath:
    run_query(
        f"OATH: oath_hearings (searching {nc})",
        f"SELECT * FROM lake.city_government.oath_hearings WHERE UPPER(CAST({nc} AS VARCHAR)) LIKE ?",
        [SEARCH],
    )


# ── 5. Federal / NYPD tables ────────────────────────────────────────────────

federal_tables = []
try:
    df = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'federal' AND table_catalog = 'lake'"
    ).fetchdf()
    federal_tables = df["table_name"].tolist()
except Exception:
    pass

print(f"\n\nFederal tables: {federal_tables}")

nypd_tables = [t for t in federal_tables if "nypd" in t.lower() or "officer" in t.lower() or "misconduct" in t.lower()]
print(f"NYPD-related tables: {nypd_tables}")

for t in federal_tables:
    cols = describe_table("federal", t)
    name_cols_fed = [c for c in cols if any(k in c.lower() for k in ["name", "officer", "last", "first", "respondent"])]
    if name_cols_fed:
        print(f"\n  {t} name columns: {name_cols_fed}")
        for nc in name_cols_fed:
            run_query(
                f"FEDERAL: {t} (searching {nc})",
                f"SELECT * FROM lake.federal.{t} WHERE UPPER(CAST({nc} AS VARCHAR)) LIKE ?",
                [SEARCH],
            )


# ── 6. Resolved Entities (cross-reference) ──────────────────────────────────

re_cols = describe_table("federal", "resolved_entities")
if re_cols:
    print(f"\n\nresolved_entities columns: {re_cols}")
    name_cols_re = [c for c in re_cols if any(k in c.lower() for k in ["name", "canonical", "entity"])]
    for nc in name_cols_re:
        run_query(
            f"RESOLVED ENTITIES (searching {nc})",
            f"SELECT * FROM lake.federal.resolved_entities WHERE UPPER(CAST({nc} AS VARCHAR)) LIKE ?",
            [SEARCH],
        )


# ── 7. Housing / Evictions ───────────────────────────────────────────────────

eviction_tables = []
try:
    df = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'lake' AND table_name LIKE '%evict%'"
    ).fetchdf()
    eviction_tables = df["table_name"].tolist()
except Exception:
    pass

print(f"\n\nEviction tables: {eviction_tables}")

for t in eviction_tables:
    # Find schema
    schema_df = conn.execute(
        f"SELECT table_schema FROM information_schema.tables "
        f"WHERE table_catalog = 'lake' AND table_name = '{t}'"
    ).fetchdf()
    schema = schema_df["table_schema"].iloc[0]
    cols = describe_table(schema, t)
    name_cols_ev = [c for c in cols if any(k in c.lower() for k in ["name", "respondent", "last", "first", "executed"])]
    for nc in name_cols_ev:
        run_query(
            f"EVICTION: {schema}.{t} (searching {nc})",
            f"SELECT * FROM lake.{schema}.{t} WHERE UPPER(CAST({nc} AS VARCHAR)) LIKE ?",
            [SEARCH],
        )


# ── 8. Civil Service ────────────────────────────────────────────────────────

civil_tables = []
try:
    df = conn.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_catalog = 'lake' AND (table_name LIKE '%civil%' OR table_name LIKE '%service%')"
    ).fetchdf()
    civil_tables = list(zip(df["table_schema"].tolist(), df["table_name"].tolist()))
except Exception:
    pass

print(f"\n\nCivil service tables: {civil_tables}")

for schema, t in civil_tables:
    cols = describe_table(schema, t)
    name_cols_cs = [c for c in cols if any(k in c.lower() for k in ["name", "last", "first"])]
    for nc in name_cols_cs:
        run_query(
            f"CIVIL SERVICE: {schema}.{t} (searching {nc})",
            f"SELECT * FROM lake.{schema}.{t} WHERE UPPER(CAST({nc} AS VARCHAR)) LIKE ?",
            [SEARCH],
        )


# ── 9. Broad search — any table with name columns ───────────────────────────

print(f"\n\n{SEP}")
print("  BROAD SEARCH — checking ALL schemas for ANGELYNIAUK")
print(SEP)

all_tables = conn.execute(
    "SELECT table_schema, table_name FROM information_schema.tables "
    "WHERE table_catalog = 'lake' AND table_schema NOT IN ('pg_catalog', 'information_schema', 'ducklake', 'public')"
).fetchdf()

searched = set()
for _, row in all_tables.iterrows():
    schema = row["table_schema"]
    table = row["table_name"]
    key = f"{schema}.{table}"
    if key in searched:
        continue
    searched.add(key)

    cols = describe_table(schema, table)
    name_cols_all = [c for c in cols if any(k in c.lower() for k in ["name", "last_name", "first_name", "officer", "respondent"])]
    for nc in name_cols_all:
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM lake.{schema}.{table} WHERE UPPER(CAST({nc} AS VARCHAR)) LIKE ?",
                [SEARCH],
            ).fetchone()[0]
            if count > 0:
                run_query(
                    f"FOUND in {schema}.{table}.{nc} ({count} rows)",
                    f"SELECT * FROM lake.{schema}.{table} WHERE UPPER(CAST({nc} AS VARCHAR)) LIKE ?",
                    [SEARCH],
                )
        except Exception:
            pass


print(f"\n\n{SEP}")
print("  INVESTIGATION COMPLETE")
print(SEP)
