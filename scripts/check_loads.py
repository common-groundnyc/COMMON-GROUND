"""Check dlt load IDs to identify duplicate loads."""
import duckdb

conn = duckdb.connect("/data/server.duckdb")
conn.execute("ATTACH 'dbname=ducklake user=dagster password=dagster host=postgres port=5432' AS lake (TYPE ducklake)")

tables = [
    "lake.business.nys_entity_addresses",
    "lake.business.nys_corp_all_filings",
    "lake.business.nys_corp_name_history",
    "lake.business.acris_pp_parties",
    "lake.housing.hpd_violations",
    "lake.city_government.citywide_payroll",
]

for fqn in tables:
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
        cols = conn.execute(f"SELECT * FROM {fqn} LIMIT 0").description
        col_names = [c[0] for c in cols]
        dlt_cols = [c for c in col_names if c.startswith("_dlt")]
        print(f"\n{fqn}: {total:,} rows, dlt cols: {dlt_cols}")

        if "_dlt_load_id" in col_names:
            loads = conn.execute(f"SELECT _dlt_load_id, COUNT(*) FROM {fqn} GROUP BY 1 ORDER BY 1").fetchall()
            for l in loads:
                print(f"  load {l[0]}: {l[1]:,}")
        else:
            print(f"  No _dlt_load_id — checking for row duplication another way")
            # Pick a likely unique-ish column
            non_dlt = [c for c in col_names if not c.startswith("_")]
            if non_dlt:
                first_col = non_dlt[0]
                dup_check = conn.execute(f"""
                    SELECT {first_col}, COUNT(*) as cnt
                    FROM {fqn}
                    GROUP BY {first_col}
                    HAVING cnt > 1
                    ORDER BY cnt DESC
                    LIMIT 5
                """).fetchall()
                if dup_check:
                    print(f"  Top dups by '{first_col}': {dup_check}")
                else:
                    print(f"  No dups found by '{first_col}'")
    except Exception as e:
        print(f"{fqn}: ERROR {e}")

conn.close()
