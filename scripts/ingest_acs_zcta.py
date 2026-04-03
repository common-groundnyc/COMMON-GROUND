"""Ingest ACS 5-Year ZCTA demographics into DuckLake.

Fetches from Census API, filters to NYC ZIP prefixes, creates
lake.federal.acs_zcta_demographics table.

Designed to run inside the duckdb-server container where DuckDB,
ducklake extension, and env vars are available.
"""

import json
import os
import sys
from urllib.request import urlopen, Request
from urllib.error import HTTPError

# ---------------------------------------------------------------------------
# Census API fetch
# ---------------------------------------------------------------------------

ACS_URL = (
    "https://api.census.gov/data/2023/acs/acs5"
    "?get=NAME,B01003_001E,B01002_001E,B19013_001E,B25064_001E,"
    "B25003_001E,B25003_002E,B17001_001E,B17001_002E,"
    "B02001_001E,B02001_002E,B02001_003E,B02001_005E,"
    "B03003_001E,B03003_003E,B25001_001E,B25002_003E,"
    "B08303_001E,B08303_013E,B25077_001E"
    "&for=zip%20code%20tabulation%20area:*"
)

# Try with state filter first, fall back to all ZCTAs
ACS_URL_STATE = ACS_URL + "&in=state:36"

NYC_PREFIXES = ("100", "101", "102", "103", "104", "110", "111", "112", "113", "114", "116")


def fetch_acs_data():
    """Fetch ACS data, filter to NYC ZCTAs."""
    for url in [ACS_URL_STATE, ACS_URL]:
        try:
            print(f"Fetching: {url[:80]}...", flush=True)
            req = Request(url, headers={"User-Agent": "CommonGround/1.0"})
            with urlopen(req, timeout=60) as resp:
                raw = json.loads(resp.read().decode())
            if len(raw) > 1:
                print(f"Got {len(raw) - 1} rows from Census API", flush=True)
                break
        except HTTPError as e:
            print(f"HTTP error {e.code}, trying without state filter...", flush=True)
            continue
    else:
        raise RuntimeError("Census API returned no data")

    header = raw[0]
    rows = raw[1:]

    # Find ZCTA column
    zcta_col = None
    for i, h in enumerate(header):
        if "zip" in h.lower() or "zcta" in h.lower():
            zcta_col = i
            break
    if zcta_col is None:
        raise RuntimeError(f"No ZCTA column found in header: {header}")

    # Filter to NYC
    nyc_rows = [r for r in rows if r[zcta_col].startswith(NYC_PREFIXES)]
    print(f"Filtered to {len(nyc_rows)} NYC ZCTAs", flush=True)
    return header, nyc_rows, zcta_col


def safe_int(val):
    if val is None or val == "" or val == "null":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_float(val):
    if val is None or val == "" or val == "null":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# DuckLake connection + insert
# ---------------------------------------------------------------------------

def main():
    import duckdb

    header, nyc_rows, zcta_col = fetch_acs_data()

    if not nyc_rows:
        print("ERROR: No NYC rows found", flush=True)
        sys.exit(1)

    # Map header positions
    col_map = {h: i for i, h in enumerate(header)}

    # Build records
    records = []
    for row in nyc_rows:
        records.append({
            "zcta": row[zcta_col],
            "acs_year": 2023,
            "name": row[col_map["NAME"]],
            "total_population": safe_int(row[col_map["B01003_001E"]]),
            "median_age": safe_float(row[col_map["B01002_001E"]]),
            "median_household_income": safe_int(row[col_map["B19013_001E"]]),
            "median_gross_rent": safe_int(row[col_map["B25064_001E"]]),
            "total_occupied_units": safe_int(row[col_map["B25003_001E"]]),
            "owner_occupied_units": safe_int(row[col_map["B25003_002E"]]),
            "poverty_universe": safe_int(row[col_map["B17001_001E"]]),
            "below_poverty": safe_int(row[col_map["B17001_002E"]]),
            "race_universe": safe_int(row[col_map["B02001_001E"]]),
            "white_alone": safe_int(row[col_map["B02001_002E"]]),
            "black_alone": safe_int(row[col_map["B02001_003E"]]),
            "asian_alone": safe_int(row[col_map["B02001_005E"]]),
            "ethnicity_universe": safe_int(row[col_map["B03003_001E"]]),
            "hispanic_latino": safe_int(row[col_map["B03003_003E"]]),
            "total_housing_units": safe_int(row[col_map["B25001_001E"]]),
            "vacant_units": safe_int(row[col_map["B25002_003E"]]),
            "commute_universe": safe_int(row[col_map["B08303_001E"]]),
            "commute_60_plus_min": safe_int(row[col_map["B08303_013E"]]),
            "median_home_value": safe_int(row[col_map["B25077_001E"]]),
        })

    # Connect to DuckLake (same as mcp_server.py lifespan)
    conn = duckdb.connect()

    for ext in ["ducklake", "postgres"]:
        try:
            conn.execute(f"INSTALL {ext}")
            conn.execute(f"LOAD {ext}")
        except duckdb.Error:
            conn.execute(f"LOAD {ext}")

    # Attach DuckLake
    pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "\\'")
    conn.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
        AS lake (METADATA_SCHEMA 'lake')
    """)
    print("DuckLake attached", flush=True)

    # Ensure federal schema exists
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.federal")
        print("Schema lake.federal ensured", flush=True)
    except duckdb.Error as e:
        print(f"Schema creation note: {e}", flush=True)

    # Drop existing table if present
    try:
        conn.execute("DROP TABLE IF EXISTS lake.federal.acs_zcta_demographics")
    except duckdb.Error:
        pass

    # Create table
    conn.execute("""
        CREATE TABLE lake.federal.acs_zcta_demographics (
            zcta VARCHAR NOT NULL,
            acs_year INTEGER NOT NULL,
            name VARCHAR,
            total_population INTEGER,
            median_age DOUBLE,
            median_household_income INTEGER,
            median_gross_rent INTEGER,
            total_occupied_units INTEGER,
            owner_occupied_units INTEGER,
            poverty_universe INTEGER,
            below_poverty INTEGER,
            race_universe INTEGER,
            white_alone INTEGER,
            black_alone INTEGER,
            asian_alone INTEGER,
            ethnicity_universe INTEGER,
            hispanic_latino INTEGER,
            total_housing_units INTEGER,
            vacant_units INTEGER,
            commute_universe INTEGER,
            commute_60_plus_min INTEGER,
            median_home_value INTEGER
        )
    """)
    print("Table created", flush=True)

    # Insert rows
    insert_sql = """
        INSERT INTO lake.federal.acs_zcta_demographics VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """
    for rec in records:
        conn.execute(insert_sql, [
            rec["zcta"], rec["acs_year"], rec["name"],
            rec["total_population"], rec["median_age"],
            rec["median_household_income"], rec["median_gross_rent"],
            rec["total_occupied_units"], rec["owner_occupied_units"],
            rec["poverty_universe"], rec["below_poverty"],
            rec["race_universe"], rec["white_alone"], rec["black_alone"],
            rec["asian_alone"], rec["ethnicity_universe"], rec["hispanic_latino"],
            rec["total_housing_units"], rec["vacant_units"],
            rec["commute_universe"], rec["commute_60_plus_min"],
            rec["median_home_value"],
        ])

    # Verify
    result = conn.execute(
        "SELECT COUNT(*) AS cnt FROM lake.federal.acs_zcta_demographics"
    ).fetchone()
    print(f"\nLoaded {result[0]} rows into lake.federal.acs_zcta_demographics", flush=True)

    sample = conn.execute("""
        SELECT zcta, name, total_population, median_household_income, median_age
        FROM lake.federal.acs_zcta_demographics
        ORDER BY total_population DESC
        LIMIT 10
    """).fetchall()
    print("\nTop 10 ZCTAs by population:", flush=True)
    print(f"{'ZCTA':<8} {'Name':<35} {'Pop':>10} {'MedIncome':>12} {'MedAge':>8}", flush=True)
    print("-" * 75, flush=True)
    for row in sample:
        pop = f"{row[2]:,}" if row[2] else "N/A"
        inc = f"${row[3]:,}" if row[3] else "N/A"
        age = f"{row[4]:.1f}" if row[4] else "N/A"
        print(f"{row[0]:<8} {(row[1] or ''):<35} {pop:>10} {inc:>12} {age:>8}", flush=True)

    conn.close()
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
