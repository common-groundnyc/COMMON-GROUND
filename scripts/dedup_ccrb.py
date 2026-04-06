"""One-time dedup of CCRB tables that have 5x duplication from blind INSERT merges.

Run: uv run python scripts/dedup_ccrb.py
"""
import duckdb
import os
import subprocess
import time


def _load_secrets():
    """Load secrets from sops-encrypted .env file."""
    result = subprocess.run(
        ["sops", "--decrypt", "--input-type", "dotenv", "--output-type", "dotenv", ".env.secrets.enc"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"sops decrypt failed: {result.stderr}")
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, val = line.partition("=")
        os.environ[key.strip()] = val.strip()


_load_secrets()

# Override catalog URL: Docker container hostname → external IP
catalog = os.environ.get("DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG", "")
if "common-ground-postgres-1" in catalog:
    catalog = catalog.replace("common-ground-postgres-1", "178.156.228.119")
    os.environ["DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG"] = catalog

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

DEDUP_TABLES = {
    # ccrb_complaints was dropped — needs re-ingestion via Dagster
    "public_safety.ccrb_allegations": None,  # no single PK — dedup on all columns
    "public_safety.ccrb_officers": None,
    "public_safety.ccrb_penalties": None,
}


def dedup_table(conn: duckdb.DuckDBPyConnection, fqn: str, key_col: str | None, max_retries: int = 5) -> tuple[int, int]:
    """Dedup a table in-place. Returns (before, after) row counts.
    Retries on DuckLake transaction conflicts."""
    before = conn.execute(f"SELECT COUNT(*) FROM lake.{fqn}").fetchone()[0]

    # Get explicit column list to avoid stale view resolution issues
    col_rows = conn.execute(f"DESCRIBE lake.{fqn}").fetchall()
    col_names = [f'"{r[0]}"' for r in col_rows]
    col_list = ", ".join(col_names)

    tmp = f"_dedup_{fqn.replace('.', '_')}"

    # DuckLake inlined data can have ghost columns that crash full-table scans.
    # Chunk the read in batches to work around this.
    CHUNK = 200_000
    conn.execute(f"DROP TABLE IF EXISTS {tmp}")

    if key_col:
        # Single-pass dedup on key (works if no ghost column issues)
        try:
            conn.execute(f"""
                CREATE TEMP TABLE {tmp} AS
                SELECT {col_list} FROM (
                    SELECT {col_list}, ROW_NUMBER() OVER (PARTITION BY "{key_col}" ORDER BY rowid) AS _rn
                    FROM lake.{fqn}
                ) WHERE _rn = 1
            """)
        except Exception:
            # Fallback: chunked read + dedup at end
            print(f"    Full scan failed, chunking {CHUNK:,} at a time...")
            for offset in range(0, before, CHUNK):
                sql = f"SELECT {col_list} FROM lake.{fqn} LIMIT {CHUNK} OFFSET {offset}"
                if offset == 0:
                    conn.execute(f"CREATE TEMP TABLE {tmp} AS {sql}")
                else:
                    conn.execute(f"INSERT INTO {tmp} {sql}")
            # Dedup in temp
            conn.execute(f"""
                CREATE OR REPLACE TEMP TABLE {tmp} AS
                SELECT {col_list} FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY "{key_col}") AS _rn FROM {tmp}
                ) WHERE _rn = 1
            """)
    else:
        # DISTINCT * — try direct, fallback to chunked
        try:
            conn.execute(f"CREATE TEMP TABLE {tmp} AS SELECT DISTINCT {col_list} FROM lake.{fqn}")
        except Exception:
            print(f"    Full scan failed, chunking {CHUNK:,} at a time...")
            for offset in range(0, before, CHUNK):
                sql = f"SELECT {col_list} FROM lake.{fqn} LIMIT {CHUNK} OFFSET {offset}"
                if offset == 0:
                    conn.execute(f"CREATE TEMP TABLE {tmp} AS {sql}")
                else:
                    conn.execute(f"INSERT INTO {tmp} {sql}")
            # Dedup in temp
            conn.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} AS SELECT DISTINCT * FROM {tmp}")

    after = conn.execute(f"SELECT COUNT(*) FROM {tmp}").fetchone()[0]

    # DROP + CREATE with retry on transaction conflicts
    for attempt in range(max_retries):
        try:
            conn.execute(f"DROP TABLE IF EXISTS lake.{fqn}")
            conn.execute(f"CREATE TABLE lake.{fqn} AS SELECT * FROM {tmp}")
            break
        except Exception as e:
            if "Transaction conflict" in str(e) and attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"    Transaction conflict, retrying in {wait}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait)
            else:
                raise

    conn.execute(f"DROP TABLE IF EXISTS {tmp}")
    return before, after


def main():
    conn = _connect_ducklake()
    try:
        for fqn, key_col in DEDUP_TABLES.items():
            print(f"\nDeduplicating lake.{fqn} (key={key_col or 'DISTINCT *'})...")
            t0 = time.time()
            before, after = dedup_table(conn, fqn, key_col)
            elapsed = time.time() - t0
            removed = before - after
            factor = round(before / after, 1) if after > 0 else 0
            print(f"  {before:,} → {after:,} ({removed:,} removed, was {factor}x duplicated, {elapsed:.1f}s)")
        print("\nDone.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
