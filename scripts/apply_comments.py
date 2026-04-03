"""Apply COMMENT ON TABLE/COLUMN statements to DuckLake from a YAML manifest."""
import argparse
import os
import sys
from pathlib import Path

import yaml


def _escape_sql(text: str) -> str:
    return text.replace("'", "''")


def apply_manifest_to_db(conn, manifest: dict, database: str = "lake", dry_run: bool = False) -> dict:
    stats = {"tables_commented": 0, "columns_commented": 0, "tables_skipped": 0, "errors": []}

    # Build set of existing (schema, table) pairs — skip in dry-run
    existing_tables = set()
    if not dry_run:
        rows = conn.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = ?",
            [database],
        ).fetchall()
        existing_tables = {(r[0], r[1]) for r in rows}

    # Cache for per-table column names (fetched lazily via DESCRIBE)
    _col_cache: dict[tuple[str, str], set[str]] = {}

    def _get_columns(schema: str, table: str) -> set[str]:
        key = (schema, table)
        if key not in _col_cache:
            try:
                desc = conn.execute(f"DESCRIBE {database}.{schema}.{table}").fetchall()
                _col_cache[key] = {r[0] for r in desc}
            except Exception:
                _col_cache[key] = set()
        return _col_cache[key]

    # Manifest can be a list of entries or a dict with "tables" key
    entries = manifest if isinstance(manifest, list) else manifest.get("tables", [])

    for entry in entries:
        schema = entry["schema"]
        table = entry["table"]

        # In dry-run, we print all SQL; in live mode, skip missing tables
        if not dry_run and (schema, table) not in existing_tables:
            stats["tables_skipped"] += 1
            continue

        # Table comment
        if entry.get("comment"):
            escaped = _escape_sql(entry["comment"])
            sql = f"COMMENT ON TABLE {database}.{schema}.{table} IS '{escaped}'"
            if dry_run:
                print(sql + ";")
            else:
                try:
                    conn.execute(sql)
                except Exception as e:
                    stats["errors"].append(f"{schema}.{table}: {e}")
                    continue
            stats["tables_commented"] += 1

        # Column comments
        for col_name, col_comment in entry.get("columns", {}).items():
            if not dry_run:
                table_cols = _get_columns(schema, table)
                if col_name not in table_cols:
                    continue

            escaped = _escape_sql(col_comment)
            sql = f'COMMENT ON COLUMN {database}.{schema}.{table}."{col_name}" IS \'{escaped}\''
            if dry_run:
                print(sql + ";")
            else:
                try:
                    conn.execute(sql)
                except Exception as e:
                    stats["errors"].append(f"{schema}.{table}.{col_name}: {e}")
                    continue
            stats["columns_commented"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description="Apply catalog comments to DuckLake")
    parser.add_argument("--manifest", default="data/catalog_comments.yaml", help="Path to YAML manifest")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"Manifest not found: {manifest_path}", file=sys.stderr)
        sys.exit(1)

    manifest = yaml.safe_load(manifest_path.read_text())

    if args.dry_run:
        stats = apply_manifest_to_db(conn=None, manifest=manifest, dry_run=True)
    else:
        import duckdb

        catalog_url = os.environ.get("DUCKLAKE_CATALOG_URL", "")
        if not catalog_url:
            print("DUCKLAKE_CATALOG_URL not set", file=sys.stderr)
            sys.exit(1)

        conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        conn.execute("LOAD ducklake")
        conn.execute("LOAD postgres")
        conn.execute("SET http_timeout=300000")
        conn.execute(f"ATTACH '{catalog_url}' AS lake (METADATA_SCHEMA 'lake')")
        stats = apply_manifest_to_db(conn, manifest)
        conn.close()

    print(f"\nTables commented: {stats['tables_commented']}")
    print(f"Columns commented: {stats['columns_commented']}")
    print(f"Tables skipped: {stats['tables_skipped']}")
    if stats["errors"]:
        print(f"Errors: {len(stats['errors'])}")
        for err in stats["errors"]:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
