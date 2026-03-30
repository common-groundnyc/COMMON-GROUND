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
    existing_columns: dict[tuple[str, str], set[str]] = {}
    if not dry_run:
        rows = conn.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = ?",
            [database],
        ).fetchall()
        existing_tables = {(r[0], r[1]) for r in rows}

        col_rows = conn.execute(
            "SELECT schema_name, table_name, column_name FROM duckdb_columns() WHERE database_name = ?",
            [database],
        ).fetchall()
        for r in col_rows:
            key = (r[0], r[1])
            existing_columns.setdefault(key, set()).add(r[2])

    for entry in manifest.get("tables", []):
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
                table_cols = existing_columns.get((schema, table), set())
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
        # Late import — only needed for live mode
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
        from dagster_pipeline.sources.socrata_direct import get_ducklake_connection

        pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "")
        if not pg_pass:
            print("DAGSTER_PG_PASSWORD not set", file=sys.stderr)
            sys.exit(1)

        catalog_url = (
            f"ducklake:postgres:dbname=ducklake user=dagster "
            f"password={pg_pass} host={os.environ.get('DUCKLAKE_HOST', 'postgres')} port=5432"
        )
        s3_config = {
            "endpoint": os.environ.get("S3_ENDPOINT", "minio:9000"),
            "access_key": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
            "secret_key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
            "use_ssl": False,
        }
        conn = get_ducklake_connection(catalog_url, s3_config)
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
