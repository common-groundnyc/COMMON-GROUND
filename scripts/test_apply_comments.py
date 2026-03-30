"""Tests for apply_comments.py."""
import io
import sys

import duckdb
import pytest

from apply_comments import apply_manifest_to_db


@pytest.fixture
def db_with_table():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA test_schema")
    conn.execute("CREATE TABLE test_schema.my_table (id INTEGER, name VARCHAR, city VARCHAR)")
    conn.execute("INSERT INTO test_schema.my_table VALUES (1, 'Alice', 'NYC')")
    return conn


def test_apply_comments_sets_table_and_column_comments(db_with_table):
    manifest = {
        "tables": [
            {
                "schema": "test_schema",
                "table": "my_table",
                "comment": "A test table with people's names",
                "columns": {
                    "id": "Unique identifier",
                    "name": "Person's full name",
                },
            }
        ]
    }

    stats = apply_manifest_to_db(db_with_table, manifest, database="memory")

    assert stats["tables_commented"] == 1
    assert stats["columns_commented"] == 2
    assert stats["tables_skipped"] == 0
    assert stats["errors"] == []

    # Verify table comment
    rows = db_with_table.execute(
        "SELECT comment FROM duckdb_tables() WHERE schema_name='test_schema' AND table_name='my_table'"
    ).fetchall()
    assert rows[0][0] == "A test table with people's names"

    # Verify column comments
    col_rows = db_with_table.execute(
        "SELECT column_name, comment FROM duckdb_columns() "
        "WHERE schema_name='test_schema' AND table_name='my_table' AND comment IS NOT NULL "
        "ORDER BY column_name"
    ).fetchall()
    assert len(col_rows) == 2
    col_map = {r[0]: r[1] for r in col_rows}
    assert col_map["id"] == "Unique identifier"
    assert col_map["name"] == "Person's full name"


def test_apply_comments_skips_missing_tables(db_with_table):
    manifest = {
        "tables": [
            {
                "schema": "nonexistent",
                "table": "nope",
                "comment": "Should be skipped",
            }
        ]
    }

    stats = apply_manifest_to_db(db_with_table, manifest, database="memory")

    assert stats["tables_skipped"] == 1
    assert stats["tables_commented"] == 0
    assert stats["columns_commented"] == 0
    assert stats["errors"] == []


def test_dry_run_prints_sql(capsys):
    manifest = {
        "tables": [
            {
                "schema": "housing",
                "table": "hpd_violations",
                "comment": "HPD violation records",
                "columns": {
                    "violation_id": "Unique violation ID",
                },
            }
        ]
    }

    stats = apply_manifest_to_db(conn=None, manifest=manifest, dry_run=True)

    captured = capsys.readouterr()
    assert "COMMENT ON TABLE lake.housing.hpd_violations" in captured.out
    assert "HPD violation records" in captured.out
    assert 'COMMENT ON COLUMN lake.housing.hpd_violations."violation_id"' in captured.out
    assert stats["tables_commented"] == 1
    assert stats["columns_commented"] == 1
