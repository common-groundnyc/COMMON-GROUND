"""Tests for DuckLake write operations."""
import duckdb
import pyarrow as pa
import pytest

from dagster_pipeline.ingestion.writer import (
    update_cursor,
    write_delta_merge,
    write_full_replace,
)


@pytest.fixture
def conn():
    """In-memory DuckDB with an attached 'lake' database."""
    c = duckdb.connect(":memory:")
    c.execute("ATTACH ':memory:' AS lake")
    yield c
    c.close()


def make_table(n=3):
    return pa.table({
        "_id": list(range(1, n + 1)),
        "name": [f"row_{i}" for i in range(1, n + 1)],
    })


# ---------------------------------------------------------------------------
# write_full_replace
# ---------------------------------------------------------------------------

def test_write_full_replace(conn):
    table = make_table(3)
    count = write_full_replace(conn, "test", "items", table)
    assert count == 3
    result = conn.execute("SELECT count(*) FROM lake.test.items").fetchone()[0]
    assert result == 3


def test_write_full_replace_overwrites(conn):
    write_full_replace(conn, "test", "items", make_table(3))
    count = write_full_replace(conn, "test", "items", make_table(5))
    assert count == 5
    result = conn.execute("SELECT count(*) FROM lake.test.items").fetchone()[0]
    assert result == 5


# ---------------------------------------------------------------------------
# write_delta_merge
# ---------------------------------------------------------------------------

def test_write_delta_merge_insert(conn):
    # Seed 3 rows
    write_full_replace(conn, "test", "items", make_table(3))

    # Merge 2 new rows (IDs 4 and 5)
    new_rows = pa.table({"_id": [4, 5], "name": ["row_4", "row_5"]})
    write_delta_merge(conn, "test", "items", new_rows, merge_key="_id")

    result = conn.execute("SELECT count(*) FROM lake.test.items").fetchone()[0]
    assert result == 5


def test_write_delta_merge_update(conn):
    write_full_replace(conn, "test", "items", make_table(3))

    # Same IDs, different names
    updated = pa.table({"_id": [1, 2, 3], "name": ["x", "y", "z"]})
    write_delta_merge(conn, "test", "items", updated, merge_key="_id")

    result = conn.execute(
        "SELECT count(*) FROM lake.test.items"
    ).fetchone()[0]
    assert result == 3

    names = {
        row[0]
        for row in conn.execute("SELECT name FROM lake.test.items").fetchall()
    }
    assert names == {"x", "y", "z"}


# ---------------------------------------------------------------------------
# update_cursor
# ---------------------------------------------------------------------------

def test_update_cursor(conn):
    update_cursor(conn, "my_dataset", 42)

    row = conn.execute(
        "SELECT dataset_name, row_count FROM lake._pipeline_state"
        " WHERE dataset_name = 'my_dataset'"
    ).fetchone()
    assert row is not None
    assert row[0] == "my_dataset"
    assert row[1] == 42


def test_update_cursor_upserts(conn):
    update_cursor(conn, "my_dataset", 10)
    update_cursor(conn, "my_dataset", 99)

    rows = conn.execute(
        "SELECT row_count FROM lake._pipeline_state WHERE dataset_name = 'my_dataset'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 99
