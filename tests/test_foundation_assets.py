"""Tests for foundation asset extension functions — runs against in-memory DuckDB."""
import duckdb
import pytest


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    c.execute("INSTALL h3 FROM community; LOAD h3")
    c.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs")
    c.execute("INSTALL hashfuncs FROM community; LOAD hashfuncs")
    yield c
    c.close()


def test_h3_latlng_to_cell(conn):
    result = conn.execute(
        "SELECT h3_latlng_to_cell(40.7128, -74.0060, 9)::VARCHAR"
    ).fetchone()[0]
    assert len(result) > 0
    # H3 index is a 64-bit integer; verify it's a valid numeric string
    int(result)


def test_h3_kring(conn):
    result = conn.execute("""
        SELECT UNNEST(h3_grid_disk(h3_latlng_to_cell(40.7128, -74.0060, 9), 1))
    """).fetchall()
    assert len(result) == 7


def test_double_metaphone(conn):
    result = conn.execute("SELECT double_metaphone('SMITH')").fetchone()[0]
    result2 = conn.execute("SELECT double_metaphone('SMYTH')").fetchone()[0]
    assert result == result2


def test_murmurhash(conn):
    r1 = conn.execute("SELECT murmurhash3_32('test_string')").fetchone()[0]
    r2 = conn.execute("SELECT murmurhash3_32('test_string')").fetchone()[0]
    assert r1 == r2
    r3 = conn.execute("SELECT murmurhash3_32('different')").fetchone()[0]
    assert r1 != r3
