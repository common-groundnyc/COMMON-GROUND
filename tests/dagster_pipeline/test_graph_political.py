"""Tests for the graph_political multi_asset SQL constants."""
from __future__ import annotations

import duckdb
import pytest


@pytest.fixture
def lake_conn(tmp_path):
    lake_file = tmp_path / "lake.db"
    conn = duckdb.connect(":memory:")
    conn.execute(f"ATTACH '{lake_file}' AS lake")
    conn.execute("CREATE SCHEMA lake.city_government")

    conn.execute("""
        CREATE TABLE lake.city_government.campaign_contributions(
            name VARCHAR, recipname VARCHAR, amnt VARCHAR, date VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO lake.city_government.campaign_contributions VALUES
        ('JOHN DOE','JANE SMITH','500','2024-01-15'),
        ('ACME LLC','JANE SMITH','2500','2024-02-01'),
        ('JOHN DOE','BILL BROWN','100','2024-03-10')
    """)

    conn.execute("""
        CREATE TABLE lake.city_government.contract_awards(
            vendor_name VARCHAR, agency_name VARCHAR, contract_amount VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO lake.city_government.contract_awards VALUES
        ('ACME LLC','Department of Transportation','1000000'),
        ('ACME LLC','Department of Parks','50000'),
        ('OTHER INC','Department of Finance','25000')
    """)

    conn.execute("""
        CREATE TABLE lake.city_government.nys_lobbyist_registration(
            principal_lobbyist_name VARCHAR,
            contractual_client_name VARCHAR,
            government_body VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO lake.city_government.nys_lobbyist_registration VALUES
        ('KASIRER LLC','ACME LLC','NYC Council'),
        ('KASIRER LLC','BIG CLIENT','NYC Mayor')
    """)
    conn.execute("CREATE SCHEMA lake.graphs")
    return conn


def test_pol_entities_sql(lake_conn):
    from dagster_pipeline.defs.graph_assets import POL_ENTITIES_SQL
    lake_conn.execute(POL_ENTITIES_SQL)
    rows = lake_conn.execute(
        "SELECT entity_name, role FROM lake.graphs.pol_entities ORDER BY 1,2"
    ).fetchall()
    names = {r[0] for r in rows}
    roles = {r[1] for r in rows}
    assert "JOHN DOE" in names
    assert "ACME LLC" in names
    assert "JANE SMITH" in names
    assert "BILL BROWN" in names
    assert roles == {"donor", "candidate"}


def test_pol_donations_sql(lake_conn):
    from dagster_pipeline.defs.graph_assets import POL_DONATIONS_SQL
    lake_conn.execute(POL_DONATIONS_SQL)
    rows = lake_conn.execute(
        "SELECT donor, recipient, amount FROM lake.graphs.pol_donations ORDER BY amount DESC"
    ).fetchall()
    assert len(rows) == 3
    assert rows[0] == ("ACME LLC", "JANE SMITH", 2500.0)


def test_pol_contracts_sql_uses_corrected_columns(lake_conn):
    from dagster_pipeline.defs.graph_assets import POL_CONTRACTS_SQL
    lake_conn.execute(POL_CONTRACTS_SQL)
    rows = lake_conn.execute(
        "SELECT vendor, agency, amount FROM lake.graphs.pol_contracts ORDER BY amount DESC"
    ).fetchall()
    assert len(rows) == 3
    assert rows[0] == ("ACME LLC", "Department of Transportation", 1000000.0)


def test_pol_lobbying_sql_uses_corrected_columns(lake_conn):
    from dagster_pipeline.defs.graph_assets import POL_LOBBYING_SQL
    lake_conn.execute(POL_LOBBYING_SQL)
    rows = lake_conn.execute(
        "SELECT lobbyist, client, target FROM lake.graphs.pol_lobbying ORDER BY client"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0] == ("KASIRER LLC", "ACME LLC", "NYC Council")


def test_all_four_constants_importable():
    from dagster_pipeline.defs.graph_assets import (
        POL_ENTITIES_SQL, POL_DONATIONS_SQL, POL_CONTRACTS_SQL, POL_LOBBYING_SQL,
        POL_TABLES, POL_SQL_BY_TABLE, graph_political,
    )
    assert POL_TABLES == ("pol_entities", "pol_donations", "pol_contracts", "pol_lobbying")
    assert len(POL_SQL_BY_TABLE) == 4
    for sql in (POL_ENTITIES_SQL, POL_DONATIONS_SQL, POL_CONTRACTS_SQL, POL_LOBBYING_SQL):
        assert isinstance(sql, str) and "CREATE OR REPLACE TABLE lake.graphs" in sql
