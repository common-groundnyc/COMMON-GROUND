"""Tests for _political_core federal-data branches (LittleSis + USAspending)."""
from __future__ import annotations

import duckdb
import pytest


@pytest.fixture
def lake_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with the federal tables _political_core needs."""
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS lake")
    conn.execute("CREATE SCHEMA lake.federal")
    conn.execute("CREATE SCHEMA lake.city_government")
    conn.execute("CREATE SCHEMA lake.financial")

    conn.execute("""
        CREATE TABLE lake.federal.nys_campaign_finance(
            filer_name VARCHAR, flng_ent_first_name VARCHAR, flng_ent_last_name VARCHAR,
            org_amt VARCHAR, sched_date VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE lake.federal.fec_contributions(
            cmte_id VARCHAR, name VARCHAR, contributor_name VARCHAR,
            transaction_amt VARCHAR, transaction_dt VARCHAR,
            contribution_receipt_amount VARCHAR, contribution_receipt_date VARCHAR,
            committee_id VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE lake.city_government.campaign_contributions(
            recipname VARCHAR, occupation VARCHAR, empname VARCHAR, empstrno VARCHAR,
            name VARCHAR, amnt VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE lake.city_government.contract_awards(
            vendor_name VARCHAR, contract_amount VARCHAR, agency_name VARCHAR,
            short_title VARCHAR, start_date VARCHAR, end_date VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE lake.financial.nys_procurement_state(
            vendor_name VARCHAR, contract_amount VARCHAR,
            contracting_agency VARCHAR, contract_description VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE lake.federal.usaspending_contracts(
            award_id VARCHAR, recipient_name VARCHAR, award_amount DOUBLE,
            total_outlays DOUBLE, start_date VARCHAR, end_date VARCHAR,
            awarding_agency VARCHAR, awarding_sub_agency VARCHAR,
            award_type VARCHAR, recipient_id VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE lake.federal.usaspending_grants(
            award_id VARCHAR, recipient_name VARCHAR, award_amount DOUBLE,
            total_outlays DOUBLE, start_date VARCHAR, end_date VARCHAR,
            awarding_agency VARCHAR, awarding_sub_agency VARCHAR,
            award_type VARCHAR, recipient_id VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE lake.federal.littlesis_entities(
            id BIGINT, name VARCHAR, blurb VARCHAR, primary_ext VARCHAR,
            types VARCHAR, aliases VARCHAR, updated_at VARCHAR,
            start_date VARCHAR, end_date VARCHAR, net_worth DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE lake.federal.littlesis_relationships(
            id BIGINT, entity1_id BIGINT, entity2_id BIGINT,
            category_id INTEGER, category VARCHAR,
            description1 VARCHAR, description2 VARCHAR,
            amount DOUBLE, currency VARCHAR,
            start_date VARCHAR, end_date VARCHAR,
            is_current BOOLEAN, updated_at VARCHAR
        )
    """)
    return conn


def test_fixture_has_all_required_tables(lake_conn):
    """Sanity: every table _political_core touches must exist in the fixture."""
    rows = lake_conn.execute(
        "SELECT table_schema || '.' || table_name "
        "FROM information_schema.tables "
        "WHERE table_catalog='lake' ORDER BY 1"
    ).fetchall()
    names = {r[0] for r in rows}
    assert "federal.nys_campaign_finance" in names
    assert "federal.fec_contributions" in names
    assert "federal.usaspending_contracts" in names
    assert "federal.usaspending_grants" in names
    assert "federal.littlesis_entities" in names
    assert "federal.littlesis_relationships" in names
    assert "city_government.campaign_contributions" in names
    assert "city_government.contract_awards" in names
    assert "financial.nys_procurement_state" in names


def test_usaspending_sql_aggregates_contracts_and_grants(lake_conn):
    """USAspending SQL must UNION contracts+grants and group by recipient + agency."""
    from tools.network import MONEY_USASPENDING_SQL

    lake_conn.execute("""
        INSERT INTO lake.federal.usaspending_contracts VALUES
        ('A1','ACME CORP',100000.0,90000.0,'2023-01-01','2024-01-01','DOD','Navy','A','r1'),
        ('A2','ACME CORP',50000.0,50000.0,'2024-01-01','2025-01-01','DOD','Navy','A','r1'),
        ('A3','OTHER LLC',9999.0,9999.0,'2024-01-01','2025-01-01','HHS','CDC','B','r2')
    """)
    lake_conn.execute("""
        INSERT INTO lake.federal.usaspending_grants VALUES
        ('G1','ACME CORP',25000.0,25000.0,'2023-06-01','2024-06-01','NSF','Bio','02','r1')
    """)

    rows = lake_conn.execute(MONEY_USASPENDING_SQL, ["ACME", "ACME"]).fetchall()
    acme_rows = [r for r in rows if r[0] == "ACME CORP"]
    assert len(acme_rows) == 2
    total = sum(r[3] for r in acme_rows)
    awards = sum(r[4] for r in acme_rows)
    assert total == 175000.0
    assert awards == 3
