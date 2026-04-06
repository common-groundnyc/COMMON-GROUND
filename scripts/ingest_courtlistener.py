"""Ingest CourtListener data directly into DuckLake via SQL.

Bypasses dlt ducklake destination (has catalog naming issues) by
fetching from CourtListener API and writing via DuckDB connection.

Usage: cd ~/Desktop/dagster-pipeline && uv run python scripts/ingest_courtlistener.py
"""
import sys
sys.path.insert(0, "src")

import json
import logging
import os
import tempfile
import time

import httpx
from dagster_pipeline.defs.name_index_asset import _connect_ducklake
from dagster_pipeline.sources.courtlistener import (
    AUTH_TOKEN, BASE_URL, PAGE_SIZE, REQUEST_TIMEOUT, _paginate,
    _make_courts_resource, _make_fjc_resource,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def ingest_endpoint(conn, table_name, endpoint, params=None, flatten_fn=None, max_pages=0):
    """Fetch all rows from a CourtListener endpoint and write to DuckLake."""
    logger.info("=== Ingesting %s from %s ===", table_name, endpoint)
    t0 = time.time()

    rows = []
    for item in _paginate(endpoint, params or {}, max_pages=max_pages):
        row = flatten_fn(item) if flatten_fn else item
        rows.append(row)
        if len(rows) % 1000 == 0:
            logger.info("  %s: %d rows fetched...", table_name, len(rows))

    if not rows:
        logger.warning("  %s: 0 rows — skipping", table_name)
        return 0

    logger.info("  %s: %d rows fetched in %.1fs, writing to lake...", table_name, len(rows), time.time() - t0)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(rows, f)
        tmp_path = f.name

    try:
        full_table = f"lake.federal.{table_name}"
        conn.execute(f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM read_json_auto('{tmp_path}')")
        count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
        logger.info("  %s: wrote %d rows to %s (%.1fs total)", table_name, count, full_table, time.time() - t0)
        return count
    finally:
        os.unlink(tmp_path)


def main():
    t0 = time.time()
    conn = _connect_ducklake()

    # 1. Courts (reference table, ~3355 rows)
    ingest_endpoint(conn, "cl_courts", "courts",
        flatten_fn=lambda item: {
            "id": item.get("id"),
            "pacer_court_id": item.get("pacer_court_id"),
            "short_name": item.get("short_name"),
            "full_name": item.get("full_name"),
            "jurisdiction": item.get("jurisdiction"),
            "citation_string": item.get("citation_string"),
            "in_use": item.get("in_use"),
        })

    # 2. FJC Integrated Database — NYC civil rights cases (defendant contains "city of new york")
    for search_term, label in [("City of New York", "nyc"), ("New York City", "nyc2")]:
        ingest_endpoint(conn, f"cl_fjc_{label}", "fjc-integrated-database",
            params={"defendant__istartswith": search_term, "dataset_source": 10},
            flatten_fn=lambda item: {
                "id": item.get("id"),
                "docket_number": item.get("docket_number"),
                "date_filed": item.get("date_filed"),
                "date_terminated": item.get("date_terminated"),
                "nature_of_suit": item.get("nature_of_suit"),
                "title": item.get("title"),
                "plaintiff": item.get("plaintiff"),
                "defendant": item.get("defendant"),
                "disposition": item.get("disposition"),
                "amount_received": item.get("amount_received"),
                "judgment": item.get("judgment"),
                "pro_se": item.get("pro_se"),
                "class_action": item.get("class_action"),
                "monetary_demand": item.get("monetary_demand"),
                "origin": item.get("origin"),
                "jurisdiction": item.get("jurisdiction"),
            })

    # 3. Judges (people)
    ingest_endpoint(conn, "cl_judges", "people",
        flatten_fn=lambda item: {
            "id": item.get("id"),
            "name_first": item.get("name_first"),
            "name_middle": item.get("name_middle"),
            "name_last": item.get("name_last"),
            "name_suffix": item.get("name_suffix"),
            "gender": item.get("gender"),
            "race": item.get("race"),
            "date_dob": item.get("date_dob"),
            "dob_city": item.get("dob_city"),
            "dob_state": item.get("dob_state"),
            "date_dod": item.get("date_dod"),
            "ftm_total_received": item.get("ftm_total_received"),
            "has_photo": item.get("has_photo"),
        })

    # 4. Positions (judicial appointments)
    ingest_endpoint(conn, "cl_positions", "positions",
        flatten_fn=lambda item: {
            "id": item.get("id"),
            "person": item.get("person"),
            "court": item.get("court"),
            "position_type": item.get("position_type"),
            "job_title": item.get("job_title"),
            "organization_name": item.get("organization_name"),
            "appointer": item.get("appointer"),
            "how_selected": item.get("how_selected"),
            "date_start": item.get("date_start"),
            "date_termination": item.get("date_termination"),
        })

    # 5. Financial disclosures
    ingest_endpoint(conn, "cl_financial_disclosures", "financial-disclosures",
        flatten_fn=lambda item: {
            "id": item.get("id"),
            "person": item.get("person"),
            "year": item.get("year"),
            "report_type": item.get("report_type"),
            "is_amended": item.get("is_amended"),
            "page_count": item.get("page_count"),
        })

    # 6. Investments
    ingest_endpoint(conn, "cl_investments", "investments",
        flatten_fn=lambda item: {
            "id": item.get("id"),
            "financial_disclosure": item.get("financial_disclosure"),
            "description": item.get("description"),
            "gross_value_code": item.get("gross_value_code"),
            "income_during_reporting_period_code": item.get("income_during_reporting_period_code"),
            "transaction_during_reporting_period": item.get("transaction_during_reporting_period"),
            "transaction_date": item.get("transaction_date"),
            "transaction_value_code": item.get("transaction_value_code"),
            "redacted": item.get("redacted"),
        })

    # 7. Debts
    ingest_endpoint(conn, "cl_debts", "debts",
        flatten_fn=lambda item: {
            "id": item.get("id"),
            "financial_disclosure": item.get("financial_disclosure"),
            "creditor_name": item.get("creditor_name"),
            "description": item.get("description"),
            "value_code": item.get("value_code"),
            "redacted": item.get("redacted"),
        })

    # 8. Gifts
    ingest_endpoint(conn, "cl_gifts", "gifts",
        flatten_fn=lambda item: {
            "id": item.get("id"),
            "financial_disclosure": item.get("financial_disclosure"),
            "source": item.get("source"),
            "description": item.get("description"),
            "value": item.get("value"),
            "redacted": item.get("redacted"),
        })

    # 9. NYPD-related RECAP search
    for court_id in ["nysd", "nyed"]:
        ingest_endpoint(conn, f"cl_nypd_cases_{court_id}", "search",
            params={"q": '"city of new york" "police"', "type": "r", "court": court_id},
            max_pages=100,
            flatten_fn=lambda item: {
                "docket_id": item.get("docket_id"),
                "case_name": item.get("caseName"),
                "docket_number": item.get("docketNumber"),
                "court_id": item.get("court_id"),
                "date_filed": item.get("dateFiled"),
                "nature_of_suit": item.get("suitNature"),
                "judge": item.get("judge"),
                "status": item.get("status"),
            })

    conn.close()
    logger.info("=== Done in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    main()
