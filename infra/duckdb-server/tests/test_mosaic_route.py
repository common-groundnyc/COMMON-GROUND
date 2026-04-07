"""Tests for the /mosaic/query endpoint."""
from unittest.mock import patch

import pytest
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from routes.mosaic_route import mosaic_query_endpoint, ALLOWED_SCHEMAS, is_query_allowed


def test_query_in_allowed_schema_passes():
    sql = "SELECT COUNT(*) FROM lake.housing.hpd_violations WHERE zip = '11201'"
    assert is_query_allowed(sql) is True


def test_query_outside_allowed_schemas_rejected():
    sql = "SELECT * FROM lake.public._subscriptions"
    assert is_query_allowed(sql) is False


def test_query_referencing_pipeline_state_rejected():
    sql = "SELECT * FROM lake.housing._pipeline_state"
    assert is_query_allowed(sql) is False


def test_destructive_keywords_rejected():
    for sql in [
        "DROP TABLE lake.housing.hpd_violations",
        "DELETE FROM lake.housing.hpd_violations",
        "UPDATE lake.housing.hpd_violations SET zip = '11201'",
        "INSERT INTO lake.housing.hpd_violations VALUES (1)",
        "ATTACH 'foo' AS bar",
    ]:
        assert is_query_allowed(sql) is False, f"should reject: {sql}"


def test_query_too_long_rejected():
    sql = "SELECT 1" + (" -- pad" * 2000)
    assert is_query_allowed(sql) is False


def make_app() -> Starlette:
    return Starlette(routes=[Route("/mosaic/query", mosaic_query_endpoint, methods=["POST"])])


@patch("routes.mosaic_route._run_mosaic_query")
def test_mosaic_endpoint_runs_allowed_query(mock_run):
    mock_run.return_value = {"data": [{"col0": 42}]}
    client = TestClient(make_app())
    response = client.post(
        "/mosaic/query",
        json={"type": "json", "sql": "SELECT COUNT(*) FROM lake.housing.hpd_violations"},
    )
    assert response.status_code == 200
    assert response.json() == {"data": [{"col0": 42}]}


def test_mosaic_endpoint_rejects_disallowed_query():
    client = TestClient(make_app())
    response = client.post(
        "/mosaic/query",
        json={"type": "json", "sql": "DROP TABLE lake.housing.hpd_violations"},
    )
    assert response.status_code == 403
    assert "rejected" in response.json()["error"].lower()


def test_unqualified_table_reference_rejected():
    assert is_query_allowed("SELECT * FROM housing._pipeline_state") is False
    assert is_query_allowed("SELECT * FROM hpd_violations") is False


def test_whitespace_separated_keywords_rejected():
    assert is_query_allowed("DROP\tTABLE lake.housing.hpd_violations") is False
    assert is_query_allowed("DROP\nTABLE lake.housing.hpd_violations") is False
    assert is_query_allowed("DROP/*comment*/TABLE lake.housing.hpd_violations") is False


def test_multistatement_with_drop_rejected():
    assert is_query_allowed("SELECT 1; DROP TABLE lake.housing.hpd_violations") is False


def test_cte_with_ddl_rejected():
    sql = "WITH x AS (SELECT 1) DELETE FROM lake.housing.hpd_violations"
    assert is_query_allowed(sql) is False


def test_select_column_named_created_at_allowed():
    sql = "SELECT created_at FROM lake.housing.hpd_violations"
    assert is_query_allowed(sql) is True


def test_pragma_rejected():
    assert is_query_allowed("PRAGMA database_list") is False


def test_uppercase_lake_prefix_allowed():
    sql = "SELECT 1 FROM LAKE.HOUSING.HPD_VIOLATIONS"
    assert is_query_allowed(sql) is True


def test_quoted_identifiers_resolved():
    sql = 'SELECT 1 FROM "lake"."housing"."hpd_violations"'
    assert is_query_allowed(sql) is True


def test_quoted_internal_table_rejected():
    sql = 'SELECT 1 FROM "lake"."housing"."_pipeline_state"'
    assert is_query_allowed(sql) is False


def test_real_column_names_returned_in_data():
    """Verify _run_mosaic_query uses parsed column names from the SELECT."""
    sql = "SELECT zip, COUNT(*) AS cnt FROM lake.housing.hpd_violations GROUP BY zip"
    with patch("routes.mosaic_route.execute") as mock_execute:
        mock_execute.return_value = [("11201", 42), ("11206", 31)]
        from routes.mosaic_route import _run_mosaic_query
        result = _run_mosaic_query({"sql": sql})
    assert result == {"data": [{"zip": "11201", "cnt": 42}, {"zip": "11206", "cnt": 31}]}
