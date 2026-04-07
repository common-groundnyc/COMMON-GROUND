"""Tests for explore query builders."""
import pytest
from shared.explore_queries import (
    build_zip_overview_query,
    build_zip_search_query,
    build_worst_buildings_query,
)


def test_zip_overview_query_uses_zip_param():
    sql, params = build_zip_overview_query(zip_code="11201", days=365)
    assert "?" in sql
    assert params == ("11201", 365, "11201", 365, "11201", 365, "11201", 365)
    assert "hpd_violations" in sql.lower()
    assert "n311_service_requests" in sql.lower()


def test_zip_search_query_returns_lowercase_match():
    sql, params = build_zip_search_query(prefix="112")
    assert params == ("112%", "%112%")
    assert "modzcta" in sql.lower()
    assert "label" in sql.lower()


def test_worst_buildings_query_limit_20():
    sql, params = build_worst_buildings_query(zip_code="11201", days=365, limit=20)
    assert "LIMIT 20" in sql
    assert params == ("11201", 365)


def test_worst_buildings_query_limit_clamped_to_100():
    sql, _ = build_worst_buildings_query(zip_code="11201", days=365, limit=999)
    assert "LIMIT 100" in sql


def test_worst_buildings_query_limit_minimum_1():
    sql, _ = build_worst_buildings_query(zip_code="11201", days=365, limit=0)
    assert "LIMIT 1" in sql
