"""Tests for explore REST route handlers."""
from unittest.mock import patch

from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.routing import Route

from routes.explore import (
    neighborhood_endpoint,
    zips_search_endpoint,
    worst_buildings_endpoint,
)


def make_app() -> Starlette:
    return Starlette(routes=[
        Route("/api/neighborhood/{zip_code}", neighborhood_endpoint),
        Route("/api/zips/search", zips_search_endpoint),
        Route("/api/buildings/worst", worst_buildings_endpoint),
    ])


@patch("routes.explore.execute")
def test_neighborhood_returns_stat_card_payload(mock_execute):
    mock_execute.return_value = [(1847, 4210, 892, 78.0)]
    client = TestClient(make_app())

    response = client.get("/api/neighborhood/11201?days=365")

    assert response.status_code == 200
    body = response.json()
    assert body["zip"] == "11201"
    assert body["days"] == 365
    assert body["stats"]["violations_count"] == 1847
    assert body["stats"]["complaints_count"] == 4210
    assert body["stats"]["crimes_count"] == 892
    assert body["stats"]["restaurants_a_pct"] == 78.0
    assert body["sources"] == ["HPD", "311", "NYPD", "DOHMH"]


def test_neighborhood_rejects_invalid_zip():
    client = TestClient(make_app())
    response = client.get("/api/neighborhood/abc")
    assert response.status_code == 400
    assert "invalid zip" in response.json()["error"].lower()


def test_neighborhood_clamps_days_to_3650():
    client = TestClient(make_app())
    with patch("routes.explore.execute") as mock_execute:
        mock_execute.return_value = [(0, 0, 0, 0.0)]
        response = client.get("/api/neighborhood/11201?days=99999")
        assert response.status_code == 200
        assert response.json()["days"] == 3650


@patch("routes.explore.execute")
def test_zips_search_returns_matches(mock_execute):
    mock_execute.return_value = [
        ("11201", "Brooklyn Heights / DUMBO", "Brooklyn"),
        ("11206", "Bushwick", "Brooklyn"),
    ]
    client = TestClient(make_app())
    response = client.get("/api/zips/search?q=112")
    assert response.status_code == 200
    body = response.json()
    assert len(body["results"]) == 2
    assert body["results"][0]["zip"] == "11201"
    assert body["results"][0]["label"] == "Brooklyn Heights / DUMBO"


def test_zips_search_requires_query():
    client = TestClient(make_app())
    response = client.get("/api/zips/search")
    assert response.status_code == 400


@patch("routes.explore.execute")
def test_worst_buildings_returns_ranked_list(mock_execute):
    from datetime import date
    mock_execute.return_value = [
        ("3037230001", "305 Linden Blvd", 47, 12, date(2026, 3, 30)),
        ("3014200001", "142 Adams St", 31, 5, date(2026, 3, 28)),
    ]
    client = TestClient(make_app())
    response = client.get("/api/buildings/worst?zip=11201&days=365&limit=10")

    assert response.status_code == 200
    body = response.json()
    assert body["zip"] == "11201"
    assert len(body["buildings"]) == 2
    assert body["buildings"][0]["bbl"] == "3037230001"
    assert body["buildings"][0]["violation_count"] == 47
    assert body["buildings"][0]["class_c_count"] == 12
    assert body["sources"] == ["HPD"]
