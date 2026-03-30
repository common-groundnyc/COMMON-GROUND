"""Tests for generate_comments.py"""
import json
from unittest.mock import MagicMock, patch

import duckdb
import pytest
import yaml

from generate_comments import (
    build_manifest_entry,
    build_rewrite_prompt,
    fetch_socrata_metadata,
    parse_rewrite_response,
    profile_table_columns,
)


# ---------------------------------------------------------------------------
# fetch_socrata_metadata
# ---------------------------------------------------------------------------

class TestFetchSocrataMetadata:
    def test_returns_name_and_description(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "name": "HPD Complaints",
            "description": "Complaints received by HPD",
            "category": "Housing",
            "tags": ["housing", "complaints"],
            "rowsUpdatedAt": 1700000000,
            "columns": [
                {
                    "fieldName": "complaint_id",
                    "name": "Complaint ID",
                    "description": "Unique identifier",
                    "dataTypeName": "number",
                },
                {
                    "fieldName": "status",
                    "name": "Status",
                    "description": "Current status",
                    "dataTypeName": "text",
                },
            ],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("generate_comments.httpx.get", return_value=mock_response) as mock_get:
            result = fetch_socrata_metadata("data.cityofnewyork.us", "ygpa-z7cr")

        assert result["name"] == "HPD Complaints"
        assert result["description"] == "Complaints received by HPD"
        assert result["category"] == "Housing"
        assert len(result["columns"]) == 2
        assert result["columns"][0]["field_name"] == "complaint_id"
        assert result["columns"][1]["description"] == "Current status"
        mock_get.assert_called_once()

    def test_handles_missing_description(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "name": "Some Dataset",
            "columns": [],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("generate_comments.httpx.get", return_value=mock_response):
            result = fetch_socrata_metadata("data.cityofnewyork.us", "xxxx-xxxx")

        assert result["description"] == ""
        assert result["columns"] == []
        assert result["tags"] == []


# ---------------------------------------------------------------------------
# profile_table_columns
# ---------------------------------------------------------------------------

class TestProfileColumns:
    def test_returns_stats(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA test_schema")
        conn.execute(
            "CREATE TABLE test_schema.test_table ("
            "  id INTEGER, "
            "  status VARCHAR, "
            "  score DOUBLE"
            ")"
        )
        conn.execute(
            "INSERT INTO test_schema.test_table VALUES "
            "(1, 'OPEN', 10.5), "
            "(2, 'CLOSED', 20.0), "
            "(3, 'OPEN', NULL), "
            "(4, 'PENDING', 15.0), "
            "(5, NULL, 25.0)"
        )

        profiles = profile_table_columns(conn, "memory", "test_schema", "test_table")

        assert len(profiles) == 3

        # Check id column
        id_col = next(p for p in profiles if p["column_name"] == "id")
        assert id_col["cardinality"] == 5
        assert id_col["null_pct"] == 0.0
        assert id_col["row_count"] == 5

        # Check status column — low cardinality, should have top_values
        status_col = next(p for p in profiles if p["column_name"] == "status")
        assert status_col["cardinality"] == 3
        assert status_col["null_pct"] == 20.0
        assert len(status_col["top_values"]) > 0
        top_vals = [tv["value"] for tv in status_col["top_values"]]
        assert "OPEN" in top_vals

        # Check score column — has NULLs
        score_col = next(p for p in profiles if p["column_name"] == "score")
        assert score_col["null_pct"] == 20.0

        conn.close()

    def test_skips_dlt_columns(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA s")
        conn.execute(
            "CREATE TABLE s.t (id INTEGER, _dlt_load_id VARCHAR, _dlt_id VARCHAR)"
        )
        conn.execute("INSERT INTO s.t VALUES (1, 'abc', 'def')")

        profiles = profile_table_columns(conn, "memory", "s", "t")

        col_names = [p["column_name"] for p in profiles]
        assert "id" in col_names
        assert "_dlt_load_id" not in col_names
        assert "_dlt_id" not in col_names

        conn.close()


# ---------------------------------------------------------------------------
# build_rewrite_prompt
# ---------------------------------------------------------------------------

class TestBuildRewritePrompt:
    def test_includes_all_context(self):
        socrata_meta = {
            "name": "HPD Complaints",
            "description": "Housing complaints data",
            "category": "Housing",
            "tags": ["housing", "hpd"],
            "columns": [
                {
                    "field_name": "complaint_id",
                    "name": "Complaint ID",
                    "description": "Unique complaint identifier",
                    "data_type": "number",
                },
            ],
        }
        column_profile = [
            {
                "column_name": "complaint_id",
                "data_type": "INTEGER",
                "row_count": 1000,
                "cardinality": 1000,
                "null_pct": 0.0,
                "top_values": [],
            },
            {
                "column_name": "status",
                "data_type": "VARCHAR",
                "row_count": 1000,
                "cardinality": 3,
                "null_pct": 5.0,
                "top_values": [
                    {"value": "OPEN", "count": 500},
                    {"value": "CLOSED", "count": 400},
                ],
            },
        ]

        prompt = build_rewrite_prompt("housing", "hpd_complaints", socrata_meta, column_profile)

        assert "Schema: housing" in prompt
        assert "Table: hpd_complaints" in prompt
        assert "HPD Complaints" in prompt
        assert "Housing complaints data" in prompt
        assert "housing, hpd" in prompt
        assert "complaint_id" in prompt
        assert "status" in prompt
        assert "OPEN" in prompt
        assert "cardinality=" in prompt
        assert "null%=" in prompt


# ---------------------------------------------------------------------------
# parse_rewrite_response
# ---------------------------------------------------------------------------

class TestParseRewriteResponse:
    def test_extracts_yaml(self):
        text = (
            'table_comment: "One row per HPD complaint. Use for housing conditions."\n'
            "columns:\n"
            '  complaint_id: "Unique complaint identifier, joins to hpd_violations"\n'
            '  status: "Complaint status: OPEN, CLOSED, PENDING"'
        )

        result = parse_rewrite_response(text)

        assert "One row per HPD complaint" in result["table_comment"]
        assert "complaint_id" in result["columns"]
        assert "status" in result["columns"]

    def test_handles_markdown_fences(self):
        text = (
            "```yaml\n"
            'table_comment: "Test comment"\n'
            "columns:\n"
            '  col1: "Col one"\n'
            "```"
        )

        result = parse_rewrite_response(text)
        assert result["table_comment"] == "Test comment"
        assert result["columns"]["col1"] == "Col one"

    def test_handles_invalid_yaml(self):
        result = parse_rewrite_response("not valid yaml: [[[")
        # Should not crash — returns defaults or parsed result
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# build_manifest_entry
# ---------------------------------------------------------------------------

class TestGenerateManifestEntry:
    def test_has_all_required_fields(self):
        socrata_meta = {
            "name": "HPD Complaints",
            "description": "Housing complaints",
        }
        rewritten = {
            "table_comment": "One row per complaint.",
            "columns": {"status": "Complaint status: OPEN, CLOSED"},
        }

        entry = build_manifest_entry("housing", "hpd_complaints", socrata_meta, rewritten)

        assert entry["schema"] == "housing"
        assert entry["table"] == "hpd_complaints"
        assert entry["socrata_name"] == "HPD Complaints"
        assert entry["socrata_description"] == "Housing complaints"
        assert entry["table_comment"] == "One row per complaint."
        assert "status" in entry["columns"]
