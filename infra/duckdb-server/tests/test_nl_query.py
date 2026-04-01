import importlib.util
import os
import sys

import pytest

# Load nl_query directly to avoid tools/__init__.py pulling in fastmcp
_nl_query_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tools",
    "nl_query.py",
)
_spec = importlib.util.spec_from_file_location("nl_query", _nl_query_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
build_schema_context = _mod.build_schema_context


def test_build_schema_context_formats_correctly():
    table_info = {
        "schema": "housing",
        "table": "hpd_violations",
        "comment": "HPD housing violations filed against NYC buildings",
        "columns": [
            {"name": "boroid", "type": "VARCHAR", "comment": "Borough code"},
            {"name": "block", "type": "VARCHAR", "comment": None},
            {"name": "violationid", "type": "BIGINT", "comment": "Unique violation ID"},
        ],
    }
    result = build_schema_context([table_info])
    assert "lake.housing.hpd_violations" in result
    assert "HPD housing violations" in result
    assert "boroid" in result
    assert "VARCHAR" in result
    assert "Borough code" in result


def test_build_schema_context_handles_no_comment():
    table_info = {
        "schema": "recreation",
        "table": "parks",
        "comment": None,
        "columns": [{"name": "park_name", "type": "VARCHAR", "comment": None}],
    }
    result = build_schema_context([table_info])
    assert "lake.recreation.parks" in result
    assert "park_name" in result


def test_build_schema_context_multiple_tables():
    tables = [
        {"schema": "housing", "table": "hpd_violations", "comment": "Violations",
         "columns": [{"name": "id", "type": "BIGINT", "comment": None}]},
        {"schema": "housing", "table": "hpd_complaints", "comment": "Complaints",
         "columns": [{"name": "id", "type": "BIGINT", "comment": None}]},
    ]
    result = build_schema_context(tables)
    assert "hpd_violations" in result
    assert "hpd_complaints" in result


def test_format_examples_returns_pairs():
    from shared.nl_examples import format_examples, NL_SQL_EXAMPLES
    result = format_examples(3)
    assert "Question:" in result
    assert "SQL:" in result
    assert result.count("Question:") == 3
    assert len(NL_SQL_EXAMPLES) >= 8
