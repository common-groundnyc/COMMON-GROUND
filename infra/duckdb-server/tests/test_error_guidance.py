"""Tests that error messages guide the model toward a working next action."""
import importlib
from unittest.mock import MagicMock

import pytest
from fastmcp.exceptions import ToolError

from shared.validation import validate_sql


def test_ddl_error_points_at_admin_mode():
    """Rejected DDL should tell the model to use query(mode='admin') or query_sql alternatives."""
    with pytest.raises(ToolError) as exc:
        validate_sql("CREATE TABLE foo AS SELECT 1")

    msg = str(exc.value)
    assert "SELECT" in msg
    assert "admin" in msg.lower() or "read-only" in msg.lower()
    assert "describe_table" in msg


def test_ddl_error_explains_what_to_do_instead():
    """The error should be actionable, not just a refusal."""
    with pytest.raises(ToolError) as exc:
        validate_sql("INSERT INTO foo VALUES (1)")

    msg = str(exc.value)
    assert "read-only" in msg.lower()


def test_binder_error_includes_describe_table_hint(monkeypatch):
    """A Binder Error from DuckDB should be wrapped with actionable guidance."""
    _query_mod = importlib.import_module("tools.query")

    def _raise(pool, sql):
        raise Exception(
            'Binder Error: Referenced column "foo_dd" not found in FROM clause!\n'
            'Candidate bindings: "foo_id", "foo_date"'
        )

    monkeypatch.setattr(_query_mod, "execute", _raise)
    monkeypatch.setattr(_query_mod, "validate_sql", lambda sql: None)

    ctx = MagicMock()
    with pytest.raises(ToolError) as exc:
        _query_mod._sql(MagicMock(), "SELECT foo_dd FROM t", "text", ctx)

    msg = str(exc.value)
    assert "Binder Error" in msg
    assert "describe_table" in msg
    assert "foo_id" in msg or "foo_date" in msg  # DuckDB's own candidates preserved
