"""Tests for anomaly detection module."""
import sys
import os
import importlib
import types

import pytest

# Load tools.anomalies directly without triggering tools/__init__.py
# (which imports fastmcp, unavailable in the test environment)
_server_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _server_dir)

# Inject a minimal tools package stub so the submodule import works
_tools_stub = types.ModuleType("tools")
_tools_stub.__path__ = [os.path.join(_server_dir, "tools")]
_tools_stub.__package__ = "tools"
sys.modules.setdefault("tools", _tools_stub)

import importlib.util as _ilu
_spec = _ilu.spec_from_file_location(
    "tools.anomalies",
    os.path.join(_server_dir, "tools", "anomalies.py"),
)
_anomalies_mod = _ilu.module_from_spec(_spec)
sys.modules["tools.anomalies"] = _anomalies_mod
_spec.loader.exec_module(_anomalies_mod)


def test_anomaly_metrics_defined():
    from tools.anomalies import ANOMALY_METRICS
    assert len(ANOMALY_METRICS) >= 5
    for m in ANOMALY_METRICS:
        assert m.name
        assert m.sql_current
        assert m.sql_baseline
        assert m.unit


def test_format_anomalies_empty():
    from tools.anomalies import format_anomalies
    result = format_anomalies([])
    assert "No significant anomalies" in result


def test_format_anomalies_with_data():
    from tools.anomalies import format_anomalies
    anomalies = [
        {
            "metric": "test",
            "headline": "Test metric up 150% in 10001",
            "direction": "up",
            "z_score": 3.2,
            "current": 500,
            "baseline_avg": 200,
            "unit": "complaints",
        }
    ]
    result = format_anomalies(anomalies)
    assert "150%" in result
    assert "10001" in result
    assert "\U0001f4c8" in result


def test_format_anomalies_down():
    from tools.anomalies import format_anomalies
    anomalies = [
        {
            "metric": "test",
            "headline": "Test metric down 60% in BRONX",
            "direction": "down",
            "z_score": -2.5,
            "current": 40,
            "baseline_avg": 100,
            "unit": "violations",
        }
    ]
    result = format_anomalies(anomalies)
    assert "\U0001f4c9" in result
    assert "60%" in result


def test_all_metrics_have_group_by():
    from tools.anomalies import ANOMALY_METRICS
    for m in ANOMALY_METRICS:
        assert m.group_by, f"Metric {m.name} missing group_by"


def test_metric_names_unique():
    from tools.anomalies import ANOMALY_METRICS
    names = [m.name for m in ANOMALY_METRICS]
    assert len(names) == len(set(names)), "Duplicate metric names found"


def test_format_anomalies_count_in_header():
    from tools.anomalies import format_anomalies
    anomalies = [
        {
            "metric": "test",
            "headline": "Noise up 200% in 10001",
            "direction": "up",
            "z_score": 4.0,
            "current": 300,
            "baseline_avg": 100,
            "unit": "complaints",
        },
        {
            "metric": "test2",
            "headline": "Evictions down 50% in 10002",
            "direction": "down",
            "z_score": -2.1,
            "current": 5,
            "baseline_avg": 10,
            "unit": "evictions",
        },
    ]
    result = format_anomalies(anomalies)
    assert "2 detected" in result
