"""Tests for the new PAD-based address resolution functions."""

import sys
import os
import importlib
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

# Import building module directly without going through tools/__init__,
# which would try to import fastmcp (not available in test environment).
_spec = importlib.util.spec_from_file_location(
    "building",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "building.py"),
)
_mod = importlib.util.module_from_spec(_spec)

# Stub out unavailable dependencies before exec
for _dep in ("fastmcp", "fastmcp.exceptions", "fastmcp.tools", "fastmcp.tools.tool"):
    if _dep not in sys.modules:
        sys.modules[_dep] = types.ModuleType(_dep)

_fastmcp_exc = sys.modules["fastmcp.exceptions"]
if not hasattr(_fastmcp_exc, "ToolError"):
    _fastmcp_exc.ToolError = Exception  # type: ignore

_fastmcp = sys.modules["fastmcp"]
if not hasattr(_fastmcp, "Context"):
    _fastmcp.Context = object  # type: ignore

_fastmcp_tool = sys.modules["fastmcp.tools.tool"]
if not hasattr(_fastmcp_tool, "ToolResult"):
    _fastmcp_tool.ToolResult = object  # type: ignore

for _dep in ("pydantic",):
    if _dep not in sys.modules:
        try:
            import pydantic  # noqa: F401
        except ImportError:
            sys.modules[_dep] = types.ModuleType(_dep)

# Stub shared.* modules
for _dep in ("shared", "shared.db", "shared.formatting", "shared.types"):
    if _dep not in sys.modules:
        sys.modules[_dep] = types.ModuleType(_dep)

_shared_types = sys.modules["shared.types"]
if not hasattr(_shared_types, "MAX_LLM_ROWS"):
    _shared_types.MAX_LLM_ROWS = 500  # type: ignore
if not hasattr(_shared_types, "BBL_PATTERN"):
    _shared_types.BBL_PATTERN = r"^\d{10}$"  # type: ignore

_shared_db = sys.modules["shared.db"]
for _name in ("execute", "safe_query", "fill_placeholders"):
    if not hasattr(_shared_db, _name):
        setattr(_shared_db, _name, None)  # type: ignore

_shared_fmt = sys.modules["shared.formatting"]
for _name in ("make_result", "format_text_table"):
    if not hasattr(_shared_fmt, _name):
        setattr(_shared_fmt, _name, None)  # type: ignore

_spec.loader.exec_module(_mod)

_normalize_address_for_pad = _mod._normalize_address_for_pad
_extract_borough = _mod._extract_borough
_extract_house_number = _mod._extract_house_number


class TestNormalizeAddressForPad:
    def test_expands_abbreviations(self):
        assert "EAST" in _normalize_address_for_pad("200 E 10th St")

    def test_strips_ordinal_suffix(self):
        result = _normalize_address_for_pad("200 E 10th St")
        assert "10TH" not in result
        assert "10 " in result or result.endswith("10")

    def test_strips_borough(self):
        result = _normalize_address_for_pad("200 E 10th St, Manhattan")
        assert "MANHATTAN" not in result

    def test_strips_state_zip(self):
        result = _normalize_address_for_pad("200 E 10th St, NY 10003")
        assert "NY" not in result
        assert "10003" not in result

    def test_strips_apartment(self):
        result = _normalize_address_for_pad("200 E 10th St APT 4B")
        assert "APT" not in result
        assert "4B" not in result

    def test_expands_spelled_ordinals(self):
        result = _normalize_address_for_pad("200 Fifth Ave")
        assert "5 AVENUE" in result

    def test_preserves_numbers(self):
        result = _normalize_address_for_pad("350 5th Ave")
        assert result.startswith("350")


class TestExtractBorough:
    def test_manhattan(self):
        assert _extract_borough("200 E 10th St, Manhattan") == "1"

    def test_brooklyn_abbreviation(self):
        assert _extract_borough("100 Atlantic Ave, BK") == "3"

    def test_staten_island_nickname(self):
        assert _extract_borough("123 Victory Blvd, Shaolin") == "5"

    def test_no_borough(self):
        assert _extract_borough("200 E 10th St") is None

    def test_queens(self):
        assert _extract_borough("100 Queens Blvd, Queens") == "4"


class TestExtractHouseNumber:
    def test_simple(self):
        assert _extract_house_number("200 E 10th St") == 200

    def test_hyphenated_queens(self):
        assert _extract_house_number("45-17 21st St, Queens") == 45

    def test_no_number(self):
        assert _extract_house_number("Broadway, Manhattan") is None

    def test_leading_zeros(self):
        assert _extract_house_number("001 Main St") == 1
