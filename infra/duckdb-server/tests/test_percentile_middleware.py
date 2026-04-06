"""Tests for percentile_middleware.py — written first (TDD), RED before GREEN."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from fastmcp.tools.tool import ToolResult

# Import the module under test (will fail until implemented)
from percentile_middleware import PercentileMiddleware, detect_entity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_result(content: str, structured_content: dict | None = None) -> ToolResult:
    sc = structured_content if structured_content is not None else {}
    return ToolResult(content=content, structured_content=sc)


def make_context(tool_name: str, lifespan: dict | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.message = MagicMock()
    ctx.message.name = tool_name
    lc = lifespan if lifespan is not None else {"percentiles_ready": True, "db": MagicMock()}
    fctx = MagicMock()
    fctx.lifespan_context = lc
    ctx.fastmcp_context = fctx
    return ctx


def result_text(result: ToolResult) -> str:
    if isinstance(result.content, list) and result.content:
        return result.content[0].text
    return str(result.content)


# ---------------------------------------------------------------------------
# TestDetectEntity
# ---------------------------------------------------------------------------

class TestDetectEntity:
    def test_bbl_top_level(self):
        assert detect_entity({"bbl": "1000670001"}, "any_tool") == ("building", "1000670001")

    def test_camis(self):
        assert detect_entity({"camis": "41234567"}, "any_tool") == ("restaurant", "41234567")

    def test_zipcode(self):
        assert detect_entity({"zipcode": "10001"}, "any_tool") == ("zip", "10001")

    def test_precinct_int_converted_to_string(self):
        assert detect_entity({"precinct": 14}, "any_tool") == ("precinct", "14")

    def test_precinct_string_kept_as_string(self):
        assert detect_entity({"precinct": "14"}, "any_tool") == ("precinct", "14")

    def test_owner_id(self):
        assert detect_entity({"owner_id": "owner_abc"}, "any_tool") == ("owner", "owner_abc")

    def test_nested_building_bbl(self):
        sc = {"building": {"bbl": "123"}}
        assert detect_entity(sc, "any_tool") == ("building", "123")

    def test_results_list_owner_id(self):
        sc = {"results": [{"owner_id": "5"}]}
        assert detect_entity(sc, "any_tool") == ("owner", "5")

    def test_none_returns_none_none(self):
        assert detect_entity(None, "any_tool") == (None, None)

    def test_empty_dict_returns_none_none(self):
        assert detect_entity({}, "any_tool") == (None, None)

    def test_rows_only_returns_none_none(self):
        sc = {"rows": [{"col": "val"}]}
        assert detect_entity(sc, "any_tool") == (None, None)

    def test_non_dict_returns_none_none(self):
        assert detect_entity("string", "any_tool") == (None, None)
        assert detect_entity(42, "any_tool") == (None, None)
        assert detect_entity([], "any_tool") == (None, None)

    def test_bbl_takes_priority_over_owner_id(self):
        # bbl is checked first — should return building, not owner
        sc = {"bbl": "1000670001", "owner_id": "owner_abc"}
        assert detect_entity(sc, "any_tool") == ("building", "1000670001")

    def test_results_list_empty_returns_none_none(self):
        sc = {"results": []}
        assert detect_entity(sc, "any_tool") == (None, None)

    def test_results_list_no_owner_id_returns_none_none(self):
        sc = {"results": [{"bbl": "123"}]}
        # rule 7 only checks owner_id in results[0]
        assert detect_entity(sc, "any_tool") == (None, None)


# ---------------------------------------------------------------------------
# TestPercentileMiddleware
# ---------------------------------------------------------------------------

class TestPercentileMiddleware:
    @pytest.mark.asyncio
    async def test_entity_detected_appends_percentile_block(self):
        """When entity detected and percentiles found, text and structured_content updated."""
        sc = {"bbl": "1000670001", "address": "123 Main St"}
        original = make_result("Building profile.", structured_content=sc)

        async def call_next(ctx):
            return original

        mock_db = MagicMock()
        ctx = make_context("building_profile", {"percentiles_ready": True, "db": mock_db})

        with patch("percentile_middleware.lookup_percentiles") as mock_lookup, \
             patch("percentile_middleware.get_population_count") as mock_pop, \
             patch("percentile_middleware.format_percentile_block") as mock_fmt:
            mock_lookup.return_value = {"violation_pctile": 0.94}
            mock_pop.return_value = 50000
            mock_fmt.return_value = "PERCENTILE RANKING (among 50,000 NYC buildings)\nViolations: 94th"

            middleware = PercentileMiddleware()
            result = await middleware.on_call_tool(ctx, call_next)

        text = result_text(result)
        assert "PERCENTILE RANKING" in text
        assert result.structured_content is not None
        assert "percentiles" in result.structured_content
        assert "percentile_population" in result.structured_content
        assert result.structured_content["percentile_population"] == 50000

    @pytest.mark.asyncio
    async def test_skip_tool_passes_through_unchanged(self):
        """Tools in _SKIP_TOOLS are returned without modification."""
        sc = {"bbl": "1000670001"}
        original = make_result("Schema list.", structured_content=sc)

        async def call_next(ctx):
            return original

        middleware = PercentileMiddleware()

        skip_tools = [
            "list_schemas", "list_tables", "describe_table", "data_catalog",
            "search_tools", "call_tool", "sql_query", "sql_admin",
            "suggest_explorations", "export_csv", "text_search",
        ]
        for tool_name in skip_tools:
            ctx = make_context(tool_name)
            result = await middleware.on_call_tool(ctx, call_next)
            assert result is original, f"Tool {tool_name!r} should pass through unchanged"

    @pytest.mark.asyncio
    async def test_percentiles_ready_false_passes_through(self):
        """When percentiles_ready is False, return result unchanged."""
        sc = {"bbl": "1000670001"}
        original = make_result("Building profile.", structured_content=sc)

        async def call_next(ctx):
            return original

        ctx = make_context("building_profile", {"percentiles_ready": False, "db": MagicMock()})
        middleware = PercentileMiddleware()
        result = await middleware.on_call_tool(ctx, call_next)
        assert result is original

    @pytest.mark.asyncio
    async def test_percentiles_ready_missing_passes_through(self):
        """When percentiles_ready key absent, treat as not ready."""
        sc = {"bbl": "1000670001"}
        original = make_result("Building profile.", structured_content=sc)

        async def call_next(ctx):
            return original

        ctx = make_context("building_profile", {"db": MagicMock()})
        middleware = PercentileMiddleware()
        result = await middleware.on_call_tool(ctx, call_next)
        assert result is original

    @pytest.mark.asyncio
    async def test_lookup_empty_passes_through_unchanged(self):
        """When lookup_percentiles returns empty dict, return original result."""
        sc = {"bbl": "1000670001"}
        original = make_result("Building profile.", structured_content=sc)

        async def call_next(ctx):
            return original

        ctx = make_context("building_profile", {"percentiles_ready": True, "db": MagicMock()})

        with patch("percentile_middleware.lookup_percentiles") as mock_lookup:
            mock_lookup.return_value = {}
            middleware = PercentileMiddleware()
            result = await middleware.on_call_tool(ctx, call_next)

        assert result is original

    @pytest.mark.asyncio
    async def test_tool_exception_propagates(self):
        """Exceptions raised by the underlying tool are NOT swallowed."""
        async def call_next(ctx):
            raise RuntimeError("DuckDB exploded")

        ctx = make_context("building_profile", {"percentiles_ready": True, "db": MagicMock()})
        middleware = PercentileMiddleware()

        with pytest.raises(RuntimeError, match="DuckDB exploded"):
            await middleware.on_call_tool(ctx, call_next)

    @pytest.mark.asyncio
    async def test_disclaimer_appended_to_text(self):
        """DISCLAIMER text is appended after the percentile block."""
        sc = {"bbl": "1000670001"}
        original = make_result("Building profile.", structured_content=sc)

        async def call_next(ctx):
            return original

        ctx = make_context("building_profile", {"percentiles_ready": True, "db": MagicMock()})

        with patch("percentile_middleware.lookup_percentiles") as mock_lookup, \
             patch("percentile_middleware.get_population_count") as mock_pop, \
             patch("percentile_middleware.format_percentile_block") as mock_fmt:
            mock_lookup.return_value = {"violation_pctile": 0.94}
            mock_pop.return_value = 50000
            mock_fmt.return_value = "PERCENTILE RANKING\nViolations: 94th"

            middleware = PercentileMiddleware()
            result = await middleware.on_call_tool(ctx, call_next)

        text = result_text(result)
        assert "Percentiles rank this entity" in text or "percentile" in text.lower()

    @pytest.mark.asyncio
    async def test_returns_new_tool_result_not_mutation(self):
        """Middleware returns a new ToolResult, doesn't mutate original."""
        sc = {"bbl": "1000670001"}
        original = make_result("Building profile.", structured_content=sc)
        original_text = result_text(original)

        async def call_next(ctx):
            return original

        ctx = make_context("building_profile", {"percentiles_ready": True, "db": MagicMock()})

        with patch("percentile_middleware.lookup_percentiles") as mock_lookup, \
             patch("percentile_middleware.get_population_count") as mock_pop, \
             patch("percentile_middleware.format_percentile_block") as mock_fmt:
            mock_lookup.return_value = {"violation_pctile": 0.94}
            mock_pop.return_value = 50000
            mock_fmt.return_value = "PERCENTILE RANKING\nViolations: 94th"

            middleware = PercentileMiddleware()
            result = await middleware.on_call_tool(ctx, call_next)

        # Must be a different object
        assert result is not original
        # Original text must be unchanged
        assert result_text(original) == original_text

    @pytest.mark.asyncio
    async def test_no_entity_detected_passes_through(self):
        """When no entity is detectable from structured_content, return original."""
        sc = {"rows": [{"col": "val"}]}
        original = make_result("Some result.", structured_content=sc)

        async def call_next(ctx):
            return original

        ctx = make_context("building_profile", {"percentiles_ready": True, "db": MagicMock()})
        middleware = PercentileMiddleware()
        result = await middleware.on_call_tool(ctx, call_next)
        assert result is original

    @pytest.mark.asyncio
    async def test_internal_exception_returns_original(self):
        """If percentile lookup raises unexpectedly, return original result (never break tools)."""
        sc = {"bbl": "1000670001"}
        original = make_result("Building profile.", structured_content=sc)

        async def call_next(ctx):
            return original

        ctx = make_context("building_profile", {"percentiles_ready": True, "db": MagicMock()})

        with patch("percentile_middleware.lookup_percentiles") as mock_lookup:
            mock_lookup.side_effect = Exception("unexpected crash in percentile lookup")
            middleware = PercentileMiddleware()
            result = await middleware.on_call_tool(ctx, call_next)

        assert result is original

    @pytest.mark.asyncio
    async def test_structured_content_preserved_from_original(self):
        """New result retains all original structured_content fields plus percentile additions."""
        sc = {"bbl": "1000670001", "address": "123 Main St", "units": 24}
        original = make_result("Building profile.", structured_content=sc)

        async def call_next(ctx):
            return original

        ctx = make_context("building_profile", {"percentiles_ready": True, "db": MagicMock()})

        with patch("percentile_middleware.lookup_percentiles") as mock_lookup, \
             patch("percentile_middleware.get_population_count") as mock_pop, \
             patch("percentile_middleware.format_percentile_block") as mock_fmt:
            mock_lookup.return_value = {"violation_pctile": 0.94}
            mock_pop.return_value = 50000
            mock_fmt.return_value = "PERCENTILE RANKING\nViolations: 94th"

            middleware = PercentileMiddleware()
            result = await middleware.on_call_tool(ctx, call_next)

        assert result.structured_content["bbl"] == "1000670001"
        assert result.structured_content["address"] == "123 Main St"
        assert result.structured_content["units"] == 24
        assert "percentiles" in result.structured_content
