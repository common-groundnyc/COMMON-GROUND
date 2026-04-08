"""
PercentileMiddleware — injects percentile rankings into MCP tool responses.

Intercepts tool results, detects entity type from structured_content,
looks up percentile rankings, and appends them to the response text
and structured_content. Never breaks a tool call — always returns the
original result on any failure.
"""

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

from percentiles import lookup_percentiles, get_population_count, format_percentile_block
from middleware.budget import extract_budget, truncate_to_budget

DISCLAIMER = (
    "\nNote: Percentiles rank this entity among ALL NYC peers. "
    "Older buildings (pre-1960) and larger complexes may rank higher "
    "due to age and scale, not necessarily negligence. "
    "Consider context (age, size, borough) when interpreting."
)

from constants import MIDDLEWARE_SKIP_TOOLS

_SKIP_TOOLS = MIDDLEWARE_SKIP_TOOLS | {"sql_query", "text_search", "export_data"}


def detect_entity(structured_content, tool_name) -> tuple[str | None, str | None]:
    """Return (entity_type, key) from structured_content, or (None, None)."""
    if not isinstance(structured_content, dict):
        return (None, None)

    sc = structured_content

    # 1. bbl at top level
    bbl = sc.get("bbl")
    if bbl is not None:
        return ("building", bbl)

    # 2. camis
    camis = sc.get("camis")
    if camis is not None:
        return ("restaurant", camis)

    # 3. zipcode
    zipcode = sc.get("zipcode")
    if zipcode is not None:
        return ("zip", zipcode)

    # 4. precinct — convert to string
    precinct = sc.get("precinct")
    if precinct is not None:
        return ("precinct", str(precinct))

    # 5. owner_id
    owner_id = sc.get("owner_id")
    if owner_id is not None:
        return ("owner", owner_id)

    # 6. Nested building.bbl
    building = sc.get("building")
    if isinstance(building, dict):
        nested_bbl = building.get("bbl")
        if nested_bbl is not None:
            return ("building", nested_bbl)

    # 7. results list: check results[0].owner_id
    results = sc.get("results")
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            nested_owner = first.get("owner_id")
            if nested_owner is not None:
                return ("owner", nested_owner)

    return (None, None)


class PercentileMiddleware(Middleware):
    async def on_call_tool(self, context: MiddlewareContext, call_next) -> ToolResult:
        # Strip budget BEFORE call_next so tool validators (additionalProperties=False)
        # don't reject `max_tokens` as an unknown argument.
        budget = self._pop_budget(context)

        result = await call_next(context)

        tool_name = context.message.name
        if tool_name in _SKIP_TOOLS:
            return self._apply_budget(result, budget)

        try:
            lifespan = context.fastmcp_context.lifespan_context
            if not lifespan.get("percentiles_ready"):
                return self._apply_budget(result, budget)

            pool = lifespan.get("pool")
            if not pool:
                return self._apply_budget(result, budget)

            entity_type, key = detect_entity(result.structured_content, tool_name)
            if entity_type is None:
                return self._apply_budget(result, budget)

            percentiles = lookup_percentiles(pool, entity_type, key)
            if not percentiles:
                return self._apply_budget(result, budget)

            population = get_population_count(pool, entity_type)
            pctile_text = format_percentile_block(percentiles, entity_type, population)

            # Build new content text
            original_text = ""
            if isinstance(result.content, list) and result.content:
                original_text = result.content[0].text
            elif isinstance(result.content, str):
                original_text = result.content

            new_text = original_text + "\n\n" + pctile_text + DISCLAIMER

            # Build new structured_content (immutable — new dict)
            new_sc = dict(result.structured_content) if result.structured_content else {}
            new_sc["percentiles"] = percentiles
            new_sc["percentile_population"] = population

            result = ToolResult(
                content=new_text,
                structured_content=new_sc,
                meta=result.meta,
            )
            return self._apply_budget(result, budget)

        except Exception:
            return self._apply_budget(result, budget)

    @staticmethod
    def _pop_budget(context) -> int | None:
        """Extract budget AND strip max_tokens from arguments so tool validators
        with additionalProperties=False don't reject it."""
        args = getattr(context.message, "arguments", None)
        if not isinstance(args, dict):
            return None
        budget = extract_budget(args)
        # Remove from top-level and nested options so the tool never sees it
        args.pop("max_tokens", None)
        nested = args.get("options")
        if isinstance(nested, dict):
            nested.pop("max_tokens", None)
        return budget

    @staticmethod
    def _apply_budget(result, budget: int | None):
        if budget is not None:
            return truncate_to_budget(result, budget)
        return result
