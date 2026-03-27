"""Shared constants for the Common Ground MCP server middleware stack."""

MIDDLEWARE_SKIP_TOOLS = frozenset({
    "list_schemas",
    "list_tables",
    "describe_table",
    "data_catalog",
    "search_tools",
    "call_tool",
    "sql_admin",
    "suggest_explorations",
    "graph_health",
    "lake_health",
    "name_variants",
})
