"""Tool-use eval harness for the Common Ground MCP server.

Run with:
    cd infra/duckdb-server
    ANTHROPIC_API_KEY=sk-ant-... uv run --with anthropic --with mcp --with pydantic \\
        python -m evals.run_eval --mode=local

See docs/runbooks/eval-harness.md for the full workflow.
"""
