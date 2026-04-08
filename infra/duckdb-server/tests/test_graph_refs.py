"""Regression test that every property graph referenced by tools/ is created in mcp_server.py.

This test prevents the 'phantom graph' bug where a tool file references a property
graph name that was never defined, causing silent failures at runtime.
"""
import re
from pathlib import Path


def _extract_graph_references(tool_files: list[Path]) -> set[str]:
    """Return the set of property graph names referenced in tool .py files.

    Detects references in three DuckPGQ patterns:
    - `GRAPH_TABLE (name ...)`
    - `weakly_connected_component(name, ...)`
    - `local_clustering_coefficient(name, ...)`
    - `pagerank(name, ...)` (for future-proofing)
    """
    patterns = [
        r"GRAPH_TABLE\s*\(\s*([a-z_][a-z0-9_]*)",
        r"weakly_connected_component\s*\(\s*([a-z_][a-z0-9_]*)",
        r"local_clustering_coefficient\s*\(\s*([a-z_][a-z0-9_]*)",
        r"pagerank\s*\(\s*'?([a-z_][a-z0-9_]*)",
    ]
    refs: set[str] = set()
    for f in tool_files:
        txt = f.read_text()
        for pat in patterns:
            for m in re.finditer(pat, txt, re.IGNORECASE):
                refs.add(m.group(1))
    return refs


def _extract_created_graphs(mcp_server_src: str) -> set[str]:
    """Return the set of property graph names created in mcp_server.py."""
    tuple_pattern = re.compile(r'"(nyc_[a-z_]+)"\s*,\s*"""', re.IGNORECASE)
    create_pattern = re.compile(r'CREATE\s+(?:OR\s+REPLACE\s+)?PROPERTY\s+GRAPH\s+(\w+)', re.IGNORECASE)
    created: set[str] = set()
    for m in tuple_pattern.finditer(mcp_server_src):
        created.add(m.group(1))
    for m in create_pattern.finditer(mcp_server_src):
        created.add(m.group(1))
    return created


def test_every_graph_reference_in_tools_has_a_create_in_mcp_server():
    """Every DuckPGQ property graph referenced in tools/*.py must have a
    corresponding CREATE PROPERTY GRAPH statement in mcp_server.py."""
    server_dir = Path(__file__).resolve().parent.parent
    tool_files = list((server_dir / "tools").glob("*.py"))
    mcp_src = (server_dir / "mcp_server.py").read_text()

    referenced = _extract_graph_references(tool_files)
    created = _extract_created_graphs(mcp_src)

    # Known pre-existing phantom graphs tracked in the audit backlog.
    # These are OUT OF SCOPE for the nyc_building_network fix (Plan 1 Task 2)
    # and will be addressed separately. Listed here so this regression test
    # still guards against NEW phantom graphs being introduced.
    known_phantoms_out_of_scope = {
        "nyc_housing",
        "nyc_influence_network",
        "nyc_transaction_network",
    }

    missing = (referenced - created) - known_phantoms_out_of_scope
    assert not missing, (
        f"Tool files reference property graphs that are never created in mcp_server.py:\n"
        f"  Missing: {sorted(missing)}\n"
        f"  Referenced: {sorted(referenced)}\n"
        f"  Created:    {sorted(created)}"
    )
