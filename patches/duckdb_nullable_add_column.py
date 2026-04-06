"""Patch dlt DuckDB client to drop NOT NULL on ALTER TABLE ADD COLUMN.

Bug: DuckLake doesn't support ALTER TABLE ADD COLUMN with constraints.
dlt generates NOT NULL for primary key / merge key columns, causing:
    duckdb.ParserException: Adding columns with constraints not yet supported

Fix: Monkey-patch _gen_not_null to always return empty string for ADD COLUMN.
Simpler and safer than injecting a method override into the class body.

Remove this patch once DuckLake supports column constraints or dlt adds a config toggle.

Usage:
    python patches/duckdb_nullable_add_column.py
"""

import importlib.util
from pathlib import Path

spec = importlib.util.find_spec("dlt.destinations.impl.duckdb.duck")
if not spec or not spec.origin:
    raise RuntimeError("dlt duckdb destination not found in environment")

duck_py = Path(spec.origin)
content = duck_py.read_text()

MARKER = "# PATCH: nullable_add_column"

if MARKER in content:
    print("Already patched — skipping")
elif "class DuckDbClient" in content:
    # Override _gen_not_null to return empty string, preventing NOT NULL on ADD COLUMN.
    # This is safe because DuckDB/DuckLake enforces nullability at write time anyway.
    old = "class DuckDbClient(InsertValuesJobClient):"
    new = f"""class DuckDbClient(InsertValuesJobClient):
    {MARKER}
    def _gen_not_null(self, v: bool) -> str:
        return ""
"""
    content = content.replace(old, new, 1)
    duck_py.write_text(content)
    print("Patched dlt duckdb/duck.py — disabled NOT NULL for DuckLake compat")
else:
    print("WARNING: Could not find expected code pattern. Manual review needed.")
