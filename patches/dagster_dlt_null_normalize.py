"""Patch dagster-dlt resource.py to handle None last_normalize_info.

Bug: dagster_dlt/resource.py line 134 crashes with
    'NoneType' object has no attribute 'row_counts'
when dlt's last_normalize_info is None (happens during DuckLake snapshot retries).

dagster-dlt 0.28.19 is the latest version and has no fix.
Remove this patch once dagster-dlt ships a fix.

Usage:
    python patches/dagster_dlt_null_normalize.py
"""

import importlib.util
import re
from pathlib import Path

spec = importlib.util.find_spec("dagster_dlt")
if not spec or not spec.origin:
    raise RuntimeError("dagster_dlt not found in environment")

resource_py = Path(spec.origin).parent / "resource.py"
content = resource_py.read_text()

OLD = (
    "rows_loaded = dlt_pipeline.last_trace.last_normalize_info.row_counts.get(\n"
    "            normalized_table_name\n"
    "        )\n"
    "        if rows_loaded:"
)

NEW = (
    "last_normalize_info = dlt_pipeline.last_trace.last_normalize_info\n"
    "        rows_loaded = (\n"
    "            last_normalize_info.row_counts.get(normalized_table_name)\n"
    "            if last_normalize_info\n"
    "            else None\n"
    "        )\n"
    "        if rows_loaded:"
)

if OLD in content:
    content = content.replace(OLD, NEW)
    resource_py.write_text(content)
    print("Patched dagster_dlt/resource.py — null-check for last_normalize_info")
elif "last_normalize_info = dlt_pipeline.last_trace.last_normalize_info" in content:
    print("Already patched — skipping")
else:
    print("WARNING: Could not find expected code pattern. Manual review needed.")
