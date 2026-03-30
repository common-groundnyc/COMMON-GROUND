"""SQL validation — reject unsafe queries before they reach DuckDB."""

import re

from fastmcp.exceptions import ToolError

from shared.types import _UNSAFE_SQL, _SAFE_DDL, _UNSAFE_FUNCTIONS


def validate_sql(sql: str) -> None:
    """Raise ToolError if *sql* contains disallowed statements or functions."""
    # Strip comments and check for stacked statements
    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)  # block comments
    stripped = re.sub(r"--[^\n]*", " ", stripped)  # line comments
    stripped = stripped.strip().rstrip(";")

    # Reject stacked statements (semicolons in the middle)
    if ";" in stripped:
        raise ToolError("Only single SQL statements are allowed.")

    if _UNSAFE_SQL.match(stripped):
        raise ToolError(
            "Only SELECT, WITH, EXPLAIN, DESCRIBE, SHOW, and PRAGMA queries are allowed."
        )

    if _UNSAFE_FUNCTIONS.search(stripped):
        raise ToolError(
            "Direct file access functions (read_parquet, read_csv, glob, etc.) are not allowed. Query lake.schema.table_name instead."
        )
