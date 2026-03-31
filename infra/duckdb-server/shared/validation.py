"""SQL validation — reject unsafe queries before they reach DuckDB."""

import re

from fastmcp.exceptions import ToolError

from shared.types import _UNSAFE_SQL, _UNSAFE_FUNCTIONS

# ---------------------------------------------------------------------------
# Safe DDL pattern — only CREATE OR REPLACE VIEW is allowed
# ---------------------------------------------------------------------------

SAFE_DDL = re.compile(
    r"^\s*CREATE\s+OR\s+REPLACE\s+VIEW\b",
    re.IGNORECASE,
)


def validate_admin_sql(sql: str) -> None:
    """Raise ToolError if DDL is not CREATE OR REPLACE VIEW."""
    if not SAFE_DDL.match(sql):
        raise ToolError("Only CREATE OR REPLACE VIEW statements are allowed in admin mode.")


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
