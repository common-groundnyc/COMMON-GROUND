"""Shared SQL utilities for the Common Ground MCP server."""

import re

_VALID_IDENTIFIERS = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]*$')


def validate_identifier(name: str) -> str:
    """Validate a SQL identifier (table name, column name). Raises ValueError if unsafe."""
    if not _VALID_IDENTIFIERS.match(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


def sanitize_error(msg: str) -> str:
    """Strip file paths and credentials from error messages before exposing to clients."""
    msg = re.sub(r'(/[a-zA-Z0-9_./-]{3,})', lambda m: '[path]/' + m.group(1).rsplit('/', 1)[-1] if '/' in m.group(1) else m.group(0), msg)
    msg = re.sub(r'password=[^\s&\'"]+', 'password=***', msg, flags=re.IGNORECASE)
    msg = re.sub(r'(dbname=\w+\s+user=\w+\s+)password=[^\s]+', r'\1password=***', msg, flags=re.IGNORECASE)
    return msg
