"""Reusable annotated types, constants, and regex patterns for the MCP server."""

import re
from typing import Annotated

from mcp.types import ToolAnnotations
from pydantic import Field

# ---------------------------------------------------------------------------
# Annotated types for tool parameters
# ---------------------------------------------------------------------------

BBL = Annotated[str, Field(description="10-digit BBL: borough(1) + block(5) + lot(4). Example: 1000670001", examples=["1000670001", "2039720033", "3012340001"])]
ZIP = Annotated[str, Field(description="5-digit NYC ZIP code. Example: 10003, 11201, 10456", examples=["10003", "11201", "10456"])]
NAME = Annotated[str, Field(description="Person or company name. Fuzzy matched. Example: 'Barton Perlbinder', 'BLACKSTONE'", examples=["Steven Croman", "Barton Perlbinder", "BLACKSTONE GROUP"])]

# ---------------------------------------------------------------------------
# Row limits
# ---------------------------------------------------------------------------

MAX_LLM_ROWS = 20
MAX_STRUCTURED_ROWS = 500
MAX_QUERY_ROWS = 1000

# ---------------------------------------------------------------------------
# Tool annotations
# ---------------------------------------------------------------------------

READONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True)
ADMIN = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True)

# ---------------------------------------------------------------------------
# Vector search constants (hnsw_acorn — cosine distance thresholds)
# ---------------------------------------------------------------------------

EMBEDDINGS_DB = "/data/common-ground/emb.duckdb"
# Cosine distance = 1 - cosine_similarity. Lower = more similar.
VS_NAME_DISTANCE = 0.21       # ~79% cosine — allows spelling variants
VS_TIGHT_DISTANCE = 0.12      # ~88% cosine — higher-confidence cross-domain
VS_CATALOG_DISTANCE = 0.58    # loose — table/schema discovery
VS_CATEGORY_SIM = 0.5         # cosine similarity for resource categories

# ---------------------------------------------------------------------------
# SQL safety regex patterns
# ---------------------------------------------------------------------------

_UNSAFE_SQL = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY"
    r"|CALL|LOAD|INSTALL|ATTACH|DETACH|EXPORT|IMPORT)\b",
    re.IGNORECASE,
)

# Block filesystem-access functions anywhere in a query (not just at start)
_UNSAFE_FUNCTIONS = re.compile(
    r"\b(read_parquet|read_csv|read_csv_auto|read_json|read_json_auto"
    r"|read_text|glob|read_blob|write_parquet|write_csv"
    r"|httpfs_|http_get|http_post)\s*\(",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Reconnect error signatures
# ---------------------------------------------------------------------------

RECONNECT_ERRORS = ("HTTP 403", "HTTP 400", "HTTP 301", "Bad Request")

# ---------------------------------------------------------------------------
# Graph cache directory
# ---------------------------------------------------------------------------

GRAPH_CACHE_DIR = "/data/common-ground/graph-cache"

# ---------------------------------------------------------------------------
# Compiled regex patterns (shared across tools)
# ---------------------------------------------------------------------------

ZIP_PATTERN = re.compile(r"^\d{5}$")
COORDS_PATTERN = re.compile(r"^(-?\d+\.?\d*),\s*(-?\d+\.?\d*)$")
BBL_PATTERN = re.compile(r"^\d{10}$")
