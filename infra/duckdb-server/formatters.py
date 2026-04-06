"""
LLM response formatting middleware for the Common Ground FastMCP DuckDB server.

Research findings (2026-03-26):
- TOON (Token-Oriented Object Notation): tabular format using a header like
  `results[N]{col1,col2}:` followed by indented comma-separated rows. Invented
  as a token-efficient alternative to JSON/markdown for LLM tabular output.
  Libraries: toon-parse 1.0.3 (PyPI, ankitpal181) and toon-parser 0.1.4
  (PyO3/Rust bindings, magi8101). Both have very low download counts (<500/mo),
  so this module implements TOON manually to avoid a fragile dependency.
- Markdown tables remain the standard for small result sets (<50 rows); TOON
  saves ~15-20% tokens on wide tables with 50+ rows by eliminating repeated
  pipe characters and column headers from each row.
- FastMCP ToolResult (3.1.0): content=str|list[ContentBlock] is what the LLM
  sees; structured_content=dict is machine-readable only (not sent to LLM);
  meta=dict carries execution metadata. Plain str auto-wraps to TextContent.
"""

import re

_TRUNCATION_MSG = (
    "(showing {shown} of {total} rows \u2014 "
    "use sql_query() with LIMIT/OFFSET for more)"
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _cell(value, max_cell: int, escape_char: str) -> str:
    """Stringify value, escape special chars, then truncate."""
    s = "" if value is None else str(value)
    s = s.replace(escape_char, "\\" + escape_char)
    if len(s) > max_cell:
        s = s[:max_cell] + "\u2026"
    return s


# ---------------------------------------------------------------------------
# format_markdown_table
# ---------------------------------------------------------------------------

def format_markdown_table(
    cols: list,
    rows: list,
    max_rows: int = 20,
    max_cell: int = 40,
    total_count: int | None = None,
) -> str:
    if not cols:
        return "(no columns)"
    if not rows:
        return "(no rows)"

    display_rows = rows[:max_rows]
    actual_total = total_count if total_count is not None else len(rows)

    header = "| " + " | ".join(cols) + " |"
    separator = "| " + " | ".join("---" for _ in cols) + " |"

    data_lines = []
    for row in display_rows:
        cells = [_cell(v, max_cell, "|") for v in row]
        data_lines.append("| " + " | ".join(cells) + " |")

    parts = [header, separator] + data_lines

    if len(display_rows) < actual_total:
        parts.append(_TRUNCATION_MSG.format(shown=len(display_rows), total=actual_total))

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# format_toon
# ---------------------------------------------------------------------------

def format_toon(
    cols: list,
    rows: list,
    max_rows: int = 500,
    max_cell: int = 60,
    total_count: int | None = None,
) -> str:
    if not cols:
        return "(no columns)"
    if not rows:
        return "(no rows)"

    display_rows = rows[:max_rows]
    actual_total = total_count if total_count is not None else len(rows)

    header = f"results[{len(display_rows)}]{{{','.join(cols)}}}:"

    data_lines = []
    for row in display_rows:
        cells = [_cell(v, max_cell, ",") for v in row]
        data_lines.append("  " + ",".join(cells))

    parts = [header] + data_lines

    if len(display_rows) < actual_total:
        parts.append(_TRUNCATION_MSG.format(shown=len(display_rows), total=actual_total))

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# format_markdown_kv
# ---------------------------------------------------------------------------

def format_markdown_kv(cols: list, rows: list) -> str | None:
    if not rows:
        return None
    if len(rows) > 3:
        return None

    blocks = []
    for row in rows:
        lines = []
        for col, val in zip(cols, row):
            display = "" if val is None else str(val)
            lines.append(f"**{col}:** {display}")
        blocks.append("\n".join(lines))

    return "\n---\n".join(blocks)


# ---------------------------------------------------------------------------
# detect_format
# ---------------------------------------------------------------------------

def detect_format(row_count: int, col_count: int) -> str:
    if row_count <= 3:
        return "kv"
    if row_count >= 51 and col_count >= 4:
        return "toon"
    return "markdown"


# ---------------------------------------------------------------------------
# filter_columns
# ---------------------------------------------------------------------------

_ID_SUFFIX = re.compile(r"(_id|_key|_pk|_fk)$", re.IGNORECASE)


def filter_columns(
    cols: list,
    rows: list,
    max_cols: int = 12,
) -> tuple[list, list]:
    if len(cols) <= max_cols:
        return cols, rows

    n_rows = len(rows)

    def score(idx: int, col: str) -> float:
        s = 1.0
        # Penalize >90% NULL columns
        if n_rows > 0:
            null_count = sum(1 for row in rows if row[idx] is None)
            if null_count / n_rows > 0.9:
                s -= 0.5
        # Penalize _id/_key/_pk/_fk suffixes (except first column)
        if idx > 0 and _ID_SUFFIX.search(col):
            s -= 0.4
        return s

    scores = [(i, col, score(i, col)) for i, col in enumerate(cols)]
    # Sort by score descending, keeping original index for stable tie-break
    top = sorted(scores, key=lambda x: (-x[2], x[0]))[:max_cols]
    # Restore original column order
    top_indices = sorted(t[0] for t in top)

    out_cols = [cols[i] for i in top_indices]
    out_rows = [[row[i] for i in top_indices] for row in rows]
    return out_cols, out_rows
