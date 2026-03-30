"""Response formatting helpers — text tables and ToolResult construction."""

from fastmcp.tools.tool import ToolResult

from shared.types import MAX_LLM_ROWS, MAX_STRUCTURED_ROWS


def format_text_table(cols: list, rows: list, max_rows: int = MAX_LLM_ROWS) -> str:
    """Render cols/rows as a Markdown table, truncated to *max_rows*."""
    if not cols:
        return "(no columns)"
    if not rows:
        return "(no rows)"

    display = rows[:max_rows]

    def cell(v: object) -> str:
        s = str(v)[:40] if v is not None else ""
        return s.replace("|", "\\|")

    lines = [
        "| " + " | ".join(str(c) for c in cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    lines.extend(
        "| " + " | ".join(cell(v) for v in row) + " |"
        for row in display
    )

    total = len(rows)
    if total > max_rows:
        lines.append(f"({max_rows} of {total} rows shown)")
    return "\n".join(lines)


def make_result(summary: str, cols: list, rows: list, meta_extra: dict | None = None) -> ToolResult:
    """Build a ToolResult with text table + structured JSON payload."""
    text = summary
    if cols and rows:
        text += "\n\n" + format_text_table(cols, rows)

    structured = (
        {"rows": [dict(zip(cols, row)) for row in rows[:MAX_STRUCTURED_ROWS]]}
        if cols and rows
        else None
    )
    meta = {"total_rows": len(rows)}
    if meta_extra:
        meta.update(meta_extra)
    return ToolResult(content=text, structured_content=structured, meta=meta)
