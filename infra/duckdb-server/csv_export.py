import csv
import io
import os
import re
from datetime import datetime, timezone
from pathlib import Path

DIVIDER = "# " + "\u2500" * 48


def _format_tool_line(tool_name, tool_args):
    if not tool_args:
        return f"# Tool: {tool_name}()"
    args_str = ", ".join(f"{k}='{v}'" for k, v in tool_args.items())
    return f"# Tool: {tool_name}({args_str})"


def generate_branded_csv(cols, rows, tool_name="", tool_args=None, sql="", sources=None) -> str:
    buf = io.StringIO()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row_count = len(rows)

    # Build header comment lines (excluding the comment-count line and closing divider)
    header_lines = [
        DIVIDER,
        "# COMMON GROUND \u2014 NYC Open Data Export",
        "# common-ground.nyc",
        DIVIDER,
    ]
    if sql:
        truncated = sql[:200]
        header_lines.append(f"# Query: {truncated}")
    if tool_name:
        header_lines.append(_format_tool_line(tool_name, tool_args))
    header_lines.append(f"# Generated: {now}")
    header_lines.append(f"# Rows: {row_count}")
    if sources:
        header_lines.append(f"# Sources: {', '.join(sources)}")

    footer_lines = [
        DIVIDER,
        "# Data: NYC public records (open data, no restrictions)",
        "# Powered by DuckDB + DuckLake | common-ground.nyc",
        DIVIDER,
    ]

    # Total comment lines = header_lines + 2 (comment-count line + closing divider) + footer_lines
    total_comment_lines = len(header_lines) + 2 + len(footer_lines)
    header_lines.append(f"# Comment lines: {total_comment_lines} (skip for raw CSV)")
    header_lines.append(DIVIDER)

    for line in header_lines:
        buf.write(line + "\n")

    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["row_id"] + list(cols))
    for i, row in enumerate(rows, start=1):
        writer.writerow([i] + ["" if v is None else v for v in row])

    for line in footer_lines:
        buf.write(line + "\n")

    return buf.getvalue()


def write_export(csv_text, name, export_dir="/data/common-ground/exports") -> str:
    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)[:100]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_name}_{timestamp}.csv"

    Path(export_dir).mkdir(parents=True, exist_ok=True)
    path = os.path.join(export_dir, filename)

    with open(path, "w") as f:
        f.write(csv_text)

    return os.path.abspath(path)
