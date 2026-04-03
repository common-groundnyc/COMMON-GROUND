"""Generate LLM-optimized table/column comments for DuckLake catalog.

Fetches Socrata metadata, profiles DuckLake columns, rewrites descriptions
with Claude, and outputs a YAML manifest for applying COMMENT ON statements.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import duckdb
import httpx
import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DOMAINS = {
    "nyc": "data.cityofnewyork.us",
    "nys": "data.ny.gov",
    "health": "health.data.ny.gov",
    "cdc": "data.cdc.gov",
}

MANIFEST_PATH = Path(__file__).parent.parent / "data" / "catalog_comments.yaml"

REWRITE_SYSTEM_PROMPT = """\
You are a data catalog writer for a civic open-data lakehouse.
Your job is to write concise, LLM-optimized descriptions for tables and columns
so that an AI agent can discover and query them without human guidance.

Rules for TABLE comments:
- 1-2 sentences, max 200 characters
- State the grain (what one row represents) and 2-3 use cases
- Example: "One row per HPD complaint. Use for housing conditions, landlord accountability, tenant complaints."

Rules for COLUMN comments:
- 10-30 words, max 120 characters
- Include enum values if low-cardinality (e.g., "Status: OPEN, CLOSED, PENDING")
- Mention join targets (e.g., "Foreign key to hpd_violations.violationid")
- Explain NULL meaning if relevant (e.g., "NULL = not yet inspected")
- Skip self-explanatory columns: latitude, longitude, zip_code, borough, created_date, updated_date
- Skip internal columns starting with _dlt_

Output format — valid YAML only, no markdown fences:
table_comment: "Your table comment here"
columns:
  column_name: "Your column comment here"
  another_column: "Another comment"
"""

SOCRATA_API_URL = "https://{domain}/api/views/{dataset_id}.json"
RATE_LIMIT_DELAY = 0.25  # seconds between Socrata API calls


# ---------------------------------------------------------------------------
# Socrata metadata fetching
# ---------------------------------------------------------------------------

def fetch_socrata_metadata(domain: str, dataset_id: str) -> dict:
    """Fetch table + column metadata from Socrata's views API."""
    url = SOCRATA_API_URL.format(domain=domain, dataset_id=dataset_id)
    resp = httpx.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    columns = []
    for col in data.get("columns", []):
        columns.append({
            "field_name": col.get("fieldName", ""),
            "name": col.get("name", ""),
            "description": col.get("description", ""),
            "data_type": col.get("dataTypeName", ""),
        })

    return {
        "name": data.get("name", ""),
        "description": data.get("description", ""),
        "columns": columns,
        "category": data.get("category", ""),
        "tags": data.get("tags", []),
        "rows_updated_at": data.get("rowsUpdatedAt"),
    }


def fetch_all_socrata_metadata(
    datasets: dict[str, list[tuple[str, str, str]]],
) -> dict[str, dict[str, dict]]:
    """Fetch metadata for all datasets, grouped by schema.

    Returns: {schema: {table_name: metadata_dict}}
    """
    result: dict[str, dict[str, dict]] = {}
    total = sum(len(tables) for tables in datasets.values())
    done = 0

    for schema, tables in datasets.items():
        result[schema] = {}
        for table_name, dataset_id, domain_key in tables:
            domain = DOMAINS.get(domain_key, domain_key)
            done += 1
            print(f"  [{done}/{total}] {schema}.{table_name} ({dataset_id})...", end=" ", file=sys.stderr)
            try:
                meta = fetch_socrata_metadata(domain, dataset_id)
                result[schema][table_name] = meta
                print(f"OK — {len(meta['columns'])} columns", file=sys.stderr)
            except httpx.HTTPStatusError as e:
                print(f"SKIP ({e.response.status_code})", file=sys.stderr)
            except Exception as e:
                print(f"ERROR ({e})", file=sys.stderr)
            time.sleep(RATE_LIMIT_DELAY)

    return result


# ---------------------------------------------------------------------------
# DuckLake column profiling
# ---------------------------------------------------------------------------

def profile_table_columns(
    conn: duckdb.DuckDBPyConnection,
    database: str,
    schema: str,
    table: str,
) -> list[dict]:
    """Profile columns in a DuckLake table — cardinality, null %, top values."""
    fqn = f"{database}.{schema}.{table}"

    # Get column names and types via DESCRIBE (information_schema is broken in DuckLake)
    try:
        cols_result = conn.execute(f"DESCRIBE {fqn}").fetchall()
        # DESCRIBE returns (column_name, column_type, null, key, default, extra)
        cols_result = [(row[0], row[1]) for row in cols_result]
    except Exception:
        return []

    if not cols_result:
        return []

    # Get row count
    try:
        row_count = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
    except Exception:
        row_count = 0

    profiles = []
    for col_name, col_type in cols_result:
        if col_name.startswith("_dlt_"):
            continue

        profile = {
            "column_name": col_name,
            "data_type": col_type,
            "row_count": row_count,
        }

        try:
            stats = conn.execute(
                f'SELECT COUNT(DISTINCT "{col_name}") AS cardinality, '
                f'ROUND(100.0 * SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) / '
                f'GREATEST(COUNT(*), 1), 1) AS null_pct '
                f"FROM {fqn}"
            ).fetchone()
            profile["cardinality"] = stats[0]
            profile["null_pct"] = float(stats[1])
        except Exception:
            profile["cardinality"] = None
            profile["null_pct"] = None

        # Top values for low-cardinality columns (< 25 distinct)
        if profile.get("cardinality") and profile["cardinality"] < 25:
            try:
                top = conn.execute(
                    f'SELECT "{col_name}", COUNT(*) AS cnt FROM {fqn} '
                    f'WHERE "{col_name}" IS NOT NULL '
                    f'GROUP BY "{col_name}" ORDER BY cnt DESC LIMIT 10'
                ).fetchall()
                profile["top_values"] = [
                    {"value": str(row[0]), "count": row[1]} for row in top
                ]
            except Exception:
                profile["top_values"] = []
        else:
            profile["top_values"] = []

        profiles.append(profile)

    return profiles


# ---------------------------------------------------------------------------
# Claude rewrite
# ---------------------------------------------------------------------------

def build_rewrite_prompt(
    schema: str,
    table: str,
    socrata_meta: dict,
    column_profile: list[dict],
) -> str:
    """Build the user prompt for Claude to rewrite descriptions."""
    lines = [
        f"Schema: {schema}",
        f"Table: {table}",
        f"Socrata name: {socrata_meta.get('name', 'N/A')}",
        f"Socrata description: {socrata_meta.get('description', 'N/A')}",
        f"Category: {socrata_meta.get('category', 'N/A')}",
        f"Tags: {', '.join(socrata_meta.get('tags', []))}",
        "",
        "Socrata columns:",
    ]

    socrata_cols = {c["field_name"]: c for c in socrata_meta.get("columns", [])}

    for col in column_profile:
        name = col["column_name"]
        soc = socrata_cols.get(name, {})
        line = f"  - {name} ({col['data_type']})"
        if soc.get("description"):
            line += f" — Socrata: \"{soc['description']}\""
        line += f" | cardinality={col.get('cardinality', '?')}, null%={col.get('null_pct', '?')}"
        if col.get("top_values"):
            vals = ", ".join(tv["value"] for tv in col["top_values"][:5])
            line += f" | top: [{vals}]"
        lines.append(line)

    lines.append("")
    lines.append("Write the table_comment and columns YAML now.")
    return "\n".join(lines)


def rewrite_with_claude(
    schema: str,
    table: str,
    socrata_meta: dict,
    column_profile: list[dict],
    client,
) -> str:
    """Call Claude API to rewrite descriptions. Returns raw response text."""
    import anthropic

    prompt = build_rewrite_prompt(schema, table, socrata_meta, column_profile)

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        system=REWRITE_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    return message.content[0].text


def parse_rewrite_response(text: str) -> dict:
    """Parse Claude's YAML response into a dict with table_comment and columns."""
    # Strip markdown fences if present
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        # Remove first and last fence lines
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines)

    try:
        parsed = yaml.safe_load(cleaned)
    except yaml.YAMLError:
        return {"table_comment": "", "columns": {}}
    if not isinstance(parsed, dict):
        return {"table_comment": "", "columns": {}}

    return {
        "table_comment": parsed.get("table_comment", ""),
        "columns": parsed.get("columns", {}),
    }


# ---------------------------------------------------------------------------
# Manifest building
# ---------------------------------------------------------------------------

def build_manifest_entry(
    schema: str,
    table: str,
    socrata_meta: dict,
    rewritten: dict,
) -> dict:
    """Build a single manifest entry for one table."""
    return {
        "schema": schema,
        "table": table,
        "socrata_name": socrata_meta.get("name", ""),
        "socrata_description": socrata_meta.get("description", ""),
        "table_comment": rewritten.get("table_comment", ""),
        "columns": rewritten.get("columns", {}),
    }


def write_manifest(entries: list[dict], path: Path) -> None:
    """Write YAML manifest to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(
            {"version": 1, "tables": entries},
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=120,
        )
    print(f"Wrote manifest: {path} ({len(entries)} tables)")


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate LLM-optimized comments for DuckLake tables"
    )
    parser.add_argument(
        "--fetch-only",
        action="store_true",
        help="Fetch Socrata metadata and dump as JSON, skip profiling/rewrite",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=MANIFEST_PATH,
        help=f"Output manifest path (default: {MANIFEST_PATH})",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help="Process only this schema (e.g., 'housing')",
    )
    args = parser.parse_args()

    # Import datasets
    src_path = str(Path(__file__).parent.parent / "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    from dagster_pipeline.sources.datasets import SOCRATA_DATASETS

    # Filter by schema if requested
    datasets = SOCRATA_DATASETS
    if args.schema:
        if args.schema not in datasets:
            print(f"Unknown schema: {args.schema}")
            print(f"Available: {', '.join(sorted(datasets.keys()))}")
            sys.exit(1)
        datasets = {args.schema: datasets[args.schema]}

    # Step 1: Fetch Socrata metadata
    print("Fetching Socrata metadata...", file=sys.stderr)
    all_meta = fetch_all_socrata_metadata(datasets)

    if args.fetch_only:
        output = json.dumps(all_meta, indent=2, default=str)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output)
        print(f"Raw metadata written to {args.output}", file=sys.stderr)
        return

    # Step 2: Connect to DuckLake
    import duckdb

    catalog_url = os.environ.get("DUCKLAKE_CATALOG_URL", "")

    if not catalog_url:
        print("ERROR: DUCKLAKE_CATALOG_URL not set")
        sys.exit(1)

    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    conn.execute("LOAD ducklake")
    conn.execute("LOAD postgres")
    conn.execute("SET http_timeout=300000")
    conn.execute("SET memory_limit='2GB'")
    conn.execute("SET threads=4")
    conn.execute(f"ATTACH '{catalog_url}' AS lake (METADATA_SCHEMA 'lake')")

    # Step 3: Profile + rewrite each table
    import anthropic
    client = anthropic.Anthropic()

    entries = []
    for schema_name, tables_meta in all_meta.items():
        for table_name, socrata_meta in tables_meta.items():
            print(f"Profiling {schema_name}.{table_name}...")
            profile = profile_table_columns(conn, "lake", schema_name, table_name)

            if not profile:
                print(f"  SKIP — no columns found in DuckLake")
                continue

            print(f"  Rewriting with Claude ({len(profile)} columns)...")
            raw = rewrite_with_claude(
                schema_name, table_name, socrata_meta, profile, client
            )
            rewritten = parse_rewrite_response(raw)
            entry = build_manifest_entry(
                schema_name, table_name, socrata_meta, rewritten
            )
            entries.append(entry)
            print(f"  OK — {len(rewritten.get('columns', {}))} column comments")

    # Step 4: Write manifest
    write_manifest(entries, args.output)


if __name__ == "__main__":
    main()
