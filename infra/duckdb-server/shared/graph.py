"""Graph infrastructure — table list, cache management, and graph readiness check."""

import pathlib
import time

from fastmcp.exceptions import ToolError

from shared.types import GRAPH_CACHE_DIR

# ---------------------------------------------------------------------------
# All graph tables (materialized in DuckDB memory from lake queries)
# ---------------------------------------------------------------------------

GRAPH_TABLES = [
    "graph_owners", "graph_buildings", "graph_owns",
    "graph_violations", "graph_has_violation", "graph_shared_owner",
    "graph_building_flags", "graph_acris_sales", "graph_rent_stabilization",
    "graph_corp_contacts", "graph_business_at_building", "graph_acris_chain",
    "graph_dob_owners", "graph_eviction_petitioners",
    "graph_doing_business", "graph_campaign_donors",
    "graph_epa_facilities",
    # Transaction network
    "graph_tx_entities", "graph_tx_edges", "graph_tx_shared",
    # Corporate web
    "graph_corps", "graph_corp_people", "graph_corp_officer_edges", "graph_corp_shared_officer",
    # Influence network
    "graph_pol_entities", "graph_pol_donations", "graph_pol_contracts", "graph_pol_lobbying",
    # Contractor network
    "graph_contractors", "graph_permit_edges", "graph_contractor_shared",
    "graph_contractor_building_shared",
    # FEC + litigation
    "graph_fec_contributions", "graph_litigation_respondents",
    # Officer misconduct network
    "graph_officers", "graph_officer_complaints", "graph_officer_shared_command",
    # COIB pay-to-play network
    "graph_coib_donors", "graph_coib_policymakers", "graph_coib_donor_edges",
    # BIC trade waste network
    "graph_bic_companies", "graph_bic_violation_edges", "graph_bic_shared_bbl",
    # DOB violations (respondent-named)
    "graph_dob_respondents", "graph_dob_respondent_bbl",
]


def require_graph(ctx: object) -> None:
    """Raise ToolError if the property graph is not available."""
    if not ctx.lifespan_context.get("graph_ready"):
        raise ToolError("Property graph not yet built — startup still in progress or graph build failed.")


def graph_cache_fresh() -> bool:
    """Check if Parquet cache exists and is < 24 hours old."""
    cache = pathlib.Path(GRAPH_CACHE_DIR)
    if not cache.exists():
        return False
    marker = cache / "_built_at.txt"
    if not marker.exists():
        return False
    try:
        built_ts = float(marker.read_text().strip())
        age_hours = (time.time() - built_ts) / 3600
        return age_hours < 24 and all((cache / f"{t}.parquet").exists() for t in GRAPH_TABLES)
    except (ValueError, OSError):
        return False


def load_graph_from_cache(conn: object) -> None:
    """Load all graph tables from Parquet cache."""
    for t in GRAPH_TABLES:
        path = f"{GRAPH_CACHE_DIR}/{t}.parquet"
        conn.execute(f"CREATE OR REPLACE TABLE main.{t} AS SELECT * FROM read_parquet('{path}')")


def save_graph_to_cache(conn: object) -> None:
    """Export all graph tables to Parquet for fast restart."""
    cache = pathlib.Path(GRAPH_CACHE_DIR)
    cache.mkdir(parents=True, exist_ok=True)
    for t in GRAPH_TABLES:
        path = f"{GRAPH_CACHE_DIR}/{t}.parquet"
        conn.execute(f"COPY main.{t} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    (cache / "_built_at.txt").write_text(str(time.time()))
