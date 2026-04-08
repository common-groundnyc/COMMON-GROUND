"""Graph-layer Dagster assets.

Each multi_asset in this file writes pre-computed graph tables to
`lake.graphs.*` so the MCP server can read them on startup instead of
rebuilding from raw lake tables in-process. See
`docs/superpowers/specs/2026-04-09-graph-assets-architecture.md`.
"""
from __future__ import annotations

import logging

import dagster as dg

from dagster_pipeline.resources.ducklake import DuckLakeResource

logger = logging.getLogger(__name__)

SCHEMA = "graphs"


# ─── SQL ───────────────────────────────────────────────────────────

POL_ENTITIES_SQL = """
CREATE OR REPLACE TABLE lake.graphs.pol_entities AS
SELECT DISTINCT COALESCE(name, '') AS entity_name, 'donor' AS role
FROM lake.city_government.campaign_contributions WHERE name IS NOT NULL
UNION
SELECT DISTINCT COALESCE(recipname, '') AS entity_name, 'candidate' AS role
FROM lake.city_government.campaign_contributions WHERE recipname IS NOT NULL
"""

POL_DONATIONS_SQL = """
CREATE OR REPLACE TABLE lake.graphs.pol_donations AS
SELECT COALESCE(name, '')     AS donor,
       COALESCE(recipname, '') AS recipient,
       TRY_CAST(amnt AS DOUBLE) AS amount,
       TRY_CAST(date AS DATE)   AS donation_date
FROM lake.city_government.campaign_contributions
WHERE name IS NOT NULL AND recipname IS NOT NULL
"""

POL_CONTRACTS_SQL = """
CREATE OR REPLACE TABLE lake.graphs.pol_contracts AS
SELECT DISTINCT
    vendor_name     AS vendor,
    agency_name     AS agency,
    TRY_CAST(contract_amount AS DOUBLE) AS amount
FROM lake.city_government.contract_awards
WHERE vendor_name IS NOT NULL
"""

POL_LOBBYING_SQL = """
CREATE OR REPLACE TABLE lake.graphs.pol_lobbying AS
SELECT DISTINCT
    COALESCE(principal_lobbyist_name, '') AS lobbyist,
    COALESCE(contractual_client_name, '') AS client,
    COALESCE(government_body, '')         AS target
FROM lake.city_government.nys_lobbyist_registration
WHERE principal_lobbyist_name IS NOT NULL
"""


# ─── Multi-asset ───────────────────────────────────────────────────

POL_TABLES = ("pol_entities", "pol_donations", "pol_contracts", "pol_lobbying")
POL_SQL_BY_TABLE = {
    "pol_entities": POL_ENTITIES_SQL,
    "pol_donations": POL_DONATIONS_SQL,
    "pol_contracts": POL_CONTRACTS_SQL,
    "pol_lobbying": POL_LOBBYING_SQL,
}


@dg.multi_asset(
    specs=[
        dg.AssetSpec(key=dg.AssetKey([SCHEMA, t]), group_name=SCHEMA)
        for t in POL_TABLES
    ],
    op_tags={"schema": SCHEMA},
)
def graph_political(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
):
    """Build the four political-network graph tables from city_government sources.

    Each sub-table is materialized independently — a failure in one does not
    affect the others."""
    conn = ducklake.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.graphs")
        for t in POL_TABLES:
            try:
                conn.execute(POL_SQL_BY_TABLE[t])
                rows = conn.execute(
                    f"SELECT COUNT(*) FROM lake.graphs.{t}"
                ).fetchone()[0]
                context.log.info("graphs.%s: wrote %d rows", t, rows)
                yield dg.MaterializeResult(
                    asset_key=dg.AssetKey([SCHEMA, t]),
                    metadata={"row_count": rows},
                )
            except Exception as e:
                context.log.error("graphs.%s build failed: %s", t, e)
                yield dg.MaterializeResult(
                    asset_key=dg.AssetKey([SCHEMA, t]),
                    metadata={"row_count": 0, "error": str(e)},
                )
    finally:
        conn.close()
