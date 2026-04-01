"""Dagster asset producing lake.foundation.entity_master."""

import hashlib
import time
import uuid
from collections import Counter

import dagster
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.resources.ducklake import DuckLakeResource

ORG_INDICATORS = (
    "LLC", "L.L.C.", "L.L.C", "CORP", "CORPORATION", "INC", "INCORPORATED",
    "LTD", "LIMITED", "LP", "L.P.", "LLP", "L.L.P.",
    "TRUST", "BANK", "FUND", "FOUNDATION", "ASSOCIATION", "ASSOC",
    "HOLDINGS", "REALTY", "PROPERTIES", "MANAGEMENT", "MGMT",
    "HOUSING DEV", "DEVELOPMENT", "ENTERPRISES", "PARTNERS",
    "CITY OF", "STATE OF", "COUNTY OF", "DEPT OF", "DEPARTMENT",
    "AUTHORITY", "COMMISSION", "BOARD OF", "AGENCY",
)

# SQL CASE expression for entity type classification (mirrors classify_entity_type)
_ENTITY_TYPE_SQL = """
    CASE
        WHEN canonical_last LIKE '%LLC%'
          OR canonical_last LIKE '%L.L.C%'
          OR canonical_last LIKE '%CORP%'
          OR canonical_last LIKE '%CORPORATION%'
          OR canonical_last LIKE '%INC%'
          OR canonical_last LIKE '%INCORPORATED%'
          OR canonical_last LIKE '%LTD%'
          OR canonical_last LIKE '%LIMITED%'
          OR canonical_last LIKE '%TRUST%'
          OR canonical_last LIKE '%BANK%'
          OR canonical_last LIKE '%FUND%'
          OR canonical_last LIKE '%FOUNDATION%'
          OR canonical_last LIKE '%ASSOCIATION%'
          OR canonical_last LIKE '%HOLDINGS%'
          OR canonical_last LIKE '%REALTY%'
          OR canonical_last LIKE '%PROPERTIES%'
          OR canonical_last LIKE '%MANAGEMENT%'
          OR canonical_last LIKE '%MGMT%'
          OR canonical_last LIKE '%DEVELOPMENT%'
          OR canonical_last LIKE '%ENTERPRISES%'
          OR canonical_last LIKE '%PARTNERS%'
          OR canonical_last LIKE '%CITY OF%'
          OR canonical_last LIKE '%STATE OF%'
          OR canonical_last LIKE '%COUNTY OF%'
          OR canonical_last LIKE '%DEPT OF%'
          OR canonical_last LIKE '%DEPARTMENT%'
          OR canonical_last LIKE '%AUTHORITY%'
          OR canonical_last LIKE '%COMMISSION%'
          OR canonical_last LIKE '%BOARD OF%'
          OR canonical_last LIKE '%AGENCY%'
        THEN 'ORGANIZATION'
        ELSE 'PERSON'
    END
"""

# Template SQL — call .format(re_table=..., ni_table=..., pp_table=...) before use
_ENTITY_MASTER_SQL_TEMPLATE = """
    WITH cluster_members AS (
        SELECT
            re.cluster_id,
            ni.unique_id,
            ni.last_name,
            ni.first_name,
            ni.source_table
        FROM {re_table} re
        JOIN {ni_table} ni ON re.unique_id = ni.unique_id
    ),
    name_freq AS (
        SELECT cluster_id, last_name, first_name, COUNT(*) AS freq
        FROM cluster_members
        GROUP BY cluster_id, last_name, first_name
    ),
    canonical AS (
        SELECT cluster_id, last_name, first_name
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY cluster_id ORDER BY freq DESC, last_name, first_name
            ) AS rn
            FROM name_freq
        )
        WHERE rn = 1
    ),
    cluster_stats AS (
        SELECT
            cm.cluster_id,
            c.last_name AS canonical_last,
            c.first_name AS canonical_first,
            LIST(DISTINCT {{last_name: cm.last_name, first_name: cm.first_name}}) AS name_variants,
            COUNT(DISTINCT cm.source_table) AS source_count,
            COUNT(*) AS record_count,
            LIST(DISTINCT cm.source_table) AS source_tables,
            LIST(cm.unique_id::VARCHAR ORDER BY cm.unique_id) AS member_ids
        FROM cluster_members cm
        JOIN canonical c ON cm.cluster_id = c.cluster_id
        GROUP BY cm.cluster_id, c.last_name, c.first_name
    ),
    cluster_confidence AS (
        SELECT
            re.cluster_id,
            AVG(pp.match_probability) AS avg_probability,
            MIN(pp.match_probability) AS min_probability,
            COUNT(*) AS pair_count
        FROM {pp_table} pp
        JOIN {re_table} re ON pp.unique_id_l = re.unique_id
        GROUP BY re.cluster_id
    ),
    with_hashes AS (
        SELECT
            cs.*,
            list_sort(list_transform(cs.member_ids, x -> md5(x))) AS sorted_hashes,
            COALESCE(cc.avg_probability, 1.0) AS confidence,
            COALESCE(cc.min_probability, 1.0) AS min_confidence,
            COALESCE(cc.pair_count, 0) AS match_pair_count
        FROM cluster_stats cs
        LEFT JOIN cluster_confidence cc ON cs.cluster_id = cc.cluster_id
    )
    SELECT
        md5(array_to_string(sorted_hashes, '|'))::UUID AS entity_id,
        cluster_id,
        canonical_last,
        canonical_first,
        name_variants,
        {entity_type_sql} AS entity_type,
        confidence,
        min_confidence,
        match_pair_count,
        source_count,
        record_count,
        source_tables,
        member_ids
    FROM with_hashes
"""


def classify_entity_type(name: str | None) -> str:
    """Classify a name as PERSON, ORGANIZATION, or UNKNOWN."""
    if not name or not name.strip():
        return "UNKNOWN"
    upper = name.upper().strip()
    for indicator in ORG_INDICATORS:
        if indicator in upper:
            return "ORGANIZATION"
    return "PERSON"


def generate_entity_id(member_unique_ids: list[str]) -> uuid.UUID:
    """Generate a deterministic UUID from sorted cluster member IDs.
    Same members in any order always produce the same UUID."""
    sorted_hashes = sorted(hashlib.md5(m.encode()).hexdigest() for m in member_unique_ids)
    combined = "|".join(sorted_hashes)
    return uuid.UUID(hashlib.md5(combined.encode()).hexdigest())


def select_canonical_name(records: list[dict]) -> tuple[str, str]:
    """Pick canonical (last_name, first_name) from records: most frequent, alphabetical tiebreak."""
    counts = Counter((r["last_name"], r["first_name"]) for r in records)
    max_count = max(counts.values())
    candidates = sorted(name for name, count in counts.items() if count == max_count)
    return candidates[0]


def aggregate_confidence(probabilities: list[float]) -> float:
    """Aggregate match probabilities into a single confidence score."""
    if not probabilities:
        return 1.0
    return sum(probabilities) / len(probabilities)


def build_entity_master(
    conn,
    table_prefix: str = "lake.federal.",
    output_table: str = "lake.foundation.entity_master",
) -> int:
    """Build entity_master table via SQL aggregation.

    Returns the number of entities produced.
    """
    sql = _ENTITY_MASTER_SQL_TEMPLATE.format(
        ni_table=f"{table_prefix}name_index",
        re_table=f"{table_prefix}resolved_entities",
        pp_table=f"{table_prefix}pairwise_probabilities",
        entity_type_sql=_ENTITY_TYPE_SQL,
    )

    conn.execute(f"CREATE OR REPLACE TABLE {output_table} AS {sql}")

    row_count = conn.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
    return row_count


@asset(
    key=AssetKey(["foundation", "entity_master"]),
    group_name="foundation",
    deps=[
        AssetKey(["federal", "resolved_entities"]),
        AssetKey(["federal", "pairwise_probabilities"]),
    ],
    description="Canonical entity table with stable UUIDs, entity types, and confidence scores",
    compute_kind="duckdb",
)
def entity_master(context: dagster.AssetExecutionContext, ducklake: DuckLakeResource):
    """Materialize entity_master from resolved_entities + pairwise_probabilities + name_index."""
    t_start = time.time()
    conn = ducklake.get_connection()

    conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")

    context.log.info("Building entity_master from resolved_entities + pairwise_probabilities...")
    row_count = build_entity_master(conn)

    person_count = conn.execute(
        "SELECT COUNT(*) FROM lake.foundation.entity_master WHERE entity_type = 'PERSON'"
    ).fetchone()[0]
    org_count = conn.execute(
        "SELECT COUNT(*) FROM lake.foundation.entity_master WHERE entity_type = 'ORGANIZATION'"
    ).fetchone()[0]
    avg_confidence = conn.execute(
        "SELECT AVG(confidence) FROM lake.foundation.entity_master"
    ).fetchone()[0] or 0.0

    elapsed = time.time() - t_start
    context.log.info(
        "entity_master complete: %s entities (%s persons, %s orgs), "
        "avg confidence: %.3f, duration: %.1fs",
        f"{row_count:,}", f"{person_count:,}", f"{org_count:,}",
        avg_confidence, elapsed,
    )

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(row_count),
            "person_count": MetadataValue.int(person_count),
            "org_count": MetadataValue.int(org_count),
            "avg_confidence": MetadataValue.float(round(avg_confidence, 3)),
            "duration_seconds": MetadataValue.float(elapsed),
        }
    )
