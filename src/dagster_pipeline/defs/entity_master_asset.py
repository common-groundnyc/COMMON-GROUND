"""Dagster asset producing lake.foundation.entity_master."""

import hashlib
import logging
import time
import uuid
from collections import Counter

import duckdb
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

logger = logging.getLogger(__name__)

ORG_INDICATORS = (
    "LLC", "L.L.C.", "L.L.C", "CORP", "CORPORATION", "INC", "INCORPORATED",
    "LTD", "LIMITED", "LP", "L.P.", "LLP", "L.L.P.",
    "TRUST", "BANK", "FUND", "FOUNDATION", "ASSOCIATION", "ASSOC",
    "HOLDINGS", "REALTY", "PROPERTIES", "MANAGEMENT", "MGMT",
    "HOUSING DEV", "DEVELOPMENT", "ENTERPRISES", "PARTNERS",
    "CITY OF", "STATE OF", "COUNTY OF", "DEPT OF", "DEPARTMENT",
    "AUTHORITY", "COMMISSION", "BOARD OF", "AGENCY",
)


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
    conn: duckdb.DuckDBPyConnection,
    table_prefix: str = "lake.federal.",
    output_table: str = "lake.foundation.entity_master",
) -> int:
    """Build entity_master table from resolved_entities + name_index + pairwise_probabilities.

    Returns the number of entities produced.
    """
    ni = f"{table_prefix}name_index"
    re_tbl = f"{table_prefix}resolved_entities"
    pp = f"{table_prefix}pairwise_probabilities"

    # Step 1: Get cluster data with names
    clusters = conn.execute(f"""
        SELECT
            re.cluster_id,
            CAST(ni.unique_id AS VARCHAR) AS unique_id_str,
            ni.last_name,
            ni.first_name,
            ni.source_table
        FROM {re_tbl} re
        JOIN {ni} ni ON re.unique_id = ni.unique_id
        ORDER BY re.cluster_id, ni.unique_id
    """).fetchall()

    # Step 2: Get pairwise probabilities per cluster
    probs_by_cluster = {}
    try:
        prob_rows = conn.execute(f"""
            SELECT re_l.cluster_id, pp.match_probability
            FROM {pp} pp
            JOIN {re_tbl} re_l ON pp.unique_id_l = re_l.unique_id
        """).fetchall()
        for cluster_id, prob in prob_rows:
            probs_by_cluster.setdefault(cluster_id, []).append(prob)
    except Exception:
        pass  # pairwise_probabilities may not exist

    # Step 3: Group by cluster_id and aggregate
    from collections import defaultdict
    cluster_data = defaultdict(list)
    for cluster_id, uid_str, last_name, first_name, source_table in clusters:
        cluster_data[cluster_id].append({
            "unique_id_str": uid_str,
            "last_name": last_name or "",
            "first_name": first_name or "",
            "source_table": source_table,
        })

    # Step 4: Build entity rows
    entity_rows = []
    for cluster_id, members in cluster_data.items():
        member_ids = [m["unique_id_str"] for m in members]
        entity_id = str(generate_entity_id(member_ids))

        records_with_names = [m for m in members if m["last_name"]]
        if records_with_names:
            canonical_last, canonical_first = select_canonical_name(records_with_names)
        else:
            canonical_last, canonical_first = "", ""

        full_name = f"{canonical_last} {canonical_first}".strip() if canonical_first else canonical_last
        entity_type = classify_entity_type(full_name)

        probs = probs_by_cluster.get(cluster_id, [])
        confidence = aggregate_confidence(probs)

        source_count = len({m["source_table"] for m in members})

        entity_rows.append((
            entity_id,
            canonical_last,
            canonical_first,
            entity_type,
            confidence,
            len(members),
            source_count,
        ))

    # Step 5: Write to output table
    if entity_rows:
        conn.execute(f"""
            CREATE OR REPLACE TABLE {output_table} (
                entity_id VARCHAR,
                canonical_last VARCHAR,
                canonical_first VARCHAR,
                entity_type VARCHAR,
                confidence DOUBLE,
                member_count INTEGER,
                source_count INTEGER
            )
        """)
        conn.executemany(
            f"INSERT INTO {output_table} VALUES (?, ?, ?, ?, ?, ?, ?)",
            entity_rows,
        )

    return len(entity_rows)


@asset(
    key=AssetKey(["foundation", "entity_master"]),
    group_name="foundation",
    deps=[
        AssetKey(["federal", "resolved_entities"]),
    ],
    description="Unified entity master — one row per resolved entity with canonical name, type, and confidence.",
    compute_kind="duckdb",
)
def entity_master(context) -> MaterializeResult:
    """Materialize entity_master from resolved_entities + pairwise_probabilities + name_index."""
    from dagster_pipeline.defs.name_index_asset import _connect_ducklake

    t_start = time.time()
    conn = _connect_ducklake()

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")

        context.log.info("Building entity_master from resolved_entities...")
        entity_count = build_entity_master(conn)

        elapsed = time.time() - t_start
        context.log.info(
            "Entity master built: %s entities in %.1fs",
            f"{entity_count:,}", elapsed,
        )

        return MaterializeResult(
            metadata={
                "entity_count": MetadataValue.int(entity_count),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
