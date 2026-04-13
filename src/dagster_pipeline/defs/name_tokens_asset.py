"""Dagster asset producing lake.foundation.name_tokens — global tokenized name index."""

import logging
import time

from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)

STOPWORDS = frozenset({
    "THE", "OF", "AND", "INC", "LLC", "CORP", "LTD", "CO",
    "NY", "NEW", "YORK", "FOR", "AT", "IN", "TO", "AS",
})


def tokenize_name(name: str | None) -> list[str]:
    """Split a name into uppercase tokens, excluding stopwords and short tokens."""
    if not name:
        return []
    tokens = name.strip().upper().split()
    return [t for t in tokens if len(t) >= 2 and t not in STOPWORDS]


@asset(
    key=AssetKey(["foundation", "name_tokens"]),
    group_name="foundation",
    description=(
        "Global tokenized name index for instant name search across the lake. "
        "Replaces LIKE '%%NAME%%' scans with equality lookups on sorted tokens."
    ),
    compute_kind="duckdb",
    deps=[
        AssetKey(["federal", "name_index"]),
        AssetKey(["housing", "acris_parties"]),
        AssetKey(["city_government", "pluto"]),
        AssetKey(["business", "nys_corporations"]),
    ],
)
def name_tokens(context) -> MaterializeResult:
    """Build tokenized name index from 4 sources incrementally to avoid OOM.

    Each source is tokenized and inserted separately — peak memory is the
    largest single source (~46M acris_parties) not all 69M rows at once.
    """
    conn = _connect_ducklake()
    t0 = time.time()

    stopword_list = ", ".join(f"'{w}'" for w in STOPWORDS)

    # Each source: (label, SQL that produces full_name, source_table, source_id)
    sources = [
        ("federal.name_index", f"""
            SELECT token, source_table, source_id, full_name
            FROM (
                SELECT
                    unnest(string_split(UPPER(last_name || ' ' || first_name), ' ')) AS token,
                    source_table,
                    unique_id AS source_id,
                    UPPER(last_name || ' ' || first_name) AS full_name
                FROM lake.federal.name_index
                WHERE last_name IS NOT NULL
            )
            WHERE LENGTH(token) >= 2 AND token NOT IN ({stopword_list})
        """),
        ("housing.acris_parties", f"""
            SELECT token, source_table, source_id, full_name
            FROM (
                SELECT
                    unnest(string_split(UPPER(name), ' ')) AS token,
                    'housing.acris_parties' AS source_table,
                    document_id AS source_id,
                    UPPER(name) AS full_name
                FROM lake.housing.acris_parties
                WHERE name IS NOT NULL AND LENGTH(TRIM(name)) > 1
            )
            WHERE LENGTH(token) >= 2 AND token NOT IN ({stopword_list})
        """),
        ("city_government.pluto", f"""
            SELECT token, source_table, source_id, full_name
            FROM (
                SELECT
                    unnest(string_split(UPPER(ownername), ' ')) AS token,
                    'city_government.pluto' AS source_table,
                    LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') AS source_id,
                    UPPER(ownername) AS full_name
                FROM lake.city_government.pluto
                WHERE ownername IS NOT NULL AND LENGTH(TRIM(ownername)) > 1
            )
            WHERE LENGTH(token) >= 2 AND token NOT IN ({stopword_list})
        """),
        ("business.nys_corporations", f"""
            SELECT token, source_table, source_id, full_name
            FROM (
                SELECT
                    unnest(string_split(UPPER(current_entity_name), ' ')) AS token,
                    'business.nys_corporations' AS source_table,
                    CAST(dos_id AS VARCHAR) AS source_id,
                    UPPER(current_entity_name) AS full_name
                FROM lake.business.nys_corporations
                WHERE current_entity_name IS NOT NULL
                  AND LENGTH(TRIM(current_entity_name)) > 1
            )
            WHERE LENGTH(token) >= 2 AND token NOT IN ({stopword_list})
        """),
    ]

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.name_tokens_staging")

        # Create empty table, then INSERT each source separately
        conn.execute("""
            CREATE TABLE lake.foundation.name_tokens_staging (
                token VARCHAR,
                source_table VARCHAR,
                source_id VARCHAR,
                full_name VARCHAR
            )
        """)

        total = 0
        for label, sql in sources:
            context.log.info("Tokenizing %s...", label)
            conn.execute(f"INSERT INTO lake.foundation.name_tokens_staging {sql}")
            cnt = conn.execute(
                "SELECT COUNT(*) FROM lake.foundation.name_tokens_staging"
            ).fetchone()[0]
            added = cnt - total
            total = cnt
            context.log.info("  %s: +%s tokens (%s total)", label, f"{added:,}", f"{total:,}")

        conn.execute("DROP TABLE IF EXISTS lake.foundation.name_tokens")
        conn.execute(
            "ALTER TABLE lake.foundation.name_tokens_staging "
            "RENAME TO name_tokens"
        )

        unique_tokens = conn.execute(
            "SELECT COUNT(DISTINCT token) FROM lake.foundation.name_tokens"
        ).fetchone()[0]

        elapsed = round(time.time() - t0, 1)
        context.log.info(
            "name_tokens: %s rows, %s unique tokens, 4 sources in %ss",
            f"{total:,}", f"{unique_tokens:,}", elapsed,
        )

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(total),
                "unique_tokens": MetadataValue.int(unique_tokens),
                "source_tables": MetadataValue.int(4),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
