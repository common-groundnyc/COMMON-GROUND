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
    """Build tokenized name index from 4 sources, sorted by token for fast lookups."""
    conn = _connect_ducklake()
    t0 = time.time()

    stopword_list = ", ".join(f"'{w}'" for w in STOPWORDS)

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.name_tokens_staging")
        conn.execute(f"""
            CREATE TABLE lake.foundation.name_tokens_staging AS
            WITH all_names AS (
                SELECT
                    UPPER(last_name || ' ' || first_name) AS full_name,
                    source_table,
                    unique_id AS source_id
                FROM lake.federal.name_index
                WHERE last_name IS NOT NULL

                UNION ALL

                SELECT
                    UPPER(name) AS full_name,
                    'housing.acris_parties' AS source_table,
                    document_id AS source_id
                FROM lake.housing.acris_parties
                WHERE name IS NOT NULL AND LENGTH(TRIM(name)) > 1

                UNION ALL

                SELECT
                    UPPER(ownername) AS full_name,
                    'city_government.pluto' AS source_table,
                    LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') AS source_id
                FROM lake.city_government.pluto
                WHERE ownername IS NOT NULL AND LENGTH(TRIM(ownername)) > 1

                UNION ALL

                SELECT
                    UPPER(current_entity_name) AS full_name,
                    'business.nys_corporations' AS source_table,
                    CAST(dos_id AS VARCHAR) AS source_id
                FROM lake.business.nys_corporations
                WHERE current_entity_name IS NOT NULL
                  AND LENGTH(TRIM(current_entity_name)) > 1
            ),
            tokenized AS (
                SELECT
                    unnest(string_split(full_name, ' ')) AS token,
                    source_table,
                    source_id,
                    full_name
                FROM all_names
            )
            SELECT token, source_table, source_id, full_name
            FROM tokenized
            WHERE LENGTH(token) >= 2
              AND token NOT IN ({stopword_list})
            ORDER BY token
        """)

        conn.execute("DROP TABLE IF EXISTS lake.foundation.name_tokens")
        conn.execute(
            "ALTER TABLE lake.foundation.name_tokens_staging "
            "RENAME TO name_tokens"
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.name_tokens"
        ).fetchone()[0]
        unique_tokens = conn.execute(
            "SELECT COUNT(DISTINCT token) FROM lake.foundation.name_tokens"
        ).fetchone()[0]
        source_count = conn.execute(
            "SELECT COUNT(DISTINCT source_table) FROM lake.foundation.name_tokens"
        ).fetchone()[0]

        elapsed = round(time.time() - t0, 1)
        context.log.info(
            "name_tokens: %s rows, %s unique tokens, %s sources in %ss",
            f"{row_count:,}", f"{unique_tokens:,}", source_count, elapsed,
        )

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(row_count),
                "unique_tokens": MetadataValue.int(unique_tokens),
                "source_tables": MetadataValue.int(source_count),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
