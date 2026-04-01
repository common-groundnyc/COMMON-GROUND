"""Dagster asset for entity name embeddings — embeds distinct person names
from the name_index into a Lance vector index for semantic search.

Reads (last_name, first_name) pairs from lake.federal.name_index,
embeds via Gemini API, writes to /data/common-ground/lance/entity_name_embeddings.lance
using DuckDB's native Lance extension (COPY ... FORMAT lance).
"""
import logging
import os
import sys
import time
from pathlib import Path

import pyarrow as pa
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)

# Write locally, then rsync to Hetzner. The MCP server reads from the Hetzner path.
LANCE_LOCAL_DIR = os.environ.get("LANCE_LOCAL_DIR", os.path.expanduser("~/Desktop/dagster-pipeline/data/lance"))
LANCE_TABLE_NAME = "entity_name_embeddings"
LANCE_PATH = os.path.join(LANCE_LOCAL_DIR, f"{LANCE_TABLE_NAME}.lance")
CHUNK_SIZE = 100_000

@asset(
    key=AssetKey(["foundation", "entity_name_embeddings"]),
    group_name="foundation",
    deps=[
        AssetKey(["federal", "name_index"]),
        AssetKey(["foundation", "entity_master"]),
    ],
    description="Lance vector index of embedded entity names from name_index for semantic search, with entity_id from entity_master.",
    compute_kind="lance",
)
def entity_name_embeddings(context) -> MaterializeResult:
    """Embed distinct names from name_index into a Lance vector index."""
    import duckdb

    # Import embedder — try relative path first, fall back to known location
    embedder_candidates = [
        str(Path(__file__).resolve().parent.parent.parent.parent / "infra" / "duckdb-server"),
        os.path.expanduser("~/Desktop/dagster-pipeline/infra/duckdb-server"),
        "/opt/common-ground/duckdb-server",  # Docker/Hetzner path
    ]
    for edir in embedder_candidates:
        if Path(edir, "embedder.py").exists() and edir not in sys.path:
            sys.path.insert(0, edir)
            break
    from embedder import create_embedder

    t_start = time.time()
    conn = _connect_ducklake()

    # Load Lance extension for COPY ... FORMAT lance
    conn.execute("INSTALL lance; LOAD lance;")

    try:
        # Check if entity_master is available for entity_id join
        has_entity_master = False
        try:
            conn.execute("SELECT 1 FROM lake.foundation.entity_master LIMIT 1")
            has_entity_master = True
            context.log.info("Entity master available — will attach entity_id")
        except Exception:
            context.log.warning("Entity master not yet available — entity_id will be NULL")

        # Step 1: Materialize distinct names into a temp table (avoid OOM on 14M rows)
        context.log.info("Creating temp table of distinct names...")
        if has_entity_master:
            conn.execute("""
                CREATE TEMP TABLE _distinct_names AS
                SELECT
                    UPPER(TRIM(ni.last_name)) || ', ' || UPPER(TRIM(ni.first_name)) AS name,
                    STRING_AGG(DISTINCT ni.source_table, ',' ORDER BY ni.source_table) AS sources,
                    MAX(em.entity_id) AS entity_id
                FROM lake.federal.name_index ni
                LEFT JOIN lake.federal.resolved_entities re
                    ON ni.unique_id = re.unique_id
                LEFT JOIN lake.foundation.entity_master em
                    ON re.cluster_id = em.cluster_id
                WHERE ni.last_name IS NOT NULL AND LENGTH(ni.last_name) >= 2
                  AND ni.first_name IS NOT NULL AND LENGTH(ni.first_name) >= 1
                GROUP BY UPPER(TRIM(ni.last_name)), UPPER(TRIM(ni.first_name))
            """)
        else:
            conn.execute("""
                CREATE TEMP TABLE _distinct_names AS
                SELECT
                    UPPER(TRIM(last_name)) || ', ' || UPPER(TRIM(first_name)) AS name,
                    STRING_AGG(DISTINCT source_table, ',' ORDER BY source_table) AS sources,
                    NULL AS entity_id
                FROM lake.federal.name_index
                WHERE last_name IS NOT NULL AND LENGTH(last_name) >= 2
                  AND first_name IS NOT NULL AND LENGTH(first_name) >= 1
                GROUP BY UPPER(TRIM(last_name)), UPPER(TRIM(first_name))
            """)
        total = conn.execute("SELECT COUNT(*) FROM _distinct_names").fetchone()[0]
        context.log.info("Distinct names: %s", f"{total:,}")

        # Step 2: Check existing Lance names for incremental updates
        existing_names: set[str] = set()
        lance_path = Path(LANCE_PATH)
        if lance_path.exists():
            try:
                existing_names = {
                    r[0] for r in conn.execute(
                        f"SELECT name FROM '{LANCE_PATH}'"
                    ).fetchall()
                }
                context.log.info("Existing embeddings: %s names", f"{len(existing_names):,}")
            except Exception as e:
                context.log.warning("Could not read existing Lance table: %s", e)

        # Step 3: Initialize embedder
        embed_fn, embed_batch_fn, dims = create_embedder()
        context.log.info("Embedder ready (dims=%d)", dims)

        # Step 4: Process in chunks — write each chunk to Lance immediately
        # DO NOT accumulate all_records in memory (14M x 256 floats = 14GB OOM)
        embedded_count = 0
        skipped_count = 0
        offset = 0
        lance_initialized = lance_path.exists() and bool(existing_names)

        lance_path.parent.mkdir(parents=True, exist_ok=True)

        while offset < total:
            chunk = conn.execute(f"""
                SELECT name, sources, entity_id FROM _distinct_names
                LIMIT {CHUNK_SIZE} OFFSET {offset}
            """).fetchall()

            new_rows = [(n, s, e) for n, s, e in chunk if n not in existing_names]
            skipped_count += len(chunk) - len(new_rows)

            if new_rows:
                names = [r[0] for r in new_rows]
                sources = [r[1] for r in new_rows]
                entity_ids = [str(r[2]) if r[2] else None for r in new_rows]
                context.log.info(
                    "Embedding chunk at offset %s: %s new names (%s skipped)...",
                    f"{offset:,}", f"{len(names):,}", f"{len(chunk) - len(new_rows):,}",
                )
                vecs = embed_batch_fn(names)

                # Write this chunk to Lance via DuckDB's native extension
                chunk_table = pa.table({
                    "name": pa.array(names, type=pa.utf8()),
                    "sources": pa.array(sources, type=pa.utf8()),
                    "entity_id": pa.array(entity_ids, type=pa.utf8()),
                    "embedding": pa.array(
                        [v.tolist() for v in vecs],
                        type=pa.list_(pa.float32(), dims),
                    ),
                })

                mode = "append" if lance_initialized else "overwrite"
                conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_emb AS SELECT * FROM chunk_table")
                conn.execute(
                    f"COPY _tmp_emb TO '{LANCE_PATH}' (FORMAT lance, mode '{mode}')"
                )
                conn.execute("DROP TABLE IF EXISTS _tmp_emb")
                lance_initialized = True

                embedded_count += len(names)
                # Add to existing set so subsequent chunks don't re-embed
                existing_names.update(names)

            offset += CHUNK_SIZE
            context.log.info(
                "Progress: %s/%s processed (%s embedded, %s skipped)",
                f"{min(offset, total):,}", f"{total:,}",
                f"{embedded_count:,}", f"{skipped_count:,}",
            )

        if embedded_count == 0:
            context.log.info("No new names to embed — Lance table is up to date")

        elapsed = time.time() - t_start
        total_in_lance = len(existing_names)  # already includes newly added names
        context.log.info(
            "Done in %.1fs: %s total embeddings (%s new, %s existing)",
            elapsed, f"{total_in_lance:,}", f"{embedded_count:,}", f"{len(existing_names):,}",
        )

        return MaterializeResult(
            metadata={
                "total_distinct_names": MetadataValue.int(total),
                "newly_embedded": MetadataValue.int(embedded_count),
                "skipped_existing": MetadataValue.int(skipped_count),
                "total_in_lance": MetadataValue.int(total_in_lance),
                "lance_local_path": MetadataValue.text(LANCE_PATH),
                "rsync_command": MetadataValue.text(
                    f"rsync -avz -e 'ssh -i ~/.ssh/id_ed25519_hetzner' {LANCE_LOCAL_DIR}/ fattie@178.156.228.119:/mnt/data/common-ground/lance/"
                ),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
