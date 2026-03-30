"""Dagster asset for entity name embeddings — embeds distinct person names
from the name_index into a Lance vector index for semantic search.

Reads (last_name, first_name) pairs from lake.federal.name_index,
embeds via Gemini API, writes to /data/common-ground/lance/entity_name_embeddings.lance.
"""
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np
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
    deps=[AssetKey(["federal", "name_index"])],
    description="Lance vector index of embedded entity names from name_index for semantic search.",
    compute_kind="lance",
)
def entity_name_embeddings(context) -> MaterializeResult:
    """Embed distinct names from name_index into a Lance vector index."""
    import lancedb

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

    try:
        # Step 1: Materialize distinct names into a temp table (avoid OOM on 14M rows)
        context.log.info("Creating temp table of distinct names...")
        conn.execute("""
            CREATE TEMP TABLE _distinct_names AS
            SELECT
                UPPER(TRIM(last_name)) || ', ' || UPPER(TRIM(first_name)) AS name,
                STRING_AGG(DISTINCT source_table, ',' ORDER BY source_table) AS sources
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
                db = lancedb.connect(LANCE_LOCAL_DIR)
                tbl = db.open_table(LANCE_TABLE_NAME)
                # Only read name column — loading embeddings would OOM
                existing_names = set(
                    tbl.to_lance().scanner(columns=["name"]).to_table()["name"].to_pylist()
                )
                context.log.info("Existing embeddings: %s names", f"{len(existing_names):,}")
            except Exception as e:
                context.log.warning("Could not read existing Lance table: %s", e)

        # Step 3: Initialize embedder
        embed_fn, embed_batch_fn, dims = create_embedder()
        context.log.info("Embedder ready (dims=%d)", dims)

        # Step 4: Process in chunks — write each chunk to Lance immediately
        # DO NOT accumulate all_records in memory (14M × 768 floats = 42GB OOM)
        embedded_count = 0
        skipped_count = 0
        offset = 0
        lance_initialized = lance_path.exists() and bool(existing_names)

        lance_path.parent.mkdir(parents=True, exist_ok=True)
        lance_db = lancedb.connect(LANCE_LOCAL_DIR)

        while offset < total:
            chunk = conn.execute(f"""
                SELECT name, sources FROM _distinct_names
                LIMIT {CHUNK_SIZE} OFFSET {offset}
            """).fetchall()

            new_rows = [(n, s) for n, s in chunk if n not in existing_names]
            skipped_count += len(chunk) - len(new_rows)

            if new_rows:
                names = [r[0] for r in new_rows]
                sources = [r[1] for r in new_rows]
                context.log.info(
                    "Embedding chunk at offset %s: %s new names (%s skipped)...",
                    f"{offset:,}", f"{len(names):,}", f"{len(chunk) - len(new_rows):,}",
                )
                vecs = embed_batch_fn(names)

                # Write this chunk to Lance immediately (don't accumulate)
                chunk_table = pa.table({
                    "name": pa.array(names, type=pa.utf8()),
                    "sources": pa.array(sources, type=pa.utf8()),
                    "embedding": pa.array(
                        [v.tolist() for v in vecs],
                        type=pa.list_(pa.float32(), dims),
                    ),
                })

                if lance_initialized:
                    lance_tbl = lance_db.open_table(LANCE_TABLE_NAME)
                    lance_tbl.add(chunk_table)
                else:
                    lance_db.create_table(LANCE_TABLE_NAME, data=chunk_table, mode="overwrite")
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
