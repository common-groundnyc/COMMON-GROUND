"""Dagster asset for entity name embeddings — embeds distinct person names
from the name_index into a Lance vector index for semantic search.

Reads (last_name, first_name) pairs from lake.federal.name_index,
embeds via Gemini API, writes to /data/common-ground/lance/entity_name_embeddings.lance.
"""
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)

LANCE_PATH = "/data/common-ground/lance/entity_name_embeddings.lance"
CHUNK_SIZE = 100_000

# Add embedder directory to path so we can import it
_EMBEDDER_DIR = str(Path(__file__).resolve().parents[4] / "infra" / "duckdb-server")
if _EMBEDDER_DIR not in sys.path:
    sys.path.insert(0, _EMBEDDER_DIR)


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
                db = lancedb.connect(str(lance_path.parent))
                tbl = db.open_table(lance_path.stem)
                existing_names = set(tbl.to_pandas()["name"].tolist())
                context.log.info("Existing embeddings: %s names", f"{len(existing_names):,}")
            except Exception as e:
                context.log.warning("Could not read existing Lance table: %s", e)

        # Step 3: Initialize embedder
        embed_fn, embed_batch_fn, dims = create_embedder()
        context.log.info("Embedder ready (dims=%d)", dims)

        # Step 4: Process in chunks
        embedded_count = 0
        skipped_count = 0
        offset = 0
        all_records: list[dict] = []

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

                for i, (name, src) in enumerate(zip(names, sources)):
                    all_records.append({
                        "name": name,
                        "sources": src,
                        "embedding": vecs[i].tolist(),
                    })
                embedded_count += len(names)

            offset += CHUNK_SIZE
            context.log.info(
                "Progress: %s/%s processed (%s embedded, %s skipped)",
                f"{min(offset, total):,}", f"{total:,}",
                f"{embedded_count:,}", f"{skipped_count:,}",
            )

        # Step 5: Write to Lance
        if all_records:
            lance_path.parent.mkdir(parents=True, exist_ok=True)
            db = lancedb.connect(str(lance_path.parent))

            table = pa.table({
                "name": pa.array([r["name"] for r in all_records], type=pa.utf8()),
                "sources": pa.array([r["sources"] for r in all_records], type=pa.utf8()),
                "embedding": pa.array(
                    [r["embedding"] for r in all_records],
                    type=pa.list_(pa.float32(), dims),
                ),
            })

            if lance_path.exists() and existing_names:
                tbl = db.open_table(lance_path.stem)
                tbl.add(table)
                context.log.info("Appended %s new embeddings to existing Lance table", f"{embedded_count:,}")
            else:
                db.create_table(lance_path.stem, data=table, mode="overwrite")
                context.log.info("Created new Lance table with %s embeddings", f"{embedded_count:,}")
        else:
            context.log.info("No new names to embed — Lance table is up to date")

        elapsed = time.time() - t_start
        total_in_lance = embedded_count + len(existing_names)
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
                "lance_path": MetadataValue.text(LANCE_PATH),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
