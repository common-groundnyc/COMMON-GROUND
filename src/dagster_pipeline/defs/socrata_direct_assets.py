"""Socrata @asset functions — direct DuckDB ingestion path (no dlt).

208 assets, one per dataset. Each asset:
  - Full replace on first run (no cursor in _pipeline_state)
  - Delta merge on subsequent runs (cursor = last_updated_at from _pipeline_state)
"""
import math
import os
import logging

import dagster as dg

from dagster_pipeline.resources.ducklake import DuckLakeResource
from dagster_pipeline.ingestion.fetcher import parallel_fetch_json
from dagster_pipeline.ingestion.socrata import (
    build_page_urls,
    get_row_count,
    DOMAINS,
    PAGE_SIZE,
)
from dagster_pipeline.ingestion.writer import write_full_replace, write_delta_merge, update_cursor
from dagster_pipeline.sources.datasets import SOCRATA_DATASETS

logger = logging.getLogger(__name__)

MAX_WORKERS = 10


def _get_cursor(conn, dataset_name: str) -> str | None:
    """Return the stored cursor timestamp string, or None if not set."""
    try:
        rows = conn.execute(
            "SELECT last_updated_at FROM lake._pipeline_state WHERE dataset_name = ?",
            [dataset_name],
        ).fetchall()
    except Exception:
        return None
    if not rows:
        return None
    ts = rows[0][0]
    if ts is None:
        return None
    # Format as Socrata-compatible ISO timestamp string
    return str(ts)


def _build_socrata_assets() -> list:
    assets = []

    for schema, datasets in SOCRATA_DATASETS.items():
        for table_name, dataset_id, domain_key in datasets:
            domain = DOMAINS[domain_key]

            @dg.asset(
                key=dg.AssetKey([schema, table_name]),
                group_name=schema,
                tags={"schema": schema},
            )
            def _asset(
                context: dg.AssetExecutionContext,
                ducklake: DuckLakeResource,
                _domain: str = domain,
                _dataset_id: str = dataset_id,
                _schema: str = schema,
                _table_name: str = table_name,
            ) -> dg.MaterializeResult:
                token = os.environ.get("SOURCES__SOCRATA__APP_TOKEN", "")
                dataset_key = f"{_schema}.{_table_name}"
                conn = ducklake.get_connection()

                try:
                    cursor = _get_cursor(conn, dataset_key)

                    # Treat the epoch sentinel as "no cursor"
                    is_initial = (
                        cursor is None
                        or cursor.startswith("1970-01-01")
                    )

                    if not is_initial:
                        # Delta sync: only rows updated since last run
                        where_clause = f":updated_at > '{cursor}'"
                        row_count = get_row_count(_domain, _dataset_id, token, where=where_clause)

                        if row_count == 0:
                            context.log.info(
                                "%s: no new rows since %s — skipping", dataset_key, cursor
                            )
                            return dg.MaterializeResult(
                                metadata={
                                    "rows": dg.MetadataValue.int(0),
                                    "dataset_id": dg.MetadataValue.text(_dataset_id),
                                    "domain": dg.MetadataValue.text(_domain),
                                    "mode": dg.MetadataValue.text("delta_skip"),
                                }
                            )

                        num_pages = math.ceil(row_count / PAGE_SIZE)
                        urls = build_page_urls(
                            _domain, _dataset_id, token, num_pages, where=where_clause
                        )
                        context.log.info(
                            "%s: delta sync — %d new rows -> %d pages",
                            dataset_key, row_count, num_pages,
                        )
                        arrow_table = parallel_fetch_json(urls, max_workers=MAX_WORKERS)

                        if arrow_table is None:
                            update_cursor(conn, dataset_key, 0)
                            return dg.MaterializeResult(
                                metadata={
                                    "rows": dg.MetadataValue.int(0),
                                    "dataset_id": dg.MetadataValue.text(_dataset_id),
                                    "domain": dg.MetadataValue.text(_domain),
                                    "mode": dg.MetadataValue.text("delta_empty"),
                                }
                            )

                        # Socrata returns `:id` with a literal colon in the JSON key
                        merge_key = ":id" if ":id" in arrow_table.schema.names else "_id"
                        written = write_delta_merge(
                            conn, _schema, _table_name, arrow_table, merge_key=merge_key
                        )

                    else:
                        # Initial full load
                        row_count = get_row_count(_domain, _dataset_id, token)
                        if row_count == 0:
                            context.log.warning(
                                "%s: 0 rows on Socrata — nothing to load", dataset_key
                            )
                            update_cursor(conn, dataset_key, 0)
                            return dg.MaterializeResult(
                                metadata={
                                    "rows": dg.MetadataValue.int(0),
                                    "dataset_id": dg.MetadataValue.text(_dataset_id),
                                    "domain": dg.MetadataValue.text(_domain),
                                    "mode": dg.MetadataValue.text("initial_empty"),
                                }
                            )

                        num_pages = math.ceil(row_count / PAGE_SIZE)
                        urls = build_page_urls(_domain, _dataset_id, token, num_pages)
                        context.log.info(
                            "%s: initial load — %d rows -> %d pages",
                            dataset_key, row_count, num_pages,
                        )
                        arrow_table = parallel_fetch_json(urls, max_workers=MAX_WORKERS)

                        if arrow_table is None:
                            update_cursor(conn, dataset_key, 0)
                            return dg.MaterializeResult(
                                metadata={
                                    "rows": dg.MetadataValue.int(0),
                                    "dataset_id": dg.MetadataValue.text(_dataset_id),
                                    "domain": dg.MetadataValue.text(_domain),
                                    "mode": dg.MetadataValue.text("initial_empty"),
                                }
                            )

                        written = write_full_replace(conn, _schema, _table_name, arrow_table)

                    update_cursor(conn, dataset_key, written)

                    return dg.MaterializeResult(
                        metadata={
                            "rows": dg.MetadataValue.int(written),
                            "dataset_id": dg.MetadataValue.text(_dataset_id),
                            "domain": dg.MetadataValue.text(_domain),
                            "mode": dg.MetadataValue.text(
                                "delta" if not is_initial else "full_replace"
                            ),
                        }
                    )

                finally:
                    conn.close()

            assets.append(_asset)

    return assets


all_socrata_direct_assets = _build_socrata_assets()
