"""Socrata @asset functions — direct DuckDB ingestion path (no dlt).

287 assets, one per dataset. Each asset:
  - Full replace on first run (no cursor in _pipeline_state)
  - Delta merge on subsequent runs (cursor = last_updated_at from _pipeline_state)
"""
import math
import os
import logging

import dagster as dg

from dagster_pipeline.resources.ducklake import DuckLakeResource
from dagster_pipeline.ingestion.fetcher import parallel_fetch_json, stream_fetch_pages
from dagster_pipeline.ingestion.socrata import (
    build_page_urls,
    get_row_count,
    DOMAINS,
    PAGE_SIZE,
)
from dagster_pipeline.ingestion.writer import (
    write_full_replace, write_streaming_replace, write_streaming_merge, write_delta_merge, update_cursor, touch_cursor,
)
from dagster_pipeline.defs.name_index_asset import _connect_ducklake
from dagster_pipeline.sources.datasets import SOCRATA_DATASETS

logger = logging.getLogger(__name__)

MAX_WORKERS = 10


def _get_cursor(conn, dataset_name: str) -> str | None:
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
    return str(ts) if ts is not None else None


def _make_socrata_asset(schema: str, table_name: str, dataset_id: str, domain: str):
    @dg.asset(
        key=dg.AssetKey([schema, table_name]),
        group_name=schema,
        op_tags={"schema": schema},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        ducklake: DuckLakeResource,
    ) -> dg.MaterializeResult:
        import httpx as _httpx
        token = os.environ.get("SOURCES__SOCRATA__APP_TOKEN", "")
        dataset_key = f"{schema}.{table_name}"
        conn = ducklake.get_connection()

        try:
            cursor = _get_cursor(conn, dataset_key)
            is_initial = cursor is None or cursor.startswith("1970-01-01")

            if not is_initial:
                # Socrata needs ISO format with T separator, not space
                cursor_iso = cursor.replace(" ", "T") if cursor else cursor
                where_clause = f":updated_at > '{cursor_iso}'"
                try:
                    row_count = get_row_count(domain, dataset_id, token, where=where_clause)
                except _httpx.HTTPStatusError as e:
                    if e.response.status_code in (404, 403):
                        context.log.warning("%s: %d from Socrata — skipping", dataset_key, e.response.status_code)
                        return dg.MaterializeResult(metadata={"rows": 0, "error": str(e.response.status_code)})
                    raise

                if row_count == 0:
                    context.log.info("%s: no new rows since %s", dataset_key, cursor)
                    touch_cursor(conn, dataset_key)
                    return dg.MaterializeResult(metadata={
                        "rows": dg.MetadataValue.int(0),
                        "dataset_id": dg.MetadataValue.text(dataset_id),
                        "domain": dg.MetadataValue.text(domain),
                        "mode": dg.MetadataValue.text("delta_skip"),
                    })

                num_pages = math.ceil(row_count / PAGE_SIZE)
                urls = build_page_urls(domain, dataset_id, token, num_pages, where=where_clause)
                # Check which ID column exists in the target table
                try:
                    existing_cols = {r[0] for r in conn.execute(f"DESCRIBE lake.{schema}.{table_name}").fetchall()}
                    merge_key = ":id" if ":id" in existing_cols else "_id"
                except Exception:
                    merge_key = "_id"
                context.log.info("%s: delta merge — %d rows -> %d pages (streaming upsert on %s)", dataset_key, row_count, num_pages, merge_key)
                written = write_streaming_merge(
                    conn, schema, table_name,
                    stream_fetch_pages(urls, max_workers=MAX_WORKERS),
                    merge_key=merge_key,
                    total_rows=row_count,
                )

            else:
                try:
                    row_count = get_row_count(domain, dataset_id, token)
                except _httpx.HTTPStatusError as e:
                    if e.response.status_code in (404, 403):
                        context.log.warning("%s: %d from Socrata — skipping", dataset_key, e.response.status_code)
                        return dg.MaterializeResult(metadata={"rows": 0, "error": str(e.response.status_code)})
                    raise
                if row_count == 0:
                    context.log.warning("%s: 0 rows on Socrata", dataset_key)
                    update_cursor(conn, dataset_key, 0)
                    return dg.MaterializeResult(metadata={
                        "rows": dg.MetadataValue.int(0),
                        "dataset_id": dg.MetadataValue.text(dataset_id),
                        "domain": dg.MetadataValue.text(domain),
                        "mode": dg.MetadataValue.text("initial_empty"),
                    })

                num_pages = math.ceil(row_count / PAGE_SIZE)
                urls = build_page_urls(domain, dataset_id, token, num_pages)
                context.log.info("%s: initial load — %d rows -> %d pages (streaming)", dataset_key, row_count, num_pages)
                written = write_streaming_replace(
                    conn, schema, table_name,
                    stream_fetch_pages(urls, max_workers=MAX_WORKERS),
                    total_rows=row_count,
                )

            update_cursor(conn, dataset_key, written)

            return dg.MaterializeResult(metadata={
                "rows": dg.MetadataValue.int(written),
                "dataset_id": dg.MetadataValue.text(dataset_id),
                "domain": dg.MetadataValue.text(domain),
                "mode": dg.MetadataValue.text("delta" if not is_initial else "full_replace"),
            })

        finally:
            conn.close()

    return _asset


def _build_socrata_assets() -> list:
    assets = []
    for schema, datasets in SOCRATA_DATASETS.items():
        for table_name, dataset_id, domain_key in datasets:
            assets.append(_make_socrata_asset(schema, table_name, dataset_id, DOMAINS[domain_key]))
    return assets


all_socrata_direct_assets = _build_socrata_assets()
