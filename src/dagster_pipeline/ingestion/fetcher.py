"""Parallel HTTP fetch → Arrow tables. The fast path.

Two modes:
  - parallel_fetch_json: accumulates all pages (small datasets)
  - stream_fetch_pages: yields pages one at a time (large datasets, zero accumulation)
"""
import json
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

import httpx
import pyarrow as pa

logger = logging.getLogger(__name__)

_CLIENT = httpx.Client(timeout=300, headers={"Accept-Encoding": "gzip"}, follow_redirects=True)


def _backoff(attempt: int, base: int = 5, cap: int = 120) -> float:
    """Exponential backoff with jitter. attempt is 0-indexed."""
    return min(cap, base * (2 ** attempt)) + random.uniform(0, 2)


def fetch_json_page(url: str, max_retries: int = 7) -> pa.Table | None:
    """Fetch one JSON page, return as Arrow table.

    Retries on:
      - network/timeout errors (httpx.TimeoutException, NetworkError, RemoteProtocolError)
      - 5xx server errors
      - 429 rate limits (respects Retry-After header when present)
      - JSONDecodeError on truncated/malformed response bodies
    Without these retries a single Socrata hiccup truncates the entire ingest
    at the page that happened to fail, leaving lake counts at multiples of PAGE_SIZE.
    """
    import time as _time
    for attempt in range(max_retries + 1):
        try:
            resp = _CLIENT.get(url)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            if attempt >= max_retries:
                raise
            wait = _backoff(attempt)
            logger.warning("Fetch network error for %s — retry %d in %.1fs: %s",
                           url[:80], attempt + 1, wait, exc)
            _time.sleep(wait)
            continue

        if resp.status_code in (404, 403):
            logger.warning("Fetch %d for %s — skipping", resp.status_code, url[:80])
            return None
        if resp.status_code == 429 and attempt < max_retries:
            retry_after = resp.headers.get("Retry-After")
            try:
                wait = float(retry_after) if retry_after else _backoff(attempt, base=10)
            except ValueError:
                wait = _backoff(attempt, base=10)
            logger.warning("Fetch 429 rate limit for %s — retry %d in %.1fs",
                           url[:80], attempt + 1, wait)
            _time.sleep(wait)
            continue
        if resp.status_code >= 500 and attempt < max_retries:
            wait = _backoff(attempt)
            logger.warning("Fetch %d for %s — retry %d in %.1fs",
                           resp.status_code, url[:80], attempt + 1, wait)
            _time.sleep(wait)
            continue
        resp.raise_for_status()

        try:
            rows = resp.json()
        except json.JSONDecodeError as exc:
            if attempt >= max_retries:
                raise
            wait = _backoff(attempt)
            logger.warning("Fetch JSON decode error for %s — retry %d in %.1fs: %s",
                           url[:80], attempt + 1, wait, exc)
            _time.sleep(wait)
            continue

        if not rows:
            return None
        table = pa.Table.from_pylist(rows)
        del rows
        return table
    return None


def parallel_fetch_json(urls: list[str], max_workers: int = 10) -> pa.Table | None:
    """Fetch multiple URLs in parallel, return concatenated Arrow table.
    Use for small datasets only (<500K rows). For larger, use stream_fetch_pages.
    """
    tables = []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(urls))) as pool:
        futures = {pool.submit(fetch_json_page, url): i for i, url in enumerate(urls)}
        for future in as_completed(futures):
            table = future.result()
            if table:
                tables.append(table)

    if not tables:
        return None
    return pa.concat_tables(tables, promote_options="permissive")


def stream_fetch_pages(urls: list[str], max_workers: int = 10):
    """Yield Arrow tables one page at a time as they complete.
    Pages are fetched in parallel but yielded immediately — only
    max_workers pages in memory at once. Zero accumulation.
    """
    with ThreadPoolExecutor(max_workers=min(max_workers, len(urls))) as pool:
        futures = deque()

        # Submit initial batch
        for url in urls[:max_workers]:
            futures.append(pool.submit(fetch_json_page, url))

        submitted = max_workers
        while futures:
            future = futures.popleft()
            table = future.result()
            if table:
                yield table

            # Submit next URL as each completes
            if submitted < len(urls):
                futures.append(pool.submit(fetch_json_page, urls[submitted]))
                submitted += 1
