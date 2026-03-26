"""Tests for Socrata URL builder, row counts, and column projection."""
from urllib.parse import urlparse, parse_qs

import pytest

from dagster_pipeline.ingestion.socrata import build_page_urls, get_row_count, PAGE_SIZE

DOMAIN = "data.cityofnewyork.us"
DATASET_ID = "c3uy-2p5r"  # air_quality
TOKEN = "test_token"


def test_build_page_urls_count():
    urls = build_page_urls(DOMAIN, DATASET_ID, TOKEN, num_pages=5)
    assert len(urls) == 5


def test_build_page_urls_params():
    urls = build_page_urls(DOMAIN, DATASET_ID, TOKEN, num_pages=3)
    for i, url in enumerate(urls):
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        assert qs["$limit"] == [str(PAGE_SIZE)]
        assert qs["$offset"] == [str(i * PAGE_SIZE)]
        assert qs["$order"] == [":id"]
        assert qs["$$app_token"] == [TOKEN]


def test_build_page_urls_with_select():
    select = "unique_key,created_date,borough"
    urls = build_page_urls(DOMAIN, DATASET_ID, TOKEN, num_pages=2, select=select)
    for url in urls:
        qs = parse_qs(urlparse(url).query)
        assert qs["$select"] == [select]


def test_build_page_urls_with_where():
    where = "date_of_interest >= '2024-01-01'"
    urls = build_page_urls(DOMAIN, DATASET_ID, TOKEN, num_pages=2, where=where)
    for url in urls:
        qs = parse_qs(urlparse(url).query)
        assert qs["$where"] == [where]


def test_build_page_urls_no_select_no_where():
    urls = build_page_urls(DOMAIN, DATASET_ID, TOKEN, num_pages=1)
    qs = parse_qs(urlparse(urls[0]).query)
    assert "$select" not in qs
    assert "$where" not in qs


@pytest.mark.live
def test_get_row_count_live():
    """Hit Socrata air_quality dataset; requires network access."""
    import os
    token = os.environ.get("SOCRATA_TOKEN", "")
    count = get_row_count("data.cityofnewyork.us", "c3uy-2p5r", token)
    assert count > 10_000, f"Expected >10000 rows, got {count}"
