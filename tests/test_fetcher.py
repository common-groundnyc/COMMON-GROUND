"""Tests for parallel HTTP fetcher."""
from unittest.mock import patch, MagicMock

import pyarrow as pa
import pytest

from dagster_pipeline.ingestion.fetcher import fetch_json_page, parallel_fetch_json


SOCRATA_URLS = [
    "https://data.cityofnewyork.us/resource/c3uy-2p5r.json?$limit=100&$offset=0&$order=:id",
    "https://data.cityofnewyork.us/resource/c3uy-2p5r.json?$limit=100&$offset=100&$order=:id",
]


def test_parallel_fetch_returns_arrow():
    result = parallel_fetch_json(SOCRATA_URLS, max_workers=2)
    assert result is not None
    assert isinstance(result, pa.Table)
    assert result.num_rows == 200


def test_parallel_fetch_empty_returns_none():
    empty_response = MagicMock()
    empty_response.raise_for_status = MagicMock()
    empty_response.json.return_value = []

    with patch("dagster_pipeline.ingestion.fetcher._CLIENT") as mock_client:
        mock_client.get.return_value = empty_response
        result = parallel_fetch_json(["https://example.com/empty"], max_workers=1)

    assert result is None
