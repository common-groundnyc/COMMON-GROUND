import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from source_links import (
    DATASET_URLS,
    DOMAINS,
    build_source_url,
    parse_bbl,
    build_bbl_urls,
)


class TestDatasetUrls:
    def test_hpd_violations_present(self):
        assert "hpd_violations" in DATASET_URLS

    def test_restaurant_inspections_present(self):
        assert "restaurant_inspections" in DATASET_URLS

    def test_n311_present(self):
        assert "n311_service_requests" in DATASET_URLS

    def test_nys_corporations_present(self):
        assert "nys_corporations" in DATASET_URLS

    def test_hpd_violations_has_key_col(self):
        assert DATASET_URLS["hpd_violations"]["key_col"] == "violationid"

    def test_nys_corporations_domain_is_nys(self):
        assert DATASET_URLS["nys_corporations"]["domain"] == "nys"


class TestBuildSourceUrl:
    def test_unknown_table_returns_none(self):
        assert build_source_url("nonexistent_table", {}) is None

    def test_dataset_page_fallback_no_row(self):
        url = build_source_url("hpd_violations", {})
        assert url == "https://data.cityofnewyork.us/d/wvxf-dwi5"

    def test_dataset_page_fallback_missing_key(self):
        url = build_source_url("hpd_violations", {"some_other_col": "123"})
        assert url == "https://data.cityofnewyork.us/d/wvxf-dwi5"

    def test_row_with_key_col_still_returns_data_page(self):
        url = build_source_url("hpd_violations", {"violationid": "456"})
        assert url == "https://data.cityofnewyork.us/d/wvxf-dwi5"

    def test_n311_returns_data_page(self):
        url = build_source_url("n311_service_requests", {"unique_key": "42"})
        assert url == "https://data.cityofnewyork.us/d/erm2-nwe9"

    def test_nys_domain_uses_correct_host(self):
        url = build_source_url("nys_corporations", {})
        assert url == "https://data.ny.gov/d/n9v6-gdp6"

    def test_nys_domain_row_returns_data_page(self):
        url = build_source_url("nys_corporations", {"dos_id": "999"})
        assert url == "https://data.ny.gov/d/n9v6-gdp6"

    def test_table_without_key_col_always_returns_page(self):
        url = build_source_url("evictions", {"anything": "val"})
        assert url == "https://data.cityofnewyork.us/d/6z8x-wfk4"


class TestParseBbl:
    def test_valid_10_digit_bbl(self):
        result = parse_bbl("1000670001")
        assert result == ("1", "00067", "0001")

    def test_valid_brooklyn_bbl(self):
        result = parse_bbl("3001230045")
        assert result == ("3", "00123", "0045")

    def test_too_short_returns_none(self):
        assert parse_bbl("123456789") is None

    def test_too_long_returns_none(self):
        assert parse_bbl("12345678901") is None

    def test_non_numeric_returns_none(self):
        assert parse_bbl("100067000X") is None

    def test_empty_returns_none(self):
        assert parse_bbl("") is None


class TestBuildBblUrls:
    def test_invalid_bbl_returns_empty_list(self):
        assert build_bbl_urls("invalid") == []

    def test_returns_three_entries(self):
        urls = build_bbl_urls("1000670001")
        assert len(urls) == 3

    def test_hpd_violations_link_present(self):
        urls = build_bbl_urls("1000670001")
        hpd = next((u for u in urls if "HPD" in u["name"]), None)
        assert hpd is not None
        assert "data.cityofnewyork.us" in hpd["url"]

    def test_dob_bis_present(self):
        urls = build_bbl_urls("1000670001")
        dob = next((u for u in urls if u["name"] == "DOB BIS"), None)
        assert dob is not None

    def test_dob_bis_uses_zero_padded(self):
        urls = build_bbl_urls("1000670001")
        dob = next(u for u in urls if u["name"] == "DOB BIS")
        assert "block=00067" in dob["url"]
        assert "lot=0001" in dob["url"]

    def test_acris_present(self):
        urls = build_bbl_urls("1000670001")
        acris = next((u for u in urls if "ACRIS" in u["name"]), None)
        assert acris is not None

    def test_acris_uses_zero_padded(self):
        urls = build_bbl_urls("1000670001")
        acris = next(u for u in urls if "ACRIS" in u["name"])
        assert "block=00067" in acris["url"]
        assert "lot=0001" in acris["url"]

    def test_all_entries_have_name_and_url(self):
        urls = build_bbl_urls("3001230045")
        for entry in urls:
            assert "name" in entry
            assert "url" in entry
            assert entry["url"].startswith("http")
