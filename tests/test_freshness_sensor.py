"""Tests for freshness_sensor pure functions."""
import pytest

from dagster_pipeline.defs.freshness_sensor import (
    build_dataset_manifest,
    compute_sync_status,
)


class TestBuildDatasetManifest:
    def test_returns_more_than_200_entries(self):
        manifest = build_dataset_manifest()
        assert len(manifest) > 200

    def test_each_entry_has_required_keys(self):
        manifest = build_dataset_manifest()
        for entry in manifest:
            assert "schema" in entry
            assert "table_name" in entry
            assert "dataset_id" in entry
            assert "domain" in entry

    def test_domain_is_resolved_url(self):
        manifest = build_dataset_manifest()
        valid_domains = {
            "data.cityofnewyork.us",
            "data.ny.gov",
            "health.data.ny.gov",
            "data.cdc.gov",
        }
        for entry in manifest:
            assert entry["domain"] in valid_domains, (
                f"Unexpected domain '{entry['domain']}' for {entry['table_name']}"
            )

    def test_includes_known_dataset_hpd_violations(self):
        manifest = build_dataset_manifest()
        table_names = {e["table_name"] for e in manifest}
        assert "hpd_violations" in table_names

    def test_schema_is_non_empty_string(self):
        manifest = build_dataset_manifest()
        for entry in manifest:
            assert isinstance(entry["schema"], str) and entry["schema"]

    def test_dataset_id_is_non_empty_string(self):
        manifest = build_dataset_manifest()
        for entry in manifest:
            assert isinstance(entry["dataset_id"], str) and entry["dataset_id"]


class TestComputeSyncStatus:
    def test_equal_counts_is_synced(self):
        assert compute_sync_status(1000, 1000) == "synced"

    def test_within_4_percent_is_synced(self):
        # 4% growth — below 5% threshold
        assert compute_sync_status(1000, 1040) == "synced"

    def test_exactly_5_percent_is_synced(self):
        # exactly 5% growth — threshold is strictly >, so 5% is still synced
        assert compute_sync_status(1000, 1050) == "synced"

    def test_just_over_5_percent_is_stale(self):
        # 5.1% growth — strictly over threshold, triggers stale
        assert compute_sync_status(1000, 1051) == "stale"

    def test_10_percent_diff_is_stale(self):
        assert compute_sync_status(1000, 1100) == "stale"

    def test_lake_zero_source_nonzero_is_stale(self):
        assert compute_sync_status(0, 500) == "stale"

    def test_both_zero_is_synced(self):
        assert compute_sync_status(0, 0) == "synced"

    def test_source_none_is_unknown(self):
        assert compute_sync_status(1000, None) == "unknown"

    def test_lake_larger_than_source_is_synced(self):
        # lake has more rows than source — considered synced
        assert compute_sync_status(1100, 1000) == "synced"

    def test_source_zero_lake_nonzero_is_synced(self):
        # source reports 0 but lake has rows — treat as synced (source <= lake)
        assert compute_sync_status(500, 0) == "synced"
