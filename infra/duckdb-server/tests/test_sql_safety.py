"""Tests for SQL validation and error sanitization."""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sql_utils import validate_identifier, sanitize_error


class TestValidateIdentifier:
    def test_valid_simple_column(self):
        assert validate_identifier("first_name") == "first_name"

    def test_valid_dotted_table(self):
        assert validate_identifier("lake.housing.hpd_violations") == "lake.housing.hpd_violations"

    def test_rejects_sql_injection(self):
        with pytest.raises(ValueError):
            validate_identifier("'; DROP TABLE --")

    def test_rejects_semicolon(self):
        with pytest.raises(ValueError):
            validate_identifier("name; DELETE")

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            validate_identifier("")


class TestSanitizeError:
    def test_strips_file_paths(self):
        result = sanitize_error("Cannot open file /data/common-ground/lance/entity.lance")
        assert "/data/common-ground" not in result

    def test_strips_password(self):
        result = sanitize_error("Connection failed: password=SuperSecret123 host=postgres")
        assert "SuperSecret123" not in result
        assert "password=***" in result

    def test_preserves_useful_message(self):
        result = sanitize_error("Table 'violations' does not exist")
        assert "does not exist" in result

    def test_strips_connection_string_password(self):
        result = sanitize_error("dbname=ducklake user=dagster password=s3cr3t host=postgres")
        assert "s3cr3t" not in result
        assert "password=***" in result
