import pytest
from api.explore import classify_column, FilterType


class TestClassifyColumn:
    def test_borough_varchar_is_geography(self):
        assert classify_column("borough", "VARCHAR") == FilterType.GEOGRAPHY

    def test_boro_varchar_is_geography(self):
        assert classify_column("boro", "VARCHAR") == FilterType.GEOGRAPHY

    def test_zipcode_varchar_is_geography(self):
        assert classify_column("zipcode", "VARCHAR") == FilterType.GEOGRAPHY

    def test_zip_code_varchar_is_geography(self):
        assert classify_column("zip_code", "VARCHAR") == FilterType.GEOGRAPHY

    def test_incident_zip_is_geography(self):
        assert classify_column("incident_zip", "VARCHAR") == FilterType.GEOGRAPHY

    def test_borocode_integer_is_geography(self):
        assert classify_column("borocode", "INTEGER") == FilterType.GEOGRAPHY

    def test_inspection_date_is_date_range(self):
        assert classify_column("inspection_date", "DATE") == FilterType.DATE_RANGE

    def test_created_at_timestamp_is_date_range(self):
        assert classify_column("created_at", "TIMESTAMP") == FilterType.DATE_RANGE

    def test_closed_date_is_date_range(self):
        assert classify_column("closed_date", "DATE") == FilterType.DATE_RANGE

    def test_penalty_amount_double_is_numeric(self):
        assert classify_column("penalty_amount", "DOUBLE") == FilterType.NUMERIC_RANGE

    def test_total_count_integer_is_numeric(self):
        assert classify_column("total_count", "INTEGER") == FilterType.NUMERIC_RANGE

    def test_score_double_is_numeric(self):
        assert classify_column("score", "DOUBLE") == FilterType.NUMERIC_RANGE

    def test_violation_id_bigint_is_none(self):
        assert classify_column("violation_id", "BIGINT") is None

    def test_random_varchar_is_none(self):
        assert classify_column("some_field", "VARCHAR") is None

    def test_address_varchar_is_text_search(self):
        assert classify_column("address", "VARCHAR") == FilterType.TEXT_SEARCH

    def test_owner_name_is_text_search(self):
        assert classify_column("owner_name", "VARCHAR") == FilterType.TEXT_SEARCH

    def test_complaint_type_varchar_is_category(self):
        assert classify_column("complaint_type", "VARCHAR") == FilterType.CATEGORY

    def test_status_varchar_is_category(self):
        assert classify_column("status", "VARCHAR") == FilterType.CATEGORY

    def test_grade_varchar_is_category(self):
        assert classify_column("grade", "VARCHAR") == FilterType.CATEGORY

    # Additional edge cases
    def test_zip_suffix_is_geography(self):
        assert classify_column("mailing_zip", "VARCHAR") == FilterType.GEOGRAPHY

    def test_varchar_date_suffix_is_date_range(self):
        assert classify_column("inspection_date", "VARCHAR") == FilterType.DATE_RANGE

    def test_numeric_type_unrecognized_name_is_none(self):
        assert classify_column("violation_id", "INTEGER") is None

    def test_category_name_on_integer_is_none(self):
        # category only matches VARCHAR
        assert classify_column("status", "INTEGER") is None

    def test_text_search_name_on_integer_is_none(self):
        # text search only matches VARCHAR
        assert classify_column("address", "INTEGER") is None

    def test_year_built_integer_is_numeric(self):
        assert classify_column("year_built", "INTEGER") == FilterType.NUMERIC_RANGE

    def test_units_integer_is_numeric(self):
        assert classify_column("units", "INTEGER") == FilterType.NUMERIC_RANGE
