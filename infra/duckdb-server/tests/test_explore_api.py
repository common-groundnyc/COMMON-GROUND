import pytest
from api.explore import classify_column, FilterType, build_filtered_query


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


class TestBuildFilteredQuery:
    def test_no_filters_returns_select_with_limit(self):
        sql, params = build_filtered_query("housing.hpd_violations", {}, None, "desc", 1, 20)
        assert "SELECT *" in sql
        assert "lake.housing.hpd_violations" in sql
        assert "LIMIT" in sql
        assert "OFFSET" in sql
        assert params == [20, 0]

    def test_borough_filter(self):
        sql, params = build_filtered_query("housing.hpd_violations", {"borough": "MANHATTAN"}, None, "desc", 1, 20)
        assert '"borough" = ?' in sql
        assert params[0] == "MANHATTAN"

    def test_date_after_filter(self):
        sql, params = build_filtered_query("housing.hpd_violations", {"after:inspection_date": "2024-01-01"}, None, "desc", 1, 20)
        assert '"inspection_date" >= ?' in sql
        assert params[0] == "2024-01-01"

    def test_date_before_filter(self):
        sql, params = build_filtered_query("housing.hpd_violations", {"before:inspection_date": "2025-01-01"}, None, "desc", 1, 20)
        assert '"inspection_date" <= ?' in sql
        assert params[0] == "2025-01-01"

    def test_text_search_filter(self):
        sql, params = build_filtered_query("housing.hpd_violations", {"q:address": "350 5TH"}, None, "desc", 1, 20)
        assert "ILIKE" in sql
        assert params[0] == "%350 5TH%"

    def test_numeric_min_filter(self):
        sql, params = build_filtered_query("housing.hpd_violations", {"min:penalty_amount": "1000"}, None, "desc", 1, 20)
        assert '"penalty_amount" >= ?' in sql

    def test_numeric_max_filter(self):
        sql, params = build_filtered_query("housing.hpd_violations", {"max:penalty_amount": "5000"}, None, "desc", 1, 20)
        assert '"penalty_amount" <= ?' in sql

    def test_sort_asc(self):
        sql, _ = build_filtered_query("housing.hpd_violations", {}, "inspection_date", "asc", 1, 20)
        assert 'ORDER BY "inspection_date" ASC' in sql

    def test_sort_desc(self):
        sql, _ = build_filtered_query("housing.hpd_violations", {}, "borough", "desc", 1, 20)
        assert 'ORDER BY "borough" DESC' in sql

    def test_pagination_page_2(self):
        sql, params = build_filtered_query("housing.hpd_violations", {}, None, "desc", 2, 20)
        assert params[-1] == 20  # OFFSET = (2-1) * 20

    def test_multiple_filters_combined(self):
        sql, params = build_filtered_query(
            "housing.hpd_violations",
            {"borough": "MANHATTAN", "status": "Open", "after:inspection_date": "2024-01-01"},
            "inspection_date", "desc", 1, 20,
        )
        assert sql.count("?") == len(params)
        assert "MANHATTAN" in params
        assert "Open" in params
        assert "2024-01-01" in params

    def test_rejects_invalid_table_name(self):
        with pytest.raises(ValueError, match="Invalid table"):
            build_filtered_query("housing; DROP TABLE --", {}, None, "desc", 1, 20)

    def test_rejects_invalid_column_in_filter(self):
        with pytest.raises(ValueError, match="Invalid column"):
            build_filtered_query("housing.hpd_violations", {"Robert'; DROP TABLE students--": "x"}, None, "desc", 1, 20)
