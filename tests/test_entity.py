"""Tests for entity fuzzy name search builders."""
import sys
sys.path.insert(0, "/Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server")

from entity import phonetic_search_sql, fuzzy_name_sql


def test_phonetic_search_sql_with_first():
    sql = phonetic_search_sql("John", "Smith")
    assert "double_metaphone" in sql
    assert "jaro_winkler_similarity" in sql
    assert "phonetic_index" in sql
    assert "0.75" in sql  # min_score default


def test_phonetic_search_sql_last_only():
    sql = phonetic_search_sql(None, "Smith")
    assert "double_metaphone" in sql
    assert "jaro_winkler_similarity" in sql


def test_fuzzy_name_sql():
    sql = fuzzy_name_sql("BLACKSTONE GROUP")
    assert "rapidfuzz_token_sort_ratio" in sql
    assert "BLACKSTONE GROUP" in sql
    assert "70" in sql  # min_score default


def test_sql_injection_escaped():
    sql = phonetic_search_sql("O'Brien", "O'Malley")
    assert "O''Brien" in sql
    assert "O''Malley" in sql


def test_phonetic_vital_search():
    from entity import phonetic_vital_search_sql
    sql = phonetic_vital_search_sql("John", "Smith",
        table="lake.federal.nys_death_index",
        first_col="first_name", last_col="last_name",
        extra_cols="age, date_of_death")
    assert "soundex" in sql
    assert "jaro_winkler_similarity" in sql
    assert "nys_death_index" in sql


def test_fuzzy_money_search():
    from entity import fuzzy_money_search_sql
    sql = fuzzy_money_search_sql("JOHN SMITH",
        table="lake.federal.fec_contributions",
        name_col="contributor_name",
        extra_cols="committee_id, contribution_receipt_amount")
    assert "rapidfuzz_token_sort_ratio" in sql
    assert "70" in sql
