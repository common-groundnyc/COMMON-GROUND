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
