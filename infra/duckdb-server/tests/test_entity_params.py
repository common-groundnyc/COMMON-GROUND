from entity import (
    phonetic_search_sql,
    fuzzy_name_sql,
    phonetic_vital_search_sql,
    fuzzy_money_search_sql,
    _validate_identifier,
)
import pytest


def test_phonetic_search_returns_tuple_with_params():
    sql, params = phonetic_search_sql(first_name="John", last_name="Smith")
    assert isinstance(sql, str)
    assert isinstance(params, list)
    assert "?" in sql
    assert "Smith" in params
    assert "John" in params
    assert "'Smith'" not in sql
    assert "'John'" not in sql


def test_phonetic_search_last_only():
    sql, params = phonetic_search_sql(first_name=None, last_name="O'Brien")
    assert "?" in sql
    assert "O'Brien" in params
    assert "'O'Brien'" not in sql


def test_fuzzy_name_returns_tuple():
    sql, params = fuzzy_name_sql(name="BLACKSTONE")
    assert isinstance(params, list)
    assert "BLACKSTONE" in params
    assert "'BLACKSTONE'" not in sql


def test_phonetic_vital_returns_tuple():
    sql, params = phonetic_vital_search_sql(
        first_name="Maria", last_name="Garcia",
        table="lake.housing.nys_death_index",
        first_col="first_name", last_col="last_name",
    )
    assert "?" in sql
    assert "Garcia" in params


def test_fuzzy_money_returns_tuple():
    sql, params = fuzzy_money_search_sql(
        name="KUSHNER",
        table="lake.city_government.campaign_contributions",
        name_col="name",
    )
    assert "?" in sql
    assert "KUSHNER" in params


def test_validate_identifier_good():
    assert _validate_identifier("lake.housing.hpd_violations") == "lake.housing.hpd_violations"
    assert _validate_identifier("first_name") == "first_name"


def test_validate_identifier_bad():
    with pytest.raises(ValueError):
        _validate_identifier("'; DROP TABLE --")
    with pytest.raises(ValueError):
        _validate_identifier("name; DELETE")
    with pytest.raises(ValueError):
        _validate_identifier("")
