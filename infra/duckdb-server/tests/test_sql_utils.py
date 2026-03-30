import pytest
from sql_utils import validate_identifier


def test_valid_simple_column():
    assert validate_identifier("first_name") == "first_name"


def test_valid_dotted_table():
    assert validate_identifier("lake.housing.hpd_violations") == "lake.housing.hpd_violations"


def test_rejects_sql_injection():
    with pytest.raises(ValueError):
        validate_identifier("'; DROP TABLE --")


def test_rejects_semicolon():
    with pytest.raises(ValueError):
        validate_identifier("name; DELETE")


def test_rejects_empty():
    with pytest.raises(ValueError):
        validate_identifier("")


def test_rejects_leading_number():
    with pytest.raises(ValueError):
        validate_identifier("1table")
