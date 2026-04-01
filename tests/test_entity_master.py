import pytest
from dagster_pipeline.defs.entity_master_asset import classify_entity_type


@pytest.mark.parametrize("name,expected", [
    ("JOHN SMITH", "PERSON"),
    ("SMITH REALTY LLC", "ORGANIZATION"),
    ("123 MAIN ST HOLDINGS", "ORGANIZATION"),
    ("CITY OF NEW YORK", "ORGANIZATION"),
    ("ABC CORP", "ORGANIZATION"),
    ("MARY O'BRIEN", "PERSON"),
    ("VAN DER BERG", "PERSON"),
    ("FIRST NATIONAL BANK", "ORGANIZATION"),
    ("GREENPOINT MANAGEMENT", "ORGANIZATION"),
    ("DEPARTMENT OF EDUCATION", "ORGANIZATION"),
    ("SMITH", "PERSON"),
    ("", "UNKNOWN"),
    (None, "UNKNOWN"),
])
def test_classify_entity_type(name, expected):
    assert classify_entity_type(name) == expected
