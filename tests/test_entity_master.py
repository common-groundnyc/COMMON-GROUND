import pytest
from dagster_pipeline.defs.entity_master_asset import classify_entity_type, generate_entity_id


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


def test_entity_id_is_deterministic():
    members_a = ["unique_id_1", "unique_id_2", "unique_id_3"]
    members_b = ["unique_id_3", "unique_id_1", "unique_id_2"]
    assert generate_entity_id(members_a) == generate_entity_id(members_b)


def test_entity_id_differs_for_different_members():
    members_a = ["unique_id_1", "unique_id_2"]
    members_b = ["unique_id_1", "unique_id_3"]
    assert generate_entity_id(members_a) != generate_entity_id(members_b)


def test_entity_id_single_member():
    result = generate_entity_id(["unique_id_42"])
    assert len(str(result)) == 36
    assert "-" in str(result)


def test_entity_id_is_uuid_format():
    import uuid as uuid_mod
    result = generate_entity_id(["a", "b", "c"])
    parsed = uuid_mod.UUID(str(result))
    assert str(parsed) == str(result)
