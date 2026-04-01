import pytest
from dagster_pipeline.defs.entity_master_asset import (
    classify_entity_type,
    generate_entity_id,
    select_canonical_name,
    aggregate_confidence,
)


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


class TestGenerateEntityId:
    def test_deterministic(self):
        """Same name always produces same ID."""
        id1 = generate_entity_id("JOHN SMITH")
        id2 = generate_entity_id("JOHN SMITH")
        assert id1 == id2

    def test_case_insensitive(self):
        """ID generation is case-insensitive."""
        assert generate_entity_id("John Smith") == generate_entity_id("JOHN SMITH")

    def test_whitespace_normalized(self):
        """Extra whitespace doesn't change the ID."""
        assert generate_entity_id("JOHN  SMITH") == generate_entity_id("JOHN SMITH")
        assert generate_entity_id("  JOHN SMITH  ") == generate_entity_id("JOHN SMITH")

    def test_different_names_differ(self):
        """Different names produce different IDs."""
        assert generate_entity_id("JOHN SMITH") != generate_entity_id("JANE DOE")


class TestSelectCanonicalName:
    def test_picks_most_frequent(self):
        names = ["JOHN SMITH", "JOHN SMITH", "J SMITH", "JOHN A SMITH"]
        assert select_canonical_name(names) == "JOHN SMITH"

    def test_tiebreak_longest(self):
        """When frequency ties, pick the longest name."""
        names = ["J SMITH", "JOHN SMITH"]
        assert select_canonical_name(names) == "JOHN SMITH"

    def test_single_name(self):
        assert select_canonical_name(["JANE DOE"]) == "JANE DOE"

    def test_empty_list(self):
        assert select_canonical_name([]) is None


class TestAggregateConfidence:
    def test_average_probabilities(self):
        probs = [0.9, 0.8, 0.95]
        result = aggregate_confidence(probs)
        assert round(result, 4) == 0.8833

    def test_single_probability(self):
        assert aggregate_confidence([0.75]) == 0.75
