"""Test that Splink model v2 loads and blocking rules reference correct columns."""
import json
import os
import pytest

MODEL_PATH = "models/splink_model_v2.json"


@pytest.fixture
def model():
    if not os.path.exists(MODEL_PATH):
        pytest.skip("splink_model_v2.json not yet trained")
    with open(MODEL_PATH) as f:
        return json.load(f)


def test_model_v2_loads(model):
    assert model["link_type"] == "dedupe_only"
    assert model["unique_id_column_name"] == "unique_id"


def test_model_v2_has_phonetic_blocking(model):
    rules = [r["blocking_rule"] for r in model["blocking_rules_to_generate_predictions"]]
    assert any("dm_last" in r for r in rules), f"No phonetic blocking rule: {rules}"


def test_model_v2_has_address_comparison(model):
    cols = [c["output_column_name"] for c in model["comparisons"]]
    assert "address" in cols, f"No address comparison: {cols}"


def test_model_v2_has_five_comparisons(model):
    assert len(model["comparisons"]) >= 5


def test_model_v2_has_fuzzy_name_levels(model):
    for comp in model["comparisons"]:
        if comp["output_column_name"] in ("first_name", "last_name"):
            levels = [l.get("sql_condition", "") for l in comp["comparison_levels"]]
            assert any("jaro_winkler" in l for l in levels), \
                f"{comp['output_column_name']} missing jaro_winkler levels"
