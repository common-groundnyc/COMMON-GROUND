from middleware.budget import extract_budget


def test_extract_budget_returns_none_when_missing():
    assert extract_budget({}) is None
    assert extract_budget({"bbl": "1000010001"}) is None


def test_extract_budget_reads_max_tokens():
    assert extract_budget({"max_tokens": 1500}) == 1500


def test_extract_budget_reads_nested_in_options():
    assert extract_budget({"options": {"max_tokens": 800}}) == 800


def test_extract_budget_clamps_below_minimum():
    # Below 100 is pointless — return minimum usable budget
    assert extract_budget({"max_tokens": 10}) == 100


def test_extract_budget_clamps_above_maximum():
    # Cap at 20k to protect the server
    assert extract_budget({"max_tokens": 999_999}) == 20_000


def test_extract_budget_ignores_non_integer():
    assert extract_budget({"max_tokens": "lots"}) is None
    assert extract_budget({"max_tokens": None}) is None
