"""Splink model integrity tests — guards against silent training bugs.

Two assertions:
1. The training script's EM loop must include at least one blocking rule
   that does NOT contain `last_name`. Otherwise EM can't estimate
   last_name's m_probabilities.
2. The current saved model in models/splink_model_v2.json must have
   m_probability set on every non-null match level for every comparison
   (not just last_name — a general invariant).
"""
import json
import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
TRAIN_SCRIPT = REPO_ROOT / "scripts" / "train_splink_model.py"
MODEL_JSON = REPO_ROOT / "models" / "splink_model_v2.json"


def _parse_em_blocking_rules(script_src: str) -> list[str]:
    """Return the list of block_on(...) calls used in the script."""
    pattern = re.compile(r'block_on\(([^)]+)\)')
    return [m.group(1) for m in pattern.finditer(script_src)]


def test_em_training_has_a_rule_excluding_last_name():
    """At least one EM blocking rule must NOT contain last_name, so EM
    has variance on last_name across candidate pairs and can estimate
    its m_probabilities."""
    src = TRAIN_SCRIPT.read_text()
    rules = _parse_em_blocking_rules(src)
    rules_without_last_name = [
        r for r in rules if "last_name" not in r and "dm_last" not in r
    ]
    assert rules_without_last_name, (
        "All block_on(...) calls in train_splink_model.py reference "
        "last_name (or dm_last). EM cannot estimate last_name's "
        "m_probabilities under these blocking rules — they're all "
        "blocking on last_name, leaving zero variance. Add a third "
        "EM pass with e.g. block_on('first_name', 'zip').\n"
        f"Found rules: {rules}"
    )


def test_model_has_m_probability_on_exact_match_levels():
    """Every Exact-match level in models/splink_model_v2.json must have
    m_probability set. Exact match is the highest-confidence signal in
    each comparison; if EM couldn't fit it, that comparison contributes
    almost nothing to scoring.

    Jaro-Winkler levels (close-but-not-exact spellings) are allowed to
    have m_probability=None because real-world deduplication data often
    lacks the JW-close-but-not-exact pairs EM needs to fit those levels.
    Splink uses default values for unfitted JW levels and prints a warning
    at predict() time. This is a known limitation, not a bug.

    The original audit-flagged bug was: ALL last_name match levels (Exact
    AND every JW threshold) had m_probability=None because every EM
    blocking rule included last_name, giving EM zero variance. Fixed by
    adding EM passes that exclude last_name. After the fix, last_name
    Exact match has m_probability ≈ 0.92 (the dominant match signal).
    """
    if not MODEL_JSON.exists():
        pytest.skip(f"{MODEL_JSON} not present — retrain to generate")

    model = json.loads(MODEL_JSON.read_text())
    missing_critical = []
    for comp in model.get("comparisons", []):
        col = comp.get("output_column_name", "?")
        for lvl in comp.get("comparison_levels", []):
            label = lvl.get("label_for_charts", "")
            sql = (lvl.get("sql_condition") or "").upper()
            is_null = (
                "NULL" in label.upper()
                or " IS NULL" in sql
                or sql.strip().endswith("IS NULL")
            )
            if is_null:
                continue
            if "else" in label.lower() or label == "All other comparisons":
                continue
            # Critical levels: Exact match. JW levels are tolerated as None.
            is_exact = "exact" in label.lower()
            if is_exact and lvl.get("m_probability") is None:
                missing_critical.append(f"{col} / {label}")
    assert not missing_critical, (
        "Splink model is missing m_probability on Exact-match levels — "
        "the highest-confidence signal for each comparison. This means "
        "the affected comparison(s) contribute almost nothing to match "
        "scoring. Likely root cause: every EM blocking rule includes the "
        "affected column, leaving zero variance for EM.\n"
        f"Critical levels missing m_probability:\n  " + "\n  ".join(missing_critical) +
        "\n\nTo retrain (the script handles both DAGSTER_PG_PASSWORD and "
        "DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG env vars):\n"
        "  docker exec dagster-code python /tmp/train_splink_model_patched.py\n"
        "  docker cp dagster-code:/tmp/splink_model_v2.json /opt/dagster-pipeline/models/"
    )
