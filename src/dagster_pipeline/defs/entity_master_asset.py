"""Dagster asset producing lake.foundation.entity_master."""

import re
import uuid
from collections import Counter

ENTITY_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

ORG_INDICATORS = (
    "LLC", "L.L.C.", "L.L.C", "CORP", "CORPORATION", "INC", "INCORPORATED",
    "LTD", "LIMITED", "LP", "L.P.", "LLP", "L.L.P.",
    "TRUST", "BANK", "FUND", "FOUNDATION", "ASSOCIATION", "ASSOC",
    "HOLDINGS", "REALTY", "PROPERTIES", "MANAGEMENT", "MGMT",
    "HOUSING DEV", "DEVELOPMENT", "ENTERPRISES", "PARTNERS",
    "CITY OF", "STATE OF", "COUNTY OF", "DEPT OF", "DEPARTMENT",
    "AUTHORITY", "COMMISSION", "BOARD OF", "AGENCY",
)


def classify_entity_type(name: str | None) -> str:
    """Classify a name as PERSON, ORGANIZATION, or UNKNOWN."""
    if not name or not name.strip():
        return "UNKNOWN"
    upper = name.upper().strip()
    for indicator in ORG_INDICATORS:
        if indicator in upper:
            return "ORGANIZATION"
    return "PERSON"


def generate_entity_id(name: str) -> str:
    """Generate a deterministic UUID for a normalized name."""
    normalized = re.sub(r"\s+", " ", name.upper().strip())
    return str(uuid.uuid5(ENTITY_NAMESPACE, normalized))


def select_canonical_name(names: list[str]) -> str | None:
    """Pick the canonical name: most frequent, then longest as tiebreak."""
    if not names:
        return None
    counts = Counter(names)
    max_count = max(counts.values())
    candidates = [n for n, c in counts.items() if c == max_count]
    return max(candidates, key=len)


def aggregate_confidence(probabilities: list[float]) -> float:
    """Aggregate match probabilities into a single confidence score."""
    return sum(probabilities) / len(probabilities)
