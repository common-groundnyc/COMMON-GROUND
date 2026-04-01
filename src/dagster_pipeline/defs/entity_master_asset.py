"""Dagster asset producing lake.foundation.entity_master."""

import hashlib
import uuid

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


def generate_entity_id(member_unique_ids: list[str]) -> uuid.UUID:
    """Generate a deterministic UUID from sorted cluster member IDs.
    Same members in any order always produce the same UUID."""
    sorted_hashes = sorted(hashlib.md5(m.encode()).hexdigest() for m in member_unique_ids)
    combined = "|".join(sorted_hashes)
    return uuid.UUID(hashlib.md5(combined.encode()).hexdigest())
