"""Column classifier: maps DuckDB column names and types to FilterType for the explore UI."""
from __future__ import annotations
from enum import Enum


class FilterType(str, Enum):
    GEOGRAPHY = "geography"
    DATE_RANGE = "date_range"
    TEXT_SEARCH = "text_search"
    CATEGORY = "category"
    NUMERIC_RANGE = "numeric_range"


_GEOGRAPHY_NAMES = frozenset({
    "borough", "boro", "borocode", "boro_code", "borough_id",
    "zipcode", "zip_code", "zip", "incident_zip", "facility_zip",
    "postcode", "postal_code",
})

_DATE_TYPES = frozenset({
    "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ",
})

_TEXT_SEARCH_NAMES = frozenset({
    "address", "street_name", "owner_name", "respondent_name",
    "description", "violation_description", "complaint_description",
    "facility_name", "dba", "business_name", "name",
})

_CATEGORY_NAMES = frozenset({
    "status", "complaint_type", "violation_type", "type", "grade",
    "disposition", "outcome", "offense_description", "law_cat_cd",
    "building_class", "zoning_district", "community_board",
})

_NUMERIC_SUFFIXES = ("_amount", "_count", "_score", "_total", "_penalty", "_fine")

_NUMERIC_NAMES = frozenset({
    "amount", "count", "score", "penalty", "total",
    "units", "stories", "num_floors", "year_built",
})

_NUMERIC_TYPES = frozenset({"INTEGER", "BIGINT", "SMALLINT", "DOUBLE", "FLOAT", "REAL", "NUMERIC", "DECIMAL"})


def classify_column(name: str, dtype: str) -> FilterType | None:
    """Return the FilterType for a column, or None if no match."""
    lower = name.lower()

    # Geography: match by name (works across any type)
    if lower in _GEOGRAPHY_NAMES or lower.endswith("_zip"):
        return FilterType.GEOGRAPHY

    # Date: match by type, or VARCHAR columns ending in _date
    if dtype in _DATE_TYPES:
        return FilterType.DATE_RANGE
    if lower.endswith("_date"):
        return FilterType.DATE_RANGE

    # Category: name match, VARCHAR only
    if lower in _CATEGORY_NAMES and dtype == "VARCHAR":
        return FilterType.CATEGORY

    # Text search: name match, VARCHAR only
    if lower in _TEXT_SEARCH_NAMES and dtype == "VARCHAR":
        return FilterType.TEXT_SEARCH

    # Numeric: name or suffix match, numeric types only
    if dtype in _NUMERIC_TYPES:
        if lower in _NUMERIC_NAMES:
            return FilterType.NUMERIC_RANGE
        if any(lower.endswith(s) for s in _NUMERIC_SUFFIXES):
            return FilterType.NUMERIC_RANGE

    return None
