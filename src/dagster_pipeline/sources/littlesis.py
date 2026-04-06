"""LittleSis — power mapping network config and helpers.

No auth required. CC BY-SA 4.0 license.
"""

LS_BASE = "https://littlesis.org/api"

REL_CATEGORIES = {
    1: "position",
    2: "education",
    3: "membership",
    4: "family",
    5: "donation",
    6: "transaction",
    7: "lobbying",
    8: "social",
    9: "professional",
    10: "ownership",
    11: "hierarchy",
    12: "generic",
}

NYC_SEED_SEARCHES = [
    "New York City",
    "NYC Council",
    "NYC Mayor",
    "New York Real Estate",
    "Related Companies",
    "Brookfield Properties",
    "SL Green",
    "Vornado Realty",
    "Two Trees Management",
    "Extell Development",
    "Silverstein Properties",
    "Tishman Speyer",
    "RXR Realty",
    "Kasirer",
    "James Capalino",
]


def _parse_entity(ent: dict) -> dict:
    attrs = ent.get("attributes", {})
    exts = attrs.get("extensions", {})
    person = exts.get("Person", {})
    org = exts.get("Org", {})
    return {
        "id": ent.get("id"),
        "name": attrs.get("name"),
        "blurb": attrs.get("blurb"),
        "primary_ext": attrs.get("primary_ext"),
        "types": ", ".join(attrs.get("types", [])),
        "aliases": ", ".join(attrs.get("aliases", [])),
        "updated_at": attrs.get("updated_at"),
        "start_date": attrs.get("start_date"),
        "end_date": attrs.get("end_date"),
        "net_worth": person.get("net_worth") or org.get("revenue"),
    }
