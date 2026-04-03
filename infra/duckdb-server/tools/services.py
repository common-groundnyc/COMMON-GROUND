"""services() super tool — absorbs resource_finder into one dispatch with views:
full, childcare, food, shelter, benefits, legal_aid, community."""

import re
import time
from typing import Annotated, Literal

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import safe_query
from shared.formatting import make_result
from shared.types import ZIP_PATTERN

# ---------------------------------------------------------------------------
# Input patterns
# ---------------------------------------------------------------------------

_ZIP_PATTERN = ZIP_PATTERN

# ---------------------------------------------------------------------------
# SQL constants — DYCD programs (food, childcare, youth)
# ---------------------------------------------------------------------------

DYCD_FOOD_SQL = """
SELECT program_site_name AS name, street_address AS address,
       provider AS phone, program_type AS type
FROM lake.social_services.dycd_program_sites
WHERE zipcode = ? AND program_type ILIKE '%food%'
ORDER BY program_site_name
LIMIT 15
"""

DYCD_CHILDCARE_SQL = """
SELECT program_site_name AS name, street_address AS address,
       provider AS phone, program_type AS type
FROM lake.social_services.dycd_program_sites
WHERE zipcode = ? AND program_type ILIKE '%child%'
ORDER BY program_site_name
LIMIT 15
"""

DYCD_YOUTH_SQL = """
SELECT program_type AS type, program_site_name AS name,
       street_address AS address, borough, zipcode, provider
FROM lake.social_services.dycd_program_sites
WHERE zipcode = ?
ORDER BY program_type, program_site_name
LIMIT 20
"""

# ---------------------------------------------------------------------------
# SQL constants — SNAP / benefits / family justice
# ---------------------------------------------------------------------------

SNAP_CENTERS_SQL = """
SELECT facility_name, street_address, city, state, zip_code, phone_number_s_
FROM lake.social_services.snap_centers
LIMIT 20
"""

BENEFITS_CENTERS_SQL = """
SELECT facility_name, street_address, city, phone_number_s, comments
FROM lake.social_services.benefits_centers
LIMIT 20
"""

FAMILY_JUSTICE_SQL = """
SELECT borough, street_address, telephone_number, comments
FROM lake.social_services.family_justice_centers
"""

# ---------------------------------------------------------------------------
# SQL constants — childcare (NYS + inspections)
# ---------------------------------------------------------------------------

NYS_CHILDCARE_SQL = """
SELECT facility_name, street_address, city, county,
       capacity, facility_status, program_type
FROM lake.social_services.nys_child_care
WHERE zip_code = ?
ORDER BY facility_name
LIMIT 20
"""

CHILDCARE_PROGRAMS_SQL = """
SELECT program_name, address, borough, program_type,
       age_range, seats
FROM lake.social_services.childcare_programs
WHERE zipcode = ?
ORDER BY program_name
LIMIT 20
"""

CHILDCARE_INSPECTIONS_SQL = """
SELECT center_name, building_address, borough,
       violation_category, health_code_sub_section,
       inspection_date, violation_status
FROM lake.social_services.childcare_inspections
WHERE zip_code = ?
ORDER BY inspection_date DESC
LIMIT 15
"""

CHILDCARE_INSPECTIONS_CURRENT_SQL = """
SELECT center_name, building_address, borough,
       program_type, maximum_capacity,
       total_educational_workers, inspection_date
FROM lake.social_services.childcare_inspections_current
WHERE zip_code = ?
ORDER BY inspection_date DESC
LIMIT 15
"""

# ---------------------------------------------------------------------------
# SQL constants — food (farmers markets, SNAP access, Shop Healthy)
# ---------------------------------------------------------------------------

FARMERS_MARKETS_SQL = """
SELECT market_name, street_address, borough,
       day, open_time, close_time, season
FROM lake.social_services.farmers_markets
WHERE zipcode = ?
ORDER BY market_name
LIMIT 15
"""

SNAP_ACCESS_INDEX_SQL = """
SELECT census_tract, snap_access_score, distance_to_store,
       vehicle_access, poverty_rate
FROM lake.social_services.snap_access_index
WHERE zipcode = ?
LIMIT 10
"""

SHOP_HEALTHY_SQL = """
SELECT store_name, address, borough, neighborhood, store_type
FROM lake.social_services.shop_healthy
WHERE zipcode = ?
ORDER BY store_name
LIMIT 15
"""

# ---------------------------------------------------------------------------
# SQL constants — shelter / housing
# ---------------------------------------------------------------------------

DHS_DAILY_REPORT_SQL = """
SELECT report_date,
       total_individuals_in_shelter,
       total_children_in_shelter,
       total_adults_in_shelter,
       total_individuals_in_families
FROM lake.social_services.dhs_daily_report
ORDER BY report_date DESC
LIMIT 7
"""

DHS_SHELTER_CENSUS_SQL = """
SELECT date, total_individuals,
       total_children, total_adults,
       total_families
FROM lake.social_services.dhs_shelter_census
ORDER BY date DESC
LIMIT 7
"""

HUD_PUBLIC_HOUSING_DEV_SQL = """
SELECT development_name, borough, total_number_of_apartments,
       manager, address
FROM lake.social_services.hud_public_housing_developments
WHERE zip_code = ?
ORDER BY development_name
LIMIT 15
"""

HUD_PUBLIC_HOUSING_BLDG_SQL = """
SELECT development_name, building_address, borough,
       number_of_apartments, program
FROM lake.social_services.hud_public_housing_buildings
WHERE zip_code = ?
ORDER BY development_name
LIMIT 15
"""

HOUSING_CONNECT_BLDG_SQL = """
SELECT project_name, street_address, borough,
       total_units, min_income, max_income
FROM lake.social_services.housing_connect_buildings
WHERE zip_code = ?
ORDER BY project_name
LIMIT 15
"""

HOUSING_CONNECT_LOTTERIES_SQL = """
SELECT project_name, borough, status,
       lottery_date, units_available
FROM lake.social_services.housing_connect_lotteries
ORDER BY lottery_date DESC
LIMIT 15
"""

# ---------------------------------------------------------------------------
# SQL constants — benefits / access
# ---------------------------------------------------------------------------

ACCESS_NYC_SQL = """
SELECT program_name, program_category, plain_language_description,
       population_served, how_to_apply
FROM lake.social_services.access_nyc
LIMIT 30
"""

KNOW_YOUR_RIGHTS_SQL = """
SELECT borough, zip_code, startdate, primary_language,
       total_attendees, list_of_languages
FROM lake.social_services.know_your_rights
ORDER BY TRY_CAST(startdate AS DATE) DESC
LIMIT 10
"""

# ---------------------------------------------------------------------------
# SQL constants — community
# ---------------------------------------------------------------------------

COMMUNITY_GARDENS_SQL = """
SELECT garden_name, address, borough, size,
       jurisdiction, status
FROM lake.social_services.community_gardens
WHERE zipcode = ?
ORDER BY garden_name
LIMIT 15
"""

COMMUNITY_ORGS_SQL = """
SELECT organization_name, address, borough,
       organization_type, services_provided
FROM lake.social_services.community_orgs
WHERE zipcode = ?
ORDER BY organization_name
LIMIT 15
"""

LITERACY_PROGRAMS_SQL = """
SELECT program_name, address, borough,
       program_type, population_served, schedule
FROM lake.social_services.literacy_programs
WHERE zipcode = ?
ORDER BY program_name
LIMIT 15
"""

# ---------------------------------------------------------------------------
# View dispatch helpers
# ---------------------------------------------------------------------------

_NEED_KEYWORDS = {
    "food": ["food", "hungry", "eat", "snap", "pantry", "market", "halal", "kosher", "meal"],
    "childcare": ["child", "daycare", "childcare", "baby", "toddler", "infant", "preschool"],
    "shelter": ["shelter", "homeless", "housing", "unhoused", "dhs"],
    "benefits": ["benefit", "medicaid", "cash", "assistance", "welfare", "access"],
    "legal_aid": ["legal", "lawyer", "attorney", "court", "rights", "domestic", "violence", "immigration"],
    "community": ["garden", "community", "literacy", "volunteer", "org"],
}


def _extract_zip(location: str) -> str:
    """Extract a 5-digit ZIP from the location string."""
    location = location.strip()
    if _ZIP_PATTERN.match(location):
        return location
    # Try to find a ZIP embedded in an address
    m = re.search(r"\b(\d{5})\b", location)
    if m:
        return m.group(1)
    raise ToolError(
        f"Could not extract a ZIP code from '{location}'. "
        "Provide a 5-digit NYC ZIP code, e.g. '10003' or '123 Main St, Brooklyn 11201'."
    )


def _matches_need(need: str, section_keywords: list[str]) -> bool:
    """Check if natural-language need matches a section's keywords."""
    if not need:
        return True
    lower = need.lower()
    return any(kw in lower for kw in section_keywords)


def _section(lines: list[str], title: str, cols: list, rows: list, max_rows: int = 10) -> int:
    """Append a titled section to lines. Returns count of rows added."""
    if not rows:
        return 0
    lines.append(f"\n{title} ({len(rows)} found):")
    for r in rows[:max_rows]:
        parts = [str(v) for v in r if v is not None and str(v).strip()]
        lines.append(f"  {' — '.join(parts[:4])}")
    if len(rows) > max_rows:
        lines.append(f"  ... and {len(rows) - max_rows} more")
    return len(rows)


# ---------------------------------------------------------------------------
# View: full — everything the old resource_finder did, plus new tables
# ---------------------------------------------------------------------------

def _view_full(pool: object, zipcode: str, need: str) -> ToolResult:
    t0 = time.time()
    lines = [f"SERVICES — ZIP {zipcode}"]
    if need:
        lines.append(f"Filtering for: {need}")
    lines.append("=" * 50)

    total = 0

    # Food
    if _matches_need(need, _NEED_KEYWORDS["food"]):
        _, rows = safe_query(pool, DYCD_FOOD_SQL, [zipcode])
        total += _section(lines, "FOOD ASSISTANCE (DYCD)", [], rows)

        _, rows = safe_query(pool, SNAP_CENTERS_SQL, [])
        total += _section(lines, "SNAP CENTERS (citywide)", [], rows, max_rows=5)

        _, rows = safe_query(pool, FARMERS_MARKETS_SQL, [zipcode])
        total += _section(lines, "FARMERS MARKETS", [], rows)

        _, rows = safe_query(pool, SHOP_HEALTHY_SQL, [zipcode])
        total += _section(lines, "SHOP HEALTHY STORES", [], rows)

    # Childcare
    if _matches_need(need, _NEED_KEYWORDS["childcare"]):
        _, rows = safe_query(pool, DYCD_CHILDCARE_SQL, [zipcode])
        total += _section(lines, "CHILDCARE (DYCD)", [], rows)

        _, rows = safe_query(pool, NYS_CHILDCARE_SQL, [zipcode])
        total += _section(lines, "LICENSED CHILDCARE (NYS)", [], rows)

        _, rows = safe_query(pool, CHILDCARE_PROGRAMS_SQL, [zipcode])
        total += _section(lines, "CHILDCARE PROGRAMS", [], rows)

    # Youth / DYCD
    if _matches_need(need, ["youth", "teen", "job", "dycd", "program", "after"]):
        _, rows = safe_query(pool, DYCD_YOUTH_SQL, [zipcode])
        total += _section(lines, "YOUTH & COMMUNITY PROGRAMS (DYCD)", [], rows)

    # Shelter
    if _matches_need(need, _NEED_KEYWORDS["shelter"]):
        lines.append("\nSHELTER & HOUSING:")
        lines.append("  DHS Intake: 400 E 30th St, Manhattan — 212-361-8000")
        lines.append("  Families: PATH Center, 151 E 151st St, Bronx — 718-503-6400")
        lines.append("  Safe Haven: 311 for nearest location")
        lines.append("  HomeBase (eviction prevention): 311, ask for HomeBase")
        _, rows = safe_query(pool, DHS_DAILY_REPORT_SQL, [])
        total += _section(lines, "DHS DAILY REPORT (recent)", [], rows, max_rows=3)
        _, rows = safe_query(pool, HUD_PUBLIC_HOUSING_DEV_SQL, [zipcode])
        total += _section(lines, "PUBLIC HOUSING DEVELOPMENTS", [], rows)
        _, rows = safe_query(pool, HOUSING_CONNECT_BLDG_SQL, [zipcode])
        total += _section(lines, "HOUSING CONNECT (affordable lotteries)", [], rows)
        total += 1

    # Benefits
    if _matches_need(need, _NEED_KEYWORDS["benefits"]):
        _, rows = safe_query(pool, BENEFITS_CENTERS_SQL, [])
        total += _section(lines, "BENEFITS ACCESS CENTERS", [], rows)

        _, rows = safe_query(pool, ACCESS_NYC_SQL, [])
        total += _section(lines, "ACCESS NYC PROGRAMS", [], rows, max_rows=8)

    # Legal aid
    if _matches_need(need, _NEED_KEYWORDS["legal_aid"]):
        _, rows = safe_query(pool, FAMILY_JUSTICE_SQL, [])
        total += _section(lines, "FAMILY JUSTICE CENTERS (domestic violence)", [], rows)

        _, rows = safe_query(pool, KNOW_YOUR_RIGHTS_SQL, [])
        total += _section(lines, "KNOW YOUR RIGHTS EVENTS", [], rows, max_rows=5)

    # Community
    if _matches_need(need, _NEED_KEYWORDS["community"]):
        _, rows = safe_query(pool, COMMUNITY_GARDENS_SQL, [zipcode])
        total += _section(lines, "COMMUNITY GARDENS", [], rows)

        _, rows = safe_query(pool, COMMUNITY_ORGS_SQL, [zipcode])
        total += _section(lines, "COMMUNITY ORGANIZATIONS", [], rows)

        _, rows = safe_query(pool, LITERACY_PROGRAMS_SQL, [zipcode])
        total += _section(lines, "LITERACY PROGRAMS", [], rows)

    if total == 0:
        lines.append(f"\nNo results for '{need}' in ZIP {zipcode}.")
        lines.append("Try view='food', 'childcare', 'shelter', 'benefits', 'legal_aid', or 'community'.")

    lines.append("\nUNIVERSAL HOTLINES:")
    lines.append("  311: NYC services (any language)")
    lines.append("  988: Suicide & Crisis Lifeline")
    lines.append("  911: Emergency")
    lines.append("  Safe Horizon: 1-800-621-4673 (crime victims)")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"zipcode": zipcode, "need": need, "results_found": total},
        meta={"zipcode": zipcode, "need": need, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# View: childcare
# ---------------------------------------------------------------------------

def _view_childcare(pool: object, zipcode: str, need: str) -> ToolResult:
    t0 = time.time()
    lines = [f"CHILDCARE SERVICES — ZIP {zipcode}", "=" * 50]
    total = 0

    _, rows = safe_query(pool, DYCD_CHILDCARE_SQL, [zipcode])
    total += _section(lines, "DYCD CHILDCARE PROGRAMS", [], rows)

    _, rows = safe_query(pool, NYS_CHILDCARE_SQL, [zipcode])
    total += _section(lines, "NYS LICENSED CHILDCARE", [], rows, max_rows=15)

    _, rows = safe_query(pool, CHILDCARE_PROGRAMS_SQL, [zipcode])
    total += _section(lines, "CHILDCARE PROGRAMS", [], rows, max_rows=15)

    _, rows = safe_query(pool, CHILDCARE_INSPECTIONS_CURRENT_SQL, [zipcode])
    total += _section(lines, "RECENT INSPECTIONS", [], rows, max_rows=10)

    _, rows = safe_query(pool, CHILDCARE_INSPECTIONS_SQL, [zipcode])
    total += _section(lines, "INSPECTION VIOLATIONS", [], rows, max_rows=10)

    if total == 0:
        lines.append(f"\nNo childcare providers found in ZIP {zipcode}.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"zipcode": zipcode, "view": "childcare", "results_found": total},
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# View: food
# ---------------------------------------------------------------------------

def _view_food(pool: object, zipcode: str, need: str) -> ToolResult:
    t0 = time.time()
    lines = [f"FOOD ASSISTANCE — ZIP {zipcode}", "=" * 50]
    total = 0

    _, rows = safe_query(pool, DYCD_FOOD_SQL, [zipcode])
    total += _section(lines, "FOOD PROGRAMS (DYCD)", [], rows)

    _, rows = safe_query(pool, SNAP_CENTERS_SQL, [])
    total += _section(lines, "SNAP CENTERS (citywide)", [], rows, max_rows=8)

    _, rows = safe_query(pool, SNAP_ACCESS_INDEX_SQL, [zipcode])
    total += _section(lines, "SNAP ACCESS INDEX", [], rows, max_rows=5)

    _, rows = safe_query(pool, FARMERS_MARKETS_SQL, [zipcode])
    total += _section(lines, "FARMERS MARKETS", [], rows)

    _, rows = safe_query(pool, SHOP_HEALTHY_SQL, [zipcode])
    total += _section(lines, "SHOP HEALTHY STORES", [], rows)

    if total == 0:
        lines.append(f"\nNo food resources found in ZIP {zipcode}.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"zipcode": zipcode, "view": "food", "results_found": total},
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# View: shelter
# ---------------------------------------------------------------------------

def _view_shelter(pool: object, zipcode: str, need: str) -> ToolResult:
    t0 = time.time()
    lines = [f"SHELTER & HOUSING — ZIP {zipcode}", "=" * 50]
    total = 0

    lines.append("\nEMERGENCY INTAKE:")
    lines.append("  DHS Intake: 400 E 30th St, Manhattan — 212-361-8000")
    lines.append("  Families: PATH Center, 151 E 151st St, Bronx — 718-503-6400")
    lines.append("  Safe Haven: 311 for nearest location")
    lines.append("  HomeBase (eviction prevention): 311, ask for HomeBase")

    _, rows = safe_query(pool, DHS_DAILY_REPORT_SQL, [])
    total += _section(lines, "DHS DAILY REPORT (recent)", [], rows, max_rows=5)

    _, rows = safe_query(pool, DHS_SHELTER_CENSUS_SQL, [])
    total += _section(lines, "DHS SHELTER CENSUS", [], rows, max_rows=5)

    _, rows = safe_query(pool, HUD_PUBLIC_HOUSING_DEV_SQL, [zipcode])
    total += _section(lines, "PUBLIC HOUSING DEVELOPMENTS", [], rows)

    _, rows = safe_query(pool, HUD_PUBLIC_HOUSING_BLDG_SQL, [zipcode])
    total += _section(lines, "PUBLIC HOUSING BUILDINGS", [], rows)

    _, rows = safe_query(pool, HOUSING_CONNECT_BLDG_SQL, [zipcode])
    total += _section(lines, "HOUSING CONNECT (affordable lotteries)", [], rows)

    _, rows = safe_query(pool, HOUSING_CONNECT_LOTTERIES_SQL, [])
    total += _section(lines, "RECENT HOUSING LOTTERIES (citywide)", [], rows, max_rows=8)

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"zipcode": zipcode, "view": "shelter", "results_found": total},
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# View: benefits
# ---------------------------------------------------------------------------

def _view_benefits(pool: object, zipcode: str, need: str) -> ToolResult:
    t0 = time.time()
    lines = [f"BENEFITS & ASSISTANCE — ZIP {zipcode}", "=" * 50]
    total = 0

    _, rows = safe_query(pool, BENEFITS_CENTERS_SQL, [])
    total += _section(lines, "BENEFITS ACCESS CENTERS (citywide)", [], rows)

    _, rows = safe_query(pool, SNAP_CENTERS_SQL, [])
    total += _section(lines, "SNAP CENTERS", [], rows, max_rows=8)

    _, rows = safe_query(pool, ACCESS_NYC_SQL, [])
    total += _section(lines, "ACCESS NYC PROGRAMS", [], rows, max_rows=15)

    if total == 0:
        lines.append("\nNo benefits data found. Call 311 for assistance.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"zipcode": zipcode, "view": "benefits", "results_found": total},
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# View: legal_aid
# ---------------------------------------------------------------------------

def _view_legal_aid(pool: object, zipcode: str, need: str) -> ToolResult:
    t0 = time.time()
    lines = [f"LEGAL AID & RIGHTS — ZIP {zipcode}", "=" * 50]
    total = 0

    _, rows = safe_query(pool, FAMILY_JUSTICE_SQL, [])
    total += _section(lines, "FAMILY JUSTICE CENTERS (domestic violence)", [], rows)

    _, rows = safe_query(pool, KNOW_YOUR_RIGHTS_SQL, [])
    total += _section(lines, "KNOW YOUR RIGHTS EVENTS (recent)", [], rows, max_rows=8)

    lines.append("\nIMMIGRATION SERVICES:")
    lines.append("  ActionNYC (free legal): 800-354-0365")
    lines.append("  IDNYC: 311 for locations (municipal ID for all residents)")
    lines.append("  New York Immigration Coalition: nyic.org")
    lines.append("  Catholic Charities: catholiccharitiesny.org")
    lines.append("  Know Your Rights: 311 or MOIA hotline")

    lines.append("\nLEGAL HOTLINES:")
    lines.append("  Legal Aid Society: 212-577-3300")
    lines.append("  NYC Bar Legal Referral: 212-626-7373")
    lines.append("  Safe Horizon: 1-800-621-4673 (crime victims)")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"zipcode": zipcode, "view": "legal_aid", "results_found": total},
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# View: community
# ---------------------------------------------------------------------------

def _view_community(pool: object, zipcode: str, need: str) -> ToolResult:
    t0 = time.time()
    lines = [f"COMMUNITY RESOURCES — ZIP {zipcode}", "=" * 50]
    total = 0

    _, rows = safe_query(pool, COMMUNITY_GARDENS_SQL, [zipcode])
    total += _section(lines, "COMMUNITY GARDENS", [], rows)

    _, rows = safe_query(pool, COMMUNITY_ORGS_SQL, [zipcode])
    total += _section(lines, "COMMUNITY ORGANIZATIONS", [], rows)

    _, rows = safe_query(pool, LITERACY_PROGRAMS_SQL, [zipcode])
    total += _section(lines, "LITERACY PROGRAMS", [], rows)

    _, rows = safe_query(pool, DYCD_YOUTH_SQL, [zipcode])
    total += _section(lines, "YOUTH & COMMUNITY PROGRAMS (DYCD)", [], rows)

    if total == 0:
        lines.append(f"\nNo community resources found in ZIP {zipcode}.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"zipcode": zipcode, "view": "community", "results_found": total},
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

_VIEW_DISPATCH = {
    "full": _view_full,
    "childcare": _view_childcare,
    "food": _view_food,
    "shelter": _view_shelter,
    "benefits": _view_benefits,
    "legal_aid": _view_legal_aid,
    "community": _view_community,
}


def services(
    location: Annotated[str, Field(
        description="ZIP code, address, or lat/lng coordinates, e.g. '10003', '123 Main St, Brooklyn', '40.7128,-74.0060'",
        examples=["10003", "10456", "123 Main St, Brooklyn", "40.7128,-74.0060"],
    )],
    view: Annotated[
        Literal["full", "childcare", "food", "shelter", "benefits", "legal_aid", "community"],
        Field(
            default="full",
            description="'full' returns all available services near the location. 'childcare' returns licensed childcare programs and inspection results. 'food' returns food pantries, farmers markets, and SNAP centers. 'shelter' returns DHS shelters and daily population. 'benefits' returns benefits centers and Access NYC programs. 'legal_aid' returns legal aid and family justice centers. 'community' returns community gardens, organizations, and literacy programs.",
        )
    ] = "full",
    need: Annotated[str, Field(
        description="Natural language filter for what you need, e.g. 'halal food', 'infant daycare', 'free wifi'",
        examples=["halal food", "infant daycare", "free wifi", "immigration legal help"],
    )] = "",
    ctx: Context = None,
) -> ToolResult:
    """Childcare, shelters, food pantries, and community services near any NYC location. Returns program listings with addresses, phone numbers, and eligibility.

    GUIDELINES: Show all services data with locations and details. Use tables for service listings.
    Present the FULL response to the user. Do not omit any entry.

    LIMITATIONS: Not for school info (use school). Not for health facilities (use health).

    RETURNS: Service listings grouped by category with contact info and addresses."""
    pool = ctx.lifespan_context["pool"]
    zipcode = _extract_zip(location)
    need_clean = need.strip()

    directive = "PRESENTATION: Show all services data with locations and details. Use tables for service listings. Do not omit any entry.\n\n"
    handler = _VIEW_DISPATCH[view]
    result = handler(pool, zipcode, need_clean)
    return ToolResult(
        content=directive + (result.content if isinstance(result.content, str) else "\n".join(str(c) for c in result.content) if result.content else ""),
        structured_content=result.structured_content,
        meta=result.meta,
    )
