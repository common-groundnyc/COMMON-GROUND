"""network() super tool — absorbs 15 graph tools into one dispatch with
filter semantics.  type="all" (the default) fans out across every edge
type and returns everything found.  A specific type narrows to one
relationship family."""

import re
import time
from typing import Annotated, Literal

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import execute, safe_query, fill_placeholders
from shared.formatting import make_result, format_text_table
from shared.graph import require_graph
from shared.lance import vector_expand_names
from shared.types import MAX_LLM_ROWS, LANCE_DIR

# ---------------------------------------------------------------------------
# Input patterns
# ---------------------------------------------------------------------------

_BBL_PATTERN = re.compile(r"^\d{10}$")

# ---------------------------------------------------------------------------
# Borough mapping (shared across several sub-functions)
# ---------------------------------------------------------------------------

_BORO_MAP = {
    "manhattan": "1", "bronx": "2", "brooklyn": "3",
    "queens": "4", "staten island": "5",
}


def _boro_code(borough: str) -> str:
    """Normalize a borough name/code to a single digit or empty string."""
    b = borough.strip().lower()
    code = _BORO_MAP.get(b, borough.strip())
    return code if code in ("1", "2", "3", "4", "5") else ""


# ---------------------------------------------------------------------------
# SQL constants — landlord watchdog
# ---------------------------------------------------------------------------

WATCHDOG_BUILDING_SQL = """
SELECT boroid, block, lot, buildingid, registrationid, bin,
       housenumber, streetname, zip,
       legalstories, legalclassa, legalclassb, managementprogram,
       (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
FROM lake.housing.hpd_jurisdiction
WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
LIMIT 1
"""

WATCHDOG_PORTFOLIO_SQL = """
SELECT buildingid, registrationid,
       housenumber, streetname, zip, boroid, block, lot,
       legalclassa, legalclassb, legalstories,
       (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
FROM lake.housing.hpd_jurisdiction
WHERE registrationid = ?
  AND registrationid != '0'
  AND registrationid NOT LIKE '%995'
ORDER BY boroid, block, lot
"""

WATCHDOG_VIOLATIONS_SQL = """
SELECT
    bbl,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_v,
    COUNT(*) FILTER (WHERE UPPER(class) = 'C') AS class_c,
    COUNT(*) FILTER (WHERE UPPER(class) = 'B') AS class_b,
    COUNT(*) FILTER (WHERE UPPER(class) = 'A') AS class_a,
    COUNT(*) FILTER (WHERE violationstatus = 'Open' AND UPPER(class) = 'C') AS open_c,
    MAX(TRY_CAST(novissueddate AS DATE)) AS latest_date
FROM lake.housing.hpd_violations
WHERE bbl IN ({placeholders})
GROUP BY bbl
"""

WATCHDOG_COMPLAINTS_SQL = """
SELECT
    bbl,
    COUNT(DISTINCT complaint_id) AS total,
    COUNT(DISTINCT complaint_id) FILTER (WHERE complaint_status = 'OPEN') AS open_c,
    MAX(TRY_CAST(received_date AS DATE)) AS latest_date
FROM lake.housing.hpd_complaints
WHERE bbl IN ({placeholders})
GROUP BY bbl
"""

WATCHDOG_TOP_COMPLAINTS_SQL = """
SELECT major_category, COUNT(DISTINCT complaint_id) AS cnt
FROM lake.housing.hpd_complaints
WHERE bbl IN ({placeholders})
GROUP BY major_category
ORDER BY cnt DESC
LIMIT 10
"""

WATCHDOG_EVICTIONS_SQL = """
SELECT COUNT(*) AS eviction_count
FROM lake.housing.evictions
WHERE bbl IN ({placeholders})
  AND "residential_commercial_ind" = 'Residential'
"""

WATCHDOG_DOB_SQL = """
SELECT COUNT(*) AS total_dob,
       COUNT(*) FILTER (WHERE TRY_CAST(balance_due AS DOUBLE) > 0) AS unpaid
FROM lake.housing.dob_ecb_violations
WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) IN ({placeholders})
"""

WATCHDOG_AEP_SQL = """
SELECT bbl, status, roundyear
FROM lake.housing.aep_buildings
WHERE bbl IN ({placeholders})
"""

WATCHDOG_CONH_SQL = """
SELECT bbl, status
FROM lake.housing.conh_pilot
WHERE bbl IN ({placeholders})
"""

WATCHDOG_UNDERLYING_SQL = """
SELECT bbl, status
FROM lake.housing.underlying_conditions
WHERE bbl IN ({placeholders})
"""

WATCHDOG_CITY_AVERAGES_SQL = """
SELECT
    AVG(v_cnt) AS avg_violations_per_bldg,
    AVG(c_cnt) AS avg_open_violations_per_bldg
FROM (
    SELECT bbl,
           COUNT(*) AS v_cnt,
           COUNT(*) FILTER (WHERE violationstatus = 'Open') AS c_cnt
    FROM lake.housing.hpd_violations
    GROUP BY bbl
    HAVING COUNT(*) > 0
)
"""

# ---------------------------------------------------------------------------
# SQL constants — money trail
# ---------------------------------------------------------------------------

MONEY_NYS_DONATIONS_SQL = """
SELECT filer_name, flng_ent_first_name, flng_ent_last_name,
       SUM(TRY_CAST(org_amt AS DOUBLE)) AS total_donated,
       COUNT(*) AS donations,
       MIN(sched_date) AS first_donation,
       MAX(sched_date) AS last_donation
FROM lake.federal.nys_campaign_finance
WHERE (flng_ent_last_name ILIKE ? AND flng_ent_first_name ILIKE ?)
   OR filer_name ILIKE '%' || ? || '%'
GROUP BY filer_name, flng_ent_first_name, flng_ent_last_name
ORDER BY total_donated DESC NULLS LAST
LIMIT 15
"""

MONEY_FEC_SQL = """
SELECT cmte_id, name,
       SUM(TRY_CAST(transaction_amt AS DOUBLE)) AS total,
       COUNT(*) AS donations,
       MIN(transaction_dt) AS first_date,
       MAX(transaction_dt) AS last_date
FROM lake.federal.fec_contributions
WHERE name ILIKE '%' || ? || '%'
GROUP BY cmte_id, name
ORDER BY total DESC NULLS LAST
LIMIT 15
"""

MONEY_NYC_DONATIONS_SQL = """
SELECT recipname, occupation, empname, empstrno,
       SUM(TRY_CAST(amnt AS DOUBLE)) AS total,
       COUNT(*) AS donations
FROM lake.city_government.campaign_contributions
WHERE (name ILIKE '%' || ? || '%')
   OR (empname ILIKE '%' || ? || '%')
GROUP BY recipname, occupation, empname, empstrno
ORDER BY total DESC NULLS LAST
LIMIT 15
"""

MONEY_CONTRACTS_SQL = """
SELECT vendor_name, contract_amount, agency_name, short_title, start_date, end_date
FROM lake.city_government.contract_awards
WHERE vendor_name ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(contract_amount AS DOUBLE) DESC NULLS LAST
LIMIT 10
"""

MONEY_PROCUREMENT_SQL = """
SELECT vendor_name, contract_amount, contracting_agency, contract_description
FROM lake.financial.nys_procurement_state
WHERE vendor_name ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(contract_amount AS DOUBLE) DESC NULLS LAST
LIMIT 10
"""


# ===================================================================
# Public entry point
# ===================================================================


def network(
    name: Annotated[str, Field(
        description="Person name, company name, or BBL to start traversal, e.g. 'Steven Croman', 'BLACKSTONE', '1000670001'",
        examples=["Steven Croman", "BLACKSTONE GROUP", "1000670001", "NACHMAN PLUMBING"],
    )],
    type: Annotated[
        Literal["all", "ownership", "corporate", "political", "property", "contractor",
                "tradewaste", "officer", "clusters", "cliques", "worst"],
        Field(
            default="all",
            description=(
                "Filter which relationship types to include. "
                "'all' returns every connection found. "
                "'ownership' limits to property and landlord edges. "
                "'corporate' limits to LLC and shared-officer edges. "
                "'political' limits to donations, lobbying, and contracts. "
                "'property' limits to ACRIS transaction co-parties. "
                "'contractor' limits to DOB permit networks. "
                "'tradewaste' limits to BIC hauler connections. "
                "'officer' limits to shared-command misconduct. "
                "'clusters' finds hidden ownership empires via WCC. "
                "'cliques' finds tight-knit ownership groups. "
                "'worst' ranks slumlord scores."
            ),
        )
    ] = "all",
    depth: Annotated[int, Field(
        description="Graph traversal hops 1-6, higher means wider network but slower, e.g. 2",
        ge=1, le=6,
    )] = 2,
    borough: Annotated[str, Field(
        description="Borough filter, empty for all boroughs, e.g. 'Brooklyn', 'Manhattan'",
        examples=["Brooklyn", "Manhattan", "Bronx"],
    )] = "",
    top_n: Annotated[int, Field(
        description="Number of results for ranked views like worst and clusters, 1-100, e.g. 25",
        ge=1, le=100,
    )] = 25,
    ctx: Context = None,
) -> ToolResult:
    """Trace connections across ownership, corporate, and influence networks \
in NYC. Fans out across all relationship types by default and returns every \
connection found. Use when asking how entities are connected: landlord \
portfolios, shell company webs, political money trails, contractor networks. \
Do NOT use for simple entity lookup (use entity) or building profile (use \
building). Default returns all connections found for the name across all \
edge types."""

    name_stripped = name.strip()
    if len(name_stripped) < 3:
        raise ToolError("Name must be at least 3 characters")

    # Dispatch map for specific types
    _dispatch = {
        "ownership": _ownership,
        "corporate": _corporate,
        "political": _political,
        "property": _property,
        "contractor": _contractor,
        "tradewaste": _tradewaste,
        "officer": _officer,
        "clusters": _clusters,
        "cliques": _cliques,
        "worst": _worst,
    }

    if type == "all":
        return _all_types(name_stripped, depth, borough, top_n, ctx)
    return _dispatch[type](name_stripped, depth, borough, top_n, ctx)


# ===================================================================
# type = "all" — fan out, merge results
# ===================================================================


def _all_types(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    t0 = time.time()
    sections: list[str] = []
    structured: dict = {"name": name, "type": "all"}
    is_bbl = bool(_BBL_PATTERN.match(name))

    # --- Ownership ---
    try:
        if is_bbl:
            ownership_text, ownership_data = _ownership_for_bbl(name, depth, ctx)
        else:
            ownership_text, ownership_data = _ownership_for_name(name, depth, ctx)
        if ownership_text:
            sections.append(ownership_text)
            structured["ownership"] = ownership_data
    except (ToolError, Exception):
        pass

    # --- Corporate ---
    if not is_bbl:
        try:
            corporate_text, corporate_data = _corporate_core(name, ctx)
            if corporate_text:
                sections.append(corporate_text)
                structured["corporate"] = corporate_data
        except (ToolError, Exception):
            pass

    # --- Political ---
    if not is_bbl:
        try:
            political_text, political_data = _political_core(name, ctx)
            if political_text:
                sections.append(political_text)
                structured["political"] = political_data
        except (ToolError, Exception):
            pass

    # --- Property transactions ---
    if not is_bbl:
        try:
            property_text, property_data = _property_core(name, ctx)
            if property_text:
                sections.append(property_text)
                structured["property"] = property_data
        except (ToolError, Exception):
            pass

    elapsed = round((time.time() - t0) * 1000)

    if not sections:
        return ToolResult(
            content=f"No network connections found for '{name}' across any edge type.",
            meta={"query_time_ms": elapsed},
        )

    header = f"NETWORK — '{name}' (all types)\n{'=' * 55}\n"
    text = header + "\n\n".join(sections) + f"\n\n({elapsed}ms)"

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"name": name, "sections": len(sections), "query_time_ms": elapsed},
    )


# ===================================================================
# type = "ownership" — landlord watchdog + landlord network + ownership graph
# ===================================================================


def _ownership(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    t0 = time.time()
    is_bbl = bool(_BBL_PATTERN.match(name))

    if is_bbl:
        text, data = _ownership_for_bbl(name, depth, ctx)
    else:
        text, data = _ownership_for_name(name, depth, ctx)

    elapsed = round((time.time() - t0) * 1000)
    if not text:
        return ToolResult(content=f"No ownership data found for '{name}'.", meta={"query_time_ms": elapsed})

    return ToolResult(
        content=text + f"\n\n({elapsed}ms)",
        structured_content=data,
        meta={"query_time_ms": elapsed},
    )


def _ownership_for_bbl(bbl: str, depth: int, ctx: Context) -> tuple[str, dict]:
    """Ownership analysis starting from a BBL — combines watchdog + graph."""
    pool = ctx.lifespan_context["pool"]

    # --- Watchdog: portfolio view ---
    _, rows = execute(pool, WATCHDOG_BUILDING_SQL, [bbl])
    if not rows:
        raise ToolError(f"No building found for BBL {bbl}")

    bldg = rows[0]
    reg_id = bldg[4]
    address = f"{bldg[6]} {bldg[7]}"
    zipcode = bldg[8]
    stories = bldg[9]
    units_a = int(bldg[10] or 0)
    units_b = int(bldg[11] or 0)
    total_units_target = units_a + units_b
    mgmt = bldg[12] or "N/A"

    portfolio_bbls = [bbl]
    portfolio_buildings = []
    if reg_id and reg_id != "0" and not reg_id.endswith("995"):
        _, port_rows = execute(pool, WATCHDOG_PORTFOLIO_SQL, [reg_id])
        portfolio_buildings = port_rows
        portfolio_bbls = list(set(r[10] for r in port_rows))
        if bbl not in portfolio_bbls:
            portfolio_bbls.append(bbl)

    portfolio_size = len(portfolio_bbls)
    total_portfolio_units = sum(
        int(r[8] or 0) + int(r[9] or 0) for r in portfolio_buildings
    ) if portfolio_buildings else total_units_target

    # Violations
    v_sql = fill_placeholders(WATCHDOG_VIOLATIONS_SQL, portfolio_bbls)
    _, v_rows = execute(pool, v_sql, portfolio_bbls)

    total_violations = sum(int(r[1] or 0) for r in v_rows)
    total_open = sum(int(r[2] or 0) for r in v_rows)
    total_class_c = sum(int(r[3] or 0) for r in v_rows)
    total_class_b = sum(int(r[4] or 0) for r in v_rows)
    total_class_a = sum(int(r[5] or 0) for r in v_rows)
    total_open_c = sum(int(r[6] or 0) for r in v_rows)

    target_v = next((r for r in v_rows if r[0] == bbl), None)
    target_violations = int(target_v[1] or 0) if target_v else 0
    target_open = int(target_v[2] or 0) if target_v else 0
    target_class_c = int(target_v[3] or 0) if target_v else 0
    target_open_c = int(target_v[6] or 0) if target_v else 0

    # Complaints
    c_sql = fill_placeholders(WATCHDOG_COMPLAINTS_SQL, portfolio_bbls)
    _, c_rows = execute(pool, c_sql, portfolio_bbls)
    total_complaints = sum(int(r[1] or 0) for r in c_rows)
    total_open_complaints = sum(int(r[2] or 0) for r in c_rows)

    tc_sql = fill_placeholders(WATCHDOG_TOP_COMPLAINTS_SQL, portfolio_bbls)
    _, tc_rows = execute(pool, tc_sql, portfolio_bbls)

    # Evictions
    e_sql = fill_placeholders(WATCHDOG_EVICTIONS_SQL, portfolio_bbls)
    _, e_rows = execute(pool, e_sql, portfolio_bbls)
    eviction_count = int(e_rows[0][0] or 0) if e_rows else 0

    # DOB violations
    d_sql = fill_placeholders(WATCHDOG_DOB_SQL, portfolio_bbls)
    _, d_rows = execute(pool, d_sql, portfolio_bbls)
    dob_violations = int(d_rows[0][0] or 0) if d_rows else 0
    dob_unpaid = int(d_rows[0][1] or 0) if d_rows else 0

    # Enforcement flags
    aep_buildings = []
    aep_sql = fill_placeholders(WATCHDOG_AEP_SQL, portfolio_bbls)
    _, aep_rows = safe_query(pool, aep_sql, portfolio_bbls)
    if aep_rows:
        aep_buildings = [(r[0], r[1], r[2]) for r in aep_rows]

    conh_buildings = []
    conh_sql = fill_placeholders(WATCHDOG_CONH_SQL, portfolio_bbls)
    _, conh_rows = safe_query(pool, conh_sql, portfolio_bbls)
    if conh_rows:
        conh_buildings = [(r[0], r[1]) for r in conh_rows]

    underlying_buildings = []
    und_sql = fill_placeholders(WATCHDOG_UNDERLYING_SQL, portfolio_bbls)
    _, und_rows = safe_query(pool, und_sql, portfolio_bbls)
    if und_rows:
        underlying_buildings = [(r[0], r[1]) for r in und_rows]

    # City averages
    _, avg_rows = execute(pool, WATCHDOG_CITY_AVERAGES_SQL, [])
    city_avg_violations = float(avg_rows[0][0] or 0) if avg_rows else 0

    violations_per_bldg = total_violations / portfolio_size if portfolio_size else 0
    vs_city = (
        f"{violations_per_bldg / city_avg_violations:.1f}x city average"
        if city_avg_violations > 0 else "N/A"
    )

    # Build output
    lines = [f"OWNERSHIP — BBL {bbl}", "=" * 50]
    lines.append(f"\nTARGET BUILDING:")
    lines.append(f"  {address}, {zipcode}")
    lines.append(f"  {stories} stories, {total_units_target} units, program: {mgmt}")
    lines.append(f"  Violations: {target_violations:,} total, {target_open:,} open, {target_class_c:,} Class C ({target_open_c:,} open)")

    lines.append(f"\nOWNER PORTFOLIO: {portfolio_size} buildings, {total_portfolio_units:,} units")
    lines.append(f"  Registration ID: {reg_id}")

    if portfolio_size > 1:
        worst = sorted(v_rows, key=lambda r: int(r[2] or 0), reverse=True)[:5]
        if worst:
            lines.append(f"  Worst buildings (by open violations):")
            for w in worst:
                lines.append(f"    BBL {w[0]}: {int(w[1] or 0):,} total, {int(w[2] or 0):,} open, {int(w[3] or 0):,} Class C")

    lines.append(f"\nPORTFOLIO VIOLATIONS:")
    lines.append(f"  Total: {total_violations:,} ({vs_city})")
    lines.append(f"  Open: {total_open:,}")
    lines.append(f"  Class C (immediately hazardous): {total_class_c:,} ({total_open_c:,} still open)")
    lines.append(f"  Class B (hazardous): {total_class_b:,}")
    lines.append(f"  Class A (non-hazardous): {total_class_a:,}")

    lines.append(f"\nCOMPLAINTS: {total_complaints:,} total, {total_open_complaints:,} open")
    if tc_rows:
        lines.append(f"  Top categories:")
        for tc in tc_rows[:5]:
            lines.append(f"    {tc[0]}: {int(tc[1]):,}")

    lines.append(f"\nEVICTIONS: {eviction_count:,} residential")
    lines.append(f"DOB/ECB VIOLATIONS: {dob_violations:,} ({dob_unpaid:,} with unpaid penalties)")

    flags = []
    if aep_buildings:
        flag_detail = ", ".join(f"BBL {b[0]} ({b[1]}, round {b[2]})" for b in aep_buildings)
        flags.append(f"AEP (worst buildings list): {flag_detail}")
    if conh_buildings:
        flag_detail = ", ".join(f"BBL {b[0]} ({b[1]})" for b in conh_buildings)
        flags.append(f"CONH (harassment indicator): {flag_detail}")
    if underlying_buildings:
        flag_detail = ", ".join(f"BBL {b[0]} ({b[1]})" for b in underlying_buildings)
        flags.append(f"Underlying Conditions (systemic neglect): {flag_detail}")

    if flags:
        lines.append(f"\nENFORCEMENT FLAGS:")
        for f in flags:
            lines.append(f"  {f}")
    else:
        lines.append(f"\nENFORCEMENT FLAGS: None on record")

    lines.append(f"\nWHAT YOU CAN DO:")
    lines.append(f"  1. File HPD complaint: call 311 or nyc.gov/311")
    lines.append(f"  2. Report emergency (no heat/hot water, gas leak): call 311, press 2")
    lines.append(f"  3. Request HPD inspection: 311 complaint triggers automatic inspection")
    lines.append(f"  4. Contact tenant hotline: (212) 979-0611 (Met Council on Housing)")
    lines.append(f"  5. Free legal help: (718) 557-1379 (Legal Aid Society Housing)")
    lines.append(f"  6. JustFix.org — free tools to document conditions and send demand letters")

    # --- Graph traversal (if available) ---
    graph_text = ""
    graph_data = {}
    try:
        require_graph(ctx)
        g_text, g_data = _landlord_network_core(bbl, pool)
        if g_text:
            graph_text = f"\n\n{g_text}"
            graph_data = g_data
    except ToolError:
        pass

    structured = {
        "target_building": {
            "bbl": bbl, "address": address, "zip": zipcode,
            "stories": stories, "units": total_units_target,
            "management_program": mgmt,
            "violations": target_violations, "open_violations": target_open,
            "class_c": target_class_c, "open_class_c": target_open_c,
        },
        "portfolio": {
            "registration_id": reg_id,
            "building_count": portfolio_size,
            "total_units": total_portfolio_units,
            "bbls": portfolio_bbls[:50],
        },
        "portfolio_violations": {
            "total": total_violations, "open": total_open,
            "class_c": total_class_c, "open_class_c": total_open_c,
            "class_b": total_class_b, "class_a": total_class_a,
            "vs_city_avg": vs_city,
        },
        "complaints": {
            "total": total_complaints, "open": total_open_complaints,
            "top_categories": [{"category": r[0], "count": int(r[1])} for r in (tc_rows or [])],
        },
        "evictions": eviction_count,
        "dob_violations": {"total": dob_violations, "unpaid_penalties": dob_unpaid},
        "enforcement_flags": {
            "aep": [{"bbl": b[0], "status": b[1], "round": b[2]} for b in aep_buildings],
            "conh": [{"bbl": b[0], "status": b[1]} for b in conh_buildings],
            "underlying_conditions": [{"bbl": b[0], "status": b[1]} for b in underlying_buildings],
        },
        **graph_data,
    }

    return "\n".join(lines) + graph_text, structured


def _landlord_network_core(bbl: str, pool: object) -> tuple[str, dict]:
    """DuckPGQ ownership graph traversal for a single BBL."""
    cols, rows = execute(pool, f"""
        FROM GRAPH_TABLE (nyc_housing
            MATCH (b1:Building WHERE b1.bbl = '{bbl}')<-[o1:Owns]-(owner:Owner)-[o2:Owns]->(b2:Building)
            COLUMNS (
                owner.owner_id,
                owner.owner_name,
                b2.bbl,
                b2.housenumber || ' ' || b2.streetname AS address,
                b2.zip,
                b2.total_units,
                b2.stories
            )
        )
        ORDER BY bbl
    """)

    if not rows:
        return "", {}

    owner_id = rows[0][0]
    owner_name = rows[0][1] or f"Registration #{owner_id}"
    portfolio_bbls = list({r[2] for r in rows})

    placeholders = ", ".join(["?"] * len(portfolio_bbls))
    v_cols, v_rows = execute(pool, f"""
        SELECT
            bbl, COUNT(*) AS total_violations,
            COUNT(*) FILTER (WHERE severity = 'C') AS class_c,
            COUNT(*) FILTER (WHERE severity = 'B') AS class_b,
            COUNT(*) FILTER (WHERE status = 'Open') AS open_violations
        FROM main.graph_violations
        WHERE bbl IN ({placeholders})
        GROUP BY bbl
    """, portfolio_bbls)

    viol_map = {r[0]: {"total": r[1], "class_c": r[2], "class_b": r[3], "open": r[4]} for r in v_rows}

    f_cols, f_rows = execute(pool, f"""
        SELECT bbl, is_aep, litigation_count, harassment_findings, lien_count, dob_violation_count
        FROM main.graph_building_flags
        WHERE bbl IN ({placeholders})
          AND (is_aep = 1 OR litigation_count > 0 OR lien_count > 0 OR dob_violation_count > 0)
    """, portfolio_bbls)
    flag_map = {r[0]: {"aep": r[1], "litigations": r[2], "harassment": r[3],
                       "liens": r[4], "dob": r[5]} for r in f_rows}

    s_cols, s_rows = execute(pool, f"""
        SELECT bbl, last_sale_price, last_sale_date
        FROM main.graph_acris_sales
        WHERE bbl IN ({placeholders})
    """, portfolio_bbls)
    sale_map = {r[0]: {"price": r[1], "date": r[2]} for r in s_rows}

    rs_cols, rs_rows = execute(pool, f"""
        SELECT bbl, earliest_stab_units, latest_stab_units
        FROM main.graph_rent_stabilization
        WHERE bbl IN ({placeholders})
    """, portfolio_bbls)
    stab_map = {r[0]: {"earliest": r[1], "latest": r[2]} for r in rs_rows}

    corp_cols, corp_rows = execute(pool, """
        SELECT dos_id, current_entity_name, dos_process_name,
               dos_process_address_1, dos_process_city,
               registered_agent_name, chairman_name
        FROM main.graph_corp_contacts
        WHERE owner_id = ?
    """, [owner_id])

    biz_cols, biz_rows = execute(pool, f"""
        SELECT bbl, business_name, business_category, license_status
        FROM main.graph_business_at_building
        WHERE bbl IN ({placeholders})
    """, portfolio_bbls)
    biz_map: dict = {}
    for r in biz_rows:
        biz_map.setdefault(r[0], []).append({"name": r[1], "category": r[2], "status": r[3]})

    chain_cols, chain_rows = execute(pool, f"""
        SELECT bbl, party_name, role, amount, doc_date
        FROM main.graph_acris_chain
        WHERE bbl IN ({placeholders})
        ORDER BY doc_date DESC
    """, portfolio_bbls)

    try:
        evict_cols, evict_rows = execute(pool, f"""
            SELECT bbl, COUNT(*) AS eviction_count,
                   MAX(executed_date) AS latest_eviction
            FROM main.graph_eviction_petitioners
            WHERE bbl IN ({placeholders})
            GROUP BY bbl
        """, portfolio_bbls)
        evict_map = {r[0]: {"count": r[1], "latest": r[2]} for r in evict_rows}
    except Exception:
        evict_map = {}

    # Build text
    lines = [f"GRAPH OWNERSHIP NETWORK for BBL {bbl}"]
    lines.append(f"Owner: {owner_name} (reg #{owner_id})")
    lines.append(f"Portfolio: {len(portfolio_bbls)} buildings")

    if corp_rows:
        people = set()
        for r in corp_rows:
            if r[2]:
                people.add(r[2])
            if r[5]:
                people.add(r[5])
            if r[6]:
                people.add(r[6])
        if people:
            lines.append(f"People behind LLC: {', '.join(list(people)[:5])}")
    lines.append("")

    total_v = sum(v.get("total", 0) for v in viol_map.values())
    total_open = sum(v.get("open", 0) for v in viol_map.values())
    total_c = sum(v.get("class_c", 0) for v in viol_map.values())
    total_litigations = sum(f.get("litigations", 0) for f in flag_map.values())
    total_aep = sum(1 for f in flag_map.values() if f.get("aep"))
    total_liens = sum(f.get("liens", 0) for f in flag_map.values())

    lines.append(f"Portfolio totals: {total_v} violations ({total_open} open, {total_c} class C)")
    flags_summary = []
    if total_litigations:
        flags_summary.append(f"{total_litigations} litigations")
    if total_aep:
        flags_summary.append(f"{total_aep} AEP buildings")
    if total_liens:
        flags_summary.append(f"{total_liens} tax liens")
    total_evictions = sum(e.get("count", 0) for e in evict_map.values())
    if total_evictions:
        flags_summary.append(f"{total_evictions} evictions")
    total_stab_earliest = sum(st.get("earliest") or 0 for st in stab_map.values())
    total_stab_latest = sum(st.get("latest") or 0 for st in stab_map.values())
    if total_stab_earliest > 0:
        lost = total_stab_earliest - total_stab_latest
        if lost > 0:
            flags_summary.append(f"{lost} stabilized units lost ({total_stab_earliest} -> {total_stab_latest})")
        else:
            flags_summary.append(f"{total_stab_latest} stabilized units")
    if flags_summary:
        lines.append(f"Red flags: {', '.join(flags_summary)}")
    lines.append("")

    seen_bbls: set = set()
    deduped_rows = []
    for r in rows:
        if r[2] not in seen_bbls:
            seen_bbls.add(r[2])
            deduped_rows.append(r)

    for r in deduped_rows:
        b = r[2]
        addr = r[3] or "Unknown"
        v = viol_map.get(b, {})
        f = flag_map.get(b, {})
        s = sale_map.get(b, {})
        st = stab_map.get(b, {})
        tags = []
        if f.get("aep"):
            tags.append("AEP")
        if f.get("harassment"):
            tags.append("HARASSMENT")
        if f.get("liens"):
            tags.append(f"LIEN({f['liens']})")
        if f.get("litigations"):
            tags.append(f"LIT({f['litigations']})")
        if st.get("earliest") and st["earliest"] > 0:
            lost = (st["earliest"] or 0) - (st["latest"] or 0)
            if lost > 0:
                tags.append(f"DESTAB(-{lost})")
            elif st.get("latest") and st["latest"] > 0:
                tags.append(f"STAB({st['latest']})")
        tag_str = f" [{','.join(tags)}]" if tags else ""
        sale_str = ""
        if s.get("price"):
            sale_str = f" | sold ${s['price']:,.0f}" + (f" ({s['date']})" if s.get("date") else "")
        lines.append(f"  {b} | {addr} | {r[4] or '?'} | {r[5] or '?'} units | "
                     f"violations: {v.get('total', 0)} (open: {v.get('open', 0)}, "
                     f"C: {v.get('class_c', 0)}){sale_str}{tag_str}")

    if biz_map:
        lines.append("\nBUSINESSES AT PORTFOLIO:")
        for b_bbl, businesses in list(biz_map.items())[:10]:
            for biz in businesses[:3]:
                lines.append(f"  {b_bbl} | {biz['name']} | {biz['category']} | {biz['status']}")

    if chain_rows:
        lines.append("\nOWNERSHIP CHAIN (recent deeds):")
        seen_chain: set = set()
        for r in chain_rows[:10]:
            key = (r[0], r[1], r[2])
            if key not in seen_chain:
                seen_chain.add(key)
                amt_str = f"${r[3]:,.0f}" if r[3] else "?"
                lines.append(f"  {r[0]} | {r[2]}: {r[1]} | {amt_str} | {r[4] or '?'}")

    data = {
        "graph_owner_id": owner_id,
        "graph_owner_name": owner_name,
        "graph_portfolio_size": len(portfolio_bbls),
        "graph_total_violations": total_v,
        "graph_buildings": [
            {
                "bbl": r[2], "address": r[3], "zip": r[4],
                "total_units": r[5], "stories": r[6],
                **viol_map.get(r[2], {}),
                **{f"flag_{k}": v for k, v in flag_map.get(r[2], {}).items()},
            }
            for r in deduped_rows
        ],
    }

    return "\n".join(lines), data


def _ownership_for_name(name: str, depth: int, ctx: Context) -> tuple[str, dict]:
    """Ownership traversal starting from a name — find matching owners then traverse."""
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    search = name.upper()

    # Find owner by name in graph_owners
    cols, rows = execute(pool, """
        SELECT owner_id, owner_name
        FROM main.graph_owners
        WHERE UPPER(owner_name) LIKE ?
        ORDER BY owner_name
        LIMIT 5
    """, [f"%{search}%"])

    if not rows:
        return "", {}

    owner_id = rows[0][0]
    owner_name = rows[0][1]

    # Find their BBLs
    _, bbl_rows = execute(pool, """
        SELECT bbl FROM main.graph_owns WHERE owner_id = ? LIMIT 1
    """, [owner_id])

    if not bbl_rows:
        return f"Owner '{owner_name}' found but no buildings linked.", {"owner_name": owner_name}

    bbl = bbl_rows[0][0]
    return _landlord_network_core(bbl, pool)


# ===================================================================
# type = "corporate" — corporate_web + llc_piercer + shell_detector
# ===================================================================


def _corporate(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    t0 = time.time()
    text, data = _corporate_core(name, ctx)
    elapsed = round((time.time() - t0) * 1000)
    if not text:
        return ToolResult(content=f"No corporate data found for '{name}'.", meta={"query_time_ms": elapsed})
    return ToolResult(
        content=text + f"\n\n({elapsed}ms)",
        structured_content=data,
        meta={"query_time_ms": elapsed},
    )


def _corporate_core(name: str, ctx: Context) -> tuple[str, dict]:
    """Corporate web + LLC piercer logic."""
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    search = name.upper()
    extra_names = vector_expand_names(ctx, search)

    # --- Corporate Web: corps by name ---
    corp_cols, corp_rows = execute(pool, """
        SELECT dos_id, current_entity_name, entity_type, county,
               dos_process_name, chairman_name, initial_dos_filing_date
        FROM main.graph_corps
        WHERE UPPER(current_entity_name) LIKE ?
           OR UPPER(dos_process_name) LIKE ?
           OR UPPER(chairman_name) LIKE ?
        ORDER BY current_entity_name
        LIMIT 20
    """, [f"%{search}%"] * 3)

    people_cols, people_rows = execute(pool, """
        SELECT person_name, corp_count, primary_address
        FROM main.graph_corp_people
        WHERE person_name LIKE ?
        ORDER BY corp_count DESC
        LIMIT 10
    """, [f"%{search}%"])

    # Graph traversal: shared officers
    connected_corps = []
    if corp_rows:
        primary_dos = corp_rows[0][0]
        try:
            gc_cols, gc_rows = execute(pool, f"""
                FROM GRAPH_TABLE (nyc_corporate_web
                    MATCH (start:Corp WHERE start.dos_id = '{primary_dos}')-[e:SharedOfficer]-{{1,2}}(target:Corp)
                    COLUMNS (
                        target.dos_id,
                        target.current_entity_name,
                        target.entity_type,
                        target.chairman_name
                    )
                )
                LIMIT 50
            """)
            connected_corps = gc_rows
        except Exception:
            gc_cols, gc_rows = execute(pool, """
                SELECT c.dos_id, c.current_entity_name, c.entity_type, c.chairman_name
                FROM main.graph_corp_shared_officer s
                JOIN main.graph_corps c ON s.corp2 = c.dos_id
                WHERE s.corp1 = ?
                UNION
                SELECT c.dos_id, c.current_entity_name, c.entity_type, c.chairman_name
                FROM main.graph_corp_shared_officer s
                JOIN main.graph_corps c ON s.corp1 = c.dos_id
                WHERE s.corp2 = ?
                LIMIT 50
            """, [primary_dos, primary_dos])
            connected_corps = gc_rows

    person_corps = []
    if people_rows:
        primary_person = people_rows[0][0]
        pc_cols, pc_rows = execute(pool, """
            SELECT c.dos_id, c.current_entity_name, c.entity_type, c.chairman_name
            FROM main.graph_corp_officer_edges e
            JOIN main.graph_corps c ON e.dos_id = c.dos_id
            WHERE e.person_name = ?
            ORDER BY c.current_entity_name
            LIMIT 50
        """, [primary_person])
        person_corps = pc_rows

    # HPD cross-reference
    hpd_matches = []
    corp_names = [r[1] for r in corp_rows] + [r[1] for r in connected_corps]
    if corp_names:
        placeholders = ", ".join(["?"] * min(len(corp_names), 50))
        hpd_cols, hpd_rows = execute(pool, f"""
            SELECT owner_name, owner_id
            FROM main.graph_owners
            WHERE UPPER(owner_name) IN ({placeholders})
            LIMIT 20
        """, [n.upper() for n in corp_names[:50]])
        hpd_matches = hpd_rows

    # --- LLC Piercer ---
    llc_corp_rows = []
    try:
        llc_mv_sql = """
            SELECT dos_id, current_entity_name, entity_type, person_name, role, person_address,
                   dos_process_name, dos_process_address_1, registered_agent_name, chairman_name
            FROM lake.foundation.mv_corp_network
            WHERE UPPER(current_entity_name) LIKE ?
            ORDER BY current_entity_name
            LIMIT 50
        """
        mv_cols, mv_rows = execute(pool, llc_mv_sql, [f"%{search}%"])
        if mv_rows:
            seen: set = set()
            for r in mv_rows:
                mv = dict(zip(mv_cols, r))
                key = mv.get("current_entity_name")
                if key not in seen:
                    seen.add(key)
                    llc_corp_rows.append((
                        mv.get("current_entity_name"), mv.get("entity_type"), None,
                        mv.get("dos_process_name"), mv.get("dos_process_address_1"), None, None,
                        mv.get("registered_agent_name"), None,
                        mv.get("chairman_name"), None, None,
                    ))
    except Exception:
        pass

    if not llc_corp_rows:
        try:
            _, llc_corp_rows = execute(pool, """
                SELECT current_entity_name, entity_type, initial_dos_filing_date,
                       dos_process_name, dos_process_address_1, dos_process_city, dos_process_state,
                       registered_agent_name, registered_agent_address_1,
                       chairman_name, chairman_address_1, county
                FROM lake.business.nys_corporations
                WHERE UPPER(current_entity_name) LIKE ?
                LIMIT 20
            """, [f"%{search}%"])
        except Exception:
            pass

    acris_cols, acris_rows = execute(pool, """
        SELECT p.name, p.party_type, p.address_1, p.city, p.state, p.zip,
               m.doc_type, TRY_CAST(m.document_amt AS DOUBLE) AS amount,
               TRY_CAST(m.document_date AS DATE) AS doc_date,
               m.document_id
        FROM lake.housing.acris_parties p
        JOIN lake.housing.acris_master m ON p.document_id = m.document_id
        WHERE UPPER(p.name) LIKE ?
        ORDER BY m.document_date DESC
        LIMIT 30
    """, [f"%{search}%"])

    hpd_pierce_cols, hpd_pierce_rows = execute(pool, """
        SELECT registrationid, type, corporationname, firstname, lastname,
               businesshousenumber, businessstreetname, businesszip
        FROM lake.housing.hpd_registration_contacts
        WHERE UPPER(corporationname) LIKE ?
           OR UPPER(firstname || ' ' || lastname) LIKE ?
        LIMIT 20
    """, [f"%{search}%", f"%{search}%"])

    if not corp_rows and not people_rows and not connected_corps and not llc_corp_rows and not acris_rows:
        return "", {}

    # Build text
    lines = [f"CORPORATE WEB — '{name}'"]

    if corp_rows:
        lines.append(f"\nDIRECT CORP MATCHES ({len(corp_rows)}):")
        for r in corp_rows:
            people = [p for p in [r[4], r[5]] if p]
            people_str = f" | People: {', '.join(people)}" if people else ""
            lines.append(f"  {r[1]} ({r[2] or '?'}) | {r[3] or '?'} | filed {r[6] or '?'}{people_str}")

    if connected_corps:
        lines.append(f"\nCONNECTED VIA SHARED OFFICERS ({len(connected_corps)}):")
        for r in connected_corps:
            lines.append(f"  {r[1]} ({r[2] or '?'}) | Chairman: {r[3] or '?'}")

    if people_rows:
        lines.append(f"\nPEOPLE MATCHES:")
        for r in people_rows:
            lines.append(f"  {r[0]} — officer of {r[1]} corps | Address: {r[2] or '?'}")

    if person_corps:
        lines.append(f"\nCORPS FOR '{people_rows[0][0]}' ({len(person_corps)}):")
        for r in person_corps:
            lines.append(f"  {r[1]} ({r[2] or '?'}) | Chairman: {r[3] or '?'}")

    if hpd_matches:
        lines.append(f"\nHPD LANDLORD MATCHES ({len(hpd_matches)}):")
        for r in hpd_matches:
            lines.append(f"  {r[0]} (registration #{r[1]})")

    # LLC piercer section
    if llc_corp_rows:
        lines.append(f"\nLLC PIERCER — NYS CORPORATIONS ({len(llc_corp_rows)} matches):")
        for r in llc_corp_rows:
            lines.append(f"  Entity: {r[0]} ({r[1] or '?'}) filed {r[2] or '?'}")
            people_list = []
            if r[3]:
                people_list.append(f"Process: {r[3]}, {r[4] or ''}")
            if r[7]:
                people_list.append(f"Agent: {r[7]}")
            if r[9]:
                people_list.append(f"Chairman: {r[9]}")
            for p in people_list:
                lines.append(f"    {p}")

    if acris_rows:
        lines.append(f"\nACRIS TRANSACTIONS ({len(acris_rows)} matches):")
        for r in acris_rows[:15]:
            party_label = "BUYER" if str(r[1]) == "2" else "SELLER" if str(r[1]) == "1" else f"TYPE-{r[1]}"
            amt_str = f"${r[7]:,.0f}" if r[7] else "?"
            lines.append(f"  {party_label}: {r[0]} | {r[6] or '?'} | {amt_str} | {r[8] or '?'}")

    if hpd_pierce_rows:
        lines.append(f"\nHPD REGISTRATIONS ({len(hpd_pierce_rows)} matches):")
        for r in hpd_pierce_rows:
            reg_name = r[2] or f"{r[3] or ''} {r[4] or ''}".strip()
            addr = f"{r[5] or ''} {r[6] or ''} {r[7] or ''}".strip()
            lines.append(f"  Reg#{r[0]} | {r[1]} | {reg_name} | {addr}")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  -> {en}")

    data = {
        "search": name,
        "direct_corps": [dict(zip(corp_cols, r)) for r in corp_rows],
        "connected_corps": [{"dos_id": r[0], "name": r[1], "type": r[2], "chairman": r[3]}
                            for r in connected_corps],
        "people": [dict(zip(people_cols, r)) for r in people_rows],
        "hpd_matches": [{"owner_name": r[0], "owner_id": r[1]} for r in hpd_matches],
        "llc_piercer": {
            "nys_corps": len(llc_corp_rows),
            "acris_transactions": len(acris_rows),
            "hpd_registrations": len(hpd_pierce_rows),
        },
    }

    return "\n".join(lines), data


# ===================================================================
# type = "political" — pay_to_play + money_trail
# ===================================================================


def _political(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    t0 = time.time()
    text, data = _political_core(name, ctx)
    elapsed = round((time.time() - t0) * 1000)
    if not text:
        return ToolResult(content=f"No political data found for '{name}'.", meta={"query_time_ms": elapsed})
    return ToolResult(
        content=text + f"\n\n({elapsed}ms)",
        structured_content=data,
        meta={"query_time_ms": elapsed},
    )


def _political_core(name: str, ctx: Context) -> tuple[str, dict]:
    """Pay-to-play + money trail combined."""
    pool = ctx.lifespan_context["pool"]
    search = name.upper()
    extra_names = vector_expand_names(ctx, search)

    # --- Money trail (broader) ---
    parts = name.split()
    first = parts[0] if len(parts) >= 2 else "%"
    last = parts[-1] if len(parts) >= 2 else name

    _, nys_rows = safe_query(pool, MONEY_NYS_DONATIONS_SQL, [f"{last}%", f"{first}%", name])
    _, fec_rows = safe_query(pool, MONEY_FEC_SQL, [name])
    _, nyc_rows = safe_query(pool, MONEY_NYC_DONATIONS_SQL, [name, name])
    _, contract_rows = safe_query(pool, MONEY_CONTRACTS_SQL, [name])
    _, procurement_rows = safe_query(pool, MONEY_PROCUREMENT_SQL, [name])

    # Fuzzy FEC
    fuzzy_fec = []
    try:
        from entity import fuzzy_money_search_sql
        fec_sql, fec_params = fuzzy_money_search_sql(
            name=name,
            table="lake.federal.fec_contributions", name_col="contributor_name",
            extra_cols="committee_id, contribution_receipt_amount, contribution_receipt_date",
            min_score=75, limit=15,
        )
        _, fuzzy_fec = execute(pool, fec_sql, fec_params)
    except Exception:
        pass

    # --- Pay-to-play (DuckPGQ graph) ---
    p2p_lines: list[str] = []
    p2p_data: dict = {}
    try:
        require_graph(ctx)
        p2p_text, p2p_data = _pay_to_play_core(search, pool, extra_names)
        if p2p_text:
            p2p_lines = [p2p_text]
    except (ToolError, Exception):
        pass

    total_sections = sum(1 for r in [nys_rows, fec_rows, nyc_rows, contract_rows, procurement_rows] if r)
    if total_sections == 0 and not p2p_lines:
        return "", {}

    lines = [f"POLITICAL MONEY — {name}", "=" * 55]

    if nys_rows:
        nys_total = sum(float(r[3] or 0) for r in nys_rows)
        nys_count = sum(int(r[4] or 0) for r in nys_rows)
        lines.append(f"\nNYS CAMPAIGN FINANCE ({nys_count} donations, ${nys_total:,.0f} total):")
        for r in nys_rows[:8]:
            filer, fn, ln, total, cnt, first_d, last_d = r
            lines.append(f"  -> {filer}: ${float(total or 0):,.0f} ({cnt} donations, {first_d or '?'}-{last_d or '?'})")

    if fec_rows:
        fec_total = sum(float(r[2] or 0) for r in fec_rows)
        lines.append(f"\nFEDERAL (FEC) CONTRIBUTIONS (${fec_total:,.0f} total):")
        for r in fec_rows[:8]:
            cmte_id, contributor, total, cnt, first_d, last_d = r
            lines.append(f"  -> {cmte_id} ({contributor}): ${float(total or 0):,.0f} ({cnt} donations)")

    if nyc_rows:
        nyc_total = sum(float(r[4] or 0) for r in nyc_rows)
        lines.append(f"\nNYC CAMPAIGN FINANCE (${nyc_total:,.0f} total):")
        for r in nyc_rows[:8]:
            recip, occ, emp, addr, total, cnt = r
            lines.append(f"  -> {recip}: ${float(total or 0):,.0f} ({cnt} donations)")
            if emp:
                lines.append(f"    Employer: {emp}")

    if contract_rows:
        lines.append(f"\nCITY CONTRACTS ({len(contract_rows)} awards):")
        for r in contract_rows[:5]:
            vendor, amount, agency, purpose, start, end = r
            lines.append(f"  {vendor}: ${float(amount or 0):,.0f} — {agency or '?'}")
            if purpose:
                lines.append(f"    {purpose[:120]}")

    if procurement_rows:
        lines.append(f"\nSTATE PROCUREMENT ({len(procurement_rows)} contracts):")
        for r in procurement_rows[:5]:
            vendor, amount, agency, desc = r
            lines.append(f"  {vendor}: ${float(amount or 0):,.0f} — {agency or '?'}")

    if fuzzy_fec and not fec_rows:
        lines.append(f"\nFUZZY FEC MATCHES ({len(fuzzy_fec)} — name variants):")
        for r in fuzzy_fec[:5]:
            lines.append(f"  {r[0]} — ${float(r[2] or 0):,.0f} (score: {r[-1]:.0f})")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  -> {en}")

    for p in p2p_lines:
        lines.append(f"\n{p}")

    data = {
        "money_trail": {
            "nys_donations": len(nys_rows),
            "fec_contributions": len(fec_rows),
            "nyc_donations": len(nyc_rows),
            "contracts": len(contract_rows),
            "procurement": len(procurement_rows),
        },
        **p2p_data,
    }

    return "\n".join(lines), data


def _pay_to_play_core(search: str, pool: object, extra_names: set) -> tuple[str, dict]:
    """DuckPGQ pay-to-play graph traversal."""
    ent_cols, ent_rows = execute(pool, """
        SELECT entity_name, roles, total_amount, total_transactions
        FROM main.graph_pol_entities
        WHERE entity_name LIKE ?
        ORDER BY total_amount DESC NULLS LAST
        LIMIT 10
    """, [f"%{search}%"])

    if not ent_rows:
        return "", {}

    primary = ent_rows[0]
    primary_name = primary[0]

    try:
        graph_cols, graph_rows = execute(pool, f"""
            FROM GRAPH_TABLE (nyc_influence_network
                MATCH (start:PoliticalEntity WHERE start.entity_name = '{primary_name.replace("'", "''")}')-[e]-{{1,2}}(target:PoliticalEntity)
                COLUMNS (
                    target.entity_name AS connected_entity,
                    target.roles AS connected_roles,
                    target.total_amount AS connected_amount
                )
            )
            ORDER BY connected_amount DESC NULLS LAST
            LIMIT 50
        """)
    except Exception:
        graph_cols, graph_rows = [], []

    don_to_cols, don_to_rows = execute(pool, """
        SELECT candidate_name, total_donated, donation_count, employer, latest_date
        FROM main.graph_pol_donations
        WHERE donor_name = ?
        ORDER BY total_donated DESC
        LIMIT 20
    """, [primary_name])

    don_from_cols, don_from_rows = execute(pool, """
        SELECT donor_name, total_donated, donation_count, employer, latest_date
        FROM main.graph_pol_donations
        WHERE candidate_name = ?
        ORDER BY total_donated DESC
        LIMIT 20
    """, [primary_name])

    con_cols, con_rows = execute(pool, """
        SELECT agency_name, total_amount, contract_count, latest_date
        FROM main.graph_pol_contracts
        WHERE vendor_name = ?
        ORDER BY total_amount DESC
        LIMIT 10
    """, [primary_name])

    lob_cols, lob_rows = execute(pool, """
        SELECT client_name, total_compensation, filing_count, industry, latest_year
        FROM main.graph_pol_lobbying
        WHERE lobbyist_name = ?
        ORDER BY total_compensation DESC NULLS LAST
        LIMIT 10
    """, [primary_name])

    client_cols, client_rows = execute(pool, """
        SELECT lobbyist_name, total_compensation, filing_count, industry, latest_year
        FROM main.graph_pol_lobbying
        WHERE client_name = ?
        ORDER BY total_compensation DESC NULLS LAST
        LIMIT 10
    """, [primary_name])

    p2p_cols, p2p_rows = execute(pool, """
        SELECT d.donor_name, d.candidate_name, d.total_donated, d.employer,
               c.agency_name, c.total_amount AS contract_amount
        FROM main.graph_pol_donations d
        JOIN main.graph_pol_contracts c ON UPPER(d.employer) = c.vendor_name
        WHERE d.candidate_name = ? OR d.donor_name = ?
        ORDER BY c.total_amount DESC
        LIMIT 20
    """, [primary_name, primary_name])

    fec_cols, fec_rows = execute(pool, """
        SELECT committee_id, total_donated, donation_count, employer, occupation, latest_cycle
        FROM main.graph_fec_contributions
        WHERE donor_name = ?
        ORDER BY total_donated DESC
        LIMIT 15
    """, [primary_name])

    lit_cols, lit_rows = execute(pool, """
        SELECT respondent_name, bbl, casetype, casestatus, case_open_date, findingofharassment
        FROM main.graph_litigation_respondents
        WHERE respondent_name = ?
        ORDER BY case_open_date DESC
        LIMIT 15
    """, [primary_name])

    lines = [f"PAY-TO-PLAY NETWORK — '{primary_name}'"]
    lines.append(f"Roles: {primary[1]} | Total value: ${primary[2]:,.0f}" if primary[2] else f"Roles: {primary[1]}")
    lines.append("")

    if don_to_rows:
        total = sum(r[1] or 0 for r in don_to_rows)
        lines.append(f"DONATES TO ({len(don_to_rows)} candidates, ${total:,.0f} total):")
        for r in don_to_rows[:10]:
            lines.append(f"  {r[0]} — ${r[1]:,.0f} ({r[2]} donations)")

    if don_from_rows:
        total = sum(r[1] or 0 for r in don_from_rows)
        lines.append(f"\nFUNDED BY ({len(don_from_rows)} donors, ${total:,.0f} total):")
        for r in don_from_rows[:10]:
            emp_str = f" [{r[3]}]" if r[3] else ""
            lines.append(f"  {r[0]} — ${r[1]:,.0f} ({r[2]} donations){emp_str}")

    if fec_rows:
        fec_total = sum(r[1] or 0 for r in fec_rows)
        lines.append(f"\nFEDERAL DONATIONS (FEC — ${fec_total:,.0f} total):")
        for r in fec_rows[:10]:
            lines.append(f"  Committee {r[0]} — ${r[1]:,.0f} ({r[2]} donations) "
                         f"| {r[3] or '?'} | cycle {r[5] or '?'}")

    if lit_rows:
        harassment_count = sum(1 for r in lit_rows if r[5] and r[5].strip())
        lines.append(f"\n*** HPD LITIGATION HISTORY ({len(lit_rows)} cases, "
                     f"{harassment_count} harassment findings) ***")
        for r in lit_rows[:8]:
            harass = " *** HARASSMENT ***" if r[5] and r[5].strip() else ""
            lines.append(f"  {r[1]} | {r[2]} | {r[3]} | {r[4] or '?'}{harass}")

    if con_rows:
        total = sum(r[1] or 0 for r in con_rows)
        lines.append(f"\nCITY CONTRACTS (${total:,.0f} total):")
        for r in con_rows:
            lines.append(f"  {r[0]} — ${r[1]:,.0f} ({r[2]} contracts)")

    if lob_rows:
        lines.append(f"\nLOBBYING CLIENTS:")
        for r in lob_rows:
            comp_str = f"${r[1]:,.0f}" if r[1] else "?"
            lines.append(f"  {r[0]} — {comp_str} ({r[3] or '?'})")

    if client_rows:
        lines.append(f"\nHIRED LOBBYISTS:")
        for r in client_rows:
            comp_str = f"${r[1]:,.0f}" if r[1] else "?"
            lines.append(f"  {r[0]} — {comp_str}")

    if p2p_rows:
        lines.append(f"\n*** PAY-TO-PLAY SIGNALS ({len(p2p_rows)}) ***")
        for r in p2p_rows[:10]:
            lines.append(f"  {r[0]} (works at {r[3]}) -> donates ${r[2]:,.0f} -> {r[1]}")
            lines.append(f"    {r[3]} has ${r[5]:,.0f} in contracts from {r[4]}")

    if graph_rows:
        lines.append(f"\nGRAPH-CONNECTED ENTITIES ({len(graph_rows)} within 2 hops):")
        for r in graph_rows[:15]:
            amt_str = f"${r[2]:,.0f}" if r[2] else "?"
            lines.append(f"  {r[0][:40]} | {r[1]} | {amt_str}")

    data = {
        "pay_to_play": {
            "entity": dict(zip(ent_cols, primary)),
            "donates_to": len(don_to_rows),
            "funded_by": len(don_from_rows),
            "contracts": len(con_rows),
            "p2p_signals": len(p2p_rows),
        }
    }

    return "\n".join(lines), data


# ===================================================================
# type = "property" — transaction_network
# ===================================================================


def _property(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    t0 = time.time()
    text, data = _property_core(name, ctx)
    elapsed = round((time.time() - t0) * 1000)
    if not text:
        return ToolResult(content=f"No transaction data found for '{name}'.", meta={"query_time_ms": elapsed})
    return ToolResult(
        content=text + f"\n\n({elapsed}ms)",
        structured_content=data,
        meta={"query_time_ms": elapsed},
    )


def _property_core(name: str, ctx: Context) -> tuple[str, dict]:
    """ACRIS transaction network via DuckPGQ."""
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    search = name.upper()
    extra_names = vector_expand_names(ctx, search)

    ent_cols, ent_rows = execute(pool, """
        SELECT entity_name, tx_count, property_count, as_seller, as_buyer, total_amount
        FROM main.graph_tx_entities
        WHERE entity_name LIKE ?
        ORDER BY tx_count DESC
        LIMIT 10
    """, [f"%{search}%"])

    if not ent_rows:
        return "", {}

    primary = ent_rows[0]
    primary_name = primary[0]

    prop_cols, prop_rows = execute(pool, """
        SELECT bbl, role, amount, doc_date, document_id
        FROM main.graph_tx_edges
        WHERE entity_name = ?
        ORDER BY doc_date DESC
    """, [primary_name])

    try:
        graph_cols, graph_rows = execute(pool, f"""
            FROM GRAPH_TABLE (nyc_transaction_network
                MATCH (start:TxEntity WHERE start.entity_name = '{primary_name.replace("'", "''")}')-[e:SharedTransaction]-{{1,2}}(target:TxEntity)
                COLUMNS (
                    target.entity_name AS connected_entity,
                    target.tx_count,
                    target.property_count,
                    target.as_seller,
                    target.as_buyer,
                    target.total_amount,
                    element_id(e) AS edge_id
                )
            )
            ORDER BY target.tx_count DESC
            LIMIT 50
        """)
    except Exception:
        graph_cols, graph_rows = execute(pool, """
            SELECT entity2 AS connected_entity, e.shared_docs AS tx_count,
                   t.property_count, t.as_seller, t.as_buyer, t.total_amount, NULL
            FROM main.graph_tx_shared e
            JOIN main.graph_tx_entities t ON e.entity2 = t.entity_name
            WHERE e.entity1 = ?
            UNION ALL
            SELECT entity1, e.shared_docs, t.property_count, t.as_seller, t.as_buyer, t.total_amount, NULL
            FROM main.graph_tx_shared e
            JOIN main.graph_tx_entities t ON e.entity1 = t.entity_name
            WHERE e.entity2 = ?
            ORDER BY 2 DESC
            LIMIT 50
        """, [primary_name, primary_name])

    lines = [f"TRANSACTION NETWORK — '{primary_name}'"]
    lines.append(f"Transactions: {primary[1]} | Properties: {primary[2]} | "
                 f"As seller: {primary[3]} | As buyer: {primary[4]}")
    if primary[5]:
        lines.append(f"Total transaction value: ${primary[5]:,.0f}")
    lines.append("")

    if prop_rows:
        lines.append(f"PROPERTIES ({len(prop_rows)} transactions):")
        for r in prop_rows[:20]:
            amt_str = f"${r[2]:,.0f}" if r[2] else "?"
            lines.append(f"  {r[0]} | {r[1]} | {amt_str} | {r[3] or '?'}")
        if len(prop_rows) > 20:
            lines.append(f"  ... and {len(prop_rows) - 20} more")
        lines.append("")

    if graph_rows:
        lines.append(f"CONNECTED ENTITIES ({len(graph_rows)} via shared transactions):")
        for r in graph_rows[:25]:
            amt_str = f"${r[5]:,.0f}" if r[5] else "?"
            lines.append(f"  {r[0][:40]} | {r[1]} txns | {r[2]} props | "
                         f"sell:{r[3]} buy:{r[4]} | {amt_str}")

    if len(ent_rows) > 1:
        lines.append(f"\nOTHER NAME MATCHES:")
        for r in ent_rows[1:]:
            lines.append(f"  {r[0]} ({r[1]} txns, {r[2]} props)")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  -> {en}")

    data = {
        "entity": dict(zip(ent_cols, primary)),
        "properties": [dict(zip(prop_cols, r)) for r in prop_rows],
        "connected_entities": [dict(zip(graph_cols, r)) for r in graph_rows] if graph_rows else [],
    }

    return "\n".join(lines), data


# ===================================================================
# type = "contractor" — contractor_network
# ===================================================================


def _contractor(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    t0 = time.time()
    search = name.upper()
    extra_names = set() if search.replace('-', '').isdigit() else vector_expand_names(ctx, search)

    if search.isdigit():
        c_cols, c_rows = execute(pool, """
            SELECT license, business_name, permit_count, building_count, first_permit, last_permit
            FROM main.graph_contractors WHERE license = ?
        """, [search])
    else:
        c_cols, c_rows = execute(pool, """
            SELECT license, business_name, permit_count, building_count, first_permit, last_permit
            FROM main.graph_contractors WHERE business_name LIKE ?
            ORDER BY permit_count DESC LIMIT 10
        """, [f"%{search}%"])

    if not c_rows:
        elapsed = round((time.time() - t0) * 1000)
        return ToolResult(content=f"No contractor found matching '{name}'.", meta={"query_time_ms": elapsed})

    primary = c_rows[0]
    license_num = primary[0]

    bldg_cols, bldg_rows = execute(pool, """
        SELECT pe.bbl, pe.permit_count, pe.job_type, pe.first_date, pe.last_date, pe.owner_name,
               b.housenumber || ' ' || b.streetname AS address, b.zip, b.total_units
        FROM main.graph_permit_edges pe
        LEFT JOIN main.graph_buildings b ON pe.bbl = b.bbl
        WHERE pe.license = ?
        ORDER BY pe.permit_count DESC
        LIMIT 30
    """, [license_num])

    building_bbls = [r[0] for r in bldg_rows if r[0]]
    violation_stats: dict = {}
    if building_bbls:
        placeholders = ", ".join(["?"] * min(len(building_bbls), 30))
        v_cols, v_rows = execute(pool, f"""
            SELECT bbl,
                   COUNT(*) AS total_violations,
                   COUNT(*) FILTER (WHERE UPPER(class) = 'C') AS class_c,
                   COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_v
            FROM lake.housing.hpd_violations
            WHERE bbl IN ({placeholders})
            GROUP BY bbl
        """, building_bbls[:30])
        violation_stats = {r[0]: {"total": r[1], "class_c": r[2], "open": r[3]} for r in v_rows}

    try:
        net_cols, net_rows = execute(pool, f"""
            FROM GRAPH_TABLE (nyc_contractor_network
                MATCH (start:Contractor WHERE start.license = '{license_num}')-[e:SharedBuilding]-(target:Contractor)
                COLUMNS (
                    target.license,
                    target.business_name,
                    target.permit_count,
                    target.building_count,
                    e.shared_buildings
                )
            )
            ORDER BY e.shared_buildings DESC
            LIMIT 25
        """)
    except Exception:
        net_cols, net_rows = execute(pool, """
            SELECT c.license, c.business_name, c.permit_count, c.building_count, s.shared_buildings
            FROM main.graph_contractor_shared s
            JOIN main.graph_contractors c ON s.license2 = c.license
            WHERE s.license1 = ?
            UNION ALL
            SELECT c.license, c.business_name, c.permit_count, c.building_count, s.shared_buildings
            FROM main.graph_contractor_shared s
            JOIN main.graph_contractors c ON s.license1 = c.license
            WHERE s.license2 = ?
            ORDER BY 5 DESC
            LIMIT 25
        """, [license_num, license_num])

    owner_cols, owner_rows = execute(pool, """
        SELECT owner_name, COUNT(*) AS buildings, SUM(permit_count) AS total_permits
        FROM main.graph_permit_edges
        WHERE license = ? AND owner_name IS NOT NULL AND LENGTH(owner_name) > 3
        GROUP BY owner_name
        ORDER BY total_permits DESC
        LIMIT 15
    """, [license_num])

    elapsed = round((time.time() - t0) * 1000)

    total_viols = sum(v.get("total", 0) for v in violation_stats.values())
    total_class_c = sum(v.get("class_c", 0) for v in violation_stats.values())
    avg_viols = total_viols / len(building_bbls) if building_bbls else 0

    lines = [f"CONTRACTOR NETWORK — {primary[1]} (License: {license_num})"]
    lines.append(f"Permits: {primary[2]:,} | Buildings: {primary[3]:,} | "
                 f"Active: {primary[4] or '?'} — {primary[5] or '?'}")
    lines.append(f"Violation exposure: {total_viols:,} total ({total_class_c:,} Class C) "
                 f"across portfolio | Avg {avg_viols:.1f}/building")
    lines.append("")

    if bldg_rows:
        lines.append(f"BUILDINGS ({len(bldg_rows)} shown):")
        for r in bldg_rows[:20]:
            v = violation_stats.get(r[0], {})
            v_str = f"viols:{v.get('total', 0)}(C:{v.get('class_c', 0)})" if v else ""
            lines.append(f"  {r[0]} | {r[6] or '?'} | {r[7] or '?'} | "
                         f"{r[5] or '?'} | {r[2] or '?'} | {v_str}")

    if net_rows:
        lines.append(f"\nCO-WORKERS ({len(net_rows)} contractors share 3+ buildings):")
        for r in net_rows[:15]:
            lines.append(f"  {r[1] or '?'} (#{r[0]}) | {r[3] or 0} buildings | "
                         f"{r[4]} shared with this contractor")

    if owner_rows:
        lines.append(f"\nTOP OWNERS SERVED:")
        for r in owner_rows[:10]:
            lines.append(f"  {r[0]} — {r[1]} buildings, {r[2]} permits")

    if len(c_rows) > 1:
        lines.append(f"\nOTHER MATCHES:")
        for r in c_rows[1:]:
            lines.append(f"  {r[1]} (#{r[0]}) — {r[2]:,} permits, {r[3]:,} buildings")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  -> {en}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "contractor": dict(zip(c_cols, primary)),
            "buildings": [dict(zip(bldg_cols, r)) for r in bldg_rows],
            "violation_stats": violation_stats,
            "connected_contractors": [dict(zip(net_cols, r)) for r in net_rows] if net_rows else [],
            "top_owners": [dict(zip(owner_cols, r)) for r in owner_rows],
        },
        meta={"building_count": len(bldg_rows), "total_violations": total_viols,
              "avg_violations": round(avg_viols, 1), "co_workers": len(net_rows or []),
              "query_time_ms": elapsed},
    )


# ===================================================================
# type = "tradewaste" — tradewaste_network
# ===================================================================


def _tradewaste(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    t0 = time.time()

    cols, rows = execute(pool, """
        SELECT bic_number, company_name, trade_name, address, record_count
        FROM main.graph_bic_companies
        WHERE UPPER(company_name) ILIKE '%' || UPPER(?) || '%'
        LIMIT 5
    """, [name])

    if not rows:
        elapsed = round((time.time() - t0) * 1000)
        return ToolResult(content=f"No trade waste company found matching '{name}'.", meta={"query_time_ms": elapsed})

    target = dict(zip(cols, rows[0]))
    bic = target["bic_number"]

    try:
        net_cols, net_rows = execute(pool, """
            SELECT c.company_name, c.trade_name, c.address, c.record_count, e.shared_properties
            FROM main.graph_bic_shared_bbl e
            JOIN main.graph_bic_companies c ON e.company2 = c.bic_number
            WHERE e.company1 = ?
            UNION ALL
            SELECT c.company_name, c.trade_name, c.address, c.record_count, e.shared_properties
            FROM main.graph_bic_shared_bbl e
            JOIN main.graph_bic_companies c ON e.company1 = c.bic_number
            WHERE e.company2 = ?
            ORDER BY shared_properties DESC
            LIMIT 20
        """, [bic, bic])
    except Exception:
        net_cols, net_rows = [], []

    viol_cols, viol_rows = execute(pool, """
        SELECT type_of_violation, COUNT(*) AS cnt,
               SUM(COALESCE(fine_amount, 0)) AS total_fines
        FROM main.graph_bic_violation_edges
        WHERE company_name = ?
        GROUP BY type_of_violation
        ORDER BY cnt DESC
        LIMIT 10
    """, [target["company_name"]])

    elapsed = round((time.time() - t0) * 1000)

    lines = [
        f"TRADE WASTE NETWORK — {target['company_name']}",
        f"  BIC #: {bic} | Trade name: {target.get('trade_name', '?')}",
        f"  Address: {target.get('address', '?')} | Records: {target.get('record_count', 0)}",
        "=" * 55,
    ]

    if viol_rows:
        lines.append(f"\nVIOLATIONS ({sum(r[1] for r in viol_rows)}):")
        for r in viol_rows:
            lines.append(f"  {r[0]}: {r[1]} violations, ${r[2]:,.0f} fines")

    if net_rows:
        lines.append(f"\nCONNECTED COMPANIES ({len(net_rows)} via shared properties):")
        for r in net_rows:
            d = dict(zip(net_cols, r))
            lines.append(f"  {d['company_name']} — {d.get('shared_properties', 0)} shared properties")
            if d.get('trade_name'):
                lines.append(f"    Trade name: {d['trade_name']}")
    else:
        lines.append("\nNo connected companies found.")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "company": target,
            "violations": [dict(zip(viol_cols, r)) for r in viol_rows],
            "connected": [dict(zip(net_cols, r)) for r in net_rows] if net_rows else [],
        },
        meta={"violations": sum(r[1] for r in viol_rows) if viol_rows else 0,
              "connected_count": len(net_rows or []), "query_time_ms": elapsed},
    )


# ===================================================================
# type = "officer" — officer_network
# ===================================================================


def _officer(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    t0 = time.time()

    parts = name.split()
    if len(parts) >= 2:
        first_name, last_name = parts[0], parts[-1]
        cols, rows = execute(pool, """
            SELECT officer_id, full_name, current_rank, current_command, total_complaints, substantiated
            FROM main.graph_officers
            WHERE UPPER(last_name) = UPPER(?) AND UPPER(first_name) LIKE UPPER(?) || '%'
            LIMIT 5
        """, [last_name, first_name])
    else:
        cols, rows = execute(pool, """
            SELECT officer_id, full_name, current_rank, current_command, total_complaints, substantiated
            FROM main.graph_officers
            WHERE UPPER(last_name) = UPPER(?) OR UPPER(full_name) ILIKE '%' || ? || '%'
            LIMIT 5
        """, [name, name])

    if not rows:
        elapsed = round((time.time() - t0) * 1000)
        raise ToolError(f"No officer found matching '{name}'. Try cop_sheet(name) for broader search.")

    target = dict(zip(cols, rows[0]))
    officer_id = target["officer_id"]

    try:
        net_cols, net_rows = execute(pool, """
            SELECT o2.full_name, o2.current_rank, o2.current_command,
                   o2.total_complaints, o2.substantiated, e.combined_complaints
            FROM main.graph_officer_shared_command e
            JOIN main.graph_officers o2 ON e.officer2 = o2.officer_id
            WHERE e.officer1 = ?
            ORDER BY e.combined_complaints DESC
            LIMIT 20
        """, [officer_id])
    except Exception:
        net_cols, net_rows = [], []

    elapsed = round((time.time() - t0) * 1000)

    lines = [
        f"OFFICER NETWORK — {target['full_name']}",
        f"  Rank: {target.get('current_rank', '?')} | Command: {target.get('current_command', '?')}",
        f"  CCRB complaints: {target.get('total_complaints', 0)} ({target.get('substantiated', 0)} substantiated)",
        "=" * 55,
    ]

    if net_rows:
        lines.append(f"\nCONNECTED OFFICERS ({len(net_rows)} via shared command):")
        for r in net_rows:
            d = dict(zip(net_cols, r))
            lines.append(f"  {d['full_name']} — {d.get('current_rank', '?')}, {d.get('current_command', '?')}")
            lines.append(f"    Complaints: {d.get('total_complaints', 0)} ({d.get('substantiated', 0)} subst), combined: {d.get('combined_complaints', 0)}")
    else:
        lines.append("\nNo connected officers found via shared command.")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "officer": target,
            "connected": [dict(zip(net_cols, r)) for r in net_rows] if net_rows else [],
        },
        meta={"connected_count": len(net_rows or []), "query_time_ms": elapsed},
    )


# ===================================================================
# type = "clusters" — ownership_clusters (WCC)
# ===================================================================


def _clusters(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    t0 = time.time()
    min_buildings = max(2, min(top_n, 100))
    boro = _boro_code(borough)

    cols, rows = execute(pool, f"""
        WITH wcc AS (
            SELECT * FROM weakly_connected_component(nyc_building_network, Building, SharedOwner)
        ),
        clusters AS (
            SELECT w.componentid,
                   COUNT(*) AS cluster_size,
                   LISTAGG(DISTINCT b.boroid, ',') AS boroughs
            FROM wcc w
            JOIN main.graph_buildings b ON w.bbl = b.bbl
            GROUP BY w.componentid
            HAVING COUNT(*) >= {min_buildings}
        )
        SELECT c.componentid, c.cluster_size, c.boroughs,
               b.bbl, b.housenumber || ' ' || b.streetname AS address, b.zip,
               b.boroid, b.total_units,
               ow.owner_name
        FROM clusters c
        JOIN wcc w ON c.componentid = w.componentid
        JOIN main.graph_buildings b ON w.bbl = b.bbl
        LEFT JOIN main.graph_owns o ON b.bbl = o.bbl
        LEFT JOIN main.graph_owners ow ON o.owner_id = ow.owner_id
        {"WHERE b.boroid = '" + boro + "'" if boro else ""}
        ORDER BY c.cluster_size DESC, c.componentid, b.bbl
        LIMIT 500
    """)

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content="No ownership clusters found matching criteria.", meta={"query_time_ms": elapsed})

    clusters: dict = {}
    for r in rows:
        cid = r[0]
        if cid not in clusters:
            clusters[cid] = {"size": r[1], "boroughs": r[2], "buildings": []}
        clusters[cid]["buildings"].append({
            "bbl": r[3], "address": r[4], "zip": r[5],
            "boroid": r[6], "total_units": r[7], "owner_name": r[8],
        })

    boro_label = f" in borough {borough.strip()}" if borough.strip() else ""
    lines = [f"OWNERSHIP CLUSTERS (min {min_buildings} buildings){boro_label}"]
    lines.append(f"Found {len(clusters)} clusters\n")

    for i, (cid, c) in enumerate(clusters.items(), 1):
        owners = list({b["owner_name"] for b in c["buildings"] if b["owner_name"]})
        lines.append(f"Cluster #{i}: {c['size']} buildings across boroughs {c['boroughs']}")
        if owners:
            lines.append(f"  Owners: {', '.join(owners[:5])}{' ...' if len(owners) > 5 else ''}")
        for b in c["buildings"][:10]:
            lines.append(f"    {b['bbl']} | {b['address'] or '?'} | {b['zip'] or '?'} | {b['total_units'] or '?'} units")
        if len(c["buildings"]) > 10:
            lines.append(f"    ... and {len(c['buildings']) - 10} more")
        lines.append("")

    lines.append(f"({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"clusters": list(clusters.values())},
        meta={"cluster_count": len(clusters), "query_time_ms": elapsed},
    )


# ===================================================================
# type = "cliques" — ownership_cliques (LCC)
# ===================================================================


def _cliques(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    t0 = time.time()
    limit = max(1, min(top_n, 100))

    cols, rows = execute(pool, f"""
        WITH lcc AS (
            SELECT * FROM local_clustering_coefficient(nyc_building_network, Building, SharedOwner)
        )
        SELECT l.local_clustering_coefficient AS lcc, l.bbl,
               b.housenumber || ' ' || b.streetname AS address, b.zip,
               b.boroid, b.total_units,
               ow.owner_name,
               (SELECT COUNT(*) FROM main.graph_shared_owner
                WHERE bbl1 = l.bbl OR bbl2 = l.bbl) AS neighbor_count
        FROM lcc l
        JOIN main.graph_buildings b ON l.bbl = b.bbl
        LEFT JOIN main.graph_owns o ON b.bbl = o.bbl
        LEFT JOIN main.graph_owners ow ON o.owner_id = ow.owner_id
        WHERE l.local_clustering_coefficient > 0
        ORDER BY l.local_clustering_coefficient DESC, neighbor_count DESC
        LIMIT {limit}
    """)

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content="No ownership cliques found (all buildings have LCC = 0).", meta={"query_time_ms": elapsed})

    lines = [f"OWNERSHIP CLIQUES — Top {len(rows)} by clustering coefficient\n"]
    lines.append(f"{'LCC':<8} {'BBL':<12} {'Address':<30} {'Owner':<30} {'Neighbors'}")
    lines.append("-" * 100)

    for r in rows:
        lcc_val = f"{r[0]:.3f}" if r[0] else "0"
        addr = (r[2] or "?")[:28]
        owner = (r[6] or "?")[:28]
        lines.append(f"{lcc_val:<8} {r[1]:<12} {addr:<30} {owner:<30} {r[7]}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"results": [dict(zip(cols, r)) for r in rows]},
        meta={"result_count": len(rows), "query_time_ms": elapsed},
    )


# ===================================================================
# type = "worst" — worst_landlords
# ===================================================================


def _worst(name: str, depth: int, borough: str, top_n: int, ctx: Context) -> ToolResult:
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    t0 = time.time()

    boro = _boro_code(borough)
    borough_filter = f"AND b.boroid = '{boro}'" if boro else ""
    limit = max(1, min(top_n, 100))

    cols, rows = execute(pool, f"""
        WITH portfolio AS (
            SELECT o.owner_id,
                   COUNT(DISTINCT o.bbl) AS buildings,
                   SUM(b.total_units) AS total_units
            FROM main.graph_owns o
            JOIN main.graph_buildings b ON o.bbl = b.bbl
            WHERE 1=1 {borough_filter}
            GROUP BY o.owner_id
            HAVING COUNT(DISTINCT o.bbl) >= {'1' if borough_filter else '2'}
        ),
        violations AS (
            SELECT o.owner_id,
                   COUNT(*) AS total_violations,
                   COUNT(*) FILTER (WHERE v.status = 'Open') AS open_violations,
                   COUNT(*) FILTER (WHERE v.severity = 'C') AS class_c
            FROM main.graph_owns o
            JOIN main.graph_violations v ON o.bbl = v.bbl
            WHERE o.owner_id IN (SELECT owner_id FROM portfolio)
            GROUP BY o.owner_id
        ),
        evictions AS (
            SELECT o.owner_id, COUNT(*) AS eviction_count
            FROM main.graph_owns o
            JOIN main.graph_eviction_petitioners e ON o.bbl = e.bbl
            WHERE o.owner_id IN (SELECT owner_id FROM portfolio)
            GROUP BY o.owner_id
        ),
        complaints AS (
            SELECT o.owner_id,
                   COUNT(DISTINCT c.complaint_id) AS complaint_count
            FROM main.graph_owns o
            JOIN lake.housing.hpd_complaints c ON o.bbl = c.bbl
            WHERE o.owner_id IN (SELECT owner_id FROM portfolio)
            GROUP BY o.owner_id
        ),
        flags AS (
            SELECT o.owner_id,
                   SUM(f.litigation_count) AS litigations,
                   SUM(f.harassment_findings) AS harassment_findings,
                   SUM(f.is_aep) AS aep_buildings,
                   SUM(f.lien_count) AS tax_liens,
                   SUM(f.dob_violation_count) AS dob_violations
            FROM main.graph_owns o
            JOIN main.graph_building_flags f ON o.bbl = f.bbl
            WHERE o.owner_id IN (SELECT owner_id FROM portfolio)
            GROUP BY o.owner_id
        )
        SELECT p.owner_id, ow.owner_name, p.buildings, p.total_units,
               COALESCE(v.total_violations, 0) AS violations,
               COALESCE(v.open_violations, 0) AS open_violations,
               COALESCE(v.class_c, 0) AS class_c,
               COALESCE(e.eviction_count, 0) AS evictions,
               COALESCE(c.complaint_count, 0) AS complaints,
               COALESCE(fg.litigations, 0) AS litigations,
               COALESCE(fg.harassment_findings, 0) AS harassment,
               COALESCE(fg.aep_buildings, 0) AS aep,
               COALESCE(fg.tax_liens, 0) AS tax_liens
        FROM portfolio p
        LEFT JOIN main.graph_owners ow ON p.owner_id = ow.owner_id
        LEFT JOIN violations v ON p.owner_id = v.owner_id
        LEFT JOIN evictions e ON p.owner_id = e.owner_id
        LEFT JOIN complaints c ON p.owner_id = c.owner_id
        LEFT JOIN flags fg ON p.owner_id = fg.owner_id
        ORDER BY (COALESCE(v.class_c, 0) * 3
               + COALESCE(fg.litigations, 0) * 5
               + COALESCE(fg.harassment_findings, 0) * 10
               + COALESCE(fg.aep_buildings, 0) * 8
               + COALESCE(v.open_violations, 0)
               + COALESCE(fg.tax_liens, 0) * 2) DESC
        LIMIT {limit}
    """)

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content="No results found.", meta={"query_time_ms": elapsed})

    # IQR anomaly flagging
    outlier_owners: set = set()
    violations_list = [r[4] for r in rows if r[4]]
    if len(violations_list) >= 5:
        sorted_v = sorted(violations_list)
        q1 = sorted_v[len(sorted_v) // 4]
        q3 = sorted_v[3 * len(sorted_v) // 4]
        iqr = q3 - q1
        upper = q3 + 1.5 * iqr
        outlier_owners = {r[0] for r in rows if (r[4] or 0) > upper}

    borough_label = f" (borough {borough.strip()})" if borough.strip() else ""
    lines = [f"WORST LANDLORDS — Top {len(rows)} by composite score{borough_label}"]
    lines.append("Ranked by violation severity among all NYC landlords\n")
    lines.append(f"{'Rank':<5} {'Owner':<30} {'Bldgs':<6} {'Units':<6} "
                 f"{'Viol':<7} {'Open':<6} {'ClsC':<6} {'Evict':<6} {'Comp':<6} "
                 f"{'Litig':<6} {'Harass':<7} {'AEP':<5} {'Liens'}")
    lines.append("-" * 130)

    for i, r in enumerate(rows, 1):
        owner_name = (r[1] or f"Reg#{r[0]}")[:28]
        outlier_tag = " [STATISTICAL OUTLIER]" if r[0] in outlier_owners else ""
        lines.append(f"{i:<5} {owner_name:<30} {r[2]:<6} {r[3] or 0:<6} "
                     f"{r[4]:<7} {r[5]:<6} {r[6]:<6} {r[7]:<6} {r[8]:<6} "
                     f"{r[9]:<6} {r[10]:<7} {r[11]:<5} {r[12]}{outlier_tag}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"borough": borough.strip() or "all", "results": [dict(zip(cols, r)) for r in rows]},
        meta={"result_count": len(rows), "query_time_ms": elapsed},
    )


# ===================================================================
# type = "ownership" with BBL shortcut — ownership_graph (BFS)
# ===================================================================


def _ownership_graph_bfs(bbl: str, depth: int, ctx: Context) -> tuple[str, dict]:
    """BFS graph traversal from a single BBL through shared-owner links."""
    require_graph(ctx)
    pool = ctx.lifespan_context["pool"]
    clamped_depth = max(1, min(depth, 6))

    cols, rows = execute(pool, f"""
        FROM GRAPH_TABLE (nyc_building_network
            MATCH p = ANY SHORTEST
                (start:Building WHERE start.bbl = '{bbl}')
                -[e:SharedOwner]-{{1,{clamped_depth}}}
                (target:Building)
            COLUMNS (
                start.bbl AS from_bbl,
                target.bbl AS to_bbl,
                target.housenumber || ' ' || target.streetname AS address,
                target.zip,
                target.total_units,
                path_length(p) AS hops
            )
        )
        ORDER BY hops, to_bbl
    """)

    if not rows:
        return "", {}

    lines = [f"OWNERSHIP GRAPH from BBL {bbl} (max {clamped_depth} hops)"]
    lines.append(f"Connected buildings: {len(rows)}\n")

    for r in rows:
        lines.append(f"  {r[1]} | {r[2] or '?'} | {r[3] or '?'} | {r[4] or '?'} units | {r[5]} hop(s)")

    data = {"from_bbl": bbl, "depth": clamped_depth, "connected": [dict(zip(cols, r)) for r in rows]}
    return "\n".join(lines), data
