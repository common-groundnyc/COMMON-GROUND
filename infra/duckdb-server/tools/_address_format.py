"""Report formatting for address_report — 360 Address Dossier."""

W = 52  # standard report width


def _divider(label: str) -> str:
    """Build '━━ LABEL ━━━━━━━━━━━━━━━━━━━━━' exactly W chars wide."""
    prefix = f"━━ {label} "
    return prefix + "━" * (W - len(prefix))


def pctile_bar(value: float | None, label: str, raw: str, width: int = 10) -> str:
    """Render: ' HPD violations      2,733   ████████░░ 87th — high'"""
    if value is None:
        return f" {label:<22s} {raw:>8s}   {'░' * width} n/a"
    n = round(value * 100)
    filled = round(value * width)
    bar = "█" * filled + "░" * (width - filled)
    sev = severity(n)
    return f" {label:<22s} {raw:>8s}   {bar} {n}th — {sev}"


def severity(pctile: int) -> str:
    """Return severity word for a percentile 0–100."""
    if pctile <= 20:
        return "low"
    if pctile <= 40:
        return "moderate"
    if pctile <= 60:
        return "typical"
    if pctile <= 80:
        return "high"
    if pctile <= 95:
        return "very high"
    return "extreme"


def fmt(n: object, kind: str = "auto") -> str:
    """Format number with commas, or 'n/a' if None.

    kind: 'auto' | 'money' | 'pct' | 'year'
    """
    if n is None:
        return "n/a"
    if kind == "year":
        return str(int(float(n)))
    if kind == "money":
        f_val = float(n)
        if f_val == int(f_val):
            return f"${int(f_val):,}"
        return f"${f_val:,.2f}"
    if kind == "pct":
        return f"{n}%"
    # auto: detect years (4-digit ints between 1800–2100)
    if isinstance(n, int):
        if 1800 <= n <= 2100:
            return str(n)
        return f"{n:,}"
    if isinstance(n, float):
        if n == int(n):
            i = int(n)
            if 1800 <= i <= 2100:
                return str(i)
            return f"{i:,}"
        return f"{n:,.1f}"
    return str(n)


# ── helpers ──────────────────────────────────────────────────────


def _get(results: dict, name: str) -> dict:
    """Get first row from results as dict, or empty dict."""
    cols, rows = results.get(name, ([], []))
    if cols and rows:
        return dict(zip(cols, rows[0]))
    return {}


def _get_all(results: dict, name: str) -> list[dict]:
    """Get all rows from results as list of dicts."""
    cols, rows = results.get(name, ([], []))
    return [dict(zip(cols, r)) for r in rows]


def _section_or_empty(lines: list[str], label: str, content_lines: list[str]) -> None:
    """Append a section. If content_lines is empty, show 'No data available'."""
    lines.append(_divider(label))
    if content_lines:
        lines.extend(content_lines)
    else:
        lines.append(" No data available")
    lines.append("")


# ── main assembly ────────────────────────────────────────────────


def assemble_report(ctx: dict, results: dict) -> str:
    """Build the full 360 report from parallel query results dict.

    ctx:     dict from _resolve_context (bbl, address, zip, borough, etc.)
    results: dict from parallel_queries  {name: (cols, rows)}
    """
    bbl = ctx.get("bbl", "")
    address = ctx.get("address", "")
    borough = ctx.get("borough", "")
    zipcode = ctx.get("zip", "")
    neighborhood = ctx.get("neighborhood", "")
    precinct = ctx.get("precinct", "")
    cd = ctx.get("community_district", "")
    council = ctx.get("council_district", "")
    zoning = ctx.get("zoning", "")
    bldg_class = ctx.get("bldgclass", "")
    floors = ctx.get("numfloors", "")
    units = ctx.get("unitsres", "")
    year = ctx.get("yearbuilt", "")
    owner = ctx.get("ownername", "")
    assessed = ctx.get("assesstot", "")

    lines: list[str] = []

    # Presentation directive — Claude reads this before deciding how to render
    lines.append("PRESENTATION: This is a complete 10-section report with quantitative data.")
    lines.append("Show ALL sections to the user. Use interactive tables or charts for")
    lines.append("violation counts, percentile rankings, and neighborhood demographics.")
    lines.append("Do not summarize or omit any section. Show the drill-deeper footer.")
    lines.append("")

    rule = "━" * W

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ HEADER ━━━━━━━━━━━━━━━━━━━━━━━━━━
    lines.append(rule)
    lines.append(" 360 ADDRESS REPORT — COMMON GROUND")
    lines.append(rule)
    loc_parts = [address, borough, f"NY {zipcode}"]
    if neighborhood:
        loc_parts.insert(1, neighborhood)
    lines.append(f" {', '.join(loc_parts)}")
    lines.append(f" BBL {bbl}  ·  CD {cd}  ·  Council {council}")
    lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ BUILDING ━━━━━━━━━━━━━━━━━━━━━━━━
    bldg_lines: list[str] = []
    bldg_lines.append(
        f" Built {fmt(year, 'year') if year else 'n/a'}"
        f"  ·  {fmt(floors)} stories  ·  {fmt(units)} units  ·  {bldg_class}"
    )
    bldg_lines.append(f" Zoning {zoning}  ·  Owner: {owner}")
    if assessed:
        bldg_lines.append(f" Assessed value: {fmt(assessed, 'money')}")

    sale = _get(results, "building_sale")
    if sale.get("price"):
        bldg_lines.append(
            f" Last sale: {fmt(sale['price'], 'money')}"
            f" ({sale.get('sale_date', 'n/a')})"
        )

    portfolio = _get(results, "building_owner_portfolio")
    if portfolio.get("portfolio_size", 0) > 1:
        bldg_lines.append(f" Owner portfolio: {fmt(portfolio['portfolio_size'])} buildings")

    bldg_lines.append("")

    # Violations with percentile bars
    v = _get(results, "building_hpd_violations")
    pv = _get(results, "pctile_violations")
    if v:
        bldg_lines.append(pctile_bar(pv.get("pctile"), "HPD violations", fmt(v.get("total", 0))))
        bldg_lines.append(
            f"   Open: {fmt(v.get('open_cnt', 0))}"
            f"  ·  Class C: {fmt(v.get('class_c', 0))}"
        )
        # Worst 5% warning
        pctile_val = pv.get("pctile")
        if pctile_val is not None and round(pctile_val * 100) > 95:
            bldg_lines.append(" ⚠ This building is in the worst 5% citywide")

    c = _get(results, "building_hpd_complaints")
    pc = _get(results, "pctile_complaints")
    if c:
        bldg_lines.append(pctile_bar(pc.get("pctile"), "HPD complaints", fmt(c.get("total", 0))))
        if c.get("top_category"):
            bldg_lines.append(f"   Top category: {c['top_category']}")

    dob = _get(results, "building_dob_violations")
    if dob and dob.get("total", 0) > 0:
        bldg_lines.append(
            f" DOB/ECB violations     {fmt(dob['total']):>8s}"
            f"   penalties: {fmt(dob.get('penalties', 0), 'money')}"
        )

    fdny = _get(results, "building_fdny")
    if fdny and fdny.get("total", 0) > 0:
        bldg_lines.append(f" FDNY violations        {fmt(fdny['total']):>8s}")

    evict = _get(results, "building_evictions")
    if evict and evict.get("total", 0) > 0:
        total_evict = evict["total"]
        line = f" Eviction filings       {fmt(total_evict):>8s}"
        if total_evict > 10:
            line += f"   {fmt(total_evict)} eviction filings on record"
        bldg_lines.append(line)

    energy = _get(results, "building_energy")
    if energy and energy.get("energy_star_score"):
        bldg_lines.append(f" Energy Star score: {fmt(energy['energy_star_score'])}")

    # Warning flags
    if _get(results, "building_aep").get("on_list"):
        bldg_lines.append(" ⚠ Alternative Enforcement Program — one of NYC's worst buildings")
    if _get(results, "building_tax_lien").get("cnt", 0) > 0:
        bldg_lines.append(" ⚠ Tax lien sale history")
    facade = _get(results, "building_facade")
    if facade and str(facade.get("filing_status", "")).upper() == "UNSAFE":
        bldg_lines.append(" ⚠ Facade: UNSAFE")
    boiler = _get(results, "building_boiler")
    if (
        boiler
        and str(boiler.get("overall_status", "")).upper() != "COMPLIANT"
        and boiler.get("overall_status")
    ):
        bldg_lines.append(f" ⚠ Boiler: {boiler['overall_status']}")

    # New danger signals
    dob_comp = _get(results, "building_dob_complaints")
    if dob_comp and dob_comp.get("total", 0) > 0:
        bldg_lines.append(
            f" DOB complaints          {fmt(dob_comp['total']):>8s}"
            f"   active: {fmt(dob_comp.get('active', 0))}"
        )

    dob_viol = _get(results, "building_dob_violations_direct")
    if dob_viol and dob_viol.get("total", 0) > 0:
        bldg_lines.append(
            f" DOB violations          {fmt(dob_viol['total']):>8s}"
            f"   ({fmt(dob_viol.get('types', 0))} violation types)"
        )

    emergency = _get(results, "building_emergency_repair")
    if emergency and emergency.get("total", 0) > 0:
        bldg_lines.append(
            f" ⚠ HPD emergency repairs: {fmt(emergency['total'])}"
            f" (${fmt(emergency.get('total_cost', 0), 'money')} spent)"
        )

    vacate_hpd = _get(results, "building_vacate_hpd")
    if vacate_hpd and vacate_hpd.get("vacate_type"):
        bldg_lines.append(
            f" ⚠ HPD vacate order: {vacate_hpd['vacate_type']}"
            f" — {vacate_hpd.get('primary_vacate_reason', 'unknown')}"
        )

    vacate_fdny = _get(results, "building_fdny_vacate")
    if vacate_fdny and vacate_fdny.get("vac_date"):
        bldg_lines.append(
            f" ⚠ FDNY vacate: {vacate_fdny.get('description', 'N/A')}"
            f" ({vacate_fdny['vac_date']})"
        )

    vacate_reloc = _get(results, "building_vacate_reloc")
    if vacate_reloc and vacate_reloc.get("vacatereason"):
        bldg_lines.append(
            f" ⚠ Vacate/relocation: {vacate_reloc['vacatereason']}"
        )

    oath = _get(results, "building_oath")
    if oath and oath.get("total", 0) > 0:
        bldg_lines.append(
            f" OATH hearings           {fmt(oath['total']):>8s}"
            f"   penalties: {fmt(oath.get('total_penalties', 0), 'money')}"
        )

    landmark = _get(results, "building_landmark")
    if landmark and landmark.get("lm_name"):
        bldg_lines.append(
            f" Landmark: {landmark['lm_name']} ({landmark.get('lm_type', '?')})"
        )
        if landmark.get("hist_distr"):
            bldg_lines.append(f"   Historic district: {landmark['hist_distr']}")

    e_desig = _get(results, "building_e_designation")
    if e_desig:
        codes = []
        if e_desig.get("hazmat_code"): codes.append("hazmat")
        if e_desig.get("air_code"): codes.append("air quality")
        if e_desig.get("noise_code"): codes.append("noise")
        if codes:
            bldg_lines.append(f" ⚠ E-Designation: {', '.join(codes)}")

    cleanup = _get(results, "building_oer_cleanup")
    if cleanup and cleanup.get("project_name"):
        bldg_lines.append(
            f" ⚠ Environmental cleanup: {cleanup['project_name']}"
            f" ({cleanup.get('phase', '?')})"
        )

    # Money & ownership
    tax_ex = _get_all(results, "building_tax_exemptions")
    if tax_ex:
        exemptions = [f"{r.get('exname') or r.get('exmp_code', '?')} ({r.get('year', '?')})"
                      for r in tax_ex[:3]]
        bldg_lines.append(f" Tax exemptions: {', '.join(exemptions)}")

    abatement = _get(results, "building_abatement")
    if abatement and abatement.get("appliedabt"):
        bldg_lines.append(f" Tax abatement: {fmt(abatement['appliedabt'], 'money')} ({abatement.get('taxyr', '?')})")

    charges = _get(results, "building_charges")
    if charges and charges.get("outstanding_balance") and float(charges.get("outstanding_balance") or 0) > 0:
        bldg_lines.append(f" ⚠ Outstanding property charges: {fmt(charges['outstanding_balance'], 'money')}")

    affordable = _get(results, "building_affordable")
    if affordable and affordable.get("all_counted_units"):
        bldg_lines.append(
            f" Affordable housing: {fmt(affordable['all_counted_units'])} units"
            f" (ELI: {fmt(affordable.get('extremely_low_income_units', 0))},"
            f" VLI: {fmt(affordable.get('very_low_income_units', 0))},"
            f" LI: {fmt(affordable.get('low_income_units', 0))})"
        )

    lottery = _get_all(results, "building_lottery")
    if lottery:
        bldg_lines.append(f" Housing Connect lotteries: {len(lottery)}")

    sro = _get(results, "building_sro")
    if sro:
        bldg_lines.append(
            f" SRO building: Class A: {fmt(sro.get('legalclassa', 0))},"
            f" Class B: {fmt(sro.get('legalclassb', 0))}"
        )

    parties = _get_all(results, "building_acris_parties")
    if parties:
        for p in parties[:3]:
            ptype = "Buyer" if str(p.get("party_type")) == "2" else "Seller"
            bldg_lines.append(
                f" {ptype}: {p.get('name', '?')} ({p.get('doc_date', '?')})"
            )

    licenses = _get_all(results, "building_licenses")
    if licenses:
        biz_names = [r.get("business_name", "?") for r in licenses[:3]]
        bldg_lines.append(f" Active businesses: {', '.join(biz_names)}")

    comps = _get_all(results, "building_comps")
    if comps:
        bldg_lines.append(f" Recent comparable sales on block:")
        for c in comps[:3]:
            price = fmt(c.get("sale_price"), "money") if c.get("sale_price") else "?"
            bldg_lines.append(f"   {c.get('address', '?')}: {price} ({c.get('sale_date', '?')})")

    _section_or_empty(lines, "BUILDING", bldg_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ BLOCK ━━━━━━━━━━━━━━━━━━━━━━━━━━
    block = _get(results, "block_buildings")
    trees = _get_all(results, "block_trees")
    films = _get_all(results, "block_film")

    block_lines: list[str] = []
    if block:
        block_lines.append(
            f" {fmt(block.get('cnt', 0))} buildings  ·  "
            f"avg {fmt(block.get('avg_floors'))} floors  ·  "
            f"avg built {fmt(block.get('avg_year'), 'year')}"
        )
        block_lines.append(f" {fmt(block.get('total_units', 0))} total units on block")
    if trees:
        top_tree = trees[0].get("species", "unknown")
        tree_count = sum(t.get("cnt", 0) for t in trees)
        block_lines.append(f" Street trees: {fmt(tree_count)}  ·  most common: {top_tree}")
    if films:
        block_lines.append(f" Film shoots nearby: {len(films)} recent")

    _section_or_empty(lines, "BLOCK", block_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━ NEIGHBORHOOD ━━━━━━━━━━━━━━━━━━━━━━
    acs = _get(results, "neighborhood_acs")
    n311 = _get_all(results, "neighborhood_311_top")
    broadband = _get(results, "neighborhood_broadband")
    recycling = _get(results, "neighborhood_recycling")

    hood_label = "NEIGHBORHOOD"
    if neighborhood:
        hood_label = f"NEIGHBORHOOD — {neighborhood}"
    elif zipcode:
        hood_label = f"NEIGHBORHOOD ({zipcode})"

    hood_lines: list[str] = []
    if acs:
        hood_lines.append(f" Population: {fmt(acs.get('total_population'))}")
        hood_lines.append(f" Median income: {fmt(acs.get('median_household_income'), 'money')}")
        poverty_rate = acs.get("poverty_rate")
        if poverty_rate is not None:
            hood_lines.append(f" Poverty rate: {fmt(poverty_rate, 'pct')}")
        hood_lines.append(f" Median rent: {fmt(acs.get('median_gross_rent'), 'money')}")
        hood_lines.append(f" Median home value: {fmt(acs.get('median_home_value'), 'money')}")
    if n311:
        top = n311[0]
        hood_lines.append(
            f" Top 311 complaint: {top.get('complaint_type', '?')} ({fmt(top.get('cnt'))})"
        )
    if broadband and broadband.get("broadband_adoption_rate"):
        hood_lines.append(f" Broadband adoption: {fmt(broadband['broadband_adoption_rate'], 'pct')}")
    if recycling and recycling.get("diversion_rate_total"):
        hood_lines.append(f" Recycling rate: {fmt(recycling['diversion_rate_total'], 'pct')}")

    restaurants = _get_all(results, "neighborhood_restaurants")
    if restaurants:
        grades = {r.get("grade", "?"): r.get("cnt", 0) for r in restaurants}
        grade_str = ", ".join(f"{g}: {fmt(c)}" for g, c in sorted(grades.items()))
        hood_lines.append(f" Restaurant grades: {grade_str}")

    health_fac = _get_all(results, "neighborhood_health_facilities")
    if health_fac:
        hood_lines.append(f" Health facilities in ZIP: {len(health_fac)}")

    childcare = _get(results, "neighborhood_childcare")
    if childcare and childcare.get("total", 0) > 0:
        hood_lines.append(
            f" Childcare programs: {fmt(childcare['total'])}"
            f" ({fmt(childcare.get('total_capacity', 0))} capacity)"
        )

    child_care = _get(results, "neighborhood_child_care")
    if child_care and child_care.get("providers", 0) > 0:
        hood_lines.append(
            f" Licensed child care: {fmt(child_care['providers'])} providers"
            f" ({fmt(child_care.get('total_slots', 0))} slots)"
        )

    gardens = _get_all(results, "neighborhood_gardens")
    if gardens:
        hood_lines.append(f" Community gardens: {len(gardens)}")

    markets = _get_all(results, "neighborhood_farmers_markets")
    if markets:
        names = [m.get("marketname", "?") for m in markets[:2]]
        hood_lines.append(f" Farmers markets: {', '.join(names)}")
        if markets[0].get("accepts_ebt"):
            hood_lines.append(f"   Accepts EBT: {'Yes' if markets[0]['accepts_ebt'] else 'No'}")

    liquor = _get(results, "neighborhood_liquor")
    if liquor and liquor.get("total", 0) > 0:
        hood_lines.append(
            f" Liquor licenses: {fmt(liquor['total'])}"
            f" ({fmt(liquor.get('bars_restaurants', 0))} bars/restaurants)"
        )

    cafes = _get(results, "neighborhood_cafes")
    if cafes and cafes.get("total", 0) > 0:
        hood_lines.append(
            f" Sidewalk cafes: {fmt(cafes['total'])}"
            f" ({fmt(cafes.get('total_tables', 0))} tables,"
            f" {fmt(cafes.get('total_chairs', 0))} chairs)"
        )

    nycha = _get_all(results, "neighborhood_nycha")
    if nycha:
        hood_lines.append(f" NYCHA developments: {len(nycha)}")
        for n in nycha[:2]:
            hood_lines.append(
                f"   {n.get('PROJECT_NAME', '?')}: {fmt(n.get('TOTAL_UNITS', 0))} units"
                f" (${fmt(n.get('avg_rent', 0))} avg rent)"
            )

    _section_or_empty(lines, hood_label, hood_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ SAFETY ━━━━━━━━━━━━━━━━━━━━━━━━━
    crimes = _get(results, "safety_crimes")
    shootings = _get(results, "safety_shootings")
    crashes = _get(results, "safety_crashes")

    safety_label = "SAFETY"
    if precinct:
        safety_label = f"SAFETY (Precinct {precinct})"

    safety_lines: list[str] = []
    if precinct:
        if crimes:
            safety_lines.append(f" Total crimes (YTD): {fmt(crimes.get('total'))}")
            safety_lines.append(
                f"   Felonies: {fmt(crimes.get('felonies'))}"
                f"  ·  Misdemeanors: {fmt(crimes.get('misdemeanors'))}"
            )
        if shootings:
            safety_lines.append(
                f" Shootings (12 mo): {fmt(shootings.get('total'))}"
                f"  ·  fatal: {fmt(shootings.get('fatal'))}"
            )
        if crashes:
            safety_lines.append(
                f" Crashes (12 mo): {fmt(crashes.get('total'))}"
                f"  ·  injured: {fmt(crashes.get('injured'))}"
                f"  ·  killed: {fmt(crashes.get('killed'))}"
            )
        arrests = _get(results, "safety_arrests")
        if arrests and arrests.get("total", 0) > 0:
            safety_lines.append(
                f" Arrests (YTD): {fmt(arrests['total'])}"
                f"  ·  felony: {fmt(arrests.get('felony_arrests', 0))}"
                f"  ·  misdemeanor: {fmt(arrests.get('misdemeanor_arrests', 0))}"
            )

        hate = _get(results, "safety_hate_crimes")
        if hate and hate.get("total", 0) > 0:
            safety_lines.append(f" Hate crimes (2yr): {fmt(hate['total'])}")

        summons = _get(results, "safety_summons")
        if summons and summons.get("total", 0) > 0:
            safety_lines.append(f" Criminal summonses (12mo): {fmt(summons['total'])}")
    else:
        safety_lines.append(" Precinct not resolved — use safety(precinct) for details")

    _section_or_empty(lines, safety_label, safety_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ SCHOOLS ━━━━━━━━━━━━━━━━━━━━━━━━
    schools = _get_all(results, "school_nearest")
    ela = _get_all(results, "school_ela")

    school_lines: list[str] = []
    if schools:
        for s in schools[:3]:
            name = s.get("school_name", "?")
            dbn = s.get("dbn", "")
            grades = f"{s.get('grade_span_min', '?')}–{s.get('grade_span_max', '?')}"
            school_lines.append(f" {name} ({dbn})  grades {grades}")
    if ela:
        top = ela[0]
        school_lines.append(
            f" Top ELA: {top.get('school_name', '?')} — "
            f"{fmt(top.get('level_3_4_pct'), 'pct')} proficient"
        )

    school_safe = _get_all(results, "school_safety")
    if school_safe:
        for s in school_safe[:2]:
            major = int(s.get("major_incidents") or 0)
            violent = int(s.get("violent_incidents") or 0)
            if major > 0 or violent > 0:
                school_lines.append(
                    f" {s.get('location_name', '?')}: {major} major,"
                    f" {violent} violent incidents"
                )

    _section_or_empty(lines, "SCHOOLS", school_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ HEALTH ━━━━━━━━━━━━━━━━━━━━━━━━━
    rats = _get(results, "health_rats")
    covid = _get(results, "health_covid")
    cdc = _get_all(results, "health_cdc")

    health_lines: list[str] = []
    if rats and rats.get("inspections", 0) > 0:
        inspections = rats["inspections"]
        active = rats.get("active_rats", 0)
        rat_pct = round(100 * active / inspections) if inspections else 0
        health_lines.append(
            f" Rat activity: {rat_pct}% of inspections positive"
            f" ({fmt(active)} of {fmt(inspections)})"
        )
    if covid:
        health_lines.append(f" COVID case rate: {fmt(covid.get('covid_case_rate'))}/100K")
        health_lines.append(f" COVID death rate: {fmt(covid.get('covid_death_rate'))}/100K")
    if cdc:
        for row in cdc[:3]:
            health_lines.append(f" {row.get('measure', '?')}: {fmt(row.get('data_value'), 'pct')}")

    _section_or_empty(lines, "HEALTH", health_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━ ENVIRONMENT ━━━━━━━━━━━━━━━━━━━━━━
    flood = _get(results, "env_flood")
    heat = _get(results, "env_heat")
    air = _get(results, "env_air")

    env_lines: list[str] = []
    if flood and flood.get("fshri"):
        score = flood["fshri"]
        level = {"1": "Minimal", "2": "Low", "3": "Moderate", "4": "High", "5": "Very High"}.get(
            str(score), str(score)
        )
        env_lines.append(f" Flood risk: {level} ({score}/5)")
    if heat and heat.get("hvi"):
        env_lines.append(f" Heat vulnerability: {fmt(heat['hvi'])}/5")
    if air and air.get("pm25"):
        env_lines.append(f" PM2.5 (fine particulate): {fmt(air['pm25'])} ug/m3")
    if energy and energy.get("energy_star_score"):
        env_lines.append(
            f" Energy Star: {fmt(energy['energy_star_score'])}"
            f"  ·  EUI: {fmt(energy.get('source_eui_kbtu_ft2'))} kBtu/ft2"
        )

    lead_pipes = _get(results, "neighborhood_lead_pipes")
    if lead_pipes and lead_pipes.get("total", 0) > 0:
        confirmed = lead_pipes.get("confirmed_lead", 0)
        if confirmed > 0:
            env_lines.append(f" ⚠ Lead service lines: {fmt(confirmed)} confirmed in ZIP")
        else:
            env_lines.append(f" Lead pipes inspected: {fmt(lead_pipes['total'])} (0 confirmed)")

    solar = _get(results, "neighborhood_solar")
    if solar and solar.get("installations", 0) > 0:
        env_lines.append(
            f" Solar installations: {fmt(solar['installations'])}"
            f" ({fmt(solar.get('total_kw', 0))} kW installed)"
        )

    _section_or_empty(lines, "ENVIRONMENT", env_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━ CIVIC ━━━━━━━━━━━━━━━━━━━━━━━━━
    contracts = _get(results, "civic_contracts")
    fec = _get(results, "civic_fec")

    civic_lines: list[str] = []
    civic_lines.append(f" Council District {council}  ·  Community Board {cd}")
    if contracts and contracts.get("total_5yr"):
        civic_lines.append(f" City contracts to ZIP (5yr): {fmt(contracts['total_5yr'], 'money')}")
    if fec and fec.get("total"):
        civic_lines.append(f" FEC donations from ZIP: {fmt(fec['total'], 'money')}")

    _section_or_empty(lines, "CIVIC", civic_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━ SERVICES ━━━━━━━━━━━━━━━━━━━━━━━━
    subway = _get_all(results, "services_subway")
    pantries = _get(results, "services_food_pantries")

    svc_lines: list[str] = []
    if subway:
        for s in subway[:3]:
            svc_lines.append(f" {s.get('station_name', '?')} — {s.get('line', '?')} line")
    if pantries and pantries.get("cnt", 0) > 0:
        svc_lines.append(f" Food pantries in ZIP: {fmt(pantries['cnt'])}")

    parking = _get(results, "transit_parking")
    if parking and parking.get("total", 0) > 0:
        svc_lines.append(f" Parking violations (12mo): {fmt(parking['total'])}")

    _section_or_empty(lines, "SERVICES", svc_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━ FUN FACTS ━━━━━━━━━━━━━━━━━━━━━━━━
    dogs = _get(results, "fun_dogs")
    babies = _get_all(results, "fun_baby_names")
    fishing = _get(results, "fun_fishing")
    corps = _get(results, "building_corps_at_address")

    fun_lines: list[str] = []
    if dogs and dogs.get("cnt", 0) > 0:
        fun_lines.append(f" Dog bag dispensers in ZIP: {fmt(dogs['cnt'])}")
    if trees:
        top_tree = trees[0].get("species", "?")
        tree_count = sum(t.get("cnt", 0) for t in trees)
        fun_lines.append(f" Your street trees: {fmt(tree_count)} {top_tree}")
    if films:
        fun_lines.append(f" Film shoots nearby: {len(films)} this year")
    if babies:
        top_baby = babies[0]
        fun_lines.append(
            f" Top baby name: {top_baby.get('nm', '?')} ({top_baby.get('cnt', '?')} born)"
        )
    if fishing and fishing.get("site"):
        fun_lines.append(f" Nearest fishing: {fishing['site']}")
    if corps and corps.get("cnt", 0) > 0:
        fun_lines.append(f" Corporations at this address: {fmt(corps['cnt'])}")

    _section_or_empty(lines, "FUN FACTS", fun_lines)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ FOOTER ━━━━━━━━━━━━━━━━━━━━━━━━━
    lines.append(rule)
    lines.append(" Data: NYC Open Data, Census ACS, DOE, DOHMH, DEP, FEC")
    lines.append(" common-ground.nyc — 294 tables, 60M+ rows")
    lines.append(rule)
    lines.append("")
    lines.append(" Drill deeper:")
    lines.append(f'   building("{bbl}", view="enforcement")')
    lines.append(f'   building("{bbl}", view="history")')
    if owner:
        lines.append(f'   network("{owner}", type="ownership")')
    lines.append(f'   neighborhood("{zipcode}", view="gentrification")')
    if precinct:
        lines.append(f'   safety("{precinct}")')

    return "\n".join(lines)
