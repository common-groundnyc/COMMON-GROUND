"""Report formatting for address_report — 360 Address Dossier."""


def pctile_bar(value, label, raw, width=10):
    """Render: ' HPD violations      2,733   ████████░░ 87th — high'"""
    if value is None:
        return f" {label:<22s} {raw:>8s}   {'░' * width} n/a"
    n = round(value * 100)
    filled = round(value * width)
    bar = "█" * filled + "░" * (width - filled)
    sev = severity(n)
    return f" {label:<22s} {raw:>8s}   {bar} {n}th — {sev}"


def severity(pctile):
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


def fmt(n):
    """Format number with commas, or 'n/a' if None."""
    if n is None:
        return "n/a"
    if isinstance(n, float):
        return f"{n:,.1f}"
    if isinstance(n, int):
        return f"{n:,}"
    return str(n)


# ── helpers ──────────────────────────────────────────────────────


def _get(results, name):
    """Get first row from results as dict, or empty dict."""
    cols, rows = results.get(name, ([], []))
    if cols and rows:
        return dict(zip(cols, rows[0]))
    return {}


def _get_all(results, name):
    """Get all rows from results as list of dicts."""
    cols, rows = results.get(name, ([], []))
    return [dict(zip(cols, r)) for r in rows]


# ── main assembly ────────────────────────────────────────────────


def assemble_report(ctx, results):
    """Build the full 360 report from parallel query results dict.

    ctx:     dict from _resolve_context (bbl, address, zip, borough, etc.)
    results: dict from parallel_queries  {name: (cols, rows)}
    """
    bbl = ctx.get("bbl", "")
    address = ctx.get("address", "")
    borough = ctx.get("borough", "")
    zipcode = ctx.get("zip", "")
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

    lines = []
    W = "━" * 52

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ HEADER ━━━━━━━━━━━━━━━━━━━━━━━━━━
    lines.append(W)
    lines.append(" 360 ADDRESS REPORT — COMMON GROUND")
    lines.append(W)
    lines.append(f" {address}, {borough}, NY {zipcode}")
    lines.append(f" BBL {bbl}  ·  CD {cd}  ·  Council {council}")
    lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ BUILDING ━━━━━━━━━━━━━━━━━━━━━━━━
    lines.append(f"━━ BUILDING {'━' * 40}")
    lines.append(f" Built {year}  ·  {floors} stories  ·  {units} units  ·  {bldg_class}")
    lines.append(f" Zoning {zoning}  ·  Owner: {owner}")
    if assessed:
        lines.append(f" Assessed value: ${fmt(assessed)}")

    sale = _get(results, "building_sale")
    if sale.get("price"):
        lines.append(f" Last sale: ${fmt(sale['price'])} ({sale.get('sale_date', 'n/a')})")

    portfolio = _get(results, "building_owner_portfolio")
    if portfolio.get("portfolio_size", 0) > 1:
        lines.append(f" Owner portfolio: {fmt(portfolio['portfolio_size'])} buildings")

    lines.append("")

    # Violations with percentile bars
    v = _get(results, "building_hpd_violations")
    pv = _get(results, "pctile_violations")
    if v:
        lines.append(pctile_bar(pv.get("pctile"), "HPD violations", fmt(v.get("total", 0))))
        lines.append(f"   Open: {fmt(v.get('open_cnt', 0))}  ·  Class C: {fmt(v.get('class_c', 0))}")

    c = _get(results, "building_hpd_complaints")
    pc = _get(results, "pctile_complaints")
    if c:
        lines.append(pctile_bar(pc.get("pctile"), "HPD complaints", fmt(c.get("total", 0))))
        if c.get("top_category"):
            lines.append(f"   Top category: {c['top_category']}")

    dob = _get(results, "building_dob_violations")
    if dob and dob.get("total", 0) > 0:
        lines.append(f" DOB/ECB violations     {fmt(dob['total']):>8s}   penalties: ${fmt(dob.get('penalties', 0))}")

    fdny = _get(results, "building_fdny")
    if fdny and fdny.get("total", 0) > 0:
        lines.append(f" FDNY violations        {fmt(fdny['total']):>8s}")

    evict = _get(results, "building_evictions")
    if evict and evict.get("total", 0) > 0:
        lines.append(f" Eviction filings       {fmt(evict['total']):>8s}")

    energy = _get(results, "building_energy")
    if energy and energy.get("energy_star_score"):
        lines.append(f" Energy Star score: {fmt(energy['energy_star_score'])}")

    # Warning flags
    flags = []
    if _get(results, "building_aep").get("on_list"):
        flags.append("⚠ AEP (worst buildings list)")
    if _get(results, "building_tax_lien").get("cnt", 0) > 0:
        flags.append("⚠ Tax lien sale history")
    facade = _get(results, "building_facade")
    if facade and str(facade.get("filing_status", "")).upper() == "UNSAFE":
        flags.append("⚠ Facade: UNSAFE")
    boiler = _get(results, "building_boiler")
    if boiler and str(boiler.get("overall_status", "")).upper() != "COMPLIANT" and boiler.get("overall_status"):
        flags.append(f"⚠ Boiler: {boiler['overall_status']}")
    for f in flags:
        lines.append(f" {f}")

    lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ BLOCK ━━━━━━━━━━━━━━━━━━━━━━━━━━
    block = _get(results, "block_buildings")
    trees = _get_all(results, "block_trees")
    films = _get_all(results, "block_film")
    has_block = block or trees or films

    if has_block:
        lines.append(f"━━ BLOCK {'━' * 43}")
        if block:
            lines.append(
                f" {fmt(block.get('cnt', 0))} buildings  ·  "
                f"avg {fmt(block.get('avg_floors'))} floors  ·  "
                f"avg built {fmt(block.get('avg_year'))}"
            )
            lines.append(f" {fmt(block.get('total_units', 0))} total units on block")
        if trees:
            top_tree = trees[0].get("species", "unknown")
            tree_count = sum(t.get("cnt", 0) for t in trees)
            lines.append(f" Street trees: {fmt(tree_count)}  ·  most common: {top_tree}")
        if films:
            lines.append(f" Film shoots nearby: {len(films)} recent")
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━ NEIGHBORHOOD ━━━━━━━━━━━━━━━━━━━━━━
    acs = _get(results, "neighborhood_acs")
    n311 = _get_all(results, "neighborhood_311_top")
    broadband = _get(results, "neighborhood_broadband")
    recycling = _get(results, "neighborhood_recycling")
    has_neighborhood = acs or n311 or broadband or recycling

    if has_neighborhood:
        zip_tag = f" ({zipcode})" if zipcode else ""
        pad = max(0, 37 - len(zip_tag))
        lines.append(f"━━ NEIGHBORHOOD{zip_tag} {'━' * pad}")
        if acs:
            lines.append(f" Population: {fmt(acs.get('total_population'))}")
            lines.append(f" Median income: ${fmt(acs.get('median_household_income'))}")
            lines.append(f" Poverty rate: {fmt(acs.get('poverty_rate'))}%")
            lines.append(f" Median rent: ${fmt(acs.get('median_gross_rent'))}")
            lines.append(f" Rent-burdened: {fmt(acs.get('pct_renter_cost_burdened'))}%")
            lines.append(f" Foreign-born: {fmt(acs.get('pct_foreign_born'))}%")
        if n311:
            top = n311[0]
            lines.append(f" Top 311 complaint: {top.get('complaint_type', '?')} ({fmt(top.get('cnt'))})")
        if broadband and broadband.get("broadband_adoption_rate"):
            lines.append(f" Broadband adoption: {fmt(broadband['broadband_adoption_rate'])}%")
        if recycling and recycling.get("diversion_rate_total"):
            lines.append(f" Recycling rate: {fmt(recycling['diversion_rate_total'])}%")
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ SAFETY ━━━━━━━━━━━━━━━━━━━━━━━━━
    crimes = _get(results, "safety_crimes")
    shootings = _get(results, "safety_shootings")
    crashes = _get(results, "safety_crashes")
    has_safety = crimes or shootings or crashes

    if has_safety and precinct:
        pct_tag = f" (Precinct {precinct})"
        pad = max(0, 41 - len(pct_tag))
        lines.append(f"━━ SAFETY{pct_tag} {'━' * pad}")
        if crimes:
            lines.append(f" Total crimes (YTD): {fmt(crimes.get('total'))}")
            lines.append(
                f"   Felonies: {fmt(crimes.get('felonies'))}  ·  "
                f"Misdemeanors: {fmt(crimes.get('misdemeanors'))}"
            )
        if shootings:
            lines.append(
                f" Shootings (12 mo): {fmt(shootings.get('total'))}  ·  "
                f"fatal: {fmt(shootings.get('fatal'))}"
            )
        if crashes:
            lines.append(
                f" Crashes (12 mo): {fmt(crashes.get('total'))}  ·  "
                f"injured: {fmt(crashes.get('injured'))}  ·  "
                f"killed: {fmt(crashes.get('killed'))}"
            )
        lines.append("")
    elif not precinct:
        lines.append(f"━━ SAFETY {'━' * 42}")
        lines.append(" Precinct not resolved — use safety(precinct) for details")
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ SCHOOLS ━━━━━━━━━━━━━━━━━━━━━━━━
    schools = _get_all(results, "school_nearest")
    ela = _get_all(results, "school_ela")

    if schools or ela:
        lines.append(f"━━ SCHOOLS {'━' * 41}")
        if schools:
            for s in schools[:3]:
                name = s.get("school_name", "?")
                dbn = s.get("dbn", "")
                grades = f"{s.get('grade_span_min', '?')}–{s.get('grade_span_max', '?')}"
                lines.append(f" {name} ({dbn})  grades {grades}")
        if ela:
            top = ela[0]
            lines.append(
                f" Top ELA: {top.get('school_name', '?')} — "
                f"{fmt(top.get('level_3_4_pct'))}% proficient"
            )
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ HEALTH ━━━━━━━━━━━━━━━━━━━━━━━━━
    rats = _get(results, "health_rats")
    covid = _get(results, "health_covid")
    cdc = _get_all(results, "health_cdc")
    has_health = rats or covid or cdc

    if has_health:
        lines.append(f"━━ HEALTH {'━' * 42}")
        if rats and rats.get("inspections", 0) > 0:
            inspections = rats["inspections"]
            active = rats.get("active_rats", 0)
            rat_pct = round(100 * active / inspections) if inspections else 0
            lines.append(f" Rat activity: {rat_pct}% of inspections positive ({fmt(active)} of {fmt(inspections)})")
        if covid:
            lines.append(f" COVID case rate: {fmt(covid.get('covid_case_rate'))}/100K")
            lines.append(f" COVID death rate: {fmt(covid.get('covid_death_rate'))}/100K")
        if cdc:
            for row in cdc[:3]:
                lines.append(f" {row.get('measure', '?')}: {fmt(row.get('data_value'))}%")
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━ ENVIRONMENT ━━━━━━━━━━━━━━━━━━━━━━
    flood = _get(results, "env_flood")
    heat = _get(results, "env_heat")
    air = _get(results, "env_air")
    has_env = flood or heat or air or energy

    if has_env:
        lines.append(f"━━ ENVIRONMENT {'━' * 37}")
        if flood:
            lines.append(f" Flood zone: {flood.get('flood_zone', 'unknown')}")
        if heat and heat.get("heat_vulnerability_index"):
            lines.append(f" Heat vulnerability: {fmt(heat['heat_vulnerability_index'])}/5")
        if air and air.get("pm25"):
            lines.append(f" PM2.5 (fine particulate): {fmt(air['pm25'])} μg/m³")
        if energy and energy.get("energy_star_score"):
            lines.append(f" Energy Star: {fmt(energy['energy_star_score'])}  ·  EUI: {fmt(energy.get('source_eui_kbtu_ft2'))} kBtu/ft²")
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━ CIVIC ━━━━━━━━━━━━━━━━━━━━━━━━━
    contracts = _get(results, "civic_contracts")
    fec = _get(results, "civic_fec")
    has_civic = contracts or fec or council or cd

    if has_civic:
        lines.append(f"━━ CIVIC {'━' * 43}")
        lines.append(f" Council District {council}  ·  Community Board {cd}")
        if contracts and contracts.get("total_5yr"):
            lines.append(f" City contracts to ZIP (5yr): ${fmt(contracts['total_5yr'])}")
        if fec and fec.get("total"):
            lines.append(f" FEC donations from ZIP: ${fmt(fec['total'])}")
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━ SERVICES ━━━━━━━━━━━━━━━━━━━━━━━━
    subway = _get_all(results, "services_subway")
    pantries = _get(results, "services_food_pantries")
    has_services = subway or pantries

    if has_services:
        lines.append(f"━━ SERVICES {'━' * 40}")
        if subway:
            for s in subway[:3]:
                lines.append(f" {s.get('station_name', '?')} — {s.get('line', '?')} line")
        if pantries and pantries.get("cnt", 0) > 0:
            lines.append(f" Food pantries in ZIP: {fmt(pantries['cnt'])}")
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━ FUN FACTS ━━━━━━━━━━━━━━━━━━━━━━━━
    dogs = _get(results, "fun_dogs")
    babies = _get_all(results, "fun_baby_names")
    fishing = _get(results, "fun_fishing")
    corps = _get(results, "building_corps_at_address")
    has_fun = dogs or babies or fishing or trees or films or corps

    if has_fun:
        lines.append(f"━━ FUN FACTS {'━' * 39}")
        if dogs and dogs.get("breed_name"):
            lines.append(f" Most popular dog breed ({zipcode}): {dogs['breed_name']}")
        if trees:
            lines.append(f" Most common street tree: {trees[0].get('species', '?')}")
        if babies:
            names = ", ".join(
                f"{b.get('name', '?')} ({b.get('gender', '?')})"
                for b in babies[:2]
            )
            lines.append(f" Top baby names ({borough}): {names}")
        if films:
            lines.append(f" Film shoots nearby: {len(films)} in last year")
        if fishing:
            lines.append(f" Nearest fishing: {fishing.get('waterbody', '?')}")
        if corps and corps.get("cnt", 0) > 0:
            lines.append(f" Corporations at this address: {fmt(corps['cnt'])}")
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━ FOOTER ━━━━━━━━━━━━━━━━━━━━━━━━━
    lines.append(W)
    lines.append(" Data: NYC Open Data, Census ACS, DOE, DOHMH, DEP, FEC")
    lines.append(" common-ground.nyc — 294 tables, 60M+ rows")
    lines.append(W)
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
