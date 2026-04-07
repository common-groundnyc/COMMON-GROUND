# Address Report 360° Expansion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the address_report tool from ~50 queries to ~90 queries, adding ~40 new data points from unused tables in the lake — making it a truly comprehensive 360° report.

**Architecture:** All new queries go into `_address_queries.py` (the query file). All new formatting goes into `_address_format.py` (the format file). Each new query is a `(name, sql, params)` tuple appended to the existing `queries` list. Each new section/line is added to the existing `assemble_report` function. No new files, no new dependencies.

**Tech Stack:** Python, DuckDB SQL, existing FastMCP ToolResult pattern

**Join key reference:**
- BBL: `LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?` (standard pattern)
- Boro+Block+Lot: `(boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?`
- Parid: `(boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?` (same as BBL)
- ZIP: `zipcode = ?` or `zip_code = ?`
- Precinct: `addr_pct_cd = ?` or `arrest_precinct = ?`
- Borough letter for education: `{"1": "M", "2": "X", "3": "K", "4": "Q", "5": "R"}`

---

## Task 1: Add building-level danger signal queries

**Files:**
- Modify: `infra/duckdb-server/tools/_address_queries.py`

Add these queries after the existing building queries (after line ~175, before the PERCENTILES section):

- [ ] **Step 1: Add DOB complaints query**

```python
    queries.append(("building_dob_complaints", """
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE UPPER(status) = 'ACTIVE') AS active
        FROM lake.housing.dob_complaints
        WHERE zip_code = ? AND house_number || ' ' || house_street ILIKE '%' || ? || '%'
    """, [zipcode, address.split(",")[0].strip() if address else ""]))
```

- [ ] **Step 2: Add DOB violations query**

```python
    queries.append(("building_dob_violations_direct", """
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT violation_type_code) AS types
        FROM lake.housing.dob_violations
        WHERE boro = ? AND block = ? AND lot = ?
    """, [boro_code, bbl[1:6] if len(bbl) >= 6 else "",
          bbl[6:] if len(bbl) >= 6 else ""]))
```

- [ ] **Step 3: Add landmark/historic designation query**

```python
    queries.append(("building_landmark", """
        SELECT lm_name, lm_type, hist_distr, status
        FROM lake.housing.designated_buildings
        WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        LIMIT 1
    """, [bbl]))
```

- [ ] **Step 4: Add emergency repair queries**

```python
    queries.append(("building_emergency_repair", """
        SELECT COUNT(*) AS total,
               SUM(TRY_CAST(hwoapprovedamount AS DOUBLE)) AS total_cost
        FROM lake.housing.emergency_repair_hwo
        WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
    """, [bbl]))
```

- [ ] **Step 5: Add vacate order queries**

```python
    queries.append(("building_vacate_hpd", """
        SELECT vacate_type, primary_vacate_reason, vacate_effective_date
        FROM lake.housing.hpd_repair_vacate
        WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        ORDER BY vacate_effective_date DESC NULLS LAST
        LIMIT 1
    """, [bbl]))

    queries.append(("building_vacate_reloc", """
        SELECT vacateagency, vacatereason, monthlyrelocationtotal
        FROM lake.housing.vacate_relocation
        WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        ORDER BY monthlyrelocationtotal DESC NULLS LAST
        LIMIT 1
    """, [bbl]))

    queries.append(("building_fdny_vacate", """
        SELECT description, vac_date, status_change_date, ocpcy_desc
        FROM lake.housing.fdny_vacate_list
        WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        ORDER BY vac_date DESC NULLS LAST
        LIMIT 1
    """, [bbl]))
```

- [ ] **Step 6: Add OATH hearings query**

```python
    queries.append(("building_oath", """
        SELECT COUNT(*) AS total,
               SUM(TRY_CAST(REPLACE(penalty_imposed, '$', '') AS DOUBLE)) AS total_penalties,
               MAX(violation_date) AS latest
        FROM lake.city_government.oath_hearings
        WHERE violation_location_block_no = ? AND violation_location_lot_no = ?
    """, [bbl[1:6] if len(bbl) >= 6 else "",
          bbl[6:] if len(bbl) >= 6 else ""]))
```

- [ ] **Step 7: Add environmental designation query**

```python
    queries.append(("building_e_designation", """
        SELECT hazmat_code, air_code, noise_code, description
        FROM lake.environment.e_designations
        WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        LIMIT 1
    """, [bbl]))
```

- [ ] **Step 8: Add environmental cleanup query**

```python
    queries.append(("building_oer_cleanup", """
        SELECT project_name, oer_program, class, phase
        FROM lake.environment.oer_cleanup
        WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        LIMIT 1
    """, [bbl]))
```

- [ ] **Step 9: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/_address_queries.py
git commit -m "feat: add building danger signal queries to address_report (DOB, vacate, OATH, env)"
```

---

## Task 2: Add building-level money & ownership queries

**Files:**
- Modify: `infra/duckdb-server/tools/_address_queries.py`

- [ ] **Step 1: Add tax exemptions query**

```python
    queries.append(("building_tax_exemptions", """
        SELECT exmp_code, exname, curexmptot, year
        FROM lake.housing.tax_exemptions
        WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?
        ORDER BY year DESC
        LIMIT 3
    """, [bbl]))
```

- [ ] **Step 2: Add property abatement query**

```python
    queries.append(("building_abatement", """
        SELECT tccode, taxyr, appliedabt, net_taxes
        FROM lake.housing.property_abatement_detail
        WHERE (SUBSTR(parid, 1, 1) || LPAD(SUBSTR(parid, 2, 5), 5, '0')
               || LPAD(SUBSTR(parid, 7, 4), 4, '0')) = ?
        ORDER BY taxyr DESC
        LIMIT 1
    """, [bbl]))
```

- [ ] **Step 3: Add property charges query**

```python
    queries.append(("building_charges", """
        SELECT SUM(TRY_CAST(sum_bal AS DOUBLE)) AS outstanding_balance,
               MAX(taxyear) AS latest_year
        FROM lake.housing.property_charges
        WHERE (SUBSTR(parid, 1, 1) || LPAD(id_block::VARCHAR, 5, '0')
               || LPAD(id_lot::VARCHAR, 4, '0')) = ?
    """, [bbl]))
```

- [ ] **Step 4: Add affordable housing query**

```python
    queries.append(("building_affordable", """
        SELECT project_name, extremely_low_income_units, very_low_income_units,
               low_income_units, moderate_income_units, all_counted_units
        FROM lake.housing.affordable_housing
        WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        LIMIT 1
    """, [bbl]))
```

- [ ] **Step 5: Add Housing Connect lottery query**

```python
    queries.append(("building_lottery", """
        SELECT lottery_name, development_type
        FROM lake.housing.housing_connect_buildings
        WHERE LPAD(TRY_CAST(TRY_CAST(address_bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
        LIMIT 3
    """, [bbl]))
```

- [ ] **Step 6: Add SRO building query**

```python
    queries.append(("building_sro", """
        SELECT managementprogram, dobbuildingclass, legalclassa, legalclassb
        FROM lake.housing.sro_buildings
        WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?
        LIMIT 1
    """, [bbl]))
```

- [ ] **Step 7: Add rolling sales (comparables) query**

```python
    queries.append(("building_comps", """
        SELECT address, sale_price, sale_date, building_class_category,
               land_square_feet, gross_square_feet, year_built
        FROM lake.housing.rolling_sales
        WHERE borough = ? AND block = ? AND lot != ?
          AND TRY_CAST(sale_price AS DOUBLE) > 10000
        ORDER BY sale_date DESC
        LIMIT 3
    """, [boro_code, bbl[1:6].lstrip('0') if len(bbl) >= 6 else "",
          bbl[6:].lstrip('0') if len(bbl) >= 6 else ""]))
```

- [ ] **Step 8: Add ACRIS parties (buyer/seller names) query**

```python
    queries.append(("building_acris_parties", """
        SELECT p.name, p.party_type,
               TRY_CAST(m.document_date AS DATE) AS doc_date,
               m.doc_type
        FROM lake.housing.acris_parties p
        JOIN lake.housing.acris_master m ON p.document_id = m.document_id
        JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
        WHERE (l.borough || LPAD(l.block::VARCHAR, 5, '0')
                          || LPAD(l.lot::VARCHAR, 4, '0')) = ?
          AND m.doc_type IN ('DEED', 'DEEDO', 'MTGE')
        ORDER BY m.document_date DESC
        LIMIT 5
    """, [bbl]))
```

- [ ] **Step 9: Add licensed businesses at address query**

```python
    queries.append(("building_licenses", """
        SELECT business_name, business_category, license_type, license_status
        FROM lake.business.issued_licenses
        WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = ?
          AND UPPER(license_status) = 'ACTIVE'
        LIMIT 5
    """, [bbl]))
```

- [ ] **Step 10: Commit**

```bash
git add infra/duckdb-server/tools/_address_queries.py
git commit -m "feat: add money, ownership, and business queries to address_report"
```

---

## Task 3: Add neighborhood-level livability queries

**Files:**
- Modify: `infra/duckdb-server/tools/_address_queries.py`

- [ ] **Step 1: Add restaurant inspections query**

```python
    queries.append(("neighborhood_restaurants", """
        SELECT grade, COUNT(*) AS cnt
        FROM lake.health.restaurant_inspections
        WHERE zipcode = ?
          AND grade IS NOT NULL AND grade != ''
        GROUP BY grade
        ORDER BY cnt DESC
    """, [zipcode]))
```

- [ ] **Step 2: Add health facilities query**

```python
    queries.append(("neighborhood_health_facilities", """
        SELECT facility_name, description
        FROM lake.health.health_facilities
        WHERE fac_zip = ?
        LIMIT 5
    """, [zipcode]))
```

- [ ] **Step 3: Add childcare programs query**

```python
    queries.append(("neighborhood_childcare", """
        SELECT COUNT(*) AS total,
               SUM(TRY_CAST(capacity AS INT)) AS total_capacity
        FROM lake.health.childcare_programs
        WHERE zipcode = ?
    """, [zipcode]))
```

- [ ] **Step 4: Add lead service lines query**

```python
    queries.append(("neighborhood_lead_pipes", """
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE UPPER(sl_category) = 'LEAD') AS confirmed_lead
        FROM lake.environment.lead_service_lines
        WHERE zip_code = ?
    """, [zipcode]))
```

- [ ] **Step 5: Add solar installations query**

```python
    queries.append(("neighborhood_solar", """
        SELECT COUNT(*) AS installations,
               SUM(TRY_CAST(totalnameplatekwdc AS DOUBLE)) AS total_kw,
               SUM(TRY_CAST(expected_kwh_annual_production AS DOUBLE)) AS annual_kwh
        FROM lake.environment.nys_solar
        WHERE zip_code = ?
          AND UPPER(project_status) = 'COMPLETED'
    """, [zipcode]))
```

- [ ] **Step 6: Add community gardens query**

```python
    queries.append(("neighborhood_gardens", """
        SELECT gardenname, address
        FROM lake.social_services.community_gardens
        WHERE zipcode = ?
        LIMIT 5
    """, [zipcode]))
```

- [ ] **Step 7: Add farmers markets query**

```python
    queries.append(("neighborhood_farmers_markets", """
        SELECT marketname, daysoperation, hoursoperations, accepts_ebt
        FROM lake.social_services.farmers_markets
        WHERE community_district = ?
        LIMIT 3
    """, [cd]))
```

- [ ] **Step 8: Add child care providers query**

```python
    queries.append(("neighborhood_child_care", """
        SELECT COUNT(*) AS providers,
               SUM(TRY_CAST(total_capacity AS INT)) AS total_slots
        FROM lake.social_services.nys_child_care
        WHERE zip_code = ?
    """, [zipcode]))
```

- [ ] **Step 9: Add liquor licenses query**

```python
    queries.append(("neighborhood_liquor", """
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE UPPER(class) ILIKE '%ON%PREMISES%'
                                   OR UPPER(type) ILIKE '%ON%PREMISES%') AS bars_restaurants
        FROM lake.business.nys_liquor_authority
        WHERE zipcode = ?
    """, [zipcode]))
```

- [ ] **Step 10: Add sidewalk cafes query**

```python
    queries.append(("neighborhood_cafes", """
        SELECT COUNT(*) AS total,
               SUM(TRY_CAST(swc_tables AS INT)) AS total_tables,
               SUM(TRY_CAST(swc_chairs AS INT)) AS total_chairs
        FROM lake.business.sidewalk_cafe
        WHERE zip = ?
    """, [zipcode]))
```

- [ ] **Step 11: Commit**

```bash
git add infra/duckdb-server/tools/_address_queries.py
git commit -m "feat: add neighborhood livability queries (restaurants, health, gardens, solar, childcare)"
```

---

## Task 4: Add safety, transit, and education expansion queries

**Files:**
- Modify: `infra/duckdb-server/tools/_address_queries.py`

- [ ] **Step 1: Add arrests query (inside the `if precinct:` block)**

After the existing `safety_crashes` query, add:

```python
        queries.append(("safety_arrests", """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE law_cat_cd = 'F') AS felony_arrests,
                   COUNT(*) FILTER (WHERE law_cat_cd = 'M') AS misdemeanor_arrests
            FROM lake.public_safety.nypd_arrests_ytd
            WHERE arrest_precinct = ?
        """, [pct_val]))

        queries.append(("safety_hate_crimes", """
            SELECT COUNT(*) AS total
            FROM lake.public_safety.hate_crimes
            WHERE complaint_precinct_code = ?
              AND TRY_CAST(record_create_date AS DATE) >= CURRENT_DATE - INTERVAL '2 years'
        """, [pct_val]))

        queries.append(("safety_summons", """
            SELECT COUNT(*) AS total
            FROM lake.public_safety.criminal_court_summons
            WHERE precinct_of_occur = ?
              AND TRY_CAST(summons_date AS DATE) >= CURRENT_DATE - INTERVAL '1 year'
        """, [pct_val]))
```

- [ ] **Step 2: Add parking violations and pothole queries (outside precinct block)**

```python
    queries.append(("transit_parking", """
        SELECT COUNT(*) AS total
        FROM lake.transportation.parking_violations
        WHERE violation_precinct = ?
          AND TRY_CAST(issue_date AS DATE) >= CURRENT_DATE - INTERVAL '1 year'
    """, [int(precinct) if precinct and precinct.isdigit() else 0]))
```

- [ ] **Step 3: Add school safety query**

```python
    queries.append(("school_safety", """
        SELECT location_name, register,
               TRY_CAST(major_n AS INT) AS major_incidents,
               TRY_CAST(vio_n AS INT) AS violent_incidents,
               TRY_CAST(prop_n AS INT) AS property_incidents
        FROM lake.education.school_safety
        WHERE SUBSTR(dbn, 3, 1) = ?
          AND school_year = (SELECT MAX(school_year) FROM lake.education.school_safety)
        ORDER BY TRY_CAST(major_n AS INT) DESC NULLS LAST
        LIMIT 3
    """, [boro_letter]))
```

- [ ] **Step 4: Add HUD public housing query**

```python
    queries.append(("neighborhood_nycha", """
        SELECT PROJECT_NAME, TOTAL_UNITS, TOTAL_OCCUPIED,
               TRY_CAST(RENT_PER_MONTH AS DOUBLE) AS avg_rent,
               TRY_CAST(PCT_OCCUPIED AS DOUBLE) AS occupancy_pct
        FROM lake.federal.hud_public_housing_developments
        WHERE STD_ZIP5 = ?
        LIMIT 3
    """, [zipcode]))
```

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/tools/_address_queries.py
git commit -m "feat: add safety, transit, education, and housing authority queries"
```

---

## Task 5: Format all new building-level data in the report

**Files:**
- Modify: `infra/duckdb-server/tools/_address_format.py`

Add formatting for all new building queries. These go inside the existing BUILDING section of `assemble_report`, after the existing warning flags block (after line ~217).

- [ ] **Step 1: Add danger signals formatting**

After the existing boiler warning (around line 217), add:

```python
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
```

- [ ] **Step 2: Add money & ownership formatting**

```python
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
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/tools/_address_format.py
git commit -m "feat: format building danger signals and money/ownership data in report"
```

---

## Task 6: Format all new neighborhood-level data in the report

**Files:**
- Modify: `infra/duckdb-server/tools/_address_format.py`

- [ ] **Step 1: Add restaurant grades to neighborhood section**

After the existing broadband/recycling lines in the NEIGHBORHOOD section, add:

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/tools/_address_format.py
git commit -m "feat: format neighborhood livability data (restaurants, gardens, NYCHA, childcare)"
```

---

## Task 7: Format safety expansion and add new environment/transit sections

**Files:**
- Modify: `infra/duckdb-server/tools/_address_format.py`

- [ ] **Step 1: Add arrests, hate crimes, summons to safety section**

After the existing crashes line in the SAFETY section, add:

```python
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
```

- [ ] **Step 2: Add lead pipes and solar to environment section**

After existing environment lines, add:

```python
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
```

- [ ] **Step 3: Add parking to services section**

After existing services lines, add:

```python
    parking = _get(results, "transit_parking")
    if parking and parking.get("total", 0) > 0:
        svc_lines.append(f" Parking violations (12mo): {fmt(parking['total'])}")
```

- [ ] **Step 4: Add school safety to schools section**

After existing school lines, add:

```python
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
```

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/tools/_address_format.py
git commit -m "feat: format safety expansion, lead pipes, solar, parking, school safety"
```

---

## Task 8: Update structured_content with new data

**Files:**
- Modify: `infra/duckdb-server/tools/address_report.py`

Update the structured_content dict to include all new data categories.

- [ ] **Step 1: Add new fields to structured dict**

In the structured_content construction (the part we modified in the earlier plan), add these new keys after the existing `"safety"` block:

```python
        "danger_signals": {
            "emergency_repairs": _get(results, "building_emergency_repair").get("total", 0) if _get(results, "building_emergency_repair") else 0,
            "vacate_orders": bool(_get(results, "building_vacate_hpd").get("vacate_type")) if _get(results, "building_vacate_hpd") else False,
            "fdny_vacate": bool(_get(results, "building_fdny_vacate").get("vac_date")) if _get(results, "building_fdny_vacate") else False,
            "oath_hearings": _get(results, "building_oath").get("total", 0) if _get(results, "building_oath") else 0,
            "e_designation": bool(_get(results, "building_e_designation")) if _get(results, "building_e_designation") else False,
            "lead_pipes_in_zip": _get(results, "neighborhood_lead_pipes").get("confirmed_lead", 0) if _get(results, "neighborhood_lead_pipes") else 0,
        },
        "livability": {
            "community_gardens": len(_get_all(results, "neighborhood_gardens")),
            "farmers_markets": len(_get_all(results, "neighborhood_farmers_markets")),
            "sidewalk_cafes": _get(results, "neighborhood_cafes").get("total", 0) if _get(results, "neighborhood_cafes") else 0,
            "liquor_licenses": _get(results, "neighborhood_liquor").get("total", 0) if _get(results, "neighborhood_liquor") else 0,
            "solar_installations": _get(results, "neighborhood_solar").get("installations", 0) if _get(results, "neighborhood_solar") else 0,
            "restaurant_grades": {r.get("grade", "?"): r.get("cnt", 0) for r in _get_all(results, "neighborhood_restaurants")},
        },
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/tools/address_report.py
git commit -m "feat: enrich structured_content with danger signals and livability data"
```

---

## Task 9: Deploy and test

**Files:** No code changes — deployment and manual testing

- [ ] **Step 1: Deploy**

```bash
cd ~/Desktop/dagster-pipeline
bash infra/deploy.sh
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && sudo docker compose build duckdb-server && sudo docker compose up -d duckdb-server"
```

- [ ] **Step 2: Wait for health check**

```bash
sleep 60 && curl -s https://mcp.common-ground.nyc/health
```

- [ ] **Step 3: Test in Claude Desktop**

Send: "Tell me about 305 Linden Blvd, Brooklyn"

Expected new data visible:
- Emergency repairs, vacate orders, OATH hearings
- Tax exemptions, abatements, property charges
- ACRIS buyer/seller names
- Active businesses
- Comparable sales
- Restaurant grades, community gardens, NYCHA
- Childcare, liquor licenses, sidewalk cafes
- Arrests, hate crimes, school safety
- Lead pipes, solar installations, parking violations

- [ ] **Step 4: Test a second address to verify edge cases**

Send: "Tell me about 350 5th Ave, Manhattan" (Empire State Building — landmark, commercial)

Verify: landmark status shows, different data profile works correctly.

- [ ] **Step 5: Test a residential building**

Send: "Tell me about 123 Main St, Bronx"

Verify: handles addresses with sparse data gracefully (no errors for missing data).
