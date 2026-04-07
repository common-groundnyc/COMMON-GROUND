# Killer MCP Tools — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build 6 cross-schema MCP tools that unlock ~250M rows of untapped data across federal, financial, environment, and city_government schemas — combining datasets nobody else combines.

**Architecture:** Each tool is a standalone addition to `mcp_server.py` following the existing pattern: SQL constants at module level → tool function with `@mcp.tool(annotations=READONLY)` → `_safe_query()` for each data source → formatted text output via `ToolResult`. All tools are read-only, name-based or location-based, with graceful degradation when tables are missing.

**Tech Stack:** Python, FastMCP, DuckDB SQL, existing `mcp_server.py` patterns (`_safe_query`, `ToolResult`, `ToolError`, `READONLY`, `NAME`/`ZIP`/`BBL` type aliases)

---

## File Structure

All changes in a single file:

- **Modify:** `infra/duckdb-server/mcp_server.py` — add SQL constants + tool functions for 6 new tools
- Insert location: After the district_report block, before the Commercial Vitality block (search for `COMMERCIAL_LICENSES_SQL` to find the boundary)

## Data Inventory

### Tool 1: `due_diligence(name)` — Professional & Financial Background Check

| Schema | Table | Rows | What We Get |
|--------|-------|------|-------------|
| financial | nys_attorney_registrations | 1.7M | first_name, last_name, status, year_admitted, law_school, company_name |
| financial | nys_re_brokers | 591k | license_holder_name, license_type, license_expiration_date, business_name |
| financial | nys_tax_warrants | 275k | debtor_name_1, debtor_name_2, warrant_filed_amount, status_code, city |
| financial | nys_child_support_warrants | 49k | debtor_name, warrant_filed_amount, status_code_a_c_u |
| business | nys_contractor_registry | 12.7k | business_name, business_officers, business_has_been_debarred, status |
| business | nys_non_responsible | 18 | nonresponsiblecontractor, agencyauthorityname |
| city_government | coib_enforcement | 1.4k | case_name, agency, fine_paid_to_coib, other_penalty |

### Tool 2: `cop_sheet(name)` — Officer Accountability Dossier

| Schema | Table | Rows | What We Get |
|--------|-------|------|-------------|
| federal | nypd_ccrb_complaints | 5.1M | first_name, last_name, shield_no, tax_id, allegation, fado_type, ccrb_disposition, penalty_desc, incident_date, impacted_race |
| federal | nypd_ccrb_officers_current | 345k | first_name, last_name, allegations, substantiated, force_allegations, incidents |
| federal | police_settlements_538 | 163k | matter_name, plaintiff_name, amount_awarded, total_incurred, summary_allegations, court, docket_number |
| federal | cl_nypd_cases_sdny | 18k | case_name, case_name_full, docket_number, date_filed, date_terminated, suit_nature, assigned_to |
| federal | cl_nypd_cases_edny | 18k | (same structure) |

### Tool 3: `climate_risk(bbl_or_zip)` — Environmental Due Diligence

| Schema | Table | Rows | What We Get | Join Key |
|--------|-------|------|-------------|----------|
| environment | flood_vulnerability | 2.2k | fshri (flood score), ss_cur/50s/80s (storm surge) | geoid (NTA) |
| environment | heat_vulnerability | 184 | hvi (heat vulnerability index) | zcta20 (ZIP) |
| environment | lead_service_lines | 4.6M | street_address, sl_category, public_sl_material, customer_sl_material | zip_code |
| environment | ll84_energy_2023 | 103k | energy_star_score, site_eui_kbtu_ft, source_eui_kbtu_ft, direct_ghg_emissions | nyc_borough_block_and_lot (BBL) |
| environment | air_quality | 37.7k | (by neighborhood) | geo_entity |
| environment | street_trees | 684k | (by ZIP) | zipcode |
| environment | oer_cleanup | 22k | cleanup sites | (address-based) |
| environment | e_designations | 13.8k | environmental designations | (BBL-based) |
| environment | asthma_ed | 420 | asthma ER visit rates | (by neighborhood) |
| federal | epa_echo_facilities | 2.2M (5 borough tables) | EPA compliance, violations | (by lat/lon or zip) |

### Tool 4: `vital_records(name)` — Historical Vital Records Search

| Schema | Table | Rows | What We Get |
|--------|-------|------|-------------|
| city_government | death_certificates_1862_1948 | 3.7M | first_name, last_name, age, year, month, day, county |
| city_government | marriage_certificates_1866_1937 | 5.3M | (bride/groom names, year) |
| city_government | birth_certificates_1855_1909 | 2.6M | (name, year) |
| city_government | marriage_licenses_1950_2017 | 4.8M | (bride/groom names, year) |
| city_government | marriage_licenses_1908_1949 | 1.1M | (bride/groom names, year) |

**Note:** `federal.nys_death_index` has 0 rows (empty table — not yet ingested). Use city_government death certificates instead.

### Tool 5: `money_trail(name)` — Full Political Money Tracker

| Schema | Table | Rows | What We Get |
|--------|-------|------|-------------|
| federal | nys_campaign_finance | 69M | filer_name, flng_ent_first_name, flng_ent_last_name, org_amt, election_year, office_desc |
| federal | fec_contributions | 44M | (federal political donations) |
| city_government | campaign_contributions | 1.7M | (NYC CFB donations) |
| city_government | campaign_expenditures | 516k | (NYC CFB spending) |
| city_government | nys_campaign_expenditures | 4.2M | (NYS spending) |
| city_government | contract_awards | 52k | (city contracts) |
| financial | nys_procurement_state | 276k | (state procurement) |

### Tool 6: `judge_profile(name)` — Federal Judge Dossier

| Schema | Table | Rows | What We Get |
|--------|-------|------|-------------|
| federal | cl_judges | 163k | name_first, name_last, gender, date_dob, date_dod, religion |
| federal | cl_judges__positions | 5.6k | (court, date_start, date_end, appointer) |
| federal | cl_judges__educations | 885 | (school, degree, year) |
| federal | cl_financial_disclosures | 36k | (year, report_type) |
| federal | cl_financial_disclosures__investments | 2M | (investment holdings) |
| federal | cl_financial_disclosures__debts | 20.7k | (debts) |
| federal | cl_financial_disclosures__gifts | 2.2k | (gifts received) |

---

## Column Name Reference

**Critical — verified against live schema March 26, 2026:**

- `nypd_ccrb_complaints`: officer name = `first_name`, `last_name`; ID = `tax_id`, `shield_no`
- `nypd_ccrb_officers_current`: `first_name`, `last_name`, `allegations` (count), `substantiated` (count)
- `police_settlements_538`: case = `matter_name`; money = `amount_awarded`, `total_incurred`
- `cl_nypd_cases_sdny/edny`: case = `case_name`, `case_name_full`, `docket_number`
- `nys_attorney_registrations`: `first_name`, `last_name`, `status`, `year_admitted`, `law_school`
- `nys_re_brokers`: `license_holder_name`, `license_type`, `license_expiration_date`
- `nys_tax_warrants`: `debtor_name_1`, `debtor_name_2`, `warrant_filed_amount`, `status_code`
- `nys_child_support_warrants`: `debtor_name`, `warrant_filed_amount`, `status_code_a_c_u`
- `nys_contractor_registry`: `business_name`, `business_officers`, `business_has_been_debarred`
- `coib_enforcement`: `case_name`, `agency`, `fine_paid_to_coib`
- `heat_vulnerability`: `hvi` (score), `zcta20` (ZIP)
- `flood_vulnerability`: `fshri` (score), `geoid` (NTA code — NOT ZIP)
- `lead_service_lines`: `street_address`, `zip_code`, `sl_category`, `public_sl_material`
- `ll84_energy_2023`: `nyc_borough_block_and_lot` (BBL), `energy_star_score`, `site_eui_kbtu_ft`
- `death_certificates_1862_1948`: `first_name`, `last_name`, `age`, `year`, `month`, `day`
- `nys_campaign_finance`: `filer_name`, `flng_ent_first_name`, `flng_ent_last_name`, `org_amt`
- `nypd_ccrb_officers_current`: `first_name`, `last_name`, `allegations`, `substantiated`, `force_allegations`, `subst_force_allegations`, `incidents`, `empl_status`, `gender`, `race_ethnicity` (ALL verified)
- `campaign_contributions`: `recipname` (NOT candname), `name`, `amnt`, `occupation`, `empname`
- `contract_awards`: `vendor_name` (NOT vendor), `agency_name` (NOT agency), `short_title`, `contract_amount`
- `fec_contributions`: `cmte_id`, `name`, `transaction_amt`, `transaction_dt`, `election_cycle`
- `cl_judges`: `name_first`, `name_last`, `id`, `date_dob`, `gender`

---

## Task 1: `due_diligence(name)` — Professional & Financial Background Check

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (insert after district_report, before Commercial Vitality)

- [ ] **Step 1: Add SQL constants**

```python
# ---------------------------------------------------------------------------
# Due Diligence — Professional & Financial Background Check
# ---------------------------------------------------------------------------

DUE_DILIGENCE_ATTORNEY_SQL = """
SELECT first_name, last_name, middle_name, status, year_admitted,
       law_school, company_name, city, state, registration_number,
       judicial_department_of_admission
FROM lake.financial.nys_attorney_registrations
WHERE (last_name ILIKE ? AND (first_name ILIKE ? OR first_name ILIKE ?))
ORDER BY year_admitted DESC NULLS LAST
LIMIT 10
"""

DUE_DILIGENCE_BROKER_SQL = """
SELECT license_holder_name, license_type, license_number,
       license_expiration_date, business_name, business_city, business_state
FROM lake.financial.nys_re_brokers
WHERE license_holder_name ILIKE '%' || ? || '%'
ORDER BY license_expiration_date DESC NULLS LAST
LIMIT 10
"""

DUE_DILIGENCE_TAX_WARRANT_SQL = """
SELECT debtor_name_1, debtor_name_2, city, state,
       warrant_filed_amount, warrant_filed_date, status_code,
       warrant_satisfaction_date, warrant_expiration_date
FROM lake.financial.nys_tax_warrants
WHERE debtor_name_1 ILIKE '%' || ? || '%'
   OR debtor_name_2 ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(warrant_filed_date AS DATE) DESC NULLS LAST
LIMIT 10
"""

DUE_DILIGENCE_CHILD_SUPPORT_SQL = """
SELECT debtor_name, city, state,
       warrant_filed_amount, warrant_filed_date, status_code_a_c_u,
       warrant_satisfaction_date, warrant_expiration_date
FROM lake.financial.nys_child_support_warrants
WHERE debtor_name ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(warrant_filed_date AS DATE) DESC NULLS LAST
LIMIT 10
"""

DUE_DILIGENCE_CONTRACTOR_SQL = """
SELECT business_name, business_officers, status, certificate_number,
       business_has_been_debarred, debarment_start_date, debarment_end_date,
       business_has_outstanding_wage_assessments,
       business_has_workers_compensation_insurance
FROM lake.business.nys_contractor_registry
WHERE business_name ILIKE '%' || ? || '%'
   OR business_officers ILIKE '%' || ? || '%'
LIMIT 10
"""

DUE_DILIGENCE_DEBARRED_SQL = """
SELECT nonresponsiblecontractor, agencyauthorityname,
       datenonresponsibilitydetermination
FROM lake.business.nys_non_responsible
WHERE nonresponsiblecontractor ILIKE '%' || ? || '%'
LIMIT 10
"""

DUE_DILIGENCE_ETHICS_SQL = """
SELECT case_name, case_number, agency, date,
       fine_paid_to_coib, other_penalty, suspension_of_days
FROM lake.city_government.coib_enforcement
WHERE case_name ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(date AS DATE) DESC NULLS LAST
LIMIT 10
"""
```

- [ ] **Step 2: Add the due_diligence tool function**

```python
@mcp.tool(annotations=READONLY, tags={"investigation"})
def due_diligence(
    name: NAME,
    ctx: Context,
) -> ToolResult:
    """Professional and financial background check — licensed attorney? Active real estate broker? Tax warrants? Child support warrants? Debarred contractor? Ethics violations? Enter a person or business name to check across 7 NYS/NYC databases. Use this for due diligence on anyone you're doing business with. For political connections, use money_trail(name). For cross-domain entity search, use entity_xray(name)."""
    name = name.strip()
    if len(name) < 3:
        raise ToolError("Name must be at least 3 characters.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Parse first/last for attorney search
    parts = name.split()
    if len(parts) >= 2:
        first_guess = parts[0]
        last_guess = parts[-1]
    else:
        first_guess = name
        last_guess = name

    # Attorney (exact last + fuzzy first)
    _, atty_rows = _safe_query(db, DUE_DILIGENCE_ATTORNEY_SQL,
                                [f"{last_guess}%", f"{first_guess}%", f"{'%' + first_guess + '%'}"])
    # Broker
    _, broker_rows = _safe_query(db, DUE_DILIGENCE_BROKER_SQL, [name])
    # Tax warrants
    _, tax_rows = _safe_query(db, DUE_DILIGENCE_TAX_WARRANT_SQL, [name, name])
    # Child support warrants
    _, cs_rows = _safe_query(db, DUE_DILIGENCE_CHILD_SUPPORT_SQL, [name])
    # Contractor
    _, contractor_rows = _safe_query(db, DUE_DILIGENCE_CONTRACTOR_SQL, [name, name])
    # Debarred
    _, debarred_rows = _safe_query(db, DUE_DILIGENCE_DEBARRED_SQL, [name])
    # Ethics
    _, ethics_rows = _safe_query(db, DUE_DILIGENCE_ETHICS_SQL, [name])

    sections_found = sum(1 for r in [atty_rows, broker_rows, tax_rows, cs_rows,
                                      contractor_rows, debarred_rows, ethics_rows] if r)

    if sections_found == 0:
        raise ToolError(f"No records found for '{name}' in any professional/financial database. Try a different name spelling.")

    lines = [f"DUE DILIGENCE — {name}", "=" * 55]

    # Attorneys
    if atty_rows:
        lines.append(f"\nATTORNEY REGISTRATIONS ({len(atty_rows)} matches):")
        for r in atty_rows:
            fname, lname, mname, status, year_adm, school, company, city, state, reg_num, dept = r
            full = f"{fname or ''} {mname or ''} {lname or ''}".strip()
            lines.append(f"  {full} — {status or '?'}")
            lines.append(f"    Reg #{reg_num} | Admitted {year_adm or '?'} | {dept or ''}")
            if school:
                lines.append(f"    Law school: {school}")
            if company:
                lines.append(f"    Firm: {company}, {city or ''} {state or ''}")

    # Brokers
    if broker_rows:
        lines.append(f"\nREAL ESTATE LICENSES ({len(broker_rows)} matches):")
        for r in broker_rows:
            holder, lic_type, lic_num, exp, biz, bcity, bstate = r
            lines.append(f"  {holder} — {lic_type or '?'} (#{lic_num})")
            lines.append(f"    Expires: {exp or '?'} | {biz or ''}, {bcity or ''} {bstate or ''}")

    # Tax warrants
    if tax_rows:
        lines.append(f"\n⚠️  TAX WARRANTS ({len(tax_rows)} matches):")
        for r in tax_rows:
            dn1, dn2, city, state, amt, filed, status, satisfied, expires = r
            amt_str = f"${float(amt):,.2f}" if amt else "?"
            lines.append(f"  {dn1} — {amt_str} ({status or '?'})")
            lines.append(f"    Filed: {filed or '?'} | {city or ''}, {state or ''}")
            if satisfied:
                lines.append(f"    Satisfied: {satisfied}")

    # Child support warrants
    if cs_rows:
        lines.append(f"\n⚠️  CHILD SUPPORT WARRANTS ({len(cs_rows)} matches):")
        for r in cs_rows:
            dn, city, state, amt, filed, status, satisfied, expires = r
            amt_str = f"${float(amt):,.2f}" if amt else "?"
            lines.append(f"  {dn} — {amt_str} (Status: {status or '?'})")
            lines.append(f"    Filed: {filed or '?'} | {city or ''}, {state or ''}")

    # Contractors
    if contractor_rows:
        lines.append(f"\nCONTRACTOR REGISTRY ({len(contractor_rows)} matches):")
        for r in contractor_rows:
            biz, officers, status, cert, debarred, debar_start, debar_end, wages, wc = r
            lines.append(f"  {biz} — {status or '?'} (Cert #{cert})")
            if officers:
                lines.append(f"    Officers: {officers[:100]}")
            flags = []
            if debarred and debarred.lower() in ('true', 'yes', '1'):
                flags.append(f"DEBARRED {debar_start or ''}–{debar_end or ''}")
            if wages and wages.lower() in ('true', 'yes', '1'):
                flags.append("OUTSTANDING WAGE ASSESSMENTS")
            if wc and wc.lower() in ('false', 'no', '0'):
                flags.append("NO WORKERS COMP INSURANCE")
            if flags:
                lines.append(f"    ⚠️  {', '.join(flags)}")

    # Debarred
    if debarred_rows:
        lines.append(f"\n⚠️  DEBARRED / NON-RESPONSIBLE ({len(debarred_rows)} matches):")
        for r in debarred_rows:
            contractor, agency, date = r
            lines.append(f"  {contractor} — by {agency or '?'} ({date or '?'})")

    # Ethics
    if ethics_rows:
        lines.append(f"\n⚠️  ETHICS VIOLATIONS ({len(ethics_rows)} matches):")
        for r in ethics_rows:
            case, num, agency, date, fine, penalty, suspension = r
            lines.append(f"  {case} — {agency or '?'} ({date or '?'})")
            parts = []
            if fine: parts.append(f"Fine: ${fine}")
            if penalty: parts.append(f"Penalty: {penalty}")
            if suspension: parts.append(f"Suspension: {suspension} days")
            if parts:
                lines.append(f"    {', '.join(parts)}")

    lines.append(f"\n{sections_found} of 7 databases returned results.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "name": name,
            "attorney_matches": len(atty_rows),
            "broker_matches": len(broker_rows),
            "tax_warrants": len(tax_rows),
            "child_support_warrants": len(cs_rows),
            "contractor_matches": len(contractor_rows),
            "debarred": len(debarred_rows),
            "ethics_violations": len(ethics_rows),
        },
        meta={"name": name, "sections_found": sections_found, "query_time_ms": elapsed},
    )
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add due_diligence tool — professional & financial background check"
```

---

## Task 2: `cop_sheet(name)` — Officer Accountability Dossier

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (insert after due_diligence)

- [ ] **Step 1: Add SQL constants**

```python
# ---------------------------------------------------------------------------
# Cop Sheet — Officer Accountability Dossier
# ---------------------------------------------------------------------------

COP_SHEET_SUMMARY_SQL = """
SELECT first_name, last_name, allegations, substantiated,
       force_allegations, subst_force_allegations, incidents,
       empl_status, gender, race_ethnicity
FROM lake.federal.nypd_ccrb_officers_current
WHERE last_name ILIKE ? AND first_name ILIKE ?
LIMIT 5
"""

COP_SHEET_COMPLAINTS_SQL = """
SELECT complaint_id, incident_date, fado_type, allegation,
       ccrb_disposition, board_cat, penalty_cat, penalty_desc,
       incident_command, incident_rank_long, shield_no,
       impacted_race, impacted_gender, impacted_age,
       contact_reason, location_type
FROM lake.federal.nypd_ccrb_complaints
WHERE last_name ILIKE ? AND first_name ILIKE ?
ORDER BY TRY_CAST(incident_date AS DATE) DESC NULLS LAST
LIMIT 30
"""

COP_SHEET_SETTLEMENTS_SQL = """
SELECT matter_name, plaintiff_name, summary_allegations,
       amount_awarded, total_incurred, court, docket_number,
       filed_date, closed_date, incident_date, case_outcome
FROM lake.federal.police_settlements_538
WHERE matter_name ILIKE '%' || ? || '%'
   OR plaintiff_name ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(amount_awarded AS DOUBLE) DESC NULLS LAST
LIMIT 15
"""

COP_SHEET_FEDERAL_SDNY_SQL = """
SELECT case_name, case_name_full, docket_number,
       date_filed, date_terminated, suit_nature, assigned_to
FROM lake.federal.cl_nypd_cases_sdny
WHERE case_name_full ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(date_filed AS DATE) DESC NULLS LAST
LIMIT 10
"""

COP_SHEET_FEDERAL_EDNY_SQL = """
SELECT case_name, case_name_full, docket_number,
       date_filed, date_terminated, suit_nature, assigned_to
FROM lake.federal.cl_nypd_cases_edny
WHERE case_name_full ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(date_filed AS DATE) DESC NULLS LAST
LIMIT 10
"""
```

- [ ] **Step 2: Add the cop_sheet tool function**

```python
@mcp.tool(annotations=READONLY, tags={"investigation", "safety"})
def cop_sheet(
    name: NAME,
    ctx: Context,
) -> ToolResult:
    """NYPD officer accountability dossier — CCRB civilian complaints (force, abuse, discourtesy, language), complaint dispositions, penalties, federal lawsuits (SDNY/EDNY), and city settlement payouts. Enter an officer's name to see their full record. Cross-references 5 databases: CCRB complaints, CCRB officer roster, FiveThirtyEight police settlements, and CourtListener federal court records (SDNY + EDNY). For broader person search, use entity_xray(name)."""
    name = name.strip()
    if len(name) < 3:
        raise ToolError("Name must be at least 3 characters.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    parts = name.split()
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
    else:
        first = name
        last = name

    # Officer summary
    _, summary_rows = _safe_query(db, COP_SHEET_SUMMARY_SQL, [f"{last}%", f"{first}%"])
    # CCRB complaints
    _, complaint_rows = _safe_query(db, COP_SHEET_COMPLAINTS_SQL, [f"{last}%", f"{first}%"])
    # Settlements (search by officer last name in matter_name)
    _, settlement_rows = _safe_query(db, COP_SHEET_SETTLEMENTS_SQL, [last, last])
    # Federal cases SDNY
    _, sdny_rows = _safe_query(db, COP_SHEET_FEDERAL_SDNY_SQL, [last])
    # Federal cases EDNY
    _, edny_rows = _safe_query(db, COP_SHEET_FEDERAL_EDNY_SQL, [last])

    if not summary_rows and not complaint_rows:
        raise ToolError(f"No CCRB records found for '{name}'. Try last name only, or check spelling.")

    lines = [f"COP SHEET — {name}", "=" * 55]

    # Summary
    if summary_rows:
        for s in summary_rows:
            fname, lname, allegations, substantiated, force, subst_force, incidents, status, gender, race = s
            lines.append(f"\n{fname} {lname} — {status or 'Unknown status'}")
            lines.append(f"  {gender or '?'} | {race or '?'}")
            lines.append(f"  Total allegations: {allegations or 0}")
            lines.append(f"  Substantiated: {substantiated or 0}")
            lines.append(f"  Force allegations: {force or 0} (substantiated: {subst_force or 0})")
            lines.append(f"  Distinct incidents: {incidents or 0}")

    # CCRB Complaints
    if complaint_rows:
        lines.append(f"\nCCRB COMPLAINTS ({len(complaint_rows)} allegations):")
        fado_counts = {}
        disposition_counts = {}
        for r in complaint_rows:
            cid, date, fado, allegation, disp, board, penalty_cat, penalty_desc, cmd, rank, shield, race, gender, age, reason, loc = r
            fado_counts[fado] = fado_counts.get(fado, 0) + 1
            disposition_counts[disp or 'Unknown'] = disposition_counts.get(disp or 'Unknown', 0) + 1

        lines.append(f"  By type (FADO): {', '.join(f'{k}: {v}' for k, v in sorted(fado_counts.items(), key=lambda x: -x[1]))}")
        lines.append(f"  By disposition: {', '.join(f'{k}: {v}' for k, v in sorted(disposition_counts.items(), key=lambda x: -x[1]))}")

        # Show recent complaints
        lines.append(f"\n  Recent complaints:")
        seen_complaints = set()
        for r in complaint_rows[:15]:
            cid, date, fado, allegation, disp, board, penalty_cat, penalty_desc, cmd, rank, shield, race, gender, age, reason, loc = r
            if cid in seen_complaints:
                continue
            seen_complaints.add(cid)
            penalty_str = f" → {penalty_desc}" if penalty_desc else ""
            lines.append(f"    {date or '?'}: {fado} — {allegation} ({disp or '?'}){penalty_str}")
            lines.append(f"      Victim: {race or '?'} {gender or '?'}, age {age or '?'} | {cmd or '?'}")

    # Settlements
    if settlement_rows:
        total_paid = sum(float(r[3] or 0) for r in settlement_rows)
        total_incurred = sum(float(r[4] or 0) for r in settlement_rows)
        lines.append(f"\nPOLICE SETTLEMENTS ({len(settlement_rows)} cases, ${total_paid:,.0f} awarded, ${total_incurred:,.0f} total incurred):")
        for r in settlement_rows[:5]:
            matter, plaintiff, summary, awarded, incurred, court, docket, filed, closed, incident, outcome = r
            lines.append(f"  {matter or plaintiff or '?'}")
            lines.append(f"    ${float(awarded or 0):,.0f} awarded | {court or '?'} | {docket or '?'}")
            if summary:
                lines.append(f"    Allegations: {summary[:150]}")

    # Federal cases
    federal_rows = (sdny_rows or []) + (edny_rows or [])
    if federal_rows:
        lines.append(f"\nFEDERAL LAWSUITS ({len(federal_rows)} cases):")
        for r in federal_rows[:8]:
            case_name, full_name, docket, filed, terminated, nature, judge = r
            status_str = f"terminated {terminated}" if terminated else "OPEN"
            lines.append(f"  {full_name or case_name or '?'}")
            lines.append(f"    {docket or '?'} | Filed: {filed or '?'} | {status_str}")
            if nature:
                lines.append(f"    Nature: {nature}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "name": name,
            "ccrb_allegations": len(complaint_rows),
            "settlements": len(settlement_rows),
            "federal_cases": len(federal_rows),
        },
        meta={"name": name, "query_time_ms": elapsed},
    )
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add cop_sheet tool — NYPD officer accountability dossier"
```

---

## Task 3: `climate_risk(zipcode)` — Environmental Due Diligence

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (insert after cop_sheet)

- [ ] **Step 1: Add SQL constants**

```python
# ---------------------------------------------------------------------------
# Climate Risk — Environmental Due Diligence
# ---------------------------------------------------------------------------

CLIMATE_HEAT_SQL = """
SELECT hvi FROM lake.environment.heat_vulnerability WHERE zcta20 = ? LIMIT 1
"""

CLIMATE_LEAD_SQL = """
SELECT sl_category, COUNT(*) AS cnt
FROM lake.environment.lead_service_lines
WHERE zip_code = ?
GROUP BY sl_category
ORDER BY cnt DESC
"""

CLIMATE_LEAD_DETAIL_SQL = """
SELECT
    COUNT(*) AS total_lines,
    COUNT(*) FILTER (WHERE LOWER(sl_category) LIKE '%lead%') AS lead_lines,
    COUNT(*) FILTER (WHERE LOWER(public_sl_material) LIKE '%lead%'
                      OR LOWER(customer_sl_material) LIKE '%lead%') AS lead_material
FROM lake.environment.lead_service_lines
WHERE zip_code = ?
"""

CLIMATE_ENERGY_BBL_SQL = """
SELECT property_name, primary_property_type, energy_star_score,
       site_eui_kbtu_ft, source_eui_kbtu_ft,
       direct_ghg_emissions_metric, total_location_based_ghg,
       year_built, number_of_buildings, occupancy
FROM lake.environment.ll84_energy_2023
WHERE nyc_borough_block_and_lot = ?
LIMIT 1
"""

CLIMATE_ENERGY_ZIP_SQL = """
SELECT
    COUNT(*) AS buildings,
    ROUND(AVG(TRY_CAST(energy_star_score AS DOUBLE)), 0) AS avg_energy_star,
    ROUND(AVG(TRY_CAST(site_eui_kbtu_ft AS DOUBLE)), 1) AS avg_site_eui,
    ROUND(AVG(TRY_CAST(direct_ghg_emissions_metric AS DOUBLE)), 1) AS avg_ghg
FROM lake.environment.ll84_energy_2023
WHERE postal_code = ?
"""

CLIMATE_TREES_SQL = """
SELECT
    COUNT(*) AS total_trees,
    COUNT(DISTINCT spc_common) AS species_count,
    spc_common AS top_species
FROM lake.environment.street_trees
WHERE zipcode = ?
GROUP BY spc_common
ORDER BY COUNT(*) DESC
LIMIT 1
"""

CLIMATE_TREE_COUNT_SQL = """
SELECT COUNT(*) AS trees FROM lake.environment.street_trees WHERE zipcode = ?
"""

CLIMATE_CLEANUP_SQL = """
SELECT COUNT(*) AS sites
FROM lake.environment.oer_cleanup
WHERE zip = ?
"""

```

- [ ] **Step 2: Add the climate_risk tool function**

```python
@mcp.tool(annotations=READONLY, tags={"environment"})
def climate_risk(
    zipcode: ZIP,
    ctx: Context,
) -> ToolResult:
    """Environmental due diligence for a NYC ZIP code — heat vulnerability, lead pipes, building energy efficiency, tree canopy, and cleanup sites. Use this when someone asks 'is this area safe environmentally?', 'should I move here?', or 'what's the environmental risk?'. For building-specific energy data, provide a BBL to building_profile(bbl) and follow up here for the ZIP context. For neighborhood lifestyle, use neighborhood_portrait(zipcode)."""
    if not re.match(r"^\d{5}$", zipcode):
        raise ToolError("Please provide a valid 5-digit NYC ZIP code.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    _, heat_rows = _safe_query(db, CLIMATE_HEAT_SQL, [zipcode])
    _, lead_rows = _safe_query(db, CLIMATE_LEAD_DETAIL_SQL, [zipcode])
    _, energy_rows = _safe_query(db, CLIMATE_ENERGY_ZIP_SQL, [zipcode])
    _, tree_count = _safe_query(db, CLIMATE_TREE_COUNT_SQL, [zipcode])
    _, tree_rows = _safe_query(db, CLIMATE_TREES_SQL, [zipcode])
    _, cleanup_rows = _safe_query(db, CLIMATE_CLEANUP_SQL, [zipcode])

    lines = [f"CLIMATE & ENVIRONMENTAL RISK — ZIP {zipcode}", "=" * 55]

    # Heat vulnerability
    if heat_rows and heat_rows[0][0] is not None:
        hvi = float(heat_rows[0][0])
        if hvi >= 4:
            risk = "HIGH — significant heat-related health risk"
        elif hvi >= 3:
            risk = "MODERATE-HIGH"
        elif hvi >= 2:
            risk = "MODERATE"
        else:
            risk = "LOW"
        lines.append(f"\nHEAT VULNERABILITY")
        lines.append(f"  Heat Vulnerability Index: {hvi:.1f}/5 — {risk}")
    else:
        lines.append(f"\nHEAT VULNERABILITY: No data for this ZIP")

    # Lead pipes
    if lead_rows and lead_rows[0][0]:
        total = int(lead_rows[0][0] or 0)
        lead_lines = int(lead_rows[0][1] or 0)
        lead_material = int(lead_rows[0][2] or 0)
        pct = round(lead_material / total * 100, 1) if total > 0 else 0
        lines.append(f"\nLEAD SERVICE LINES")
        lines.append(f"  Total service lines surveyed: {total:,}")
        lines.append(f"  Lines categorized as lead: {lead_lines:,}")
        lines.append(f"  Lines with lead material: {lead_material:,} ({pct}%)")
        if pct > 20:
            lines.append(f"  ⚠️  HIGH lead pipe concentration")
        elif pct > 5:
            lines.append(f"  Moderate lead pipe presence")
    else:
        lines.append(f"\nLEAD SERVICE LINES: No data for this ZIP")

    # Energy
    if energy_rows and energy_rows[0][0]:
        e = energy_rows[0]
        buildings = int(e[0] or 0)
        avg_star = e[1]
        avg_eui = e[2]
        avg_ghg = e[3]
        lines.append(f"\nBUILDING ENERGY (LL84 benchmarking, {buildings} large buildings)")
        if avg_star:
            star_str = f"{avg_star:.0f}/100"
            if float(avg_star) >= 75:
                star_str += " (good)"
            elif float(avg_star) <= 50:
                star_str += " (poor)"
            lines.append(f"  Avg Energy Star score: {star_str}")
        if avg_eui:
            lines.append(f"  Avg Site EUI: {avg_eui} kBtu/ft²")
        if avg_ghg:
            lines.append(f"  Avg GHG emissions: {avg_ghg} metric tons CO₂e")

    # Trees
    if tree_count and tree_count[0][0]:
        trees = int(tree_count[0][0])
        top_species = tree_rows[0][2] if tree_rows else "Unknown"
        lines.append(f"\nTREE CANOPY")
        lines.append(f"  Street trees: {trees:,}")
        lines.append(f"  Most common species: {top_species}")

    # Cleanup sites
    if cleanup_rows and cleanup_rows[0][0]:
        sites = int(cleanup_rows[0][0])
        if sites > 0:
            lines.append(f"\nCLEANUP SITES")
            lines.append(f"  Active/historical OER cleanup sites: {sites}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"zipcode": zipcode},
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add climate_risk tool — environmental due diligence by ZIP"
```

---

## Task 4: `vital_records(name)` — Historical Vital Records Search

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (insert after climate_risk)

- [ ] **Step 1: Add SQL constants**

```python
# ---------------------------------------------------------------------------
# Vital Records — Historical NYC Records Search
# ---------------------------------------------------------------------------

VITAL_DEATHS_SQL = """
SELECT first_name, last_name, age, year, month, day, county
FROM lake.city_government.death_certificates_1862_1948
WHERE last_name ILIKE ? AND first_name ILIKE ?
ORDER BY year DESC NULLS LAST
LIMIT 15
"""

VITAL_MARRIAGES_EARLY_SQL = """
SELECT first_name, last_name, year, month, day, county
FROM lake.city_government.marriage_certificates_1866_1937
WHERE last_name ILIKE ?
ORDER BY year DESC NULLS LAST
LIMIT 15
"""

VITAL_MARRIAGES_MID_SQL = """
SELECT license_number, license_year, county
FROM lake.city_government.marriage_licenses_1908_1949
ORDER BY license_year DESC NULLS LAST
LIMIT 0
"""
-- NOTE: marriage_licenses_1908_1949 has NO name columns (only license_number, license_year, county).
-- This query is a placeholder that returns 0 rows. Cannot search by name.

VITAL_MARRIAGES_MODERN_SQL = """
SELECT GROOM_FIRST_NAME, GROOM_SURNAME, BRIDE_FIRST_NAME, BRIDE_SURNAME,
       DATE_OF_MARRIAGE, LICENSE_YEAR, LICENSE_CITY
FROM lake.city_government.marriage_licenses_1950_2017
WHERE GROOM_SURNAME ILIKE ? OR BRIDE_SURNAME ILIKE ?
ORDER BY TRY_CAST(LICENSE_YEAR AS INT) DESC NULLS LAST
LIMIT 15
"""

VITAL_BIRTHS_SQL = """
SELECT first_name, last_name, year, county
FROM lake.city_government.birth_certificates_1855_1909
WHERE last_name ILIKE ? AND first_name ILIKE ?
ORDER BY year DESC NULLS LAST
LIMIT 15
"""
```

**Verified column names:**
- `marriage_certificates_1866_1937`: `first_name`, `last_name`, `year`, `month`, `day`, `county` (single-person records, no groom/bride prefix)
- `marriage_licenses_1908_1949`: `license_number`, `license_year`, `county` ONLY — **no name columns**, cannot search by name
- `marriage_licenses_1950_2017`: `GROOM_SURNAME`, `BRIDE_SURNAME`, `GROOM_FIRST_NAME`, `BRIDE_FIRST_NAME`, `DATE_OF_MARRIAGE`, `LICENSE_YEAR` (uppercase columns)

- [ ] **Step 2: Add the vital_records tool function**

```python
@mcp.tool(annotations=READONLY, tags={"investigation"})
def vital_records(
    name: NAME,
    ctx: Context,
) -> ToolResult:
    """Search historical NYC vital records — death certificates (1862-1948, 3.7M records), marriage certificates (1866-1937, 5.3M), marriage licenses (1908-2017, 5.9M), and birth certificates (1855-1909, 2.6M). Use this for genealogy, confirming if someone is deceased, tracing family connections, or historical research. For modern marriage records (1950-2017), use marriage_search(surname). For comprehensive person search, use entity_xray(name)."""
    name = name.strip()
    if len(name) < 3:
        raise ToolError("Name must be at least 3 characters.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    parts = name.split()
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
    else:
        first = "%"
        last = name

    _, death_rows = _safe_query(db, VITAL_DEATHS_SQL, [f"{last}%", f"{first}%"])
    _, marriage_early = _safe_query(db, VITAL_MARRIAGES_EARLY_SQL, [f"{last}%"])
    marriage_mid = []  # 1908-1949 table has no name columns — skip
    _, marriage_modern = _safe_query(db, VITAL_MARRIAGES_MODERN_SQL, [f"{last}%", f"{last}%"])
    _, birth_rows = _safe_query(db, VITAL_BIRTHS_SQL, [f"{last}%", f"{first}%"])

    all_marriages = (marriage_early or []) + (marriage_mid or []) + (marriage_modern or [])
    total = len(death_rows or []) + len(all_marriages) + len(birth_rows or [])

    if total == 0:
        raise ToolError(f"No vital records found for '{name}'. Note: death records cover 1862-1948, marriages 1866-2017, births 1855-1909.")

    lines = [f"VITAL RECORDS — {name}", f"Found {total} records across 16.4M historical NYC records", "=" * 55]

    if death_rows:
        lines.append(f"\nDEATH CERTIFICATES ({len(death_rows)} matches, 1862-1948):")
        for r in death_rows:
            fname, lname, age, year, month, day, county = r
            date_str = f"{year or '?'}-{month or '?'}-{day or '?'}"
            lines.append(f"  {fname or '?'} {lname or '?'} — died {date_str}, age {age or '?'}, {county or '?'}")

    if all_marriages:
        lines.append(f"\nMARRIAGE RECORDS ({len(all_marriages)} matches, 1866-2017):")
        for r in all_marriages[:15]:
            # Flexible: different marriage tables have different schemas
            # Display whatever we can extract
            lines.append(f"  {' | '.join(str(v) for v in r[:6] if v)}")

    if birth_rows:
        lines.append(f"\nBIRTH CERTIFICATES ({len(birth_rows)} matches, 1855-1909):")
        for r in birth_rows:
            fname, lname, year, county = r
            lines.append(f"  {fname or '?'} {lname or '?'} — born {year or '?'}, {county or '?'}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"name": name, "deaths": len(death_rows or []), "marriages": len(all_marriages), "births": len(birth_rows or [])},
        meta={"name": name, "total_records": total, "query_time_ms": elapsed},
    )
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add vital_records tool — historical NYC vital records search"
```

---

## Task 5: `money_trail(name)` — Full Political Money Tracker

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (insert after vital_records)

- [ ] **Step 1: Add SQL constants**

```python
# ---------------------------------------------------------------------------
# Money Trail — Full Political Money Tracker
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
```

- [ ] **Step 2: Add the money_trail tool function**

```python
@mcp.tool(annotations=READONLY, tags={"finance", "investigation"})
def money_trail(
    name: NAME,
    ctx: Context,
) -> ToolResult:
    """Trace ALL political money for a person or entity — NYS campaign donations (69M records), federal FEC contributions (44M), NYC campaign finance, city contracts, and state procurement. Goes broader than pay_to_play which only covers city-level donations. Use this for political influence, donation history, or government contract analysis. For corporate network analysis, use corporate_web(name). For lobbying specifically, use pay_to_play(name)."""
    name = name.strip()
    if len(name) < 3:
        raise ToolError("Name must be at least 3 characters.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    parts = name.split()
    first = parts[0] if len(parts) >= 2 else "%"
    last = parts[-1] if len(parts) >= 2 else name

    _, nys_rows = _safe_query(db, MONEY_NYS_DONATIONS_SQL, [f"{last}%", f"{first}%", name])
    _, fec_rows = _safe_query(db, MONEY_FEC_SQL, [name])
    _, nyc_rows = _safe_query(db, MONEY_NYC_DONATIONS_SQL, [name, name])
    _, contract_rows = _safe_query(db, MONEY_CONTRACTS_SQL, [name])
    _, procurement_rows = _safe_query(db, MONEY_PROCUREMENT_SQL, [name])

    total_sections = sum(1 for r in [nys_rows, fec_rows, nyc_rows, contract_rows, procurement_rows] if r)
    if total_sections == 0:
        raise ToolError(f"No political money records found for '{name}'.")

    lines = [f"MONEY TRAIL — {name}", "=" * 55]

    # NYS Campaign Finance
    if nys_rows:
        nys_total = sum(float(r[3] or 0) for r in nys_rows)
        nys_count = sum(int(r[4] or 0) for r in nys_rows)
        lines.append(f"\nNYS CAMPAIGN FINANCE ({nys_count} donations, ${nys_total:,.0f} total):")
        for r in nys_rows[:8]:
            filer, fn, ln, total, cnt, first_d, last_d = r
            lines.append(f"  → {filer}: ${float(total or 0):,.0f} ({cnt} donations, {first_d or '?'}–{last_d or '?'})")

    # FEC Federal
    if fec_rows:
        fec_total = sum(float(r[2] or 0) for r in fec_rows)
        lines.append(f"\nFEDERAL (FEC) CONTRIBUTIONS (${fec_total:,.0f} total):")
        for r in fec_rows[:8]:
            cmte_id, contributor, total, cnt, first_d, last_d = r
            lines.append(f"  → {cmte_id} ({contributor}): ${float(total or 0):,.0f} ({cnt} donations)")

    # NYC CFB
    if nyc_rows:
        nyc_total = sum(float(r[4] or 0) for r in nyc_rows)
        lines.append(f"\nNYC CAMPAIGN FINANCE (${nyc_total:,.0f} total):")
        for r in nyc_rows[:8]:
            recip, occ, emp, addr, total, cnt = r
            lines.append(f"  → {recip}: ${float(total or 0):,.0f} ({cnt} donations)")
            if emp:
                lines.append(f"    Employer: {emp}")

    # City contracts
    if contract_rows:
        lines.append(f"\nCITY CONTRACTS ({len(contract_rows)} awards):")
        for r in contract_rows[:5]:
            vendor, amount, agency, purpose, start, end = r
            lines.append(f"  {vendor}: ${float(amount or 0):,.0f} — {agency or '?'}")
            if purpose:
                lines.append(f"    {purpose[:120]}")

    # State procurement
    if procurement_rows:
        lines.append(f"\nSTATE PROCUREMENT ({len(procurement_rows)} contracts):")
        for r in procurement_rows[:5]:
            vendor, amount, agency, desc = r
            lines.append(f"  {vendor}: ${float(amount or 0):,.0f} — {agency or '?'}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"name": name, "nys_donations": len(nys_rows), "fec_donations": len(fec_rows), "nyc_donations": len(nyc_rows), "contracts": len(contract_rows)},
        meta={"name": name, "query_time_ms": elapsed},
    )
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add money_trail tool — full political money tracker across city/state/federal"
```

---

## Task 6: `judge_profile(name)` — Federal Judge Dossier

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (insert after money_trail)

- [ ] **Step 1: Add SQL constants**

```python
# ---------------------------------------------------------------------------
# Judge Profile — Federal Judge Dossier
# ---------------------------------------------------------------------------

JUDGE_BIO_SQL = """
SELECT id, name_first, name_last, name_middle, name_suffix,
       gender, date_dob, date_dod, dob_city, dob_state, religion
FROM lake.federal.cl_judges
WHERE name_last ILIKE ? AND name_first ILIKE ?
LIMIT 5
"""

JUDGE_POSITIONS_SQL = """
SELECT position_type, court_full_name, date_start, date_retirement,
       date_termination, appointer, how_selected, nomination_process
FROM lake.federal.cl_judges__positions
WHERE person_id = ?
ORDER BY TRY_CAST(date_start AS DATE) DESC NULLS LAST
"""

JUDGE_EDUCATION_SQL = """
SELECT school_name, degree_detail, degree_year
FROM lake.federal.cl_judges__educations
WHERE person_id = ?
ORDER BY TRY_CAST(degree_year AS INT) ASC NULLS LAST
"""

JUDGE_DISCLOSURES_SQL = """
SELECT year, report_type
FROM lake.federal.cl_financial_disclosures
WHERE person_id = ?
ORDER BY TRY_CAST(year AS INT) DESC
LIMIT 10
"""

JUDGE_INVESTMENTS_SQL = """
SELECT description_1, description_2, description_3,
       gross_value_code, income_during_reporting_period_code
FROM lake.federal.cl_financial_disclosures__investments
WHERE financial_disclosure_id IN (
    SELECT id FROM lake.federal.cl_financial_disclosures WHERE person_id = ?
)
LIMIT 20
"""

JUDGE_GIFTS_SQL = """
SELECT source, description, value
FROM lake.federal.cl_financial_disclosures__gifts
WHERE financial_disclosure_id IN (
    SELECT id FROM lake.federal.cl_financial_disclosures WHERE person_id = ?
)
ORDER BY TRY_CAST(value AS DOUBLE) DESC NULLS LAST
LIMIT 10
"""
```

**Note:** The `person_id` join key between `cl_judges` and related tables (`cl_judges__positions`, `cl_judges__educations`) and between `cl_judges` and `cl_financial_disclosures` needs verification at implementation time. The column may be `id` in cl_judges mapping to `person_id` in sub-tables, or the dlt-flattened structure may use `_dlt_parent_id`. Use `_safe_query` for graceful degradation.

- [ ] **Step 2: Add the judge_profile tool function**

```python
@mcp.tool(annotations=READONLY, tags={"investigation"})
def judge_profile(
    name: NAME,
    ctx: Context,
) -> ToolResult:
    """Federal judge dossier — career history, education, political affiliation, and financial disclosures (investments, gifts, debts). Enter a judge's name to see their background, who appointed them, where they went to school, and their financial holdings. Useful for checking conflicts of interest. Cross-references CourtListener judge database with financial disclosure filings. For case-level data, use sql_query() against cl_fjc_cases."""
    name = name.strip()
    if len(name) < 3:
        raise ToolError("Name must be at least 3 characters.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    parts = name.split()
    first = parts[0] if len(parts) >= 2 else "%"
    last = parts[-1] if len(parts) >= 2 else name

    _, bio_rows = _safe_query(db, JUDGE_BIO_SQL, [f"{last}%", f"{first}%"])

    if not bio_rows:
        raise ToolError(f"No federal judge found for '{name}'. Try last name only.")

    lines = [f"JUDGE PROFILE — {name}", "=" * 55]

    for bio in bio_rows:
        judge_id, fn, ln, mn, sfx, gender, dob, dod, dob_city, dob_state, religion = bio
        full_name = f"{fn or ''} {mn or ''} {ln or ''} {sfx or ''}".strip()
        lines.append(f"\n{full_name}")
        lines.append(f"  {gender or '?'} | Born: {dob or '?'} in {dob_city or '?'}, {dob_state or '?'}")
        if dod:
            lines.append(f"  Died: {dod}")
        if religion:
            lines.append(f"  Religion: {religion}")

        # Positions
        _, pos_rows = _safe_query(db, JUDGE_POSITIONS_SQL, [judge_id])
        if pos_rows:
            lines.append(f"\n  CAREER:")
            for p in pos_rows:
                pos_type, court, start, retire, term, appointer, how, nom = p
                status = "retired" if retire else ("terminated" if term else "active")
                lines.append(f"    {pos_type or '?'} — {court or '?'}")
                lines.append(f"      {start or '?'} – {retire or term or 'present'} ({status})")
                if appointer:
                    lines.append(f"      Appointed by: {appointer}")

        # Education
        _, edu_rows = _safe_query(db, JUDGE_EDUCATION_SQL, [judge_id])
        if edu_rows:
            lines.append(f"\n  EDUCATION:")
            for e in edu_rows:
                school, degree, year = e
                lines.append(f"    {school or '?'} — {degree or '?'} ({year or '?'})")

        # Financial disclosures
        _, disc_rows = _safe_query(db, JUDGE_DISCLOSURES_SQL, [judge_id])
        if disc_rows:
            lines.append(f"\n  FINANCIAL DISCLOSURES ({len(disc_rows)} filings):")
            for d in disc_rows[:5]:
                lines.append(f"    {d[0] or '?'}: {d[1] or 'Annual'}")

        # Top investments
        _, inv_rows = _safe_query(db, JUDGE_INVESTMENTS_SQL, [judge_id])
        if inv_rows:
            lines.append(f"\n  TOP INVESTMENTS ({len(inv_rows)} holdings):")
            for i in inv_rows[:10]:
                desc = ' / '.join(str(d) for d in [i[0], i[1], i[2]] if d)
                value = i[3] or '?'
                lines.append(f"    {desc[:80]} (value code: {value})")

        # Gifts
        _, gift_rows = _safe_query(db, JUDGE_GIFTS_SQL, [judge_id])
        if gift_rows:
            lines.append(f"\n  GIFTS RECEIVED ({len(gift_rows)}):")
            for g in gift_rows:
                source, desc, value = g
                lines.append(f"    From: {source or '?'} — {desc or '?'} (${value or '?'})")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"name": name, "judges_found": len(bio_rows)},
        meta={"name": name, "query_time_ms": elapsed},
    )
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add judge_profile tool — federal judge dossier with financial disclosures"
```

---

## Task 7: Update Routing Instructions

- [ ] **Step 1: Update the INSTRUCTIONS routing block**

Add these lines to the ROUTING section at the top of the file (search for `ROUTING — pick the FIRST match`):

```
* OFFICER/COP by name → cop_sheet
* JUDGE by name → judge_profile
* ENVIRONMENT/CLIMATE by ZIP → climate_risk
* BACKGROUND CHECK by name → due_diligence
* POLITICAL MONEY by name → money_trail (broader), pay_to_play (city-level)
* VITAL RECORDS/GENEALOGY → vital_records, marriage_search
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "docs(mcp): add routing for 6 new investigation/environment/finance tools"
```

---

## Task 8: Deploy and Smoke Test

- [ ] **Step 1: Deploy**

```bash
scp -i ~/.ssh/id_ed25519_hetzner infra/duckdb-server/mcp_server.py fattie@178.156.228.119:/opt/common-ground/duckdb-server/mcp_server.py
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "cd /opt/common-ground && sudo docker compose build --no-cache duckdb-server && sudo docker compose up -d duckdb-server"
```

Wait ~90 seconds for full startup (graph loading).

- [ ] **Step 2: Smoke test each tool via search_tools**

```
search_tools("due diligence background check") → should return due_diligence
search_tools("cop officer CCRB") → should return cop_sheet
search_tools("climate environment lead") → should return climate_risk
search_tools("vital death marriage") → should return vital_records
search_tools("money donation campaign") → should return money_trail
search_tools("judge financial disclosure") → should return judge_profile
```

- [ ] **Step 3: Functional smoke tests**

```
due_diligence("PERLBINDER") → should return attorney + possible contractor matches
cop_sheet("Daniel Pantaleo") → should return CCRB complaints
climate_risk("10003") → should return heat/lead/energy/trees data
vital_records("SMITH") → should return death/marriage/birth records
money_trail("CATSIMATIDIS") → should return NYS + FEC + NYC donations
judge_profile("KAPLAN") → should return judge bio + positions + disclosures
```

- [ ] **Step 4: Final commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): 6 killer tools deployed — due_diligence, cop_sheet, climate_risk, vital_records, money_trail, judge_profile"
```
