"""entity() super tool — absorbs 10 person/company tools into one dispatch.

Roles:
  auto       → entity_xray (full cross-reference across all datasets)
  background → due_diligence (professional/financial background)
  cop        → cop_sheet (CCRB complaints, penalties, federal lawsuits)
  judge      → judge_profile (career history, financial disclosures)
  vitals     → vital_records + marriage_search
  top        → top_crossrefs (most cross-referenced people)
"""

import time
from typing import Annotated, Literal

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import execute, safe_query, parallel_queries
from shared.formatting import format_text_table
from shared.lance import vector_expand_names, lance_route_entity

from entity import phonetic_search_sql

# ---------------------------------------------------------------------------
# Tables always queried during entity xray (even when Lance routing is active)
# ---------------------------------------------------------------------------

_ALWAYS_QUERY = frozenset({
    "nys_corporations", "issued_licenses", "restaurant_inspections",
    "campaign_expenditures", "dcwp_charges",
    "graph_dob_owners", "graph_doing_business", "graph_epa_facilities",
    "marriage_licenses_1950_2017", "nys_lobbyist_registration",
})

# ---------------------------------------------------------------------------
# SQL constants — Due Diligence
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

# ---------------------------------------------------------------------------
# SQL constants — Cop Sheet
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

# ---------------------------------------------------------------------------
# SQL constants — Vital Records
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

# ---------------------------------------------------------------------------
# SQL constants — Judge Profile
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


# ---------------------------------------------------------------------------
# Helper: entity_master availability check + lookup
# ---------------------------------------------------------------------------

def _has_entity_master(pool) -> bool:
    """Check if lake.foundation.entity_master is available."""
    try:
        execute(pool, "SELECT 1 FROM lake.foundation.entity_master LIMIT 1", [])
        return True
    except Exception:
        return False


def _lookup_entity_master(pool, cluster_ids: list) -> dict:
    """Look up entity_master rows for given cluster_ids.

    Direct join on cluster_id — entity_master stores cluster_id from resolved_entities.
    Returns dict mapping cluster_id -> {entity_id, entity_type, confidence, ...}.
    Returns empty dict if entity_master unavailable.
    """
    if not cluster_ids:
        return {}
    try:
        placeholders = ", ".join(["?"] * len(cluster_ids))
        cols, rows = execute(pool, f"""
            SELECT
                em.cluster_id,
                em.entity_id,
                em.canonical_last,
                em.canonical_first,
                em.entity_type,
                em.confidence,
                em.record_count,
                em.source_count
            FROM lake.foundation.entity_master em
            WHERE em.cluster_id IN ({placeholders})
        """, list(cluster_ids))
        result = {}
        for r in rows:
            result[r[0]] = {
                "entity_id": r[1], "canonical_last": r[2], "canonical_first": r[3],
                "entity_type": r[4], "confidence": r[5],
                "record_count": r[6], "source_count": r[7],
            }
        return result
    except Exception as e:
        print(f"entity_master lookup failed: {e}", flush=True)
        return {}


# ---------------------------------------------------------------------------
# Helper: resolve Splink name variants
# ---------------------------------------------------------------------------

def _resolve_name_variants(pool, name: str) -> list[tuple[str, str]]:
    """Look up Splink resolved_entities to find all name variants in the same cluster."""
    try:
        search = name.strip().upper()
        words = [w for w in search.split() if len(w) >= 2]
        if not words:
            return []

        candidates = []
        if len(words) >= 2:
            candidates.append((words[-1], words[0]))
            candidates.append((words[0], words[-1]))
        else:
            candidates.append((words[0], ""))

        cluster_ids = set()
        for last, first in candidates:
            if first:
                cols, rows = execute(pool, """
                    SELECT DISTINCT cluster_id
                    FROM lake.federal.resolved_entities
                    WHERE last_name = ? AND first_name = ?
                    LIMIT 5
                """, [last, first])
            else:
                cols, rows = execute(pool, """
                    SELECT DISTINCT cluster_id
                    FROM lake.federal.resolved_entities
                    WHERE last_name = ?
                    LIMIT 5
                """, [last])
            for r in rows:
                if r[0] is not None:
                    cluster_ids.add(r[0])

        if not cluster_ids:
            return []

        placeholders = ", ".join(["?"] * len(cluster_ids))
        cols, rows = execute(pool, f"""
            SELECT DISTINCT last_name, first_name
            FROM lake.federal.resolved_entities
            WHERE cluster_id IN ({placeholders})
              AND last_name IS NOT NULL AND first_name IS NOT NULL
            LIMIT 20
        """, list(cluster_ids))

        return [(r[0], r[1]) for r in rows]

    except Exception:
        return []


# ---------------------------------------------------------------------------
# Role: auto — entity_xray
# ---------------------------------------------------------------------------

def _entity_xray(name: str, pool, ctx) -> ToolResult:
    """Full X-ray of a person or entity across ALL NYC datasets."""
    t0 = time.time()
    search = name.strip().upper()
    words = [w for w in search.split() if len(w) >= 2]

    extra_names = vector_expand_names(ctx, search)

    # Splink probabilistic name resolution
    name_variants = _resolve_name_variants(pool, name)

    # Route via Lance — find which tables actually have this name
    lance_route = lance_route_entity(ctx, search)
    routed_sources = lance_route.get("sources", set())
    use_routing = len(routed_sources) > 0

    lance_matched = set(lance_route.get("matched_names", []))
    extra_names = extra_names | lance_matched

    def _should_query(source_table: str) -> bool:
        if source_table in _ALWAYS_QUERY:
            return True
        if not use_routing:
            return True
        return source_table in routed_sources

    # Build SQL for variable queries (word-count-dependent) before batching
    if len(words) > 1:
        word_clauses = " AND ".join(["UPPER(name) LIKE ?"] * len(words))
        camp_sql = f"""
            SELECT name, recipname, TRY_CAST(amnt AS DOUBLE) AS amount,
                   date, occupation, empname, city, state
            FROM lake.city_government.campaign_contributions
            WHERE (UPPER(name) LIKE ? OR UPPER(empname) LIKE ?)
               OR ({word_clauses})
            ORDER BY TRY_CAST(amnt AS DOUBLE) DESC NULLS LAST
            LIMIT 20
        """
        camp_params = [f"%{search}%", f"%{search}%"] + [f"%{w}%" for w in words]
    else:
        camp_sql = """
            SELECT name, recipname, TRY_CAST(amnt AS DOUBLE) AS amount,
                   date, occupation, empname, city, state
            FROM lake.city_government.campaign_contributions
            WHERE UPPER(name) LIKE ? OR UPPER(empname) LIKE ?
            ORDER BY TRY_CAST(amnt AS DOUBLE) DESC NULLS LAST
            LIMIT 20
        """
        camp_params = [f"%{search}%"] * 2

    if len(words) > 1:
        oath_word_clauses = " OR ".join(
            [f"(UPPER(respondent_last_name) LIKE ? AND UPPER(respondent_first_name) LIKE ?)"
             for _ in range(len(words))]
        )
        oath_sql = f"""
            SELECT respondent_last_name,
                   respondent_first_name,
                   issuing_agency,
                   charge_1_code_description,
                   TRY_CAST(total_violation_amount AS DOUBLE) AS fine,
                   hearing_result,
                   violation_date,
                   violation_location_house || ' ' || violation_location_street_name AS location
            FROM lake.city_government.oath_hearings
            WHERE UPPER(respondent_last_name) LIKE ?
               OR UPPER(respondent_first_name || ' ' || respondent_last_name) LIKE ?
               OR ({oath_word_clauses})
            ORDER BY violation_date DESC
            LIMIT 20
        """
        oath_params = [f"%{search}%", f"%{search}%"]
        for i, w in enumerate(words):
            other = " ".join(words[:i] + words[i+1:])
            oath_params.extend([f"%{w}%", f"%{other}%"])
    else:
        oath_sql = """
            SELECT respondent_last_name,
                   respondent_first_name,
                   issuing_agency,
                   charge_1_code_description,
                   TRY_CAST(total_violation_amount AS DOUBLE) AS fine,
                   hearing_result,
                   violation_date,
                   violation_location_house || ' ' || violation_location_street_name AS location
            FROM lake.city_government.oath_hearings
            WHERE UPPER(respondent_last_name) LIKE ?
               OR UPPER(respondent_first_name) LIKE ?
            ORDER BY violation_date DESC
            LIMIT 20
        """
        oath_params = [f"%{search}%"] * 2

    if len(words) > 1:
        dob_sql = """
            SELECT owner_s_business_name, owner_s_first_name, owner_s_last_name,
                   permit_type, job_type, issuance_date,
                   house__ || ' ' || street_name AS address,
                   (borough || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
            FROM lake.housing.dob_permit_issuance
            WHERE UPPER(owner_s_business_name) LIKE ?
               OR UPPER(owner_s_last_name) LIKE ?
               OR (UPPER(owner_s_last_name) LIKE ? AND UPPER(owner_s_first_name) LIKE ?)
            ORDER BY issuance_date DESC
            LIMIT 15
        """
        dob_params = [f"%{search}%", f"%{search}%", f"%{words[-1]}%", f"%{words[0]}%"]
    else:
        dob_sql = """
            SELECT owner_s_business_name, owner_s_first_name, owner_s_last_name,
                   permit_type, job_type, issuance_date,
                   house__ || ' ' || street_name AS address,
                   (borough || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
            FROM lake.housing.dob_permit_issuance
            WHERE UPPER(owner_s_business_name) LIKE ?
               OR UPPER(owner_s_last_name) LIKE ?
            ORDER BY issuance_date DESC
            LIMIT 15
        """
        dob_params = [f"%{search}%"] * 2

    if len(words) > 1:
        sbs_word_clauses = " OR ".join(
            [f"(UPPER(last_name) LIKE ? AND UPPER(first_name) LIKE ?)"] * 1
        )
        sbs_sql = f"""
            SELECT vendor_formal_name, vendor_dba, first_name, last_name,
                   certification, ethnicity, business_description,
                   city, bbl
            FROM lake.business.sbs_certified
            WHERE UPPER(vendor_formal_name) LIKE ?
               OR UPPER(vendor_dba) LIKE ?
               OR ({sbs_word_clauses})
            LIMIT 10
        """
        sbs_params = [f"%{search}%", f"%{search}%", f"%{words[-1]}%", f"%{words[0]}%"]
    else:
        sbs_sql = """
            SELECT vendor_formal_name, vendor_dba, first_name, last_name,
                   certification, ethnicity, business_description,
                   city, bbl
            FROM lake.business.sbs_certified
            WHERE UPPER(vendor_formal_name) LIKE ?
               OR UPPER(vendor_dba) LIKE ?
               OR UPPER(last_name) LIKE ?
            LIMIT 10
        """
        sbs_params = [f"%{search}%"] * 3

    if len(words) > 1:
        payroll_sql = """
            SELECT DISTINCT last_name, first_name, agency_name, title_description,
                   TRY_CAST(base_salary AS DOUBLE) AS salary,
                   TRY_CAST(total_ot_paid AS DOUBLE) AS overtime,
                   fiscal_year, work_location_borough
            FROM lake.city_government.citywide_payroll
            WHERE (UPPER(last_name) LIKE ? AND UPPER(first_name) LIKE ?)
            ORDER BY fiscal_year DESC, salary DESC NULLS LAST
            LIMIT 10
        """
        payroll_params = [f"%{words[-1]}%", f"%{words[0]}%"]
    else:
        payroll_sql = """
            SELECT DISTINCT last_name, first_name, agency_name, title_description,
                   TRY_CAST(base_salary AS DOUBLE) AS salary,
                   TRY_CAST(total_ot_paid AS DOUBLE) AS overtime,
                   fiscal_year, work_location_borough
            FROM lake.city_government.citywide_payroll
            WHERE UPPER(last_name) LIKE ?
            ORDER BY fiscal_year DESC, salary DESC NULLS LAST
            LIMIT 10
        """
        payroll_params = [f"%{search}%"]

    if len(words) > 1:
        civil_sql = """
            SELECT first_name, last_name, list_title_desc, exam_no,
                   list_no, adj_fa, list_agency_desc, established_date
            FROM lake.city_government.civil_service_active
            WHERE (UPPER(last_name) LIKE ? AND UPPER(first_name) LIKE ?)
            LIMIT 20
        """
        civil_params = [f"%{words[-1]}%", f"%{words[0]}%"]
    else:
        civil_sql = """
            SELECT first_name, last_name, list_title_desc, exam_no,
                   list_no, adj_fa, list_agency_desc, established_date
            FROM lake.city_government.civil_service_active
            WHERE UPPER(last_name) LIKE ?
            LIMIT 20
        """
        civil_params = [f"%{search}%"]

    if len(words) > 1:
        death_sql = """
            SELECT last_name, first_name, year, age, date_of_death,
                   residence_code, place_of_death_code
            FROM lake.federal.nys_death_index
            WHERE (UPPER(last_name) LIKE ? AND UPPER(first_name) LIKE ?)
            ORDER BY year DESC
            LIMIT 20
        """
        death_params = [f"%{words[-1]}%", f"%{words[0]}%"]
    else:
        death_sql = """
            SELECT last_name, first_name, year, age, date_of_death,
                   residence_code, place_of_death_code
            FROM lake.federal.nys_death_index
            WHERE UPPER(last_name) LIKE ?
            ORDER BY year DESC
            LIMIT 20
        """
        death_params = [f"%{search}%"]

    # Build parallel query list — only include tables that pass routing check
    queries = []
    if _should_query("nys_corporations"):
        queries.append(("corp", """
            SELECT current_entity_name, entity_type, initial_dos_filing_date,
                   dos_process_name, registered_agent_name, chairman_name, county
            FROM lake.business.nys_corporations
            WHERE UPPER(current_entity_name) LIKE ?
               OR UPPER(dos_process_name) LIKE ?
               OR UPPER(registered_agent_name) LIKE ?
               OR UPPER(chairman_name) LIKE ?
            LIMIT 30
        """, [f"%{search}%"] * 4))
    if _should_query("issued_licenses"):
        queries.append(("biz", """
            SELECT business_name, business_category,
                   license_type, license_status, bbl,
                   address_building || ' ' || address_street_name AS address,
                   address_zip
            FROM lake.business.issued_licenses
            WHERE UPPER(business_name) LIKE ?
            LIMIT 20
        """, [f"%{search}%"]))
    if _should_query("restaurant_inspections"):
        queries.append(("rest", """
            SELECT DISTINCT dba, cuisine_description, building || ' ' || street AS address,
                   boro, zipcode, grade, bbl
            FROM lake.health.restaurant_inspections
            WHERE UPPER(dba) LIKE ?
            LIMIT 20
        """, [f"%{search}%"]))
    if _should_query("campaign_contributions"):
        queries.append(("camp", camp_sql, camp_params))
    if _should_query("oath_hearings"):
        queries.append(("oath", oath_sql, oath_params))
    if _should_query("acris_parties"):
        queries.append(("acris", """
            SELECT p.name AS party_name, p.party_type,
                   m.doc_type, TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                   m.document_date,
                   (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                   l.street_name, l.unit
            FROM lake.housing.acris_parties p
            JOIN lake.housing.acris_master m ON p.document_id = m.document_id
            JOIN lake.housing.acris_legals l ON p.document_id = l.document_id
            WHERE UPPER(p.name) LIKE ?
            ORDER BY m.document_date DESC
            LIMIT 20
        """, [f"%{search}%"]))
    if _should_query("pluto"):
        queries.append(("pluto", """
            SELECT ownername, bbl, address, zonedist1,
                   TRY_CAST(assesstot AS DOUBLE) AS assessed_total,
                   TRY_CAST(unitsres AS INTEGER) AS res_units,
                   TRY_CAST(numfloors AS DOUBLE) AS floors,
                   TRY_CAST(bldgarea AS DOUBLE) AS bldg_sqft,
                   yearbuilt, bldgclass, zipcode
            FROM lake.city_government.pluto
            WHERE UPPER(ownername) LIKE ?
            ORDER BY TRY_CAST(assesstot AS DOUBLE) DESC NULLS LAST
            LIMIT 20
        """, [f"%{search}%"]))
    if _should_query("campaign_expenditures"):
        queries.append(("expend", """
            SELECT name, candlast || ', ' || candfirst AS candidate,
                   TRY_CAST(amnt AS DOUBLE) AS amount,
                   purpose, explain, date, city
            FROM lake.city_government.campaign_expenditures
            WHERE UPPER(name) LIKE ?
            ORDER BY TRY_CAST(amnt AS DOUBLE) DESC NULLS LAST
            LIMIT 20
        """, [f"%{search}%"]))
    if _should_query("dob_permit_issuance"):
        queries.append(("dob", dob_sql, dob_params))
    if _should_query("dcwp_charges"):
        queries.append(("dcwp", """
            SELECT business_name, business_category,
                   charge, violation_date, outcome, charge_count
            FROM lake.business.dcwp_charges
            WHERE UPPER(business_name) LIKE ?
            ORDER BY violation_date DESC
            LIMIT 15
        """, [f"%{search}%"]))
    if _should_query("sbs_certified"):
        queries.append(("sbs", sbs_sql, sbs_params))
    if _should_query("citywide_payroll"):
        queries.append(("payroll", payroll_sql, payroll_params))
    if _should_query("graph_dob_owners"):
        queries.append(("dob_app", """
            SELECT owner_name, business_name,
                   owner_address, owner_city, owner_state, owner_zip
            FROM main.graph_dob_owners
            WHERE UPPER(owner_name) LIKE ? OR UPPER(business_name) LIKE ?
            LIMIT 15
        """, [f"%{search}%"] * 2))
    if _should_query("graph_doing_business"):
        queries.append(("doing_biz", """
            SELECT entity_name, person_name, title, transaction_type,
                   entity_address, entity_city, entity_state
            FROM main.graph_doing_business
            WHERE UPPER(entity_name) LIKE ? OR UPPER(person_name) LIKE ?
            LIMIT 15
        """, [f"%{search}%"] * 2))
    if _should_query("graph_epa_facilities"):
        queries.append(("epa", """
            SELECT facility_name, address, city, zip, county,
                   current_violation, total_penalties, inspection_count,
                   formal_action_count, last_penalty_amount
            FROM main.graph_epa_facilities
            WHERE UPPER(facility_name) LIKE ?
            LIMIT 15
        """, [f"%{search}%"]))
    if _should_query("nys_attorney_registrations"):
        queries.append(("atty", """
            SELECT first_name, last_name, registration_number, law_school,
                   company_name, status, year_admitted, city, state
            FROM lake.financial.nys_attorney_registrations
            WHERE UPPER(last_name) LIKE ? OR UPPER(company_name) LIKE ?
            LIMIT 20
        """, [f"%{search}%"] * 2))
    if _should_query("acris_pp_parties"):
        queries.append(("pp", """
            SELECT name, party_type, document_id, address_1, city, state, zip
            FROM lake.business.acris_pp_parties
            WHERE UPPER(name) LIKE ?
            LIMIT 20
        """, [f"%{search}%"]))
    if _should_query("civil_service_active"):
        queries.append(("civil", civil_sql, civil_params))
    if _should_query("nys_lobbyist_registration"):
        queries.append(("lobby", """
            SELECT principal_lobbyist_name, contractual_client_name,
                   lobbying_subjects, compensation_amount, reporting_year,
                   level_of_government, individual_lobbyist_s
            FROM lake.city_government.nys_lobbyist_registration
            WHERE UPPER(principal_lobbyist_name) LIKE ?
               OR UPPER(contractual_client_name) LIKE ?
               OR UPPER(individual_lobbyist_s) LIKE ?
            LIMIT 20
        """, [f"%{search}%"] * 3))
    if _should_query("nys_death_index"):
        queries.append(("death", death_sql, death_params))
    if _should_query("nys_re_brokers"):
        queries.append(("broker", """
            SELECT license_holder_name, business_name, license_type,
                   license_number, license_expiration_date, county,
                   business_city, business_state
            FROM lake.financial.nys_re_brokers
            WHERE UPPER(license_holder_name) LIKE ? OR UPPER(business_name) LIKE ?
            LIMIT 20
        """, [f"%{search}%"] * 2))
    if _should_query("nys_notaries"):
        queries.append(("notary", """
            SELECT commission_holder_name, commissioned_county,
                   commission_type_traditional_or_electronic,
                   term_issue_date, term_expiration_date,
                   business_name_if_available
            FROM lake.financial.nys_notaries
            WHERE UPPER(commission_holder_name) LIKE ?
               OR UPPER(business_name_if_available) LIKE ?
            LIMIT 20
        """, [f"%{search}%"] * 2))

    # Execute all independent queries in parallel
    results = parallel_queries(pool, queries) if queries else {}

    corp_cols, corp_rows = results.get("corp", ([], []))
    biz_cols, biz_rows = results.get("biz", ([], []))
    rest_cols, rest_rows = results.get("rest", ([], []))
    camp_cols, camp_rows = results.get("camp", ([], []))
    oath_cols, oath_rows = results.get("oath", ([], []))
    acris_cols, acris_rows = results.get("acris", ([], []))
    pluto_cols, pluto_rows = results.get("pluto", ([], []))
    expend_cols, expend_rows = results.get("expend", ([], []))
    dob_cols, dob_rows = results.get("dob", ([], []))
    dcwp_cols, dcwp_rows = results.get("dcwp", ([], []))
    sbs_cols, sbs_rows = results.get("sbs", ([], []))
    payroll_cols, payroll_rows = results.get("payroll", ([], []))
    dob_app_cols, dob_app_rows = results.get("dob_app", ([], []))
    doing_biz_cols, doing_biz_rows = results.get("doing_biz", ([], []))
    epa_cols, epa_rows = results.get("epa", ([], []))
    atty_cols, atty_rows = results.get("atty", ([], []))
    pp_cols, pp_rows = results.get("pp", ([], []))
    civil_cols, civil_rows = results.get("civil", ([], []))
    lobby_cols, lobby_rows = results.get("lobby", ([], []))
    death_cols, death_rows = results.get("death", ([], []))
    broker_cols, broker_rows = results.get("broker", ([], []))
    notary_cols, notary_rows = results.get("notary", ([], []))

    elapsed = round((time.time() - t0) * 1000)

    total = (len(corp_rows) + len(biz_rows) + len(rest_rows) + len(camp_rows)
             + len(oath_rows) + len(acris_rows) + len(pluto_rows) + len(expend_rows)
             + len(dob_rows) + len(dcwp_rows) + len(sbs_rows) + len(payroll_rows)
             + len(dob_app_rows) + len(doing_biz_rows) + len(epa_rows)
             + len(atty_rows) + len(pp_rows) + len(civil_rows) + len(lobby_rows)
             + len(death_rows) + len(broker_rows) + len(notary_rows))
    if total == 0:
        return ToolResult(content=f"No records found for '{name}' across any NYC dataset.")

    lines = [f"ENTITY X-RAY — '{name}'\n"]
    if use_routing:
        lines.append(f"Lance-routed: {len(routed_sources)} sources matched, {len(lance_route.get('matched_names', []))} name variants found")
    else:
        lines.append("Full scan (Lance index unavailable)")
    lines.append("")

    if corp_rows:
        lines.append(f"--- NYS CORPORATIONS ({len(corp_rows)} entities) ---")
        for r in corp_rows:
            lines.append(f"  {r[0]} ({r[1] or '?'}) | filed {r[2] or '?'} | {r[6] or '?'} county")
            people = []
            if r[3] and search not in (r[3] or "").upper():
                people.append(f"Process: {r[3]}")
            if r[4] and search not in (r[4] or "").upper():
                people.append(f"Agent: {r[4]}")
            if r[5] and search not in (r[5] or "").upper():
                people.append(f"Chairman: {r[5]}")
            for p in people:
                lines.append(f"    {p}")
        lines.append("")

    if biz_rows:
        lines.append(f"--- BUSINESS LICENSES ({len(biz_rows)} licenses) ---")
        for r in biz_rows:
            dba = f" (dba: {r[1]})" if r[1] else ""
            lines.append(f"  {r[0]}{dba} | {r[2] or '?'} | {r[3]} | {r[4] or '?'} | {r[5] or '?'} {r[6] or ''}")
        lines.append("")

    if rest_rows:
        lines.append(f"--- RESTAURANTS ({len(rest_rows)} locations) ---")
        for r in rest_rows:
            grade_str = f"Grade {r[5]}" if r[5] else "ungraded"
            lines.append(f"  {r[0]} | {r[1] or '?'} | {r[2] or '?'}, {r[3] or '?'} {r[4] or ''} | {grade_str}")
        lines.append("")

    if camp_rows:
        total_donated = sum(r[2] or 0 for r in camp_rows)
        recipients = list({r[1] for r in camp_rows if r[1]})
        lines.append(f"--- CAMPAIGN CONTRIBUTIONS ({len(camp_rows)} donations, ${total_donated:,.0f} total) ---")
        if recipients:
            lines.append(f"  Recipients: {', '.join(recipients[:8])}")
        for r in camp_rows[:10]:
            amt = f"${r[2]:,.0f}" if r[2] else "?"
            lines.append(f"  {r[0]} | {amt} → {r[1]} | {r[3] or '?'} | {r[4] or ''} at {r[5] or ''}")
        lines.append("")

    if oath_rows:
        total_fines = sum(r[4] or 0 for r in oath_rows)
        lines.append(f"--- OATH HEARINGS ({len(oath_rows)} violations, ${total_fines:,.0f} in fines) ---")
        for r in oath_rows[:10]:
            fine_str = f"${r[4]:,.0f}" if r[4] else "?"
            lines.append(f"  {r[0]}, {r[1] or ''} | {r[2] or '?'} | {r[3] or '?'} | {fine_str} | {r[6] or '?'}")
        lines.append("")

    if acris_rows:
        total_value = sum(r[3] or 0 for r in acris_rows)
        lines.append(f"--- ACRIS PROPERTY TRANSACTIONS ({len(acris_rows)} records, ${total_value:,.0f} total value) ---")
        for r in acris_rows[:10]:
            party_tag = "BUYER" if str(r[1] or "").strip() == "2" else "SELLER" if str(r[1] or "").strip() == "1" else r[1] or "?"
            amt_str = f"${r[3]:,.0f}" if r[3] else "?"
            lines.append(f"  {r[0]} ({party_tag}) | {r[2] or '?'} | {amt_str} | {r[4] or '?'} | BBL {r[5] or '?'} {r[6] or ''}")
        lines.append("")

    if pluto_rows:
        total_assessed = sum(r[4] or 0 for r in pluto_rows)
        total_units = sum(r[5] or 0 for r in pluto_rows)
        lines.append(f"--- PROPERTY PORTFOLIO / PLUTO ({len(pluto_rows)} parcels, ${total_assessed:,.0f} assessed, {total_units} res units) ---")
        for r in pluto_rows[:10]:
            val_str = f"${r[4]:,.0f}" if r[4] else "?"
            units_str = f"{r[5]} units" if r[5] else ""
            lines.append(f"  {r[0]} | {r[2] or '?'} {r[10] or ''} | {r[3] or '?'} | {val_str} | {units_str} | {r[8] or '?'} | BBL {r[1] or '?'}")
        lines.append("")

    if expend_rows:
        total_paid = sum(r[2] or 0 for r in expend_rows)
        lines.append(f"--- CAMPAIGN EXPENDITURES ({len(expend_rows)} payments, ${total_paid:,.0f} received) ---")
        for r in expend_rows[:10]:
            amt_str = f"${r[2]:,.0f}" if r[2] else "?"
            lines.append(f"  {r[0]} | {amt_str} from {r[1] or '?'} | {r[3] or ''}: {r[4] or ''} | {r[5] or '?'}")
        lines.append("")

    if dob_rows:
        lines.append(f"--- DOB BUILDING PERMITS ({len(dob_rows)} permits) ---")
        for r in dob_rows[:10]:
            owner = r[0] or f"{r[1] or ''} {r[2] or ''}".strip() or "?"
            lines.append(f"  {owner} | {r[3] or '?'} ({r[4] or '?'}) | {r[6] or '?'} | {r[5] or '?'} | BBL {r[7] or '?'}")
        lines.append("")

    if dcwp_rows:
        lines.append(f"--- CONSUMER PROTECTION CHARGES ({len(dcwp_rows)} charges) ---")
        for r in dcwp_rows[:10]:
            dba = f" (dba: {r[1]})" if r[1] else ""
            lines.append(f"  {r[0]}{dba} | {r[2] or '?'} | {r[3] or '?'} | {r[5] or '?'} | {r[4] or '?'}")
        lines.append("")

    if sbs_rows:
        lines.append(f"--- M/WBE CERTIFIED BUSINESSES ({len(sbs_rows)} firms) ---")
        for r in sbs_rows:
            dba = f" (dba: {r[1]})" if r[1] else ""
            owner_name = f"{r[2] or ''} {r[3] or ''}".strip()
            lines.append(f"  {r[0]}{dba} | {owner_name} | {r[4] or '?'} | {r[5] or '?'} | {r[6] or '?'}")
        lines.append("")

    if payroll_rows:
        lines.append(f"--- CITY PAYROLL ({len(payroll_rows)} records) ---")
        for r in payroll_rows:
            sal_str = f"${r[4]:,.0f}" if r[4] else "?"
            ot_str = f"+${r[5]:,.0f} OT" if r[5] and r[5] > 0 else ""
            lines.append(f"  {r[0]}, {r[1] or ''} | {r[2] or '?'} | {r[3] or '?'} | {sal_str} {ot_str} | FY{r[6] or '?'}")
        lines.append("")

    if dob_app_rows:
        lines.append(f"--- DOB APPLICATION OWNERS ({len(dob_app_rows)} permits) ---")
        for r in dob_app_rows:
            biz = f" ({r[1]})" if r[1] else ""
            lines.append(f"  {r[0] or '?'}{biz} | BBL {r[2] or '?'} | {r[3] or ''}, {r[4] or ''} {r[5] or ''}")
        lines.append("")

    if doing_biz_rows:
        lines.append(f"--- DOING BUSINESS / CITY CONTRACTORS ({len(doing_biz_rows)} records) ---")
        for r in doing_biz_rows:
            person = f" — {r[1]}" if r[1] else ""
            title = f" ({r[2]})" if r[2] else ""
            lines.append(f"  {r[0] or '?'}{person}{title} | {r[3] or '?'} | {r[4] or ''}, {r[5] or ''}")
        lines.append("")

    if epa_rows:
        try:
            total_penalties = sum(float(str(r[6] or 0).replace('$', '').replace(',', '') or 0) for r in epa_rows)
        except (ValueError, TypeError):
            total_penalties = 0
        lines.append(f"--- EPA ECHO FACILITIES ({len(epa_rows)} facilities, ${total_penalties:,.0f} total penalties) ---")
        for r in epa_rows:
            viol = "VIOLATION" if r[5] and r[5] == "Y" else "compliant"
            try:
                pen_str = f"${float(str(r[9] or 0).replace('$', '').replace(',', '') or 0):,.0f}" if r[9] else "$0"
            except (ValueError, TypeError):
                pen_str = str(r[9] or "$0")
            lines.append(f"  {r[0] or '?'} | {r[1] or '?'}, {r[2] or ''} {r[3] or ''} | {viol} | {pen_str} last penalty | {r[7] or 0} inspections")
        lines.append("")

    if atty_rows:
        lines.append(f"--- NYS ATTORNEY REGISTRATIONS ({len(atty_rows)} records) ---")
        for r in atty_rows:
            company = f" @ {r[4]}" if r[4] else ""
            lines.append(f"  {r[1] or ''}, {r[0] or ''} | Bar #{r[2] or '?'} | {r[3] or '?'} | {r[5] or '?'} | admitted {r[6] or '?'}{company}")
        lines.append("")

    if pp_rows:
        lines.append(f"--- ACRIS PERSONAL PROPERTY / UCC ({len(pp_rows)} filings) ---")
        for r in pp_rows:
            role = "DEBTOR" if str(r[1] or "").strip() == "1" else "SECURED" if str(r[1] or "").strip() == "2" else r[1] or "?"
            addr = f"{r[3] or ''}, {r[4] or ''} {r[5] or ''} {r[6] or ''}".strip(", ")
            lines.append(f"  {r[0] or '?'} ({role}) | Doc {r[2] or '?'} | {addr}")
        lines.append("")

    if civil_rows:
        lines.append(f"--- CIVIL SERVICE LISTS ({len(civil_rows)} entries) ---")
        for r in civil_rows:
            lines.append(f"  {r[1] or ''}, {r[0] or ''} | {r[2] or '?'} | Exam #{r[3] or '?'} List #{r[4] or '?'} | Score {r[5] or '?'} | {r[6] or '?'} | {r[7] or '?'}")
        lines.append("")

    if lobby_rows:
        lines.append(f"--- NYS LOBBYIST REGISTRATIONS ({len(lobby_rows)} records) ---")
        for r in lobby_rows:
            try:
                comp = f"${float(str(r[3] or 0).replace('$', '').replace(',', '') or 0):,.0f}" if r[3] else "?"
            except (ValueError, TypeError):
                comp = str(r[3] or "?")
            lines.append(f"  {r[0] or '?'} → {r[1] or '?'} | {r[2] or '?'} | {comp} | {r[4] or '?'} | {r[5] or ''}")
        lines.append("")

    if death_rows:
        lines.append(f"--- NYS DEATH INDEX ({len(death_rows)} records) ---")
        for r in death_rows:
            age_str = f"age {r[3]}" if r[3] else ""
            lines.append(f"  {r[0] or ''}, {r[1] or ''} | {r[2] or '?'} | {age_str} | {r[4] or ''} | res: {r[5] or '?'}")
        lines.append("")

    if broker_rows:
        lines.append(f"--- NYS REAL ESTATE BROKERS ({len(broker_rows)} licenses) ---")
        for r in broker_rows:
            biz = f" ({r[1]})" if r[1] else ""
            lines.append(f"  {r[0] or '?'}{biz} | {r[2] or '?'} #{r[3] or '?'} | exp {r[4] or '?'} | {r[5] or ''}")
        lines.append("")

    if notary_rows:
        lines.append(f"--- NYS NOTARIES ({len(notary_rows)} commissions) ---")
        for r in notary_rows:
            biz = f" ({r[5]})" if r[5] else ""
            lines.append(f"  {r[0] or '?'}{biz} | {r[1] or '?'} county | {r[2] or '?'} | {r[3] or '?'} — {r[4] or '?'}")
        lines.append("")

    # 16. Marriage records (1866-2017)
    marriage_cols, marriage_rows = [], []
    if _should_query("marriage_licenses_1950_2017"):
        try:
            if len(words) > 1:
                mar_where = """(UPPER(groom_surname) = ? AND UPPER(groom_first_name) LIKE ?)
                    OR (UPPER(bride_surname) = ? AND UPPER(bride_first_name) LIKE ?)
                    OR (UPPER(groom_surname) = ? AND UPPER(groom_first_name) LIKE ?)
                    OR (UPPER(bride_surname) = ? AND UPPER(bride_first_name) LIKE ?)"""
                mar_params = [
                    words[-1], words[0] + '%',
                    words[-1], words[0] + '%',
                    words[0], words[-1] + '%',
                    words[0], words[-1] + '%',
                ]
            else:
                mar_where = "UPPER(groom_surname) = ? OR UPPER(bride_surname) = ?"
                mar_params = [search, search]
            marriage_cols, marriage_rows = safe_query(pool, f"""
                SELECT groom_first_name, groom_middle_name, groom_surname,
                       bride_first_name, bride_middle_name, bride_surname,
                       LICENSE_BOROUGH_ID AS license_borough, LICENSE_YEAR AS license_year
                FROM lake.city_government.marriage_licenses_1950_2017
                WHERE {mar_where}
                ORDER BY license_year
                LIMIT 20
            """, mar_params)
        except Exception:
            pass

    # 16b. Marriage certificates 1866-1937
    hist_marriage_rows = []
    if _should_query("marriage_certificates_1866_1937"):
        try:
            if len(words) > 1:
                _, hist_marriage_rows = safe_query(pool, """
                    SELECT first_name, NULL, last_name, NULL, NULL, NULL, county, year
                    FROM lake.city_government.marriage_certificates_1866_1937
                    WHERE (UPPER(last_name) = ? AND UPPER(first_name) LIKE ?)
                       OR (UPPER(last_name) = ? AND UPPER(first_name) LIKE ?)
                    ORDER BY year
                    LIMIT 10
                """, [words[-1], words[0] + '%', words[0], words[-1] + '%'])
            else:
                _, hist_marriage_rows = safe_query(pool, """
                    SELECT first_name, NULL, last_name, NULL, NULL, NULL, county, year
                    FROM lake.city_government.marriage_certificates_1866_1937
                    WHERE UPPER(last_name) = ?
                    ORDER BY year
                    LIMIT 10
                """, [search])
        except Exception:
            pass

    all_marriage = list(marriage_rows) + hist_marriage_rows
    if all_marriage:
        lines.append(f"--- NYC MARRIAGE RECORDS ({len(all_marriage)} records, 1866-2017) ---")
        all_marriage.sort(key=lambda r: int(r[7] or 0) if r[7] else 0)
        for r in all_marriage:
            groom = f"{r[0] or ''} {r[1] or ''} {r[2] or ''}".strip()
            bride = f"{r[3] or ''} {r[4] or ''} {r[5] or ''}".strip()
            if bride:
                lines.append(f"  {r[7] or '?'} | {r[6] or '?'} | {groom}  ×  {bride}")
            else:
                lines.append(f"  {r[7] or '?'} | {r[6] or '?'} | {groom} (certificate)")
        lines.append("")

    # Splink cluster + entity_master
    splink_cluster_rows = []
    entity_info = {}
    if name_variants:
        try:
            variant_last = name_variants[0][0]
            variant_first = name_variants[0][1]
            _, cid_rows = safe_query(pool, """
                SELECT DISTINCT cluster_id
                FROM lake.federal.resolved_entities
                WHERE last_name = ? AND first_name = ?
                LIMIT 5
            """, [variant_last, variant_first])
            cluster_ids = [r[0] for r in cid_rows if r[0] is not None]

            if cluster_ids:
                # Look up entity_master for entity_id and confidence
                entity_info = _lookup_entity_master(pool, cluster_ids)

                placeholders = ", ".join(["?"] * len(cluster_ids))
                splink_cols, splink_cluster_rows = safe_query(pool, f"""
                    SELECT source_table, last_name, first_name,
                           city, zip, cluster_id
                    FROM lake.federal.resolved_entities
                    WHERE cluster_id IN ({placeholders})
                    ORDER BY source_table, last_name, first_name
                    LIMIT 50
                """, cluster_ids)
        except Exception:
            splink_cluster_rows = []

    if splink_cluster_rows:
        variant_names = sorted({f"{r[1] or ''} {r[2] or ''}".strip() for r in splink_cluster_rows})
        header = f"--- ENTITY RESOLUTION ({len(splink_cluster_rows)} records, {len(variant_names)} name variants)"
        if entity_info:
            first_entity = next(iter(entity_info.values()))
            header += f" | entity_id={first_entity['entity_id'][:8]}..."
            header += f" | type={first_entity['entity_type']}"
            header += f" | confidence={first_entity['confidence']:.2f}"
        header += " ---"
        lines.append(header)
        lines.append(f"  Name variants: {', '.join(variant_names[:10])}")
        current_table = None
        for r in splink_cluster_rows:
            table = r[0] or "unknown"
            if table != current_table:
                current_table = table
                lines.append(f"  [{table}]")
            loc = f"{r[3] or ''} {r[4] or ''}".strip() or "?"
            em = entity_info.get(r[5], {})
            eid_tag = f" | entity={em['entity_id'][:8]}..." if em.get("entity_id") else ""
            lines.append(f"    {r[1] or ''}, {r[2] or ''} | {loc}{eid_tag}")
        lines.append("")

    # Phonetic cross-reference
    phonetic_matches = []
    try:
        parts = name.strip().split()
        if len(parts) >= 2:
            phonetic_sql, phonetic_params = phonetic_search_sql(
                first_name=parts[0], last_name=parts[-1], min_score=0.7, limit=20
            )
        else:
            phonetic_sql, phonetic_params = phonetic_search_sql(
                first_name=None, last_name=name, min_score=0.7, limit=20
            )
        ph_cols, ph_rows = safe_query(pool, phonetic_sql, phonetic_params)
        phonetic_matches = [dict(zip(ph_cols, r)) for r in ph_rows]
    except Exception:
        pass

    if phonetic_matches:
        lines.append(f"--- PHONETIC MATCHES ({len(phonetic_matches)} cross-references) ---")
        for m in phonetic_matches[:10]:
            score = m.get("combined_score", 0)
            src = m.get("source_table", "?")
            lines.append(f"  {m.get('first_name', '')} {m.get('last_name', '')} | {src} | score: {score:.2f}")
        lines.append("")

    # Vector-matched name variants
    if extra_names:
        lines.append(f"\n{'='*45}")
        lines.append("SIMILAR NAMES (vector match):")
        lines.append("These names are similar and may be the same entity:")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")
        lines.append("Run entity(name) on any of these for their full X-ray.")

    lines.append(f"({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "name": name,
            "nys_corps": [dict(zip(corp_cols, r)) for r in corp_rows],
            "business_licenses": [dict(zip(biz_cols, r)) for r in biz_rows],
            "restaurants": [dict(zip(rest_cols, r)) for r in rest_rows],
            "campaign_contributions": [dict(zip(camp_cols, r)) for r in camp_rows],
            "campaign_expenditures": [dict(zip(expend_cols, r)) for r in expend_rows],
            "oath_hearings": [dict(zip(oath_cols, r)) for r in oath_rows],
            "acris_transactions": [dict(zip(acris_cols, r)) for r in acris_rows],
            "pluto_portfolio": [dict(zip(pluto_cols, r)) for r in pluto_rows],
            "dob_permits": [dict(zip(dob_cols, r)) for r in dob_rows],
            "dcwp_charges": [dict(zip(dcwp_cols, r)) for r in dcwp_rows],
            "sbs_certified": [dict(zip(sbs_cols, r)) for r in sbs_rows],
            "city_payroll": [dict(zip(payroll_cols, r)) for r in payroll_rows],
            "dob_application_owners": [dict(zip(dob_app_cols, r)) for r in dob_app_rows],
            "doing_business": [dict(zip(doing_biz_cols, r)) for r in doing_biz_rows],
            "epa_facilities": [dict(zip(epa_cols, r)) for r in epa_rows],
            "attorney_registrations": [dict(zip(atty_cols, r)) for r in atty_rows],
            "acris_personal_property": [dict(zip(pp_cols, r)) for r in pp_rows],
            "civil_service": [dict(zip(civil_cols, r)) for r in civil_rows],
            "lobbyist_registrations": [dict(zip(lobby_cols, r)) for r in lobby_rows],
            "death_index": [dict(zip(death_cols, r)) for r in death_rows],
            "re_brokers": [dict(zip(broker_cols, r)) for r in broker_rows],
            "notaries": [dict(zip(notary_cols, r)) for r in notary_rows],
            "marriages": [dict(zip(marriage_cols, r)) for r in marriage_rows]
                + [{"first_name": r[0], "last_name": r[2], "county": r[6], "year": r[7],
                    "source": "certificates_1866_1937"} for r in hist_marriage_rows],
            "splink_cluster": [{"source_table": r[0], "last_name": r[1], "first_name": r[2],
                                "city": r[3], "zip": r[4], "cluster_id": r[5],
                                **(entity_info.get(r[5], {}))}
                               for r in splink_cluster_rows],
            "entity_master": entity_info if entity_info else None,
            "name_variants": [{"last_name": ln, "first_name": fn} for ln, fn in name_variants],
            "phonetic_matches": phonetic_matches,
        },
        meta={"total_matches": total, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Role: background — due_diligence
# ---------------------------------------------------------------------------

def _due_diligence(name: str, pool, ctx) -> ToolResult:
    """Professional and financial background check."""
    t0 = time.time()

    extra_names = vector_expand_names(ctx, name.strip().upper())

    parts = name.split()
    if len(parts) >= 2:
        first_guess = parts[0]
        last_guess = parts[-1]
    else:
        first_guess = name
        last_guess = name

    dd_results = parallel_queries(pool, [
        ("atty", DUE_DILIGENCE_ATTORNEY_SQL,
         [f"{last_guess}%", f"{first_guess}%", f"{'%' + first_guess + '%'}"]),
        ("broker", DUE_DILIGENCE_BROKER_SQL, [name]),
        ("tax", DUE_DILIGENCE_TAX_WARRANT_SQL, [name, name]),
        ("cs", DUE_DILIGENCE_CHILD_SUPPORT_SQL, [name]),
        ("contractor", DUE_DILIGENCE_CONTRACTOR_SQL, [name, name]),
        ("debarred", DUE_DILIGENCE_DEBARRED_SQL, [name]),
        ("ethics", DUE_DILIGENCE_ETHICS_SQL, [name]),
    ])
    _, atty_rows = dd_results.get("atty", ([], []))
    _, broker_rows = dd_results.get("broker", ([], []))
    _, tax_rows = dd_results.get("tax", ([], []))
    _, cs_rows = dd_results.get("cs", ([], []))
    _, contractor_rows = dd_results.get("contractor", ([], []))
    _, debarred_rows = dd_results.get("debarred", ([], []))
    _, ethics_rows = dd_results.get("ethics", ([], []))

    # Phonetic fallback for professional databases
    phonetic_pro = []
    try:
        ph_sql, ph_params = phonetic_search_sql(
            first_name=first_guess if len(parts) >= 2 else None,
            last_name=last_guess, min_score=0.8, limit=10)
        ph_cols, ph_rows = safe_query(pool, ph_sql, ph_params)
        if ph_rows and not atty_rows and not broker_rows:
            phonetic_pro = [dict(zip(ph_cols, r)) for r in ph_rows]
            phonetic_pro = [m for m in phonetic_pro if any(s in m.get("source_table", "")
                for s in ["attorney", "broker", "notary", "tax_warrant", "child_support"])]
    except Exception:
        pass

    sections_found = sum(1 for r in [atty_rows, broker_rows, tax_rows, cs_rows,
                                      contractor_rows, debarred_rows, ethics_rows] if r)
    if phonetic_pro:
        sections_found += 1

    if sections_found == 0:
        raise ToolError(f"No records found for '{name}' in any professional/financial database. Try a different name spelling.")

    lines = [f"DUE DILIGENCE — {name}", "=" * 55]

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

    if broker_rows:
        lines.append(f"\nREAL ESTATE LICENSES ({len(broker_rows)} matches):")
        for r in broker_rows:
            holder, lic_type, lic_num, exp, biz, bcity, bstate = r
            lines.append(f"  {holder} — {lic_type or '?'} (#{lic_num})")
            lines.append(f"    Expires: {exp or '?'} | {biz or ''}, {bcity or ''} {bstate or ''}")

    if tax_rows:
        lines.append(f"\n⚠️  TAX WARRANTS ({len(tax_rows)} matches):")
        for r in tax_rows:
            dn1, dn2, city, state, amt, filed, status, satisfied, expires = r
            amt_str = f"${float(amt):,.2f}" if amt else "?"
            lines.append(f"  {dn1} — {amt_str} ({status or '?'})")
            lines.append(f"    Filed: {filed or '?'} | {city or ''}, {state or ''}")
            if satisfied:
                lines.append(f"    Satisfied: {satisfied}")

    if cs_rows:
        lines.append(f"\n⚠️  CHILD SUPPORT WARRANTS ({len(cs_rows)} matches):")
        for r in cs_rows:
            dn, city, state, amt, filed, status, satisfied, expires = r
            amt_str = f"${float(amt):,.2f}" if amt else "?"
            lines.append(f"  {dn} — {amt_str} (Status: {status or '?'})")
            lines.append(f"    Filed: {filed or '?'} | {city or ''}, {state or ''}")

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

    if debarred_rows:
        lines.append(f"\n⚠️  DEBARRED / NON-RESPONSIBLE ({len(debarred_rows)} matches):")
        for r in debarred_rows:
            contractor, agency, date = r
            lines.append(f"  {contractor} — by {agency or '?'} ({date or '?'})")

    if ethics_rows:
        lines.append(f"\n⚠️  ETHICS VIOLATIONS ({len(ethics_rows)} matches):")
        for r in ethics_rows:
            case, num, agency, date, fine, penalty, suspension = r
            lines.append(f"  {case} — {agency or '?'} ({date or '?'})")
            parts_list = []
            if fine: parts_list.append(f"Fine: ${fine}")
            if penalty: parts_list.append(f"Penalty: {penalty}")
            if suspension: parts_list.append(f"Suspension: {suspension} days")
            if parts_list:
                lines.append(f"    {', '.join(parts_list)}")

    if phonetic_pro:
        lines.append(f"\nPHONETIC MATCHES ({len(phonetic_pro)} — name variants in professional databases):")
        for m in phonetic_pro[:5]:
            lines.append(f"  {m.get('first_name', '')} {m.get('last_name', '')} in {m.get('source_table', '')} (score: {m.get('combined_score', 0):.2f})")

    lines.append(f"\n{sections_found} of 7 databases returned results.")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")

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


# ---------------------------------------------------------------------------
# Role: cop — cop_sheet
# ---------------------------------------------------------------------------

def _cop_sheet(name: str, pool, ctx) -> ToolResult:
    """NYPD officer accountability dossier."""
    t0 = time.time()

    extra_names = vector_expand_names(ctx, name.strip().upper())

    parts = name.split()
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
    else:
        first = name
        last = name

    cop_results = parallel_queries(pool, [
        ("summary", COP_SHEET_SUMMARY_SQL, [f"{last}%", f"{first}%"]),
        ("complaints", COP_SHEET_COMPLAINTS_SQL, [f"{last}%", f"{first}%"]),
        ("settlements", COP_SHEET_SETTLEMENTS_SQL, [last, last]),
        ("sdny", COP_SHEET_FEDERAL_SDNY_SQL, [last]),
        ("edny", COP_SHEET_FEDERAL_EDNY_SQL, [last]),
    ])
    _, summary_rows = cop_results.get("summary", ([], []))
    _, complaint_rows = cop_results.get("complaints", ([], []))
    _, settlement_rows = cop_results.get("settlements", ([], []))
    _, sdny_rows = cop_results.get("sdny", ([], []))
    _, edny_rows = cop_results.get("edny", ([], []))

    if not summary_rows and not complaint_rows:
        raise ToolError(f"No CCRB records found for '{name}'. Try last name only, or check spelling.")

    lines = [f"COP SHEET — {name}", "=" * 55]

    if summary_rows:
        for s in summary_rows:
            fname, lname, allegations, substantiated, force, subst_force, incidents, status, gender, race = s
            lines.append(f"\n{fname} {lname} — {status or 'Unknown status'}")
            lines.append(f"  {gender or '?'} | {race or '?'}")
            lines.append(f"  Total allegations: {allegations or 0}")
            lines.append(f"  Substantiated: {substantiated or 0}")
            lines.append(f"  Force allegations: {force or 0} (substantiated: {subst_force or 0})")
            lines.append(f"  Distinct incidents: {incidents or 0}")

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

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")

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


# ---------------------------------------------------------------------------
# Role: judge — judge_profile
# ---------------------------------------------------------------------------

def _judge_profile(name: str, pool, ctx) -> ToolResult:
    """Federal judge dossier."""
    t0 = time.time()

    parts = name.split()
    first = parts[0] if len(parts) >= 2 else "%"
    last = parts[-1] if len(parts) >= 2 else name

    _, bio_rows = safe_query(pool, JUDGE_BIO_SQL, [f"{last}%", f"{first}%"])

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

        _, pos_rows = safe_query(pool, JUDGE_POSITIONS_SQL, [judge_id])
        if pos_rows:
            lines.append(f"\n  CAREER:")
            for p in pos_rows:
                pos_type, court, start, retire, term, appointer, how, nom = p
                status = "retired" if retire else ("terminated" if term else "active")
                lines.append(f"    {pos_type or '?'} — {court or '?'}")
                lines.append(f"      {start or '?'} – {retire or term or 'present'} ({status})")
                if appointer:
                    lines.append(f"      Appointed by: {appointer}")

        _, edu_rows = safe_query(pool, JUDGE_EDUCATION_SQL, [judge_id])
        if edu_rows:
            lines.append(f"\n  EDUCATION:")
            for e in edu_rows:
                school, degree, year = e
                lines.append(f"    {school or '?'} — {degree or '?'} ({year or '?'})")

        _, disc_rows = safe_query(pool, JUDGE_DISCLOSURES_SQL, [judge_id])
        if disc_rows:
            lines.append(f"\n  FINANCIAL DISCLOSURES ({len(disc_rows)} filings):")
            for d in disc_rows[:5]:
                lines.append(f"    {d[0] or '?'}: {d[1] or 'Annual'}")

        _, inv_rows = safe_query(pool, JUDGE_INVESTMENTS_SQL, [judge_id])
        if inv_rows:
            lines.append(f"\n  TOP INVESTMENTS ({len(inv_rows)} holdings):")
            for i in inv_rows[:10]:
                desc = ' / '.join(str(d) for d in [i[0], i[1], i[2]] if d)
                value = i[3] or '?'
                lines.append(f"    {desc[:80]} (value code: {value})")

        _, gift_rows = safe_query(pool, JUDGE_GIFTS_SQL, [judge_id])
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


# ---------------------------------------------------------------------------
# Role: vitals — vital_records + marriage_search
# ---------------------------------------------------------------------------

def _vitals(name: str, pool, ctx) -> ToolResult:
    """Search historical NYC vital records + marriage records."""
    t0 = time.time()

    parts = name.split()
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
    else:
        first = "%"
        last = name

    # --- Vital records ---
    _, death_rows = safe_query(pool, VITAL_DEATHS_SQL, [f"{last}%", f"{first}%"])
    _, marriage_early = safe_query(pool, VITAL_MARRIAGES_EARLY_SQL, [f"{last}%"])
    _, marriage_modern = safe_query(pool, VITAL_MARRIAGES_MODERN_SQL, [f"{last}%", f"{last}%"])
    _, birth_rows = safe_query(pool, VITAL_BIRTHS_SQL, [f"{last}%", f"{first}%"])

    # Phonetic enhancement for historical records
    phonetic_deaths = []
    try:
        from entity import phonetic_vital_search_sql
        ph_sql, ph_params = phonetic_vital_search_sql(
            first_name=first if first != "%" else None, last_name=last,
            table="lake.federal.nys_death_index",
            first_col="first_name", last_col="last_name",
            extra_cols="age, date_of_death", limit=20)
        _, phonetic_deaths = execute(pool, ph_sql, ph_params)
    except Exception:
        pass

    all_marriages = (marriage_early or []) + (marriage_modern or [])

    # --- Marriage search (detailed) ---
    search_last = last.strip().upper()
    search_first = first.strip().upper() if first != "%" else ""

    extra_names = vector_expand_names(ctx, f"{search_first} {search_last}".strip())

    phonetic_results = []
    try:
        from entity import phonetic_vital_search_sql
        ph_sql, ph_params = phonetic_vital_search_sql(
            first_name=search_first or None, last_name=search_last,
            table="lake.city_government.marriage_licenses_1950_2017",
            first_col="groom_first_name", last_col="groom_surname",
            extra_cols="bride_first_name, bride_surname, LICENSE_BOROUGH_ID, LICENSE_YEAR",
            limit=30)
        _, ph_rows = safe_query(pool, ph_sql, ph_params)
        if ph_rows:
            phonetic_results = list(ph_rows)
    except Exception:
        pass

    # Marriage index (1950-2017)
    if search_first:
        mar_cols, mar_rows = safe_query(pool, """
            SELECT groom_first_name, groom_middle_name, groom_surname,
                   bride_first_name, bride_middle_name, bride_surname,
                   LICENSE_BOROUGH_ID AS license_borough, LICENSE_YEAR AS license_year
            FROM lake.city_government.marriage_licenses_1950_2017
            WHERE (UPPER(groom_surname) = ? AND UPPER(groom_first_name) LIKE ?)
               OR (UPPER(bride_surname) = ? AND UPPER(bride_first_name) LIKE ?)
            ORDER BY license_year
            LIMIT 50
        """, [search_last, f"{search_first}%", search_last, f"{search_first}%"])
    else:
        mar_cols, mar_rows = safe_query(pool, """
            SELECT groom_first_name, groom_middle_name, groom_surname,
                   bride_first_name, bride_middle_name, bride_surname,
                   LICENSE_BOROUGH_ID AS license_borough, LICENSE_YEAR AS license_year
            FROM lake.city_government.marriage_licenses_1950_2017
            WHERE UPPER(groom_surname) = ? OR UPPER(bride_surname) = ?
            ORDER BY license_year
            LIMIT 50
        """, [search_last, search_last])

    # Historical certificates (1866-1937)
    hist_rows = []
    try:
        if search_first:
            _, hist_rows = safe_query(pool, """
                SELECT first_name, NULL AS middle_name, last_name,
                       NULL AS spouse_first, NULL AS spouse_middle, NULL AS spouse_last,
                       county, year
                FROM lake.city_government.marriage_certificates_1866_1937
                WHERE UPPER(last_name) = ? AND UPPER(first_name) LIKE ?
                ORDER BY year
                LIMIT 30
            """, [search_last, f"{search_first}%"])
        else:
            _, hist_rows = safe_query(pool, """
                SELECT first_name, NULL AS middle_name, last_name,
                       NULL AS spouse_first, NULL AS spouse_middle, NULL AS spouse_last,
                       county, year
                FROM lake.city_government.marriage_certificates_1866_1937
                WHERE UPPER(last_name) = ?
                ORDER BY year
                LIMIT 30
            """, [search_last])
    except Exception:
        pass

    all_mar_rows = list(mar_rows) + hist_rows
    all_mar_rows.sort(key=lambda r: int(r[7] or 0) if r[7] else 0)

    total_vital = len(death_rows or []) + len(all_marriages) + len(birth_rows or [])
    total = total_vital + len(all_mar_rows)

    if total == 0:
        raise ToolError(f"No vital or marriage records found for '{name}'. Death records: 1862-1948, marriages: 1866-2017, births: 1855-1909.")

    lines = [f"VITAL RECORDS & MARRIAGES — {name}", "=" * 55]

    # Vital records section
    if death_rows:
        lines.append(f"\nDEATH CERTIFICATES ({len(death_rows)} matches, 1862-1948):")
        for r in death_rows:
            fname, lname, age, year, month, day, county = r
            date_str = f"{year or '?'}-{month or '?'}-{day or '?'}"
            lines.append(f"  {fname or '?'} {lname or '?'} — died {date_str}, age {age or '?'}, {county or '?'}")

    if all_marriages:
        lines.append(f"\nMARRIAGE RECORDS ({len(all_marriages)} matches, 1866-2017):")
        for r in all_marriages[:15]:
            lines.append(f"  {' | '.join(str(v) for v in r[:6] if v)}")

    if birth_rows:
        lines.append(f"\nBIRTH CERTIFICATES ({len(birth_rows)} matches, 1855-1909):")
        for r in birth_rows:
            fname, lname, year, county = r
            lines.append(f"  {fname or '?'} {lname or '?'} — born {year or '?'}, {county or '?'}")

    if phonetic_deaths:
        lines.append(f"\nPHONETIC DEATH MATCHES ({len(phonetic_deaths)} — spelling variants):")
        for r in phonetic_deaths[:10]:
            lines.append(f"  {r[0] or '?'} {r[1] or '?'} — score: {r[-2]:.2f}/{r[-1]:.2f}")

    # Detailed marriage search section
    if all_mar_rows:
        lines.append(f"\nDETAILED MARRIAGE SEARCH ({len(all_mar_rows)} matches, 1866-2017):")
        if hist_rows:
            lines.append(f"  [{len(hist_rows)} from certificates 1866-1937, {len(mar_rows)} from index 1950-2017]")
        for r in all_mar_rows:
            groom = f"{r[0] or ''} {r[1] or ''} {r[2] or ''}".strip()
            bride = f"{r[3] or ''} {r[4] or ''} {r[5] or ''}".strip()
            if bride:
                lines.append(f"  {r[7] or '?'} | {r[6] or '?'} | {groom}  ×  {bride}")
            else:
                lines.append(f"  {r[7] or '?'} | {r[6] or '?'} | {groom} (certificate)")

    if phonetic_results:
        lines.append(f"\nPHONETIC MARRIAGE MATCHES ({len(phonetic_results)} — spelling variants):")
        for r in phonetic_results[:10]:
            lines.append(f"  {' | '.join(str(v) for v in r[:6] if v)}")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "name": name,
            "deaths": len(death_rows or []),
            "marriages_vital": len(all_marriages),
            "births": len(birth_rows or []),
            "marriages_detailed": len(all_mar_rows),
        },
        meta={"name": name, "total_records": total, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Role: top — top_crossrefs
# ---------------------------------------------------------------------------

def _top_crossrefs(name: str, pool, ctx) -> ToolResult:
    """Find the most cross-referenced people in the NYC data lake."""
    t0 = time.time()

    # Parse name as optional last_name_prefix filter
    prefix = name.strip().upper() if name.strip() else ""
    min_tables = 5
    limit = 50

    try:
        # Try entity_master first (fast, pre-aggregated)
        use_em = _has_entity_master(pool)
        rows = []

        if use_em:
            if prefix:
                cols, rows = execute(pool, """
                    SELECT entity_id, canonical_last, canonical_first, entity_type,
                           confidence, source_count, record_count, source_tables
                    FROM lake.foundation.entity_master
                    WHERE canonical_last LIKE ? || '%'
                      AND source_count >= ?
                    ORDER BY source_count DESC, record_count DESC
                    LIMIT ?
                """, [prefix, min_tables, limit])
            else:
                cols, rows = execute(pool, """
                    SELECT entity_id, canonical_last, canonical_first, entity_type,
                           confidence, source_count, record_count, source_tables
                    FROM lake.foundation.entity_master
                    WHERE source_count >= ?
                    ORDER BY source_count DESC, record_count DESC
                    LIMIT ?
                """, [min_tables, limit])

        # Fallback to resolved_entities if entity_master unavailable or empty
        if not rows:
            if prefix:
                cols, rows = safe_query(pool, """
                    SELECT NULL as entity_id,
                           MIN(last_name) as last_name,
                           MIN(first_name) as first_name,
                           NULL as entity_type,
                           NULL as confidence,
                           COUNT(DISTINCT source_table) as source_count,
                           COUNT(*) as record_count,
                           ARRAY_AGG(DISTINCT source_table ORDER BY source_table) as tables
                    FROM lake.federal.resolved_entities
                    WHERE last_name IS NOT NULL
                      AND last_name LIKE ? || '%'
                    GROUP BY cluster_id
                    HAVING COUNT(DISTINCT source_table) >= ?
                    ORDER BY source_count DESC, record_count DESC
                    LIMIT ?
                """, [prefix, min_tables, limit])
            else:
                cols, rows = safe_query(pool, """
                    SELECT NULL as entity_id,
                           MIN(last_name) as last_name,
                           MIN(first_name) as first_name,
                           NULL as entity_type,
                           NULL as confidence,
                           COUNT(DISTINCT source_table) as source_count,
                           COUNT(*) as record_count,
                           ARRAY_AGG(DISTINCT source_table ORDER BY source_table) as tables
                    FROM lake.federal.resolved_entities
                    WHERE last_name IS NOT NULL
                    GROUP BY cluster_id
                    HAVING COUNT(DISTINCT source_table) >= ?
                    ORDER BY source_count DESC, record_count DESC
                    LIMIT ?
                """, [min_tables, limit])

        elapsed = int((time.time() - t0) * 1000)

        if not rows:
            msg = f"No entities found with {min_tables}+ source tables"
            if prefix:
                msg += f" and last name starting with '{prefix}'"
            return ToolResult(content=msg + ".")

        # Unified output — both paths produce same column order
        lines = [f"# Top Cross-Referenced Entities ({min_tables}+ tables)"]
        if prefix:
            lines[0] += f" — last name '{prefix}*'"
        lines.append("")

        has_types = any(r[3] is not None for r in rows)
        if has_types:
            lines.append("| Last Name | First Name | Type | Confidence | Sources | Records | Source Tables |")
            lines.append("|---|---|---|---|---|---|---|")
        else:
            lines.append("| Last Name | First Name | Sources | Records | Source Tables |")
            lines.append("|---|---|---|---|---|")

        for r in rows:
            tables_list = r[7] if r[7] else []
            tables_str = ", ".join(tables_list[:8])
            if len(tables_list) > 8:
                tables_str += f" (+{len(tables_list) - 8})"
            if has_types:
                etype = r[3] or "?"
                conf = f"{r[4]:.2f}" if r[4] is not None else "?"
                lines.append(f"| {r[1]} | {r[2]} | {etype} | {conf} | {r[5]} | {r[6]} | {tables_str} |")
            else:
                lines.append(f"| {r[1]} | {r[2]} | {r[5]} | {r[6]} | {tables_str} |")

        lines.append(f"\nTotal: {len(rows)} entities shown")
        lines.append(f"({elapsed}ms)")

        return ToolResult(
            content="\n".join(lines),
            structured_content={
                "people": [
                    {"entity_id": str(r[0]) if r[0] else None,
                     "last_name": r[1], "first_name": r[2],
                     "entity_type": r[3], "confidence": r[4],
                     "source_count": r[5], "record_count": r[6],
                     "tables": r[7] if r[7] else []}
                    for r in rows
                ],
            },
            meta={"result_count": len(rows), "min_tables": min_tables,
                  "source": "entity_master" if use_em else "resolved_entities",
                  "query_time_ms": elapsed},
        )

    except Exception as e:
        elapsed = int((time.time() - t0) * 1000)
        print(f"_top_crossrefs failed: {e}", flush=True)
        return ToolResult(
            content=f"Top cross-refs lookup failed: {e}",
        )


# ---------------------------------------------------------------------------
# Main entry point — role dispatch
# ---------------------------------------------------------------------------

def entity(
    name: Annotated[str, Field(
        description="Person or company name, fuzzy matched. Partial names and misspellings OK, e.g. 'Steven Croman', 'BLACKSTONE', 'J Smith'",
        examples=["Steven Croman", "Barton Perlbinder", "BLACKSTONE GROUP", "Jane Smith"],
    )],
    role: Annotated[
        Literal["auto", "background", "cop", "judge", "vitals", "top"],
        Field(
            default="auto",
            description="'auto' returns full X-ray across all datasets. 'background' returns professional and financial records. 'cop' returns CCRB complaints, penalties, and federal lawsuits. 'judge' returns career history and financial disclosures. 'vitals' returns death, marriage, and birth records 1855-2017. 'top' returns most cross-referenced people in the lake.",
        )
    ] = "auto",
    ctx: Context = None,
) -> ToolResult:
    """Look up any person or company across all NYC datasets. Lance vector index resolves identity across 44 tables. Returns every dataset hit, corporate ties, property records, and donations. Use for any question about a specific person, company, landlord, or organization. Do NOT use for building lookups by address (use building) or relationship traversal (use network). Default returns the full cross-reference across all datasets."""
    name = name.strip()
    if len(name) < 3:
        raise ToolError("Name must be at least 3 characters.")

    pool = ctx.lifespan_context["pool"]

    dispatch = {
        "auto": _entity_xray,
        "background": _due_diligence,
        "cop": _cop_sheet,
        "judge": _judge_profile,
        "vitals": _vitals,
        "top": _top_crossrefs,
    }

    handler = dispatch[role]
    return handler(name, pool, ctx)
