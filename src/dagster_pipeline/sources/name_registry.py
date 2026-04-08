"""Auto-discovered registry of every person-name column in the NYC DuckLake.

Built by querying information_schema across all non-staging schemas, then
classifying each table's name columns by extraction pattern.  Excludes
non-person names (school_name, park_name, agency_name, street_name, etc.),
_staging schemas, _dlt_* system tables, and v_* view duplicates.

Discovery date: 2026-03-17
Tables discovered: 47 (1 excluded: city_government.nys_campaign_finance empty)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class NamePattern(str, Enum):
    """How person names are stored in a table."""

    STRUCTURED = "STRUCTURED"  # separate first_name, last_name columns
    COMBINED_COMMA = "COMBINED_COMMA"  # "LASTNAME, FIRSTNAME" in one column
    COMBINED_SPACE = "COMBINED_SPACE"  # "FIRSTNAME LASTNAME" in one column
    OWNER = "OWNER"  # property owner field (may contain LLC names)
    RESPONDENT = "RESPONDENT"  # free-text respondent (litigation, OATH)
    MULTI = "MULTI"  # multiple name-bearing column sets in one table


@dataclass(frozen=True)
class NameSource:
    schema: str
    table: str
    pattern: NamePattern
    name_columns: tuple[tuple[str, str | None], ...]  # ((last_col, first_col), ...)
    address_columns: dict[str, str] = field(default_factory=dict)
    notes: str = ""
    last_name_first: bool = False  # COMBINED_SPACE: first token is last name
    needs_llc_filter: bool = False  # COMBINED_SPACE: apply LLC_FILTER_TERMS exclusion


# LLC / corporate entity filter — applied to OWNER and RESPONDENT patterns
LLC_FILTER_TERMS = (
    "LLC", "CORP", "INC", "TRUST", "BANK", "CITY OF", "NYC ",
    "L.L.C", "L.P.", "ASSOCIATES", "HOLDINGS", "REALTY",
    "HOUSING DEV", "MANAGEMENT", "PROPERTIES",
)


def _llc_where(col: str) -> str:
    """Generate WHERE clause fragment to exclude corporate entities."""
    clauses = [f"{col} NOT LIKE '%{term}%'" for term in LLC_FILTER_TERMS]
    return " AND ".join(clauses)


# ──────────────────────────────────────────────────────────────────────
#  THE REGISTRY — 47 tables, auto-discovered 2026-03-17
# ──────────────────────────────────────────────────────────────────────

NAME_REGISTRY: list[NameSource] = [
    # ── STRUCTURED: separate first_name / last_name ──────────────────

    NameSource(
        schema="business", table="sbs_certified",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
        address_columns={"city": "city", "zip": "zip"},
    ),
    NameSource(
        schema="city_government", table="citywide_payroll",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
        address_columns={"city": "work_location_borough"},
    ),
    NameSource(
        schema="city_government", table="civil_service_active",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
    ),
    NameSource(
        schema="city_government", table="oath_hearings",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("respondent_last_name", "respondent_first_name"),),
        address_columns={
            "address": "respondent_address_house",
            "city": "respondent_address_city",
            "zip": "respondent_address_zip_code",
        },
    ),
    NameSource(
        schema="city_government", table="coib_policymakers",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("name_of_employee", None),),
    ),
    NameSource(
        schema="city_government", table="coib_legal_defense_trust",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
        address_columns={"city": "city", "zip": "zip_code"},
    ),
    NameSource(
        schema="city_government", table="birth_certificates_1855_1909",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
    ),
    NameSource(
        schema="city_government", table="death_certificates_1862_1948",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
    ),
    NameSource(
        schema="city_government", table="marriage_certificates_1866_1937",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
    ),
    # city_government.nys_campaign_finance — EXCLUDED: table is empty (0 rows)
    # The federal.nys_campaign_finance copy has the actual data (7M rows).
    NameSource(
        schema="federal", table="nypd_ccrb_officers_current",
        pattern=NamePattern.STRUCTURED,
        # Source uses literal "Last Name" / "First Name" with capital + space.
        # The SQL builder must quote these — see get_extraction_sql.
        name_columns=(('"Last Name"', '"First Name"'),),
    ),
    NameSource(
        schema="federal", table="nypd_ccrb_complaints",
        pattern=NamePattern.STRUCTURED,
        # Source uses camelCase LastName/FirstName, not snake_case.
        name_columns=(("LastName", "FirstName"),),
    ),
    NameSource(
        schema="federal", table="nys_death_index",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
    ),
    # federal.nys_campaign_finance — EXCLUDED: table does not exist in lake.
    # Was registered for the future federal copy with flng_ent_last_name /
    # flng_ent_first_name columns. Re-add when the source is ingested.
    NameSource(
        schema="financial", table="nys_attorney_registrations",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
        address_columns={"address": "street_1", "city": "city", "zip": "zip"},
    ),
    NameSource(
        schema="financial", table="nys_elevator_licenses",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("last_name", "first_name"),),
    ),
    NameSource(
        schema="housing", table="hpd_registration_contacts",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("lastname", "firstname"),),
        address_columns={
            "address": "businesshousenumber",
            "city": "businesscity",
            "zip": "businesszip",
        },
    ),
    NameSource(
        schema="housing", table="evictions",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("marshal_last_name", "marshal_first_name"),),
        address_columns={"city": "borough"},
    ),
    NameSource(
        schema="housing", table="dob_safety_boiler",
        pattern=NamePattern.STRUCTURED,
        name_columns=(
            ("owner_last_name", "owner_first_name"),
            ("applicant_last_name", "applicantfirst_name"),
        ),
        notes="Two name sets: owner + applicant",
    ),
    NameSource(
        schema="public_safety", table="ccrb_officers",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("officer_last_name", "officer_first_name"),),
    ),

    # ── COMBINED_COMMA: "LASTNAME, FIRSTNAME" ────────────────────────

    NameSource(
        schema="city_government", table="campaign_contributions",
        pattern=NamePattern.COMBINED_COMMA,
        name_columns=(("name", None),),
        address_columns={"city": "city", "zip": "zip"},
        notes="Format: 'Lastname, Firstname'",
    ),
    NameSource(
        schema="city_government", table="campaign_expenditures",
        pattern=NamePattern.COMBINED_COMMA,
        name_columns=(("name", None),),
        address_columns={"city": "city", "zip": "zip"},
    ),
    NameSource(
        schema="city_government", table="cfb_intermediaries",
        pattern=NamePattern.COMBINED_COMMA,
        name_columns=(("name", None),),
        address_columns={"city": "city", "zip": "zip"},
    ),
    NameSource(
        schema="city_government", table="cfb_offyear_contributions",
        pattern=NamePattern.COMBINED_COMMA,
        name_columns=(("name", None),),
        address_columns={"city": "city", "zip": "zip"},
    ),
    NameSource(
        schema="federal", table="fec_contributions",
        pattern=NamePattern.COMBINED_COMMA,
        name_columns=(("name", None),),
        address_columns={"city": "city", "zip": "zip_code"},
        notes="Format: 'LASTNAME, FIRSTNAME'",
    ),
    NameSource(
        schema="public_safety", table="nypd_officer_profile",
        pattern=NamePattern.COMBINED_COMMA,
        name_columns=(("name", None),),
        notes="Format: 'LASTNAME, FIRSTNAME MI'",
    ),

    # ── COMBINED_SPACE: "FIRSTNAME LASTNAME" ─────────────────────────

    NameSource(
        schema="city_government", table="civil_list",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("name", None),),
        notes="Format: 'F M LASTNAME' (initials + last name)",
    ),
    NameSource(
        schema="city_government", table="cfb_enforcement_audits",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("name", None),),
        notes="Format: 'Firstname Lastname' (elected officials)",
    ),
    NameSource(
        schema="city_government", table="coib_enforcement",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("case_name", None),),
        notes="Last name only (single word)",
    ),
    NameSource(
        schema="financial", table="nys_child_support_warrants",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("debtor_name", None),),
        address_columns={"city": "city", "zip": "zip_code"},
        notes="Format: 'FIRSTNAME MI LASTNAME'",
    ),
    NameSource(
        schema="financial", table="nys_cosmetology_licenses",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("license_holder_name", None),),
        notes="Format: 'LASTNAME FIRSTNAME MI' (inconsistent)",
        last_name_first=True,
    ),
    NameSource(
        schema="financial", table="nys_re_appraisers",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("applicant_name", None),),
        address_columns={"city": "business_city", "zip": "zip_code"},
        notes="Format: 'LASTNAME FIRSTNAME MI'",
        last_name_first=True,
    ),
    NameSource(
        schema="financial", table="nys_re_brokers",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("license_holder_name", None),),
        address_columns={"city": "business_city"},
        notes="Mixed person/business names — needs LLC filter",
        last_name_first=True,
        needs_llc_filter=True,
    ),
    NameSource(
        schema="financial", table="nys_notaries",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("commission_holder_name", None),),
        notes="Format: 'LASTNAME, FIRSTNAME' but sometimes space-separated",
    ),
    NameSource(
        schema="housing", table="dob_permit_issuance",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("superintendent_first___last_name", None),),
        notes="Superintendent name only — 'FIRSTNAME LASTNAME' format",
    ),
    NameSource(
        schema="business", table="nys_daily_corp_filings",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("filer_name", None),),
        notes="Person who filed — 'FIRSTNAME LASTNAME' format",
    ),
    NameSource(
        schema="city_government", table="city_record",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("contact_name", None),),
        # No address columns — table has no city/zip fields, only the
        # contact_name plus agency/notice metadata.
        notes="Contact person — 'Firstname Lastname' format",
    ),
    NameSource(
        schema="social_services", table="nys_child_care",
        pattern=NamePattern.COMBINED_SPACE,
        name_columns=(("provider_name", None),),
        address_columns={"city": "city", "zip": "zip_code"},
        notes="Person names for home providers, org names for centers — needs LLC filter",
        needs_llc_filter=True,
    ),

    # ── OWNER: property owner fields (need LLC filtering) ────────────

    NameSource(
        schema="city_government", table="pluto",
        pattern=NamePattern.OWNER,
        name_columns=(("ownername", None),),
        notes="Mixed person/LLC — comma-separated 'LASTNAME, FIRST' or org name",
    ),
    NameSource(
        schema="housing", table="acris_parties",
        pattern=NamePattern.OWNER,
        name_columns=(("name", None),),
        address_columns={"address": "address_1", "city": "city", "zip": "zip"},
        notes="ACRIS real property parties — mixed person/corporate",
    ),
    NameSource(
        schema="business", table="acris_pp_parties",
        pattern=NamePattern.OWNER,
        name_columns=(("name", None),),
        notes="ACRIS personal property parties — mixed person/corporate",
    ),
    NameSource(
        schema="housing", table="dob_safety_facades",
        pattern=NamePattern.OWNER,
        name_columns=(("owner_name", None), ("qewi_name", None)),
        address_columns={"city": "borough"},
        notes="owner_name = property owner; qewi_name = qualified inspector (person)",
    ),

    # ── RESPONDENT: free-text respondent fields ──────────────────────

    NameSource(
        schema="housing", table="dob_ecb_violations",
        pattern=NamePattern.RESPONDENT,
        name_columns=(("respondent_name", None),),
        address_columns={
            "address": "respondent_house_number",
            "city": "respondent_city",
            "zip": "respondent_zip",
        },
        notes="Mixed person/business — needs LLC filter",
    ),
    NameSource(
        schema="housing", table="hpd_litigations",
        pattern=NamePattern.RESPONDENT,
        name_columns=(("respondent", None),),
        address_columns={"zip": "zip"},
        notes="Free text — person names, building names, LLCs mixed",
    ),

    # ── MULTI: tables with multiple distinct name-bearing column sets ─

    NameSource(
        schema="housing", table="dob_permit_issuance",
        pattern=NamePattern.MULTI,
        name_columns=(
            ("owner_s_last_name", "owner_s_first_name"),
            ("permittee_s_last_name", "permittee_s_first_name"),
            ("site_safety_mgr_s_last_name", "site_safety_mgr_s_first_name"),
        ),
        address_columns={"city": "borough", "zip": "zip_code"},
        notes="Owner + permittee + site safety manager (all structured)",
    ),
    NameSource(
        schema="housing", table="dob_now_build_filings",
        pattern=NamePattern.MULTI,
        name_columns=(
            ("applicant_last_name", "applicant_first_name"),
            ("filing_representative_last_name", "filing_representative_first_name"),
        ),
        address_columns={"city": "city", "zip": "zip"},
        notes="Applicant + filing representative (both structured)",
    ),
    NameSource(
        schema="housing", table="dob_application_owners",
        pattern=NamePattern.STRUCTURED,
        name_columns=(("owner_s_last_name", "owner_s_first_name"),),
        # Table has no zip column — only first/last name + business name + phone + job description.
    ),
    NameSource(
        schema="city_government", table="lobbyist_fundraising",
        pattern=NamePattern.MULTI,
        name_columns=(
            ("candidate_last_name", "candidate_first_name"),
            ("principal_last_name", "principal_first_name"),
        ),
        address_columns={"city": "city", "zip": "zip"},
        notes="Candidate + principal lobbyist (both structured)",
    ),
]


def get_extraction_sql(source: NameSource) -> list[str]:
    """Generate SELECT statements that produce (source_table, last_name, first_name,
    address, city, zip) for a given registry entry.

    Returns a list of SQL strings (one per name-column set).
    """
    full_table = f"lake.{source.schema}.{source.table}"
    source_label = source.table

    addr_col = source.address_columns.get("address")
    city_col = source.address_columns.get("city")
    zip_col = source.address_columns.get("zip")

    # Use a table alias 't' and qualify all column references so DuckDB
    # never confuses a source column with the alias being defined in the
    # same SELECT (e.g., 'UPPER(city) AS city' fails without qualification).
    def _qualify(col: str | None) -> str:
        """Qualify a column reference with the 't' alias.

        Handles bare identifiers ('city' → 't.city'), already-quoted
        identifiers ('"Last Name"' → 't."Last Name"'), and pass-throughs
        for None and SQL expressions that already contain a dot or paren.
        """
        if not col:
            return col
        if "." in col or "(" in col:
            return col  # already qualified or an expression
        return f"t.{col}"

    def _addr_expr(col: str | None) -> str:
        return f"UPPER({_qualify(col)})" if col else "NULL"

    def _zip_expr(col: str | None) -> str:
        return f"CAST({_qualify(col)} AS VARCHAR)" if col else "NULL"

    results = []

    for last_col, first_col in source.name_columns:
        last_q = _qualify(last_col)
        first_q = _qualify(first_col) if first_col else None
        if source.pattern == NamePattern.STRUCTURED:
            if first_col is None:
                raise ValueError(f"STRUCTURED pattern requires first_col: {source}")
            sql = f"""
                SELECT
                    '{source_label}' AS source_table,
                    UPPER(TRIM({last_q})) AS last_name,
                    UPPER(TRIM({first_q})) AS first_name,
                    {_addr_expr(addr_col)} AS address,
                    {_addr_expr(city_col)} AS city,
                    {_zip_expr(zip_col)} AS zip
                FROM {full_table} t
                WHERE {last_q} IS NOT NULL AND LENGTH(TRIM({last_q})) >= 2
            """

        elif source.pattern == NamePattern.COMBINED_COMMA:
            sql = f"""
                SELECT
                    '{source_label}' AS source_table,
                    UPPER(TRIM(SPLIT_PART({last_q}, ', ', 1))) AS last_name,
                    UPPER(TRIM(SPLIT_PART(SPLIT_PART({last_q}, ', ', 2), ' ', 1))) AS first_name,
                    {_addr_expr(addr_col)} AS address,
                    {_addr_expr(city_col)} AS city,
                    {_zip_expr(zip_col)} AS zip
                FROM {full_table} t
                WHERE {last_q} IS NOT NULL AND LENGTH(TRIM({last_q})) > 2
                  AND {last_q} LIKE '%,%'
            """

        elif source.pattern == NamePattern.COMBINED_SPACE:
            if source.last_name_first:
                ln_expr = f"UPPER(TRIM(SPLIT_PART({last_q}, ' ', 1)))"
                fn_expr = f"UPPER(TRIM(SPLIT_PART({last_q}, ' ', 2)))"
            else:
                ln_expr = f"UPPER(TRIM(SPLIT_PART({last_q}, ' ', -1)))"
                fn_expr = f"UPPER(TRIM(SPLIT_PART({last_q}, ' ', 1)))"
            llc_clause = f"\n                  AND {_llc_where(last_q)}" if source.needs_llc_filter else ""
            sql = f"""
                SELECT
                    '{source_label}' AS source_table,
                    {ln_expr} AS last_name,
                    {fn_expr} AS first_name,
                    {_addr_expr(addr_col)} AS address,
                    {_addr_expr(city_col)} AS city,
                    {_zip_expr(zip_col)} AS zip
                FROM {full_table} t
                WHERE {last_q} IS NOT NULL AND LENGTH(TRIM({last_q})) > 2{llc_clause}
            """

        elif source.pattern == NamePattern.OWNER:
            sql = f"""
                SELECT
                    '{source_label}' AS source_table,
                    UPPER(TRIM(SPLIT_PART({last_q}, ', ', 1))) AS last_name,
                    UPPER(TRIM(SPLIT_PART({last_q}, ', ', 2))) AS first_name,
                    {_addr_expr(addr_col)} AS address,
                    {_addr_expr(city_col)} AS city,
                    {_zip_expr(zip_col)} AS zip
                FROM {full_table} t
                WHERE {last_q} IS NOT NULL AND LENGTH(TRIM({last_q})) > 2
                  AND {_llc_where(last_q)}
            """

        elif source.pattern == NamePattern.RESPONDENT:
            sql = f"""
                SELECT
                    '{source_label}' AS source_table,
                    UPPER(TRIM(SPLIT_PART({last_q}, ', ', 1))) AS last_name,
                    UPPER(TRIM(SPLIT_PART({last_q}, ', ', 2))) AS first_name,
                    {_addr_expr(addr_col)} AS address,
                    {_addr_expr(city_col)} AS city,
                    {_zip_expr(zip_col)} AS zip
                FROM {full_table} t
                WHERE {last_q} IS NOT NULL AND LENGTH(TRIM({last_q})) > 2
                  AND {_llc_where(last_q)}
            """

        elif source.pattern == NamePattern.MULTI:
            if first_col is not None:
                # Structured pair within a MULTI table
                sql = f"""
                    SELECT
                        '{source_label}' AS source_table,
                        UPPER(TRIM({last_q})) AS last_name,
                        UPPER(TRIM({first_q})) AS first_name,
                        {_addr_expr(addr_col)} AS address,
                        {_addr_expr(city_col)} AS city,
                        {_zip_expr(zip_col)} AS zip
                    FROM {full_table} t
                    WHERE {last_q} IS NOT NULL AND LENGTH(TRIM({last_q})) >= 2
                """
            else:
                # Combined name within a MULTI table
                sql = f"""
                    SELECT
                        '{source_label}' AS source_table,
                        UPPER(TRIM(SPLIT_PART({last_q}, ' ', -1))) AS last_name,
                        UPPER(TRIM(SPLIT_PART({last_q}, ' ', 1))) AS first_name,
                        {_addr_expr(addr_col)} AS address,
                        {_addr_expr(city_col)} AS city,
                        {_zip_expr(zip_col)} AS zip
                    FROM {full_table} t
                    WHERE {last_q} IS NOT NULL AND LENGTH(TRIM({last_q})) > 2
                """
        else:
            raise ValueError(f"Unknown pattern: {source.pattern}")

        results.append(sql.strip())

    return results


def get_pattern_counts() -> dict[str, int]:
    """Return count of tables per pattern type."""
    counts: dict[str, int] = {}
    for source in NAME_REGISTRY:
        key = source.pattern.value
        counts[key] = counts.get(key, 0) + 1
    return counts
