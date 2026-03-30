"""Shared constants for the Common Ground MCP server middleware stack."""

MIDDLEWARE_SKIP_TOOLS = frozenset({
    "list_schemas",
    "list_tables",
    "describe_table",
    "data_catalog",
    "search_tools",
    "call_tool",
    "sql_admin",
    "suggest_explorations",
    "graph_health",
    "lake_health",
    "name_variants",
})

TOOL_SOURCES = {
    "building_profile": ["housing.hpd_jurisdiction", "housing.hpd_violations", "housing.hpd_complaints"],
    "landlord_watchdog": ["housing.hpd_registration_contacts", "housing.hpd_violations", "housing.evictions", "housing.hpd_litigations"],
    "owner_violations": ["housing.hpd_violations", "housing.dob_ecb_violations"],
    "enforcement_web": ["housing.hpd_violations", "housing.dob_ecb_violations", "housing.fdny_violations", "city_government.oath_hearings"],
    "property_history": ["housing.acris_master", "housing.acris_parties", "housing.acris_legals"],
    "neighborhood_portrait": ["health.restaurant_inspections", "housing.hpd_jurisdiction", "social_services.n311_service_requests", "business.issued_licenses"],
    "safety_report": ["public_safety.nypd_arrests", "public_safety.nypd_complaints", "public_safety.nypd_shooting_incidents"],
    "school_report": ["education.demographics_2020", "education.ela_results", "education.math_results", "education.chronic_absenteeism", "education.school_safety"],
    "school_search": ["education.demographics_2020", "education.school_safety"],
    "cop_sheet": ["federal.nypd_ccrb_complaints", "federal.nypd_ccrb_officers_current", "federal.police_settlements_538", "federal.cl_nypd_cases_sdny"],
    "due_diligence": ["financial.nys_attorney_registrations", "financial.nys_re_brokers", "financial.nys_tax_warrants", "financial.nys_child_support_warrants"],
    "climate_risk": ["environment.heat_vulnerability", "environment.lead_service_lines", "environment.ll84_energy_2023", "environment.street_trees"],
    "money_trail": ["federal.nys_campaign_finance", "federal.fec_contributions", "city_government.campaign_contributions", "city_government.contract_awards"],
    "judge_profile": ["federal.cl_judges", "federal.cl_financial_disclosures"],
    "vital_records": ["city_government.death_certificates_1862_1948", "city_government.marriage_certificates_1866_1937", "city_government.marriage_licenses_1950_2017"],
    "entity_xray": ["business.nys_corporations", "housing.acris_parties", "city_government.campaign_contributions", "city_government.citywide_payroll"],
    "pay_to_play": ["city_government.campaign_contributions", "city_government.nys_lobbyist_registration", "city_government.contract_awards"],
}
