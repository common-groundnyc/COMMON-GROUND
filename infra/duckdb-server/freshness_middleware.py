"""
FreshnessMiddleware — warns when queried data is stale.

Checks _pipeline_state.last_run_at for the tables a tool queries.
If any table hasn't been refreshed in > 7 days, appends a warning.
"""

import time
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

# Reuse the same tool->table mapping from citation middleware (with local fallback)
try:
    from citation_middleware import TOOL_SOURCES
except ImportError:
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

STALE_THRESHOLD_DAYS = 7

_SKIP = frozenset({
    "list_schemas", "list_tables", "describe_table", "data_catalog",
    "search_tools", "call_tool", "sql_admin", "suggest_explorations",
    "graph_health", "lake_health", "sql_query", "vital_records",
})


class FreshnessMiddleware(Middleware):
    """Warn when data hasn't been refreshed recently."""

    async def on_call_tool(self, context: MiddlewareContext, call_next) -> ToolResult:
        result = await call_next(context)

        tool_name = context.message.name
        if tool_name in _SKIP:
            return result

        sources = TOOL_SOURCES.get(tool_name)
        if not sources:
            return result

        try:
            lifespan = context.fastmcp_context.lifespan_context
            pipeline_state = lifespan.get("pipeline_state", {})
            now = time.time()

            stale_tables = []
            for table_key in sources:
                ps = pipeline_state.get(table_key)
                if not ps or not ps.get("last_run_at"):
                    continue
                try:
                    from datetime import datetime
                    last_run = datetime.fromisoformat(ps["last_run_at"].replace("Z", "+00:00"))
                    age_days = (now - last_run.timestamp()) / 86400
                    if age_days > STALE_THRESHOLD_DAYS:
                        stale_tables.append((table_key, int(age_days)))
                except (ValueError, TypeError):
                    pass

            if not stale_tables:
                return result

            warning_parts = [f"{t} ({d}d old)" for t, d in stale_tables]
            warning = f"Data freshness warning: {', '.join(warning_parts)} — last refreshed over {STALE_THRESHOLD_DAYS} days ago."

            original_text = ""
            if isinstance(result.content, list) and result.content:
                original_text = result.content[0].text
            elif isinstance(result.content, str):
                original_text = result.content

            new_text = original_text + "\n\n" + warning

            return ToolResult(
                content=new_text,
                structured_content=result.structured_content,
                meta=result.meta,
            )
        except Exception:
            return result
