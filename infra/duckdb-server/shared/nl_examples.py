"""Few-shot SQL examples for NL-to-SQL prompt engineering.

Each example is a (question, sql) pair demonstrating correct DuckDB SQL
against the Common Ground lake. Covers common patterns: aggregation,
joins, filtering, ILIKE, TRY_CAST, GROUP BY ALL, and lake.schema.table
qualified names.
"""

NL_SQL_EXAMPLES: list[tuple[str, str]] = [
    (
        "What are the worst buildings by open violations?",
        "SELECT ownername, bbl, address, "
        "COUNT(*) FILTER (WHERE currentstatus = 'OPEN') AS open_violations, "
        "COUNT(*) AS total_violations "
        "FROM lake.housing.hpd_violations "
        "GROUP BY ownername, bbl, address "
        "HAVING open_violations > 0 "
        "ORDER BY open_violations DESC "
        "LIMIT 20",
    ),
    (
        "How many 311 noise complaints per ZIP code?",
        "SELECT incident_zip, COUNT(*) AS complaints "
        "FROM lake.social_services.n311_service_requests "
        "WHERE complaint_type ILIKE '%noise%' "
        "AND incident_zip IS NOT NULL "
        "GROUP BY incident_zip "
        "ORDER BY complaints DESC "
        "LIMIT 20",
    ),
    (
        "Which restaurants failed their last inspection?",
        "SELECT DISTINCT dba, building || ' ' || street AS address, "
        "boro, zipcode, grade, inspection_date "
        "FROM lake.health.restaurant_inspections "
        "WHERE grade IN ('C', 'Z', 'P') "
        "ORDER BY inspection_date DESC "
        "LIMIT 20",
    ),
    (
        "Top campaign donors in NYC",
        "SELECT name, SUM(TRY_CAST(amnt AS DOUBLE)) AS total_donated, "
        "COUNT(*) AS num_donations, "
        "LIST(DISTINCT recipname) AS recipients "
        "FROM lake.city_government.campaign_contributions "
        "WHERE TRY_CAST(amnt AS DOUBLE) > 0 "
        "GROUP BY name "
        "ORDER BY total_donated DESC "
        "LIMIT 20",
    ),
    (
        "Crime counts by precinct in Brooklyn",
        "SELECT addr_pct_cd AS precinct, "
        "COUNT(*) AS total_crimes, "
        "COUNT(*) FILTER (WHERE ofns_desc ILIKE '%assault%') AS assaults, "
        "COUNT(*) FILTER (WHERE ofns_desc ILIKE '%robbery%') AS robberies "
        "FROM lake.public_safety.nypd_complaint_data "
        "WHERE boro_nm = 'BROOKLYN' "
        "GROUP BY precinct "
        "ORDER BY total_crimes DESC "
        "LIMIT 20",
    ),
    (
        "Average city employee salary by agency",
        "SELECT agency_name, "
        "ROUND(AVG(TRY_CAST(base_salary AS DOUBLE)), 0) AS avg_salary, "
        "COUNT(*) AS employees "
        "FROM lake.city_government.citywide_payroll "
        "WHERE fiscal_year = '2025' "
        "GROUP BY agency_name "
        "ORDER BY avg_salary DESC "
        "LIMIT 20",
    ),
    (
        "Property sales over $10 million in Manhattan",
        "SELECT p.name AS party, m.doc_type, "
        "TRY_CAST(m.document_amt AS DOUBLE) AS amount, "
        "m.document_date, l.street_name "
        "FROM lake.housing.acris_parties p "
        "JOIN lake.housing.acris_master m ON p.document_id = m.document_id "
        "JOIN lake.housing.acris_legals l ON p.document_id = l.document_id "
        "WHERE m.doc_type IN ('DEED', 'DEED, RP') "
        "AND TRY_CAST(m.document_amt AS DOUBLE) > 10000000 "
        "AND l.borough = '1' "
        "ORDER BY amount DESC "
        "LIMIT 20",
    ),
    (
        "How many evictions happened in each borough last year?",
        "SELECT eviction_zip, COUNT(*) AS evictions "
        "FROM lake.housing.evictions "
        "WHERE executed_date >= '2025-01-01' "
        "GROUP BY eviction_zip "
        "ORDER BY evictions DESC "
        "LIMIT 20",
    ),
]


def format_examples(n: int = 4) -> str:
    """Format the first n examples as a prompt section."""
    lines = []
    for question, sql in NL_SQL_EXAMPLES[:n]:
        lines.append(f"Question: {question}")
        lines.append(f"SQL: {sql}")
        lines.append("")
    return "\n".join(lines)
