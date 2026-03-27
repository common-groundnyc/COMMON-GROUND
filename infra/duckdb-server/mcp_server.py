"""Common Ground — FastMCP server for NYC open data lake."""

import os
import re
import threading
import time

import duckdb
import posthog
from fastmcp import FastMCP, Context
from fastmcp.server.lifespan import lifespan
from fastmcp.server.middleware import Middleware, MiddlewareContext
from response_middleware import OutputFormatterMiddleware
from csv_export import generate_branded_csv, write_export
from percentiles import build_percentile_tables, build_lake_percentile_tables
from percentile_middleware import PercentileMiddleware
from fastmcp.server.middleware.response_limiting import ResponseLimitingMiddleware
from fastmcp.server.dependencies import get_http_request, get_http_headers
from fastmcp.tools.tool import ToolResult
from fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from fastmcp.server.transforms.search import BM25SearchTransform

from typing import Annotated
from pydantic import Field

from entity import phonetic_search_sql, fuzzy_name_sql
from spatial import h3_kring_sql, h3_aggregate_sql, h3_heatmap_sql

# ---------------------------------------------------------------------------
# Reusable annotated types
# ---------------------------------------------------------------------------

BBL = Annotated[str, Field(description="10-digit BBL: borough(1) + block(5) + lot(4). Example: 1000670001")]
ZIP = Annotated[str, Field(description="5-digit NYC ZIP code. Example: 10003, 11201, 10456")]
NAME = Annotated[str, Field(description="Person or company name. Fuzzy matched. Example: 'Barton Perlbinder', 'BLACKSTONE'")]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_LLM_ROWS = 20
MAX_STRUCTURED_ROWS = 500
MAX_QUERY_ROWS = 1000

READONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True)
ADMIN = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True)

_UNSAFE_SQL = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY)\b",
    re.IGNORECASE,
)

_SAFE_DDL = re.compile(
    r"^\s*CREATE\s+OR\s+REPLACE\s+VIEW\b",
    re.IGNORECASE,
)

_db_lock = threading.Lock()
LANCE_DIR = "/data/common-ground/lance"

# Distance thresholds for embedding similarity
LANCE_NAME_DISTANCE = 0.4      # ~60% cosine — allows spelling variants
LANCE_TIGHT_DISTANCE = 0.3     # ~70% cosine — higher-confidence cross-domain
LANCE_CATALOG_DISTANCE = 0.7   # loose — table/schema discovery
LANCE_CATEGORY_SIM = 0.5       # cosine similarity for resource categories


def _vector_expand_names(ctx, search_term: str, threshold: float = LANCE_NAME_DISTANCE, k: int = 5) -> set:
    """Find similar entity names via Lance vector search. Returns empty set on failure."""
    embed_fn = ctx.lifespan_context.get("embed_fn")
    if not embed_fn:
        return set()
    try:
        from embedder import vec_to_sql
        query_vec = embed_fn(search_term)
        vec_literal = vec_to_sql(query_vec)
        sql = f"""
            SELECT name, _distance AS dist
            FROM lance_vector_search('{LANCE_DIR}/entity_name_embeddings.lance', 'embedding', {vec_literal}, k={k})
            ORDER BY _distance ASC
        """
        db = ctx.lifespan_context["db"]
        lock = ctx.lifespan_context.get("lock")
        if lock:
            with lock:
                rows = db.execute(sql).fetchall()
        else:
            rows = db.execute(sql).fetchall()
        result = set()
        for name, dist in rows:
            if dist < threshold:
                result.add(name.upper())
        result.discard(search_term.upper())
        return result
    except Exception:
        return set()

# ---------------------------------------------------------------------------
# SQL constants — domain tools
# ---------------------------------------------------------------------------

BUILDING_PROFILE_SQL = """
WITH building AS (
    SELECT boroid, block, lot, buildingid, bin, streetname, housenumber,
           legalstories, legalclassa, legalclassb, managementprogram, zip,
           (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
    FROM lake.housing.hpd_jurisdiction
    WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
    LIMIT 1
),
violation_counts AS (
    SELECT
        COUNT(*) AS total_violations,
        COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_violations,
        MAX(TRY_CAST(novissueddate AS DATE)) AS latest_violation_date
    FROM lake.housing.hpd_violations
    WHERE bbl = ?::VARCHAR
),
complaint_counts AS (
    SELECT
        COUNT(DISTINCT complaint_id) AS total_complaints,
        COUNT(DISTINCT complaint_id) FILTER (WHERE complaint_status = 'OPEN') AS open_complaints,
        MAX(TRY_CAST(received_date AS DATE)) AS latest_complaint_date
    FROM lake.housing.hpd_complaints
    WHERE bbl = ?::VARCHAR
),
dob_counts AS (
    SELECT
        COUNT(*) AS total_dob_violations,
        MAX(TRY_CAST(issue_date AS DATE)) AS latest_dob_date
    FROM lake.housing.dob_ecb_violations
    WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
)
SELECT
    b.bbl, b.housenumber || ' ' || b.streetname AS address, b.zip,
    b.legalstories AS stories,
    (COALESCE(TRY_CAST(b.legalclassa AS INTEGER), 0) + COALESCE(TRY_CAST(b.legalclassb AS INTEGER), 0)) AS total_units,
    b.managementprogram,
    v.total_violations, v.open_violations, v.latest_violation_date,
    c.total_complaints, c.open_complaints, c.latest_complaint_date,
    d.total_dob_violations, d.latest_dob_date
FROM building b
CROSS JOIN violation_counts v
CROSS JOIN complaint_counts c
CROSS JOIN dob_counts d
"""

COMPLAINTS_BY_ZIP_SQL = """
WITH hpd AS (
    SELECT major_category AS category, 'HPD' AS source,
           COUNT(DISTINCT complaint_id) AS cnt
    FROM lake.housing.hpd_complaints
    WHERE post_code = ?
      AND TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL (?) DAY
    GROUP BY major_category
),
svc311 AS (
    SELECT problem_formerly_complaint_type AS category, '311' AS source,
           COUNT(*) AS cnt
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip = ?
      AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL (?) DAY
    GROUP BY problem_formerly_complaint_type
),
combined AS (
    SELECT * FROM hpd
    UNION ALL
    SELECT * FROM svc311
)
SELECT source, category, cnt
FROM combined
ORDER BY cnt DESC
"""

OWNER_VIOLATIONS_SQL = """
WITH hpd AS (
    SELECT 'HPD' AS source, violationid AS violation_id,
           class, novdescription AS description,
           violationstatus AS status, novissueddate AS issue_date,
           currentstatus AS current_status, currentstatusdate AS status_date
    FROM lake.housing.hpd_violations
    WHERE bbl = ?
    ORDER BY TRY_CAST(novissueddate AS DATE) DESC NULLS LAST
    LIMIT 200
),
dob AS (
    SELECT 'DOB/ECB' AS source, ecb_violation_number AS violation_id,
           severity AS class, violation_description AS description,
           ecb_violation_status AS status, issue_date,
           certification_status AS current_status, hearing_date AS status_date
    FROM lake.housing.dob_ecb_violations
    WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
    ORDER BY TRY_CAST(issue_date AS DATE) DESC NULLS LAST
    LIMIT 200
),
combined AS (
    SELECT *, TRY_CAST(issue_date AS DATE) AS sort_date FROM hpd
    UNION ALL
    SELECT *, TRY_CAST(issue_date AS DATE) AS sort_date FROM dob
)
SELECT source, violation_id, class, description, status, issue_date, current_status, status_date
FROM combined
ORDER BY sort_date DESC NULLS LAST
"""

DATA_CATALOG_SQL = """
WITH table_matches AS (
    SELECT DISTINCT t.schema_name, t.table_name, t.comment,
           t.column_count, t.estimated_size, 'table/comment' AS match_type,
           GREATEST(
               rapidfuzz_partial_ratio(LOWER(t.table_name), LOWER(?)),
               rapidfuzz_partial_ratio(LOWER(COALESCE(t.comment, '')), LOWER(?))
           ) AS score
    FROM duckdb_tables() t
    WHERE t.database_name = 'lake'
      AND t.schema_name NOT IN ('information_schema', 'pg_catalog')
),
column_matches AS (
    SELECT DISTINCT c.schema_name, c.table_name, t.comment,
           t.column_count, t.estimated_size, 'column' AS match_type,
           rapidfuzz_partial_ratio(LOWER(c.column_name), LOWER(?)) AS score
    FROM duckdb_columns() c
    JOIN duckdb_tables() t ON c.schema_name = t.schema_name
         AND c.table_name = t.table_name AND t.database_name = 'lake'
    WHERE c.database_name = 'lake'
),
h3_tables AS (
    SELECT DISTINCT source_table, COUNT(*) AS geo_rows
    FROM lake.foundation.h3_index
    GROUP BY source_table
),
all_matches AS (
    SELECT schema_name, table_name, comment, column_count, estimated_size,
           match_type, MAX(score) AS score
    FROM (
        SELECT * FROM table_matches
        UNION ALL
        SELECT * FROM column_matches
    )
    GROUP BY schema_name, table_name, comment, column_count, estimated_size, match_type
)
SELECT a.schema_name, a.table_name, a.comment, a.estimated_size, a.column_count,
       a.match_type, a.score,
       h.geo_rows IS NOT NULL AS has_geo,
       COALESCE(h.geo_rows, 0) AS geo_row_count
FROM all_matches a
LEFT JOIN h3_tables h ON (a.schema_name || '.' || a.table_name) = h.source_table
WHERE a.score >= 60
ORDER BY a.score DESC, a.estimated_size DESC
LIMIT 30
"""

NEIGHBORHOOD_COMPARE_SQL = """
WITH input_zips AS (
    SELECT UNNEST(?::VARCHAR[]) AS zip
),
-- 311 is the universal crosswalk: ZIP → precinct, ZIP → community district
zip_to_precinct AS (
    SELECT incident_zip AS zip,
           REGEXP_EXTRACT(police_precinct, '\\d+') AS precinct,
           COUNT(*) AS weight
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT zip FROM input_zips)
      AND police_precinct NOT IN ('Unspecified', '')
      AND police_precinct IS NOT NULL
    GROUP BY 1, 2
),
zip_primary_precinct AS (
    SELECT zip, precinct
    FROM (
        SELECT zip, precinct,
               ROW_NUMBER() OVER (PARTITION BY zip ORDER BY weight DESC) AS rn
        FROM zip_to_precinct
    ) WHERE rn = 1
),
zip_to_cd AS (
    SELECT incident_zip AS zip,
           CASE SPLIT_PART(community_board, ' ', 2)
               WHEN 'MANHATTAN' THEN '1'
               WHEN 'BRONX' THEN '2'
               WHEN 'BROOKLYN' THEN '3'
               WHEN 'QUEENS' THEN '4'
               WHEN 'STATEN ISLAND' THEN '5'
           END || LPAD(SPLIT_PART(community_board, ' ', 1), 2, '0') AS borocd,
           COUNT(*) AS weight
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT zip FROM input_zips)
      AND community_board NOT LIKE '%Unspecified%'
      AND community_board IS NOT NULL
      AND community_board != ''
    GROUP BY 1, 2
),
zip_primary_cd AS (
    SELECT zip, borocd
    FROM (
        SELECT zip, borocd,
               ROW_NUMBER() OVER (PARTITION BY zip ORDER BY weight DESC) AS rn
        FROM zip_to_cd
    ) WHERE rn = 1
),
-- Dimension: Crime (via precinct crosswalk, historic + YTD)
all_crimes AS (
    SELECT addr_pct_cd, law_cat_cd, ofns_desc, rpt_dt
    FROM lake.public_safety.nypd_complaints_historic
    WHERE TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 730 DAY
    UNION ALL
    SELECT addr_pct_cd, law_cat_cd, ofns_desc, rpt_dt
    FROM lake.public_safety.nypd_complaints_ytd
),
crime AS (
    SELECT zp.zip,
           COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies,
           COUNT(*) FILTER (WHERE ofns_desc ILIKE '%assault%') AS assaults
    FROM all_crimes c
    JOIN zip_primary_precinct zp ON c.addr_pct_cd = zp.precinct
    WHERE TRY_CAST(c.rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY zp.zip
),
-- Dimension: Restaurants (direct ZIP join)
restaurants AS (
    SELECT zipcode AS zip,
           COUNT(DISTINCT camis) AS total_restaurants,
           ROUND(100.0 * COUNT(DISTINCT camis) FILTER (WHERE grade = 'A')
               / NULLIF(COUNT(DISTINCT camis) FILTER (WHERE grade IN ('A','B','C')), 0), 1) AS pct_grade_a
    FROM lake.health.restaurant_inspections
    WHERE zipcode IN (SELECT zip FROM input_zips)
    GROUP BY zipcode
),
-- Dimension: HPD housing complaints (direct ZIP join)
housing AS (
    SELECT post_code AS zip,
           COUNT(DISTINCT complaint_id) AS hpd_complaints_1yr
    FROM lake.housing.hpd_complaints
    WHERE post_code IN (SELECT zip FROM input_zips)
      AND TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY post_code
),
-- Dimension: 311 NYPD calls as noise/quality-of-life proxy (direct ZIP join)
noise AS (
    SELECT incident_zip AS zip,
           COUNT(*) AS nypd_311_calls_1yr
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT zip FROM input_zips)
      AND agency = 'NYPD'
      AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY incident_zip
),
-- Dimension: IRS income (direct ZIP join)
income AS (
    SELECT zipcode AS zip,
           ROUND(1000.0 * SUM(TRY_CAST(agi_amount AS DOUBLE)) / NULLIF(SUM(TRY_CAST(num_returns AS DOUBLE)), 0), 0) AS avg_agi
    FROM lake.economics.irs_soi_zip_income
    WHERE zipcode IN (SELECT zip FROM input_zips)
      AND TRY_CAST(tax_year AS INTEGER) = (
          SELECT MAX(TRY_CAST(tax_year AS INTEGER)) FROM lake.economics.irs_soi_zip_income
      )
    GROUP BY zipcode
),
-- Dimension: Community district profiles (via CD crosswalk)
cd_profiles AS (
    SELECT zc.zip,
           TRY_CAST(cd.mean_commute AS DOUBLE) AS mean_commute_min,
           TRY_CAST(cd.crime_per_1000 AS DOUBLE) AS crime_per_1000,
           TRY_CAST(cd.poverty_rate AS DOUBLE) AS poverty_rate,
           TRY_CAST(cd.pct_served_parks AS DOUBLE) AS pct_park_access,
           TRY_CAST(cd.pct_bach_deg AS DOUBLE) AS pct_bachelors
    FROM lake.city_government.cdprofiles_districts cd
    JOIN zip_primary_cd zc ON cd.borocd = zc.borocd
)
SELECT z.zip,
       cr.felonies,
       cr.assaults,
       n.nypd_311_calls_1yr AS noise_calls,
       r.total_restaurants AS restaurants,
       r.pct_grade_a,
       h.hpd_complaints_1yr AS housing_complaints,
       i.avg_agi AS avg_income,
       cp.mean_commute_min AS commute_min,
       cp.poverty_rate,
       cp.pct_park_access AS park_access_pct,
       cp.pct_bachelors AS bachelors_pct
FROM input_zips z
LEFT JOIN crime cr ON z.zip = cr.zip
LEFT JOIN restaurants r ON z.zip = r.zip
LEFT JOIN housing h ON z.zip = h.zip
LEFT JOIN noise n ON z.zip = n.zip
LEFT JOIN income i ON z.zip = i.zip
LEFT JOIN cd_profiles cp ON z.zip = cp.zip
ORDER BY z.zip
"""

SCHEMA_DESCRIPTIONS = {
    "business": "BLS employment stats, ACS census demographics, business licenses, M/WBE certs. 15 tables. Ask: which industries are growing? where are minority-owned businesses concentrated?",
    "city_government": "PLUTO lot data (every parcel in NYC), city payroll, OATH hearings, zoning, facilities. 25 tables, 3M+ rows. Ask: who owns this lot? what's the zoning? how much do city employees earn?",
    "economics": "FRED metro economic indicators, IRS income by ZIP. 5 tables. Ask: how does income vary by neighborhood? what's the median household income in this ZIP?",
    "education": "DOE school surveys, test scores, enrollment, NYSED data, College Scorecard. 20 tables. Ask: how do schools compare? what's the graduation rate? which schools are overcrowded?",
    "environment": "Air quality, tree census (680k trees), energy benchmarking, FEMA flood zones, EPA enforcement. 18 tables. Ask: what's the air quality here? which buildings waste the most energy?",
    "financial": "CFPB consumer complaints with full narratives (searchable via text_search). 1 table, 1.2M rows. Ask: what financial companies get the most complaints? what are people complaining about?",
    "health": "Restaurant inspections (27k restaurants, letter grades), rat inspections, community health indicators. 10 tables. Ask: is this restaurant safe? where are the worst rat problems?",
    "housing": "HPD complaints/violations, DOB permits, evictions, NYCHA, HMDA mortgages, ACRIS transactions (85M records since 1966). 40 tables, 30M+ rows. THE richest schema. Ask: who owns this building? how many violations? when was it last sold? who's the worst landlord?",
    "public_safety": "NYPD crimes/arrests/shootings, motor vehicle collisions, hate crimes. 12 tables, 10M+ rows. Ask: is this neighborhood safe? what crimes are most common? how do precincts compare?",
    "recreation": "Parks, pools, permits, events. 8 tables. Ask: what parks are nearby? what events are happening?",
    "social_services": "311 service requests (30M+ rows), food assistance, childcare. 8 tables. Ask: what do people complain about? where are food deserts?",
    "transportation": "MTA ridership, parking tickets (40M+), traffic speeds, street conditions. 15 tables. Ask: which subway stations are busiest? where do people get the most parking tickets?",
}

_HIDDEN_SCHEMAS = frozenset({
    "business_staging", "city_government_staging", "education_staging",
    "environment_staging", "federal_staging", "financial_staging",
    "health_staging", "housing_staging", "public_safety_staging",
    "recreation_staging", "social_services_staging", "transportation_staging",
    "test_curl", "test_direct", "test_pure",
})

# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def format_text_table(cols, rows, max_rows=MAX_LLM_ROWS):
    if not cols:
        return "(no columns)"
    if not rows:
        return "(no rows)"

    display = rows[:max_rows]

    def cell(v):
        s = str(v)[:40] if v is not None else ""
        return s.replace("|", "\\|")

    lines = [
        "| " + " | ".join(str(c) for c in cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    lines.extend(
        "| " + " | ".join(cell(v) for v in row) + " |"
        for row in display
    )

    total = len(rows)
    if total > max_rows:
        lines.append(f"({max_rows} of {total} rows shown)")
    return "\n".join(lines)


def make_result(summary, cols, rows, meta_extra=None):
    text = summary
    if cols and rows:
        text += "\n\n" + format_text_table(cols, rows)

    structured = (
        {"rows": [dict(zip(cols, row)) for row in rows[:MAX_STRUCTURED_ROWS]]}
        if cols and rows
        else None
    )
    meta = {"total_rows": len(rows)}
    if meta_extra:
        meta.update(meta_extra)
    return ToolResult(content=text, structured_content=structured, meta=meta)


def _build_explorations(db):
    """Pre-compute interesting data highlights for suggest_explorations tool."""
    highlights = []

    queries = [
        (
            "Worst landlords by open violations",
            """SELECT o.owner_name, COUNT(DISTINCT b.bbl) AS buildings,
                      COUNT(*) FILTER (WHERE v.status = 'Open') AS open_violations,
                      COUNT(*) AS total_violations
               FROM main.graph_owners o
               JOIN main.graph_owns ow ON o.owner_id = ow.owner_id
               JOIN main.graph_buildings b ON ow.bbl = b.bbl
               JOIN main.graph_violations v ON b.bbl = v.bbl
               WHERE o.owner_name IS NOT NULL
               GROUP BY o.owner_name
               ORDER BY open_violations DESC LIMIT 5""",
            "These landlords have the most open housing violations across their portfolios.",
            "Try: worst_landlords() or entity_xray(name)",
        ),
        (
            "Biggest property flips",
            """WITH deed_sales AS (
                   SELECT (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                          TRY_CAST(m.document_amt AS DOUBLE) AS price,
                          TRY_CAST(m.document_date AS DATE) AS sale_date
                   FROM lake.housing.acris_master m
                   JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
                   WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
                     AND TRY_CAST(m.document_amt AS DOUBLE) > 50000
                     AND TRY_CAST(m.document_date AS DATE) >= '2015-01-01'
               ),
               ranked AS (
                   SELECT bbl, price, sale_date,
                          LAG(price) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_price,
                          LAG(sale_date) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_date
                   FROM deed_sales
               )
               SELECT bbl,
                      prev_date AS buy_date, sale_date AS sell_date,
                      prev_price AS buy_price, price AS sell_price,
                      (price - prev_price) AS profit,
                      ((price - prev_price) / prev_price * 100)::INT AS profit_pct
               FROM ranked
               WHERE prev_price IS NOT NULL
                 AND price > prev_price
                 AND DATEDIFF('month', prev_date, sale_date) <= 24
                 AND DATEDIFF('month', prev_date, sale_date) > 0
               ORDER BY profit DESC LIMIT 5""",
            "Properties with the biggest buy-sell price differences.",
            "Try: flipper_detector() or property_history(bbl)",
        ),
        (
            "Most complained-about restaurants",
            """SELECT camis, dba, zipcode, COUNT(*) AS violations,
                      COUNT(*) FILTER (WHERE violation_code LIKE '04%') AS critical
               FROM lake.health.restaurant_inspections
               GROUP BY camis, dba, zipcode
               ORDER BY critical DESC LIMIT 5""",
            "Restaurants with the most critical inspection violations.",
            "Try: restaurant_lookup(name) or text_search('mice kitchen', corpus='restaurants')",
        ),
        (
            "Neighborhoods with most 311 complaints",
            """SELECT incident_zip, complaint_type, COUNT(*) AS complaints
               FROM lake.social_services.n311_service_requests
               WHERE incident_zip IS NOT NULL AND incident_zip != ''
               GROUP BY incident_zip, complaint_type
               ORDER BY complaints DESC LIMIT 5""",
            "What NYC residents complain about most, by ZIP.",
            "Try: neighborhood_portrait(zip) or neighborhood_compare([zip1, zip2])",
        ),
        (
            "Corporate shell networks",
            """SELECT COUNT(*) AS total_corps,
                      COUNT(DISTINCT dos_process_name) AS distinct_agents
               FROM lake.business.nys_corporations
               WHERE jurisdiction = 'NEW YORK'
                 AND dos_process_name IS NOT NULL""",
            "Active NYC corporations and how many share registered agents — a signal for shell company networks.",
            "Try: shell_detector() or corporate_web(name)",
        ),
    ]

    for title, sql, description, follow_up in queries:
        try:
            with _db_lock:
                result = db.execute(sql).fetchall()
            highlights.append({
                "title": title,
                "description": description,
                "sample": str(result[:3]) if result else "No data",
                "follow_up": follow_up,
            })
        except Exception as e:
            highlights.append({
                "title": title,
                "description": f"(query failed: {e})",
                "sample": "",
                "follow_up": "",
            })

    return highlights


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _validate_sql(sql):
    # Strip comments and check for stacked statements
    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)  # block comments
    stripped = re.sub(r"--[^\n]*", " ", stripped)  # line comments
    stripped = stripped.strip().rstrip(";")

    # Reject stacked statements (semicolons in the middle)
    if ";" in stripped:
        raise ToolError("Only single SQL statements are allowed.")

    if _UNSAFE_SQL.match(stripped):
        raise ToolError(
            "Only SELECT, WITH, EXPLAIN, DESCRIBE, SHOW, and PRAGMA queries are allowed."
        )


def _execute(conn, sql, params=None):
    with _db_lock:
        try:
            cur = conn.execute(sql, params or [])
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(MAX_QUERY_ROWS)
            return cols, rows
        except duckdb.Error as e:
            err = str(e)
            hint = ""
            if "does not exist" in err.lower():
                hint = " Use data_catalog(keyword) to find table names, or list_tables(schema) to browse a schema."
            elif "not found" in err.lower():
                hint = " Use describe_table(schema, table) to check column names."
            elif "permission" in err.lower() or "read-only" in err.lower():
                hint = " Only SELECT queries are allowed. Use sql_query() for reads."
            raise ToolError(f"SQL error: {e}{hint}")


def _build_catalog(conn):
    catalog = {}
    try:
        cur = conn.execute("""
            SELECT schema_name, table_name, estimated_size, column_count
            FROM duckdb_tables()
            WHERE database_name = 'lake'
              AND schema_name NOT IN ('information_schema', 'pg_catalog')
            ORDER BY schema_name, table_name
        """)
        for schema, table, est_size, col_count in cur.fetchall():
            catalog.setdefault(schema, {})[table] = {
                "row_count": est_size or 0,
                "column_count": col_count or 0,
            }
    except Exception as e:
        print(f"Warning: catalog cache build failed: {e}", flush=True)
    return catalog


def _fuzzy_match_schema(db, input_schema, catalog):
    """Return the best-matching schema name, or raise ToolError with suggestion."""
    if input_schema in catalog:
        return input_schema
    # Try rapidfuzz to find closest match
    try:
        cur = db.execute("""
            SELECT schema_name, rapidfuzz_token_set_ratio(LOWER(?), LOWER(schema_name)) AS score
            FROM (SELECT UNNEST(?::VARCHAR[]) AS schema_name)
            WHERE score >= 60
            ORDER BY score DESC
            LIMIT 1
        """, [input_schema, list(catalog.keys())])
        row = cur.fetchone()
        if row:
            return row[0]
    except Exception:
        pass
    raise ToolError(
        f"Schema '{input_schema}' not found. Available: {', '.join(sorted(catalog))}"
    )


def _fuzzy_match_table(db, input_table, schema, catalog):
    """Return the best-matching table name in a schema, or raise ToolError."""
    tables = catalog.get(schema, {})
    if input_table in tables:
        return input_table
    # Try rapidfuzz — token_set_ratio rewards matching all tokens, penalizes extra noise
    try:
        cur = db.execute("""
            SELECT table_name, rapidfuzz_token_set_ratio(LOWER(?), LOWER(table_name)) AS score
            FROM (SELECT UNNEST(?::VARCHAR[]) AS table_name)
            WHERE score >= 50
            ORDER BY score DESC
            LIMIT 1
        """, [input_table, list(tables.keys())])
        row = cur.fetchone()
        if row:
            return row[0]
    except Exception:
        pass
    raise ToolError(
        f"Table '{input_table}' not found in '{schema}'. Use list_tables('{schema}') to see available tables."
    )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@lifespan
async def app_lifespan(server):
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute("PRAGMA disable_checkpoint_on_shutdown")
    try:
        conn.execute('PRAGMA wal_autocheckpoint="0"')
    except duckdb.Error:
        pass  # Not supported in DuckDB 1.5+

    from extensions import load_extensions
    ext_status = load_extensions(conn)

    # Configure S3 for MinIO (DuckLake stores parquet in s3://ducklake/data/)
    minio_user = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    minio_pass = os.environ.get("MINIO_ROOT_PASSWORD", "")
    conn.execute("SET s3_region = 'us-east-1'")
    conn.execute("SET s3_endpoint = 'minio:9000'")
    conn.execute(f"SET s3_access_key_id = '{minio_user}'")
    conn.execute(f"SET s3_secret_access_key = '{minio_pass}'")
    conn.execute("SET s3_use_ssl = false")
    conn.execute("SET s3_url_style = 'path'")
    print("S3/MinIO credentials configured", flush=True)

    # Performance tuning
    conn.execute("SET memory_limit = '8GB'")
    conn.execute("SET threads = 8")
    conn.execute("SET temp_directory = '/tmp/duckdb_temp'")
    conn.execute("SET max_temp_directory_size = '50GB'")

    # HTTP/S3 resilience (curl backend in DuckDB 1.5)
    conn.execute("SET enable_http_metadata_cache = true")
    conn.execute("SET http_retries = 5")
    conn.execute("SET http_retry_wait_ms = 200")

    # NYC timezone + case-insensitive search
    conn.execute("SET TimeZone = 'America/New_York'")
    conn.execute("SET default_collation = 'NOCASE'")
    print("Performance tuning applied", flush=True)

    pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "\\'")
    conn.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
        AS lake (METADATA_SCHEMA 'lake')
    """)
    print("DuckLake catalog attached", flush=True)

    # Warm up: first query triggers compactor which may crash on orphaned snapshots.
    # If it does, close and reconnect — the second attach skips the compactor.
    try:
        conn.execute("SELECT 1 FROM lake.information_schema.tables LIMIT 1")
        print("DuckLake warm-up OK", flush=True)
    except Exception as e:
        print(f"DuckLake warm-up failed (expected on corrupted snapshots): {e}", flush=True)
        conn.close()
        conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        from extensions import load_extensions
        ext_status = load_extensions(conn)
        # Reconfigure S3 and performance tuning on new connection
        conn.execute("SET s3_region = 'us-east-1'")
        conn.execute("SET s3_endpoint = 'minio:9000'")
        conn.execute(f"SET s3_access_key_id = '{minio_user}'")
        conn.execute(f"SET s3_secret_access_key = '{minio_pass}'")
        conn.execute("SET s3_use_ssl = false")
        conn.execute("SET s3_url_style = 'path'")
        conn.execute("SET memory_limit = '8GB'")
        conn.execute("SET threads = 8")
        conn.execute("SET temp_directory = '/tmp/duckdb_temp'")
        conn.execute("SET max_temp_directory_size = '50GB'")
        conn.execute("SET enable_http_metadata_cache = true")
        conn.execute("SET http_retries = 5")
        conn.execute("SET http_retry_wait_ms = 200")
        conn.execute("SET TimeZone = 'America/New_York'")
        conn.execute("SET default_collation = 'NOCASE'")
        conn.execute(f"""
            ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
            AS lake (METADATA_SCHEMA 'lake')
        """)
        print("DuckLake reconnected after warm-up failure", flush=True)
    # Disable compactor to avoid crash on orphaned snapshots
    try:
        conn.execute("CALL lake.set_option('enable_compaction', 'false')")
        print("DuckLake compaction disabled", flush=True)
    except duckdb.Error as e:
        print(f"Could not disable compaction: {e}", flush=True)

    # DuckLake catalog options (persistent in Postgres, idempotent)
    _ducklake_opts = [
        ("parquet_compression", "zstd"),
        ("parquet_compression_level", "3"),
        ("expire_older_than", "7 days"),
        ("rewrite_delete_threshold", "0.10"),
    ]
    for opt_name, opt_val in _ducklake_opts:
        try:
            conn.execute(f"CALL lake.set_option('{opt_name}', '{opt_val}')")
        except duckdb.Error:
            pass
    print("DuckLake catalog options applied", flush=True)

    # Sorted tables for date-filtered queries (persistent in catalog, idempotent)
    _sorted_tables = [
        ("health", "restaurant_inspections", "inspection_date DESC"),
        ("housing", "hpd_violations", "novissueddate DESC"),
        ("housing", "hpd_complaints", "received_date DESC"),
        ("housing", "dob_ecb_violations", "issue_date DESC"),
        ("public_safety", "nypd_complaints_historic", "cmplnt_fr_dt DESC"),
        # ("public_safety", "motor_vehicle_collisions", "crash_date DESC"),  # not loaded yet
        ("social_services", "n311_service_requests", "created_date DESC"),
        ("city_government", "oath_hearings", "hearing_date DESC"),
        ("transportation", "mta_daily_ridership", "date DESC"),
    ]
    for schema, table, sort_key in _sorted_tables:
        try:
            conn.execute(f"ALTER TABLE lake.{schema}.{table} SET SORTED BY ({sort_key})")
        except duckdb.Error as e:
            print(f"Warning: sorted table {schema}.{table}: {e}", flush=True)
    print("Sorted table keys applied", flush=True)

    # CHECKPOINT: compact files + apply sort orders + expire old snapshots
    try:
        conn.execute("CHECKPOINT lake")
        print("DuckLake CHECKPOINT completed", flush=True)
    except duckdb.Error as e:
        print(f"Warning: CHECKPOINT failed: {e}", flush=True)

    # Start DuckDB UI on :8080 (same process, shared connection)
    try:
        conn.execute("FORCE INSTALL ui; LOAD ui;")
        conn.execute("SET ui_local_port = 8080")
        conn.execute("CALL start_ui_server()")
        print("DuckDB UI running on :8080", flush=True)
    except duckdb.Error as e:
        print(f"Warning: DuckDB UI unavailable: {e}", flush=True)

    # ---------------------------------------------------------------------------
    # JSON-parsed views — extract lat/lon from GeoJSON and legacy location cols
    # ---------------------------------------------------------------------------
    _JSON_VIEWS = [
        # GeoJSON Point columns → lat, lon
        # Format: (schema, table, json_col, geojson_type)
        #   geojson_type: "geojson_point" → $.coordinates[0]=lon, $.coordinates[1]=lat
        #                 "legacy_latlon" → $.latitude=lat, $.longitude=lon
        #                 "url_object"    → $.url, $.description

        # -- business --
        ("business", "nys_contractor_registry", "georeference", "geojson_point"),
        ("business", "nys_liquor_authority", "georeference", "geojson_point"),
        # -- environment --
        ("environment", "lead_service_lines", "location", "geojson_point"),
        ("environment", "nys_solar", "georeference", "geojson_point"),
        ("environment", "waste_transfer", "point", "geojson_point"),
        # -- health --
        ("health", "cdc_places", "geolocation", "geojson_point"),
        ("health", "restaurant_inspections", "location", "geojson_point"),
        # -- housing --
        ("housing", "designated_buildings", "the_geom", "geojson_point"),
        # -- public_safety --
        ("public_safety", "criminal_court_summons", "geocoded_column", "geojson_point"),
        ("public_safety", "nypd_arrests_historic", "lon_lat", "geojson_point"),
        ("public_safety", "nypd_arrests_ytd", "geocoded_column", "geojson_point"),
        ("public_safety", "nypd_complaints_ytd", "geocoded_column", "geojson_point"),
        # -- recreation --
        ("recreation", "canine_waste", "point", "geojson_point"),
        ("recreation", "drinking_fountains", "the_geom", "geojson_point"),
        ("recreation", "fishing_sites", "the_geom", "geojson_point"),
        ("recreation", "play_areas", "point", "geojson_point"),
        ("recreation", "signs", "point", "geojson_point"),
        ("recreation", "spray_showers", "point", "geojson_point"),
        # -- social_services --
        ("social_services", "dycd_program_sites", "geocoded_column", "geojson_point"),
        ("social_services", "literacy_programs", "location_1", "geojson_point"),
        ("social_services", "n311_service_requests", "location", "geojson_point"),
        ("social_services", "nys_child_care", "georeference", "geojson_point"),
        # -- transportation --
        ("transportation", "mta_entrances", "entrance_georeference", "geojson_point"),
        ("transportation", "pedestrian_ramps", "the_geom", "geojson_point"),
        # -- financial --
        ("financial", "nys_child_support_warrants", "georeference", "geojson_point"),
        ("financial", "nys_notaries", "georeference", "geojson_point"),
        ("financial", "nys_tax_warrants", "georeference", "geojson_point"),

        # Legacy Socrata location objects → lat, lon
        ("health", "health_facilities", "facility_location", "legacy_latlon"),
        ("health", "rodent_inspections", "location", "legacy_latlon"),
        ("public_safety", "nypd_complaints_historic", "lat_lon", "legacy_latlon"),
        ("public_safety", "nypd_complaints_ytd", "lat_lon", "legacy_latlon"),

        # Complex geometries (MultiPolygon, Polygon, MultiLineString) → centroid
        # Extracts first coordinate pair as approximate anchor point
        ("environment", "flood_vulnerability", "the_geom", "geojson_first_coord"),
        ("health", "covid_by_zip", "the_geom", "geojson_first_coord"),
        ("recreation", "active_passive_rec", "shape", "geojson_first_coord"),
        ("recreation", "functional_parkland", "multipolygon", "geojson_first_coord"),
        ("recreation", "permit_areas", "multipolygon", "geojson_first_coord"),
        ("recreation", "pools", "polygon", "geojson_first_coord"),
        ("recreation", "properties", "multipolygon", "geojson_first_coord"),
        ("recreation", "restrictive_declarations", "the_geom", "geojson_first_coord"),
        ("recreation", "synthetic_turf", "shape", "geojson_first_coord"),
        ("recreation", "waterfront", "the_geom", "geojson_first_coord"),
        ("recreation", "waterfront_access", "the_geom", "geojson_first_coord"),
        ("social_services", "community_gardens", "multipolygon", "geojson_first_coord"),
        ("transportation", "pavement_rating", "the_geom", "geojson_first_coord"),
        ("transportation", "pothole_orders", "the_geom", "geojson_first_coord"),

        # URL/link objects → url, link_description
        ("city_government", "contract_awards", "document_links", "url_object"),
        ("city_government", "nys_coelig_enforcement", "status", "url_object"),
        ("environment", "oer_cleanup", "project_specific_document", "url_object"),
        ("financial", "nys_tax_warrants", "url", "url_object"),
        ("recreation", "nys_farmers_markets", "market_link", "url_object"),
        ("social_services", "community_orgs", "website", "url_object"),
        ("social_services", "nys_child_care", "additional_information", "url_object"),
    ]

    # Group by (schema, table) to create one view per table
    from collections import defaultdict
    _view_groups = defaultdict(list)
    for schema, table, col, kind in _JSON_VIEWS:
        _view_groups[(schema, table)].append((col, kind))

    json_views_created = 0
    json_views_failed = 0
    for (schema, table), cols_list in _view_groups.items():
        extra_cols = []
        for col, kind in cols_list:
            if kind == "geojson_point":
                extra_cols.append(
                    f"TRY_CAST(json_extract_string({col}, '$.coordinates[0]') AS DOUBLE) AS {col}_lon"
                )
                extra_cols.append(
                    f"TRY_CAST(json_extract_string({col}, '$.coordinates[1]') AS DOUBLE) AS {col}_lat"
                )
            elif kind == "legacy_latlon":
                extra_cols.append(
                    f"TRY_CAST(json_extract_string({col}, '$.latitude') AS DOUBLE) AS {col}_lat"
                )
                extra_cols.append(
                    f"TRY_CAST(json_extract_string({col}, '$.longitude') AS DOUBLE) AS {col}_lon"
                )
            elif kind == "geojson_first_coord":
                # For MultiPolygon/Polygon/MultiLineString, grab the first coordinate pair
                # MultiPolygon: $.coordinates[0][0][0] = [lon, lat]
                # Polygon: $.coordinates[0][0] = [lon, lat]
                # MultiLineString: $.coordinates[0][0] = [lon, lat]
                # Use a COALESCE chain to handle all geometry types
                extra_cols.append(
                    f"COALESCE("
                    f"TRY_CAST(json_extract_string({col}, '$.coordinates[0][0][0][0]') AS DOUBLE), "
                    f"TRY_CAST(json_extract_string({col}, '$.coordinates[0][0][0]') AS DOUBLE), "
                    f"TRY_CAST(json_extract_string({col}, '$.coordinates[0][0]') AS DOUBLE)"
                    f") AS {col}_lon"
                )
                extra_cols.append(
                    f"COALESCE("
                    f"TRY_CAST(json_extract_string({col}, '$.coordinates[0][0][0][1]') AS DOUBLE), "
                    f"TRY_CAST(json_extract_string({col}, '$.coordinates[0][0][1]') AS DOUBLE), "
                    f"TRY_CAST(json_extract_string({col}, '$.coordinates[0][1]') AS DOUBLE)"
                    f") AS {col}_lat"
                )
            elif kind == "url_object":
                extra_cols.append(
                    f"json_extract_string({col}, '$.url') AS {col}_url"
                )
                extra_cols.append(
                    f"json_extract_string({col}, '$.description') AS {col}_desc"
                )

        extra_sql = ",\n           ".join(extra_cols)
        view_sql = (
            f"CREATE OR REPLACE VIEW lake.{schema}.v_{table} AS\n"
            f"SELECT *,\n"
            f"           {extra_sql}\n"
            f"FROM lake.{schema}.{table}"
        )
        try:
            conn.execute(view_sql)
            json_views_created += 1
        except duckdb.Error as e:
            json_views_failed += 1
            print(f"Warning: JSON view {schema}.v_{table} failed: {e}", flush=True)

    print(f"JSON-parsed views: {json_views_created} created, {json_views_failed} failed", flush=True)

    # Lock configuration so sql_query can't change settings
    try:
        conn.execute("SET lock_configuration = true")
    except duckdb.Error:
        pass

    catalog = _build_catalog(conn)
    schema_count = len(catalog)
    table_count = sum(len(t) for t in catalog.values())
    print(f"Catalog cached: {table_count} tables across {schema_count} schemas", flush=True)

    # FTS removed — queries hit lake tables directly via ILIKE (NVMe is fast enough)
    # Marriage index queried via read_parquet() — no in-memory copy needed
    MARRIAGE_PARQUET = "/data/common-ground/nyc_marriage_index.parquet"
    import pathlib
    marriage_available = pathlib.Path(MARRIAGE_PARQUET).exists()
    if marriage_available:
        print(f"Marriage index available at {MARRIAGE_PARQUET} (queried on demand)", flush=True)
    else:
        print(f"Warning: Marriage index not found at {MARRIAGE_PARQUET}", flush=True)
    print("FTS in-memory copies removed — queries use lake tables directly", flush=True)

    # Build DuckPGQ property graph — landlord-building-violation network
    # Tables are cached as Parquet on the volume; rebuild only when cache is stale
    graph_ready = False
    GRAPH_CACHE_DIR = "/data/common-ground/graph-cache"
    GRAPH_TABLES = [
        "graph_owners", "graph_buildings", "graph_owns",
        "graph_violations", "graph_has_violation", "graph_shared_owner",
        "graph_building_flags", "graph_acris_sales", "graph_rent_stabilization",
        "graph_corp_contacts", "graph_business_at_building", "graph_acris_chain",
        "graph_dob_owners", "graph_eviction_petitioners",
        "graph_doing_business", "graph_campaign_donors",
        "graph_epa_facilities",
        # Transaction network
        "graph_tx_entities", "graph_tx_edges", "graph_tx_shared",
        # Corporate web
        "graph_corps", "graph_corp_people", "graph_corp_officer_edges", "graph_corp_shared_officer",
        # Influence network
        "graph_pol_entities", "graph_pol_donations", "graph_pol_contracts", "graph_pol_lobbying",
        # Contractor network
        "graph_contractors", "graph_permit_edges", "graph_contractor_shared",
        # FEC + litigation
        "graph_fec_contributions", "graph_litigation_respondents",
        # Officer misconduct network
        "graph_officers", "graph_officer_complaints", "graph_officer_shared_command",
        # COIB pay-to-play network
        "graph_coib_donors", "graph_coib_policymakers", "graph_coib_donor_edges",
        # BIC trade waste network
        "graph_bic_companies", "graph_bic_violation_edges", "graph_bic_shared_bbl",
        # DOB violations (respondent-named)
        "graph_dob_respondents", "graph_dob_respondent_bbl",
    ]

    def _graph_cache_fresh():
        """Check if Parquet cache exists and is < 24 hours old."""
        import pathlib
        cache = pathlib.Path(GRAPH_CACHE_DIR)
        if not cache.exists():
            return False
        marker = cache / "_built_at.txt"
        if not marker.exists():
            return False
        try:
            built_ts = float(marker.read_text().strip())
            age_hours = (time.time() - built_ts) / 3600
            return age_hours < 24 and all((cache / f"{t}.parquet").exists() for t in GRAPH_TABLES)
        except (ValueError, OSError):
            return False

    def _load_graph_from_cache(conn):
        """Load all graph tables from Parquet cache."""
        for t in GRAPH_TABLES:
            path = f"{GRAPH_CACHE_DIR}/{t}.parquet"
            conn.execute(f"CREATE OR REPLACE TABLE main.{t} AS SELECT * FROM read_parquet('{path}')")

    def _save_graph_to_cache(conn):
        """Export all graph tables to Parquet for fast restart."""
        import pathlib
        cache = pathlib.Path(GRAPH_CACHE_DIR)
        cache.mkdir(parents=True, exist_ok=True)
        for t in GRAPH_TABLES:
            path = f"{GRAPH_CACHE_DIR}/{t}.parquet"
            conn.execute(f"COPY main.{t} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        (cache / "_built_at.txt").write_text(str(time.time()))

    # --- Persistent embeddings on volume (Lance format) ---
    # Embeddings persist as Lance datasets on /data/common-ground/lance/ (Docker volume).
    # Lance supports incremental append, ANN indexes, and direct SQL querying.

    def _lance_path(name):
        return f"{LANCE_DIR}/{name}.lance"

    def _lance_exists(name):
        import pathlib
        return pathlib.Path(_lance_path(name)).exists()

    def _lance_count(conn, name):
        try:
            return conn.execute(f"SELECT COUNT(*) FROM '{_lance_path(name)}'").fetchone()[0]
        except Exception:
            return 0

    def _build_catalog_embeddings(bg_conn, embed_batch, dims):
        """Embed schema descriptions — always rebuilt (tiny, ~12 rows)."""
        rows = []
        for schema_name, description in SCHEMA_DESCRIPTIONS.items():
            rows.append((schema_name, "__schema__", description))
        if not rows:
            return
        texts = [r[2] for r in rows]
        vecs = embed_batch(texts)
        bg_conn.execute(f"CREATE OR REPLACE TEMP TABLE _tmp_catalog (schema_name VARCHAR, table_name VARCHAR, description VARCHAR, embedding FLOAT[{dims}])")
        for (s, t, d), vec in zip(rows, vecs):
            bg_conn.execute(f"INSERT INTO _tmp_catalog VALUES (?, ?, ?, ?::FLOAT[{dims}])", [s, t, d, vec.tolist()])
        bg_conn.execute(f"COPY _tmp_catalog TO '{_lance_path('catalog_embeddings')}' (FORMAT lance, mode 'overwrite')")
        bg_conn.execute("DROP TABLE _tmp_catalog")
        print(f"  Catalog embeddings: {len(rows)} rows (rebuilt → Lance)", flush=True)

    def _build_description_embeddings(conn, bg_conn, embed_batch, dims):
        """Embed complaint/violation descriptions — incremental append."""
        sources = [
            ("311", """
                SELECT DISTINCT (complaint_type || ': ' || COALESCE(descriptor, '')) AS description
                FROM lake.social_services.n311_service_requests
                WHERE complaint_type IS NOT NULL LIMIT 3000
            """),
            ("restaurant", """
                SELECT DISTINCT violation_description AS description
                FROM lake.health.restaurant_inspections
                WHERE violation_description IS NOT NULL LIMIT 500
            """),
            ("hpd", """
                SELECT DISTINCT novdescription AS description
                FROM lake.housing.hpd_violations
                WHERE novdescription IS NOT NULL LIMIT 1000
            """),
            ("oath", """
                SELECT DISTINCT charge_1_code_description AS description
                FROM lake.city_government.oath_hearings
                WHERE charge_1_code_description IS NOT NULL LIMIT 500
            """),
        ]
        total_new = 0
        for source_name, sql in sources:
            try:
                with _db_lock:
                    all_descs = {r[0] for r in conn.execute(sql).fetchall() if r[0]}
                existing = set()
                if _lance_exists("description_embeddings"):
                    try:
                        existing = {r[0] for r in bg_conn.execute(
                            f"SELECT description FROM '{_lance_path('description_embeddings')}' WHERE source = '{source_name}'"
                        ).fetchall()}
                    except Exception:
                        pass
                new_descs = sorted(all_descs - existing)
                if not new_descs:
                    print(f"  Descriptions [{source_name}]: {len(existing)} existing, 0 new", flush=True)
                    continue
                print(f"  Descriptions [{source_name}]: {len(existing)} existing, {len(new_descs)} new — embedding...", flush=True)
                vecs = embed_batch(new_descs)
                import pyarrow as pa
                tbl = pa.table({
                    "source": [source_name] * len(new_descs),
                    "description": new_descs,
                    "embedding": [v.tolist() for v in vecs],
                })
                bg_conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_desc AS SELECT * FROM tbl")
                mode = "append" if _lance_exists("description_embeddings") else "overwrite"
                bg_conn.execute(f"COPY _tmp_desc TO '{_lance_path('description_embeddings')}' (FORMAT lance, mode '{mode}')")
                bg_conn.execute("DROP TABLE _tmp_desc")
                total_new += len(new_descs)
            except Exception as e:
                print(f"  Warning: descriptions [{source_name}] failed: {e}", flush=True)
        print(f"  Description embeddings: {total_new} new rows → Lance", flush=True)

    def _build_entity_name_embeddings(conn, bg_conn, embed_batch, dims):
        """Embed entity names from graph tables — incremental append."""
        name_sources = [
            ("owner", "SELECT DISTINCT owner_name AS name FROM main.graph_owners WHERE owner_name IS NOT NULL AND LENGTH(owner_name) > 2"),
            ("corp_officer", "SELECT DISTINCT person_name AS name FROM main.graph_corp_people WHERE person_name IS NOT NULL AND LENGTH(person_name) > 2"),
            ("donor", "SELECT DISTINCT donor_name AS name FROM main.graph_campaign_donors WHERE donor_name IS NOT NULL AND LENGTH(donor_name) > 2"),
            ("tx_party", "SELECT DISTINCT entity_name AS name FROM main.graph_tx_entities WHERE entity_name IS NOT NULL AND LENGTH(entity_name) > 2"),
        ]
        total_new = 0
        for source_name, sql in name_sources:
            try:
                with _db_lock:
                    all_names = {r[0] for r in conn.execute(sql).fetchall() if r[0]}
                existing = set()
                if _lance_exists("entity_name_embeddings"):
                    try:
                        existing = {r[0] for r in bg_conn.execute(
                            f"SELECT name FROM '{_lance_path('entity_name_embeddings')}' WHERE source = '{source_name}'"
                        ).fetchall()}
                    except Exception:
                        pass
                new_names = sorted(all_names - existing)
                if not new_names:
                    print(f"  Names [{source_name}]: {len(existing):,} existing, 0 new", flush=True)
                    continue
                print(f"  Names [{source_name}]: {len(existing):,} existing, {len(new_names):,} new — embedding...", flush=True)
                vecs = embed_batch(new_names)
                import pyarrow as pa
                tbl = pa.table({"source": [source_name] * len(new_names), "name": new_names, "embedding": [v.tolist() for v in vecs]})
                bg_conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_names AS SELECT * FROM tbl")
                mode = "append" if _lance_exists("entity_name_embeddings") else "overwrite"
                bg_conn.execute(f"COPY _tmp_names TO '{_lance_path('entity_name_embeddings')}' (FORMAT lance, mode '{mode}')")
                bg_conn.execute("DROP TABLE _tmp_names")
                total_new += len(new_names)
                print(f"  Names [{source_name}]: {len(new_names):,} embedded → Lance", flush=True)
            except Exception as e:
                print(f"  Warning: names [{source_name}] failed: {e}", flush=True)
        print(f"  Entity name embeddings: {total_new:,} new rows total → Lance", flush=True)

    def _build_building_vectors(conn, bg_conn):
        """Build numeric feature vectors for building similarity (no embeddings needed)."""
        try:
            with _db_lock:
                result = conn.execute("""
                    WITH base AS (
                        SELECT
                            b.bbl, b.borough,
                            COALESCE(TRY_CAST(b.stories AS DOUBLE), 0) AS stories,
                            COALESCE(TRY_CAST(b.units AS DOUBLE), 0) AS units,
                            COALESCE(TRY_CAST(b.year_built AS DOUBLE), 0) AS year_built,
                            COUNT(v.violation_id) AS viol_count,
                            COUNT(CASE WHEN v.currentstatus = 'Open' THEN 1 END) AS open_viol_count
                        FROM main.graph_buildings b
                        LEFT JOIN main.graph_violations v ON b.bbl = v.bbl
                        GROUP BY b.bbl, b.borough, b.stories, b.units, b.year_built
                    ),
                    normed AS (
                        SELECT bbl, borough,
                            stories / NULLIF(MAX(stories) OVER (), 0) AS n1,
                            units / NULLIF(MAX(units) OVER (), 0) AS n2,
                            year_built / NULLIF(MAX(year_built) OVER (), 0) AS n3,
                            viol_count / NULLIF(MAX(viol_count) OVER (), 0) AS n4,
                            open_viol_count / NULLIF(MAX(open_viol_count) OVER (), 0) AS n5,
                            CASE WHEN units > 0 THEN viol_count::DOUBLE / units ELSE 0 END /
                                NULLIF(MAX(CASE WHEN units > 0 THEN viol_count::DOUBLE / units ELSE 0 END) OVER (), 0) AS n6
                        FROM base
                    )
                    SELECT bbl, borough,
                        [COALESCE(n1,0), COALESCE(n2,0), COALESCE(n3,0),
                         COALESCE(n4,0), COALESCE(n5,0), COALESCE(n6,0)]::FLOAT[] AS features
                    FROM normed
                """).fetchall()
            if not result:
                return
            import pyarrow as pa
            tbl = pa.table({
                "bbl": [r[0] for r in result],
                "borough": [r[1] for r in result],
                "features": [list(r[2]) for r in result],
            })
            bg_conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_bv AS SELECT * FROM tbl")
            bg_conn.execute(f"COPY (SELECT bbl, borough, features::FLOAT[] AS features FROM _tmp_bv) TO '{_lance_path('building_vectors')}' (FORMAT lance, mode 'overwrite')")
            bg_conn.execute("DROP TABLE _tmp_bv")
            print(f"  Building vectors: {len(result):,} rows (rebuilt → Lance)", flush=True)
        except Exception as e:
            print(f"  Warning: building vectors failed: {e}", flush=True)

    def _create_lance_indexes(bg_conn):
        try:
            if _lance_count(bg_conn, "entity_name_embeddings") > 10000:
                bg_conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS name_emb_idx
                    ON '{_lance_path("entity_name_embeddings")}' (embedding)
                    USING IVF_FLAT WITH (num_partitions=32, metric_type='cosine')
                """)
                print("  ANN index created on entity_name_embeddings", flush=True)
        except Exception as e:
            print(f"  Warning: Lance ANN index failed: {e}", flush=True)

    try:
        if _graph_cache_fresh():
            print("Loading graph tables from Parquet cache...", flush=True)
            _load_graph_from_cache(conn)
            owner_count = conn.execute("SELECT COUNT(*) FROM main.graph_owners").fetchone()[0]
            building_count = conn.execute("SELECT COUNT(*) FROM main.graph_buildings").fetchone()[0]
            violation_count = conn.execute("SELECT COUNT(*) FROM main.graph_violations").fetchone()[0]
            owns_count = conn.execute("SELECT COUNT(*) FROM main.graph_owns").fetchone()[0]
            shared_count = conn.execute("SELECT COUNT(*) FROM main.graph_shared_owner").fetchone()[0]
            print(f"Graph loaded from cache: {owner_count:,} owners, {building_count:,} buildings, "
                  f"{violation_count:,} violations", flush=True)
        else:
            print("Building graph tables from lake (no cache or stale)...", flush=True)

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_owners AS
                SELECT
                    j.registrationid AS owner_id,
                    j.registrationid,
                    COALESCE(c.corporationname,
                        NULLIF(TRIM(COALESCE(c.firstname, '') || ' ' || COALESCE(c.lastname, '')), '')
                    ) AS owner_name
                FROM (
                    SELECT DISTINCT registrationid
                    FROM lake.housing.hpd_jurisdiction
                    WHERE registrationid IS NOT NULL
                      AND registrationid != '0'
                      AND registrationid NOT LIKE '%995'
                ) j
                LEFT JOIN (
                    SELECT registrationid,
                           FIRST(corporationname) AS corporationname,
                           FIRST(firstname) AS firstname,
                           FIRST(lastname) AS lastname
                    FROM lake.housing.hpd_registration_contacts
                    WHERE type IN ('CorporateOwner', 'IndividualOwner')
                    GROUP BY registrationid
                ) c ON j.registrationid::VARCHAR = c.registrationid::VARCHAR
            """)
            owner_count = conn.execute("SELECT COUNT(*) FROM main.graph_owners").fetchone()[0]

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_buildings AS
                SELECT
                    bbl,
                    FIRST(housenumber) AS housenumber,
                    FIRST(streetname) AS streetname,
                    FIRST(zip) AS zip,
                    FIRST(boroid) AS boroid,
                    MAX(COALESCE(TRY_CAST(legalclassa AS INTEGER), 0)
                      + COALESCE(TRY_CAST(legalclassb AS INTEGER), 0)) AS total_units,
                    MAX(TRY_CAST(legalstories AS INTEGER)) AS stories
                FROM (
                    SELECT *,
                        (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
                    FROM lake.housing.hpd_jurisdiction
                    WHERE boroid IS NOT NULL
                )
                GROUP BY bbl
            """)
            building_count = conn.execute("SELECT COUNT(*) FROM main.graph_buildings").fetchone()[0]

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_owns AS
                SELECT DISTINCT
                    registrationid AS owner_id,
                    (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
                FROM lake.housing.hpd_jurisdiction
                WHERE registrationid IS NOT NULL
                  AND registrationid != '0'
                  AND registrationid NOT LIKE '%995'
                  AND boroid IS NOT NULL
            """)
            owns_count = conn.execute("SELECT COUNT(*) FROM main.graph_owns").fetchone()[0]

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_violations AS
                SELECT
                    violation_id,
                    FIRST(bbl) AS bbl,
                    FIRST(severity) AS severity,
                    FIRST(status) AS status,
                    FIRST(issued_date) AS issued_date
                FROM (
                    SELECT
                        violationid AS violation_id,
                        bbl,
                        UPPER(class) AS severity,
                        violationstatus AS status,
                        TRY_CAST(novissueddate AS DATE) AS issued_date
                    FROM lake.housing.hpd_violations
                    WHERE violationid IS NOT NULL AND bbl IS NOT NULL
                )
                GROUP BY violation_id
            """)
            violation_count = conn.execute("SELECT COUNT(*) FROM main.graph_violations").fetchone()[0]

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_has_violation AS
                SELECT DISTINCT
                    bbl,
                    violationid AS violation_id
                FROM lake.housing.hpd_violations
                WHERE violationid IS NOT NULL AND bbl IS NOT NULL
            """)

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_shared_owner AS
                SELECT DISTINCT a.bbl AS bbl1, b.bbl AS bbl2
                FROM main.graph_owns a
                JOIN main.graph_owns b ON a.owner_id = b.owner_id AND a.bbl < b.bbl
            """)
            shared_count = conn.execute("SELECT COUNT(*) FROM main.graph_shared_owner").fetchone()[0]

            # --- Enrichment: building flags (AEP, litigations, tax liens, DOB) ---
            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_building_flags AS
                WITH aep AS (
                    SELECT DISTINCT bbl, 1 AS is_aep,
                           FIRST(current_status) AS aep_status,
                           FIRST(aep_start_date) AS aep_start_date
                    FROM lake.housing.aep_buildings
                    WHERE bbl IS NOT NULL
                    GROUP BY bbl
                ),
                litigations AS (
                    SELECT bbl,
                           COUNT(*) AS litigation_count,
                           COUNT(*) FILTER (WHERE findingofharassment IS NOT NULL
                               AND findingofharassment != '') AS harassment_findings,
                           MAX(TRY_CAST(caseopendate AS DATE)) AS latest_litigation_date
                    FROM lake.housing.hpd_litigations
                    WHERE bbl IS NOT NULL
                    GROUP BY bbl
                ),
                tax_liens AS (
                    SELECT
                        (borough || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl,
                        COUNT(*) AS lien_count
                    FROM lake.housing.tax_lien_sales
                    WHERE borough IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
                    GROUP BY 1
                ),
                dob_v AS (
                    SELECT
                        (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl,
                        COUNT(*) AS dob_violation_count
                    FROM lake.housing.dob_ecb_violations
                    WHERE boro IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
                    GROUP BY 1
                )
                SELECT b.bbl,
                       COALESCE(a.is_aep, 0) AS is_aep,
                       a.aep_status,
                       a.aep_start_date,
                       COALESCE(l.litigation_count, 0) AS litigation_count,
                       COALESCE(l.harassment_findings, 0) AS harassment_findings,
                       l.latest_litigation_date,
                       COALESCE(t.lien_count, 0) AS lien_count,
                       COALESCE(d.dob_violation_count, 0) AS dob_violation_count
                FROM main.graph_buildings b
                LEFT JOIN aep a ON b.bbl = a.bbl
                LEFT JOIN litigations l ON b.bbl = l.bbl
                LEFT JOIN tax_liens t ON b.bbl = t.bbl
                LEFT JOIN dob_v d ON b.bbl = d.bbl
            """)
            flag_count = conn.execute(
                "SELECT COUNT(*) FROM main.graph_building_flags WHERE is_aep=1 OR litigation_count>0 OR lien_count>0"
            ).fetchone()[0]
            print(f"Building flags built: {flag_count:,} flagged buildings "
                  f"(AEP/litigation/tax-lien/DOB)", flush=True)

            # --- Enrichment: ACRIS latest deed sale per BBL ---
            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_acris_sales AS
                WITH deed_docs AS (
                    SELECT m.document_id, m.doc_type,
                           TRY_CAST(m.document_amt AS DOUBLE) AS sale_price,
                           TRY_CAST(m.document_date AS DATE) AS sale_date,
                           (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl
                    FROM lake.housing.acris_master m
                    JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
                    WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
                      AND m.document_amt IS NOT NULL
                      AND TRY_CAST(m.document_amt AS DOUBLE) > 0
                      AND l.borough IS NOT NULL
                ),
                ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY bbl ORDER BY sale_date DESC) AS rn
                    FROM deed_docs
                )
                SELECT bbl, sale_price AS last_sale_price, sale_date AS last_sale_date,
                       document_id AS last_deed_id
                FROM ranked WHERE rn = 1
            """)
            acris_count = conn.execute("SELECT COUNT(*) FROM main.graph_acris_sales").fetchone()[0]
            print(f"ACRIS sales built: {acris_count:,} properties with deed history", flush=True)

            # --- Enrichment: rent stabilization (taxbills.nyc 2007-2017 + JustFix 2018-2024) ---
            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_rent_stabilization AS
                WITH taxbills AS (
                    SELECT
                        LPAD(ucbbl::VARCHAR, 10, '0') AS bbl,
                        unitsstab2007 AS stab_2007,
                        unitsstab2017 AS stab_2017,
                        diff AS stab_change_2007_2017,
                        unitstotal AS total_units,
                        address, ownername, yearbuilt
                    FROM read_csv_auto('https://taxbillsnyc.s3.amazonaws.com/changes-summary.csv')
                ),
                justfix AS (
                    SELECT
                        LPAD(ucbbl::VARCHAR, 10, '0') AS bbl,
                        uc2018 AS stab_2018, uc2019 AS stab_2019,
                        uc2020 AS stab_2020, uc2021 AS stab_2021,
                        uc2022 AS stab_2022, uc2023 AS stab_2023,
                        uc2024 AS stab_2024
                    FROM read_csv_auto('https://s3.amazonaws.com/justfix-data/rentstab_counts_from_doffer_2024.csv')
                )
                SELECT
                    COALESCE(t.bbl, j.bbl) AS bbl,
                    t.stab_2007, t.stab_2017,
                    j.stab_2018, j.stab_2019, j.stab_2020,
                    j.stab_2021, j.stab_2022, j.stab_2023, j.stab_2024,
                    COALESCE(j.stab_2024, j.stab_2023, t.stab_2017) AS latest_stab_units,
                    COALESCE(t.stab_2007, j.stab_2018) AS earliest_stab_units,
                    t.total_units, t.address, t.ownername, t.yearbuilt
                FROM taxbills t
                FULL OUTER JOIN justfix j ON t.bbl = j.bbl
            """)
            stab_count = conn.execute("SELECT COUNT(*) FROM main.graph_rent_stabilization").fetchone()[0]
            destab = conn.execute("""
                SELECT COUNT(*) FROM main.graph_rent_stabilization
                WHERE earliest_stab_units > 0
                  AND (latest_stab_units IS NULL OR latest_stab_units = 0)
            """).fetchone()[0]
            print(f"Rent stabilization built: {stab_count:,} buildings, "
                  f"{destab:,} fully destabilized", flush=True)

            # --- Enrichment: NYS corp contacts matched to graph owners ---
            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_corp_contacts AS
                WITH corp_owners AS (
                    SELECT owner_id, owner_name
                    FROM main.graph_owners
                    WHERE owner_name IS NOT NULL
                      AND (owner_name LIKE '%LLC%' OR owner_name LIKE '%CORP%'
                           OR owner_name LIKE '%INC%' OR owner_name LIKE '% LP%'
                           OR owner_name LIKE '%TRUST%' OR owner_name LIKE '%ASSOC%'
                           OR owner_name LIKE '%REALTY%' OR owner_name LIKE '%MGMT%'
                           OR owner_name LIKE '%HOLDING%' OR owner_name LIKE '%GROUP%')
                )
                SELECT
                    co.owner_id,
                    co.owner_name,
                    c.dos_id,
                    c.current_entity_name,
                    c.entity_type,
                    c.initial_dos_filing_date,
                    c.dos_process_name,
                    c.dos_process_address_1,
                    c.dos_process_city,
                    c.dos_process_state,
                    c.registered_agent_name,
                    c.chairman_name,
                    c.county
                FROM corp_owners co
                JOIN lake.business.nys_corporations c
                    ON UPPER(c.current_entity_name) = UPPER(co.owner_name)
            """)
            corp_match_count = conn.execute("SELECT COUNT(*) FROM main.graph_corp_contacts").fetchone()[0]
            unique_owners = conn.execute("SELECT COUNT(DISTINCT owner_id) FROM main.graph_corp_contacts").fetchone()[0]
            print(f"Corp contacts built: {corp_match_count:,} matches across "
                  f"{unique_owners:,} graph owners", flush=True)

            # --- Enrichment: business licenses at graph buildings ---
            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_business_at_building AS
                SELECT
                    bbl,
                    business_name,
                    dba_trade_name,
                    business_category,
                    license_type,
                    license_status,
                    license_creation_date,
                    lic_expir_dd
                FROM lake.business.issued_licenses
                WHERE bbl IS NOT NULL AND bbl != ''
                  AND license_status IN ('Active', 'Expired')
            """)
            biz_count = conn.execute("SELECT COUNT(*) FROM main.graph_business_at_building").fetchone()[0]
            biz_bbls = conn.execute("SELECT COUNT(DISTINCT bbl) FROM main.graph_business_at_building").fetchone()[0]
            print(f"Business licenses built: {biz_count:,} licenses across "
                  f"{biz_bbls:,} buildings", flush=True)

            # --- Enrichment: ACRIS ownership chain (recent buyers/sellers per BBL) ---
            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_acris_chain AS
                WITH ranked AS (
                    SELECT
                        (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                        p.name AS party_name,
                        CASE WHEN p.party_type = '1' THEN 'SELLER'
                             WHEN p.party_type = '2' THEN 'BUYER'
                             ELSE 'OTHER' END AS role,
                        m.doc_type,
                        TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                        TRY_CAST(m.document_date AS DATE) AS doc_date,
                        p.address_1,
                        p.city,
                        p.state,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')),
                                p.party_type
                            ORDER BY m.document_date DESC
                        ) AS rn
                    FROM lake.housing.acris_parties p
                    JOIN lake.housing.acris_master m ON p.document_id = m.document_id
                    JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
                    WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
                      AND l.borough IS NOT NULL
                      AND p.name IS NOT NULL AND TRIM(p.name) != ''
                )
                SELECT bbl, party_name, role, doc_type, amount, doc_date,
                       address_1, city, state
                FROM ranked
                WHERE rn <= 3
            """)
            chain_count = conn.execute("SELECT COUNT(*) FROM main.graph_acris_chain").fetchone()[0]
            chain_bbls = conn.execute("SELECT COUNT(DISTINCT bbl) FROM main.graph_acris_chain").fetchone()[0]
            print(f"ACRIS chain built: {chain_count:,} party records across "
                  f"{chain_bbls:,} buildings", flush=True)

            # --- Enrichment: DOB permit application owners (different entity than HPD) ---
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_dob_owners AS
                    SELECT
                        owner_s_first_name,
                        owner_s_last_name,
                        TRIM(COALESCE(owner_s_first_name, '') || ' ' || COALESCE(owner_s_last_name, '')) AS owner_name,
                        owner_s_business_name AS business_name,
                        TRIM(COALESCE(owner_s_house_number, '') || ' ' || COALESCE(owner_shouse_street_name, '')) AS owner_address,
                        cityx AS owner_city,
                        state AS owner_state,
                        zip AS owner_zip
                    FROM lake.housing.dob_application_owners
                    WHERE owner_s_first_name IS NOT NULL OR owner_s_business_name IS NOT NULL
                """)
                dob_own_count = conn.execute("SELECT COUNT(*) FROM main.graph_dob_owners").fetchone()[0]
                print(f"DOB owners built: {dob_own_count:,} records", flush=True)
            except Exception as e:
                print(f"DOB owners skipped (table not yet loaded): {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_dob_owners AS SELECT NULL AS owner_name, NULL AS business_name WHERE FALSE")

            # --- Enrichment: eviction petitioners (landlords who file evictions) ---
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_eviction_petitioners AS
                    SELECT
                        bbl,
                        court_index_number,
                        docket_number,
                        eviction_address,
                        eviction_zip,
                        executed_date,
                        residential_commercial_ind AS property_type
                    FROM lake.housing.evictions
                    WHERE bbl IS NOT NULL
                """)
                evict_count = conn.execute("SELECT COUNT(*) FROM main.graph_eviction_petitioners").fetchone()[0]
                evict_bbls = conn.execute("SELECT COUNT(DISTINCT bbl) FROM main.graph_eviction_petitioners").fetchone()[0]
                print(f"Eviction records built: {evict_count:,} records across "
                      f"{evict_bbls:,} buildings", flush=True)
            except Exception as e:
                print(f"Evictions skipped (table not yet loaded): {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_eviction_petitioners AS SELECT NULL AS bbl WHERE FALSE")

            # --- Enrichment: Doing Business disclosures (city contractor principals) ---
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_doing_business AS
                    WITH people AS (
                        SELECT
                            organization_name,
                            TRIM(COALESCE(person_name_first, '') || ' ' || COALESCE(person_name_last, '')) AS person_name,
                            relationship_type_code AS title,
                            doing_business_start_date AS start_date
                        FROM lake.city_government.doing_business_people
                        WHERE organization_name IS NOT NULL
                    ),
                    entities AS (
                        SELECT
                            organization_name,
                            ownership_structure_code,
                            organization_phone,
                            doing_business_start_date AS start_date
                        FROM lake.city_government.doing_business_entities
                        WHERE organization_name IS NOT NULL
                    )
                    SELECT
                        COALESCE(p.organization_name, e.organization_name) AS entity_name,
                        p.person_name,
                        p.title,
                        p.start_date,
                        e.ownership_structure_code,
                        e.organization_phone
                    FROM people p
                    FULL OUTER JOIN entities e ON UPPER(p.organization_name) = UPPER(e.organization_name)
                """)
                db_count = conn.execute("SELECT COUNT(*) FROM main.graph_doing_business").fetchone()[0]
                db_entities = conn.execute("SELECT COUNT(DISTINCT entity_name) FROM main.graph_doing_business").fetchone()[0]
                print(f"Doing Business built: {db_count:,} records, "
                      f"{db_entities:,} entities", flush=True)
            except Exception as e:
                print(f"Doing Business skipped (table not yet loaded): {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_doing_business AS SELECT NULL AS entity_name WHERE FALSE")

            # --- Enrichment: campaign donors matched to graph owners ---
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_campaign_donors AS
                    WITH owner_names AS (
                        SELECT DISTINCT UPPER(owner_name) AS name
                        FROM main.graph_owners
                        WHERE owner_name IS NOT NULL AND LENGTH(owner_name) > 3
                    )
                    SELECT
                        c.recipname AS recipient_name,
                        TRIM(COALESCE(c.candfirst, '') || ' ' || COALESCE(c.candmi, '') || ' ' || COALESCE(c.recipname, '')) AS candidate_name,
                        c.officecd AS office,
                        c.name AS donor_name,
                        c.c_code AS contributor_type,
                        TRIM(COALESCE(c.strno, '') || ' ' || COALESCE(c.strname, '')) AS donor_address,
                        c.city AS donor_city,
                        c.state AS donor_state,
                        c.zip AS donor_zip,
                        c.occupation,
                        c.empname AS employer,
                        TRY_CAST(c.amnt AS DOUBLE) AS amount,
                        c.election
                    FROM lake.city_government.campaign_contributions c
                    INNER JOIN owner_names o ON UPPER(c.name) = o.name
                    WHERE c.name IS NOT NULL AND LENGTH(c.name) > 3
                """)
                donor_count = conn.execute("SELECT COUNT(*) FROM main.graph_campaign_donors").fetchone()[0]
                donor_names = conn.execute("SELECT COUNT(DISTINCT donor_name) FROM main.graph_campaign_donors").fetchone()[0]
                print(f"Campaign donors built: {donor_count:,} donations from "
                      f"{donor_names:,} matching owner names", flush=True)
            except Exception as e:
                print(f"Campaign donors skipped (table not yet loaded): {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_campaign_donors AS SELECT NULL AS donor_name WHERE FALSE")

            # --- Enrichment: EPA ECHO regulated facilities in NYC ---
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_epa_facilities AS
                    SELECT
                        registry_id,
                        name AS facility_name,
                        address,
                        city,
                        state,
                        zip,
                        county,
                        lat,
                        lon,
                        current_violation,
                        total_penalties,
                        inspection_count,
                        formal_action_count,
                        last_inspection_date,
                        last_penalty_date,
                        last_penalty_amount,
                        air_flag,
                        cwa_flag,
                        rcra_flag,
                        sdwa_flag
                    FROM lake.federal.epa_echo_facilities
                    WHERE registry_id IS NOT NULL
                """)
                epa_count = conn.execute("SELECT COUNT(*) FROM main.graph_epa_facilities").fetchone()[0]
                print(f"EPA facilities built: {epa_count:,} regulated facilities", flush=True)
            except Exception as e:
                print(f"EPA facilities skipped (table not yet loaded): {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_epa_facilities AS SELECT NULL AS registry_id WHERE FALSE")

            # =================================================================
            # GRAPH 1: Transaction Network (ACRIS deed chains)
            # =================================================================
            try:
                # Entities: anyone involved in 2+ DEED transactions (filters 10.7M → ~500K active entities)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_tx_entities AS
                    WITH deed_parties AS (
                        SELECT p.name, p.party_type,
                               (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                               TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                               TRY_CAST(m.document_date AS DATE) AS doc_date,
                               m.document_id
                        FROM lake.housing.acris_parties p
                        JOIN lake.housing.acris_master m ON p.document_id = m.document_id
                        JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
                        WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
                          AND p.name IS NOT NULL AND LENGTH(TRIM(p.name)) > 3
                          AND l.borough IS NOT NULL
                    ),
                    entity_stats AS (
                        SELECT UPPER(TRIM(name)) AS entity_name,
                               COUNT(DISTINCT document_id) AS tx_count,
                               COUNT(DISTINCT bbl) AS property_count,
                               COUNT(*) FILTER (WHERE party_type = '1') AS as_seller,
                               COUNT(*) FILTER (WHERE party_type = '2') AS as_buyer,
                               MIN(doc_date) AS first_tx,
                               MAX(doc_date) AS last_tx,
                               SUM(amount) AS total_amount
                        FROM deed_parties
                        GROUP BY UPPER(TRIM(name))
                        HAVING COUNT(DISTINCT document_id) >= 2
                    )
                    SELECT entity_name, tx_count, property_count,
                           as_seller, as_buyer, first_tx, last_tx, total_amount
                    FROM entity_stats
                """)
                tx_ent_count = conn.execute("SELECT COUNT(*) FROM main.graph_tx_entities").fetchone()[0]
                print(f"TX entities built: {tx_ent_count:,} entities with 2+ deed transactions", flush=True)

                # Edges: entity → BBL (bought or sold)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_tx_edges AS
                    SELECT DISTINCT
                        UPPER(TRIM(p.name)) AS entity_name,
                        (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                        CASE WHEN p.party_type = '1' THEN 'SOLD'
                             WHEN p.party_type = '2' THEN 'BOUGHT' ELSE 'OTHER' END AS role,
                        TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                        TRY_CAST(m.document_date AS DATE) AS doc_date,
                        m.document_id
                    FROM lake.housing.acris_parties p
                    JOIN lake.housing.acris_master m ON p.document_id = m.document_id
                    JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
                    WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
                      AND p.name IS NOT NULL AND LENGTH(TRIM(p.name)) > 3
                      AND l.borough IS NOT NULL
                      AND UPPER(TRIM(p.name)) IN (SELECT entity_name FROM main.graph_tx_entities)
                """)
                tx_edge_count = conn.execute("SELECT COUNT(*) FROM main.graph_tx_edges").fetchone()[0]
                print(f"TX edges built: {tx_edge_count:,} entity→property edges", flush=True)

                # Shared transactions: entity↔entity on same document (co-signers, buyer-seller pairs)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_tx_shared AS
                    WITH doc_entities AS (
                        SELECT DISTINCT document_id, entity_name, role
                        FROM main.graph_tx_edges
                    )
                    SELECT a.entity_name AS entity1, b.entity_name AS entity2,
                           COUNT(DISTINCT a.document_id) AS shared_docs,
                           FIRST(a.role) AS role1, FIRST(b.role) AS role2
                    FROM doc_entities a
                    JOIN doc_entities b ON a.document_id = b.document_id
                        AND a.entity_name < b.entity_name
                    GROUP BY a.entity_name, b.entity_name
                """)
                tx_shared_count = conn.execute("SELECT COUNT(*) FROM main.graph_tx_shared").fetchone()[0]
                print(f"TX shared built: {tx_shared_count:,} entity↔entity co-transaction edges", flush=True)
            except Exception as e:
                print(f"Warning: Transaction network build failed: {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_tx_entities AS SELECT NULL AS entity_name WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_tx_edges AS SELECT NULL AS entity_name, NULL AS bbl WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_tx_shared AS SELECT NULL AS entity1, NULL AS entity2 WHERE FALSE")

            # =================================================================
            # GRAPH 2: Corporate Web (NYS corps linked to HPD/ACRIS/PLUTO)
            # =================================================================
            try:
                # Corps: filter 8.3M to NYC-relevant (~100K) via HPD + PLUTO matches
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_corps AS
                    WITH relevant_corps AS (
                        SELECT c.dos_id, c.current_entity_name, c.entity_type,
                               c.initial_dos_filing_date, c.county,
                               c.dos_process_name, c.dos_process_address_1, c.dos_process_city,
                               c.registered_agent_name, c.chairman_name
                        FROM lake.business.nys_corporations c
                        WHERE c.current_entity_name IS NOT NULL
                          AND (
                              -- Match HPD landlords
                              UPPER(c.current_entity_name) IN (
                                  SELECT DISTINCT UPPER(owner_name) FROM main.graph_owners
                                  WHERE owner_name IS NOT NULL AND LENGTH(owner_name) > 3
                              )
                              -- Match PLUTO owners
                              OR UPPER(c.current_entity_name) IN (
                                  SELECT DISTINCT UPPER(ownername) FROM lake.city_government.pluto
                                  WHERE ownername IS NOT NULL AND LENGTH(ownername) > 5
                                  AND (UPPER(ownername) LIKE '%LLC%' OR UPPER(ownername) LIKE '%CORP%'
                                       OR UPPER(ownername) LIKE '%INC%' OR UPPER(ownername) LIKE '%LP%')
                              )
                          )
                    )
                    SELECT * FROM relevant_corps
                """)
                corp_count = conn.execute("SELECT COUNT(*) FROM main.graph_corps").fetchone()[0]
                print(f"Corps built: {corp_count:,} NYC-relevant corporations", flush=True)

                # People: officers/agents/chairmen for those corps
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_corp_people AS
                    WITH corp_ids AS (
                        SELECT DISTINCT dos_id FROM main.graph_corps
                    ),
                    raw_people AS (
                        SELECT DISTINCT
                            UPPER(TRIM(a.name)) AS person_name,
                            a.corpid_num AS dos_id,
                            a.addr_type,
                            UPPER(TRIM(a.addr1)) AS address
                        FROM lake.business.nys_entity_addresses a
                        WHERE a.corpid_num IN (SELECT dos_id FROM corp_ids)
                          AND a.name IS NOT NULL AND LENGTH(TRIM(a.name)) > 3
                          AND UPPER(a.name) NOT LIKE '%THE CORPORATION%'
                          AND UPPER(a.name) NOT LIKE '%SECRETARY OF STATE%'
                          AND UPPER(a.name) NOT IN ('NONE', 'NA', 'N/A', 'SAME', 'THE LLC')
                    )
                    SELECT person_name,
                           COUNT(DISTINCT dos_id) AS corp_count,
                           FIRST(address) AS primary_address
                    FROM raw_people
                    GROUP BY person_name
                """)
                corp_people_count = conn.execute("SELECT COUNT(*) FROM main.graph_corp_people").fetchone()[0]
                print(f"Corp people built: {corp_people_count:,} unique people", flush=True)

                # Edges: person → corp (officer_of)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_corp_officer_edges AS
                    SELECT DISTINCT
                        UPPER(TRIM(a.name)) AS person_name,
                        a.corpid_num AS dos_id,
                        a.addr_type AS role_code
                    FROM lake.business.nys_entity_addresses a
                    WHERE a.corpid_num IN (SELECT dos_id FROM main.graph_corps)
                      AND a.name IS NOT NULL AND LENGTH(TRIM(a.name)) > 3
                      AND UPPER(a.name) NOT LIKE '%THE CORPORATION%'
                      AND UPPER(a.name) NOT LIKE '%SECRETARY OF STATE%'
                      AND UPPER(a.name) NOT IN ('NONE', 'NA', 'N/A', 'SAME', 'THE LLC')
                      AND UPPER(TRIM(a.name)) IN (SELECT person_name FROM main.graph_corp_people)
                """)
                corp_edge_count = conn.execute("SELECT COUNT(*) FROM main.graph_corp_officer_edges").fetchone()[0]
                print(f"Corp officer edges built: {corp_edge_count:,}", flush=True)

                # Derived: corp↔corp via shared officers (excluding mega-agents with 100+ corps)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_corp_shared_officer AS
                    WITH eligible_people AS (
                        SELECT person_name FROM main.graph_corp_people
                        WHERE corp_count BETWEEN 2 AND 100
                    )
                    SELECT a.dos_id AS corp1, b.dos_id AS corp2,
                           COUNT(DISTINCT a.person_name) AS shared_officers
                    FROM main.graph_corp_officer_edges a
                    JOIN main.graph_corp_officer_edges b
                        ON a.person_name = b.person_name AND a.dos_id < b.dos_id
                    WHERE a.person_name IN (SELECT person_name FROM eligible_people)
                    GROUP BY a.dos_id, b.dos_id
                """)
                corp_shared_count = conn.execute("SELECT COUNT(*) FROM main.graph_corp_shared_officer").fetchone()[0]
                print(f"Corp shared officer edges built: {corp_shared_count:,}", flush=True)
            except Exception as e:
                print(f"Warning: Corporate web build failed: {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_corps AS SELECT NULL::VARCHAR AS dos_id, NULL::VARCHAR AS current_entity_name, NULL::VARCHAR AS entity_type, NULL::VARCHAR AS initial_dos_filing_date, NULL::VARCHAR AS county, NULL::VARCHAR AS dos_process_name, NULL::VARCHAR AS dos_process_address_1, NULL::VARCHAR AS dos_process_city, NULL::VARCHAR AS registered_agent_name, NULL::VARCHAR AS chairman_name WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_corp_people AS SELECT NULL::VARCHAR AS person_name, 0::BIGINT AS corp_count, NULL::VARCHAR AS primary_address WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_corp_officer_edges AS SELECT NULL::VARCHAR AS person_name, NULL::VARCHAR AS dos_id, NULL::VARCHAR AS role_code WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_corp_shared_officer AS SELECT NULL::VARCHAR AS corp1, NULL::VARCHAR AS corp2, 0::BIGINT AS shared_officers WHERE FALSE")

            # =================================================================
            # GRAPH 3: Influence Network (political pay-to-play)
            # =================================================================
            try:
                # Unified political entities (donors, candidates, lobbyists, vendors)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_pol_entities AS
                    WITH donors AS (
                        SELECT UPPER(TRIM(name)) AS entity_name, 'DONOR' AS entity_type,
                               SUM(TRY_CAST(amnt AS DOUBLE)) AS total_amount,
                               COUNT(*) AS tx_count
                        FROM lake.city_government.campaign_contributions
                        WHERE name IS NOT NULL AND LENGTH(name) > 3
                        GROUP BY UPPER(TRIM(name))
                        HAVING COUNT(*) >= 2 OR SUM(TRY_CAST(amnt AS DOUBLE)) >= 1000
                    ),
                    candidates AS (
                        SELECT UPPER(TRIM(recipname)) AS entity_name, 'CANDIDATE' AS entity_type,
                               SUM(TRY_CAST(amnt AS DOUBLE)) AS total_amount,
                               COUNT(DISTINCT name) AS tx_count
                        FROM lake.city_government.campaign_contributions
                        WHERE recipname IS NOT NULL
                        GROUP BY UPPER(TRIM(recipname))
                    ),
                    vendors AS (
                        SELECT UPPER(TRIM(vendor_name)) AS entity_name, 'VENDOR' AS entity_type,
                               SUM(TRY_CAST(contract_amount AS DOUBLE)) AS total_amount,
                               COUNT(*) AS tx_count
                        FROM lake.city_government.contract_awards
                        WHERE vendor_name IS NOT NULL AND LENGTH(vendor_name) > 3
                        GROUP BY UPPER(TRIM(vendor_name))
                    ),
                    lobbyists AS (
                        SELECT UPPER(TRIM(lobbyist_name)) AS entity_name, 'LOBBYIST' AS entity_type,
                               SUM(TRY_CAST(compensation_total AS DOUBLE)) AS total_amount,
                               COUNT(*) AS tx_count
                        FROM lake.city_government.elobbyist
                        WHERE lobbyist_name IS NOT NULL
                        GROUP BY UPPER(TRIM(lobbyist_name))
                    ),
                    clients AS (
                        SELECT UPPER(TRIM(client_name)) AS entity_name, 'CLIENT' AS entity_type,
                               SUM(TRY_CAST(compensation_total AS DOUBLE)) AS total_amount,
                               COUNT(*) AS tx_count
                        FROM lake.city_government.elobbyist
                        WHERE client_name IS NOT NULL AND LENGTH(client_name) > 3
                        GROUP BY UPPER(TRIM(client_name))
                    ),
                    all_entities AS (
                        SELECT * FROM donors UNION ALL
                        SELECT * FROM candidates UNION ALL
                        SELECT * FROM vendors UNION ALL
                        SELECT * FROM lobbyists UNION ALL
                        SELECT * FROM clients
                    )
                    SELECT entity_name,
                           LISTAGG(DISTINCT entity_type, ',') AS roles,
                           SUM(total_amount) AS total_amount,
                           SUM(tx_count) AS total_transactions
                    FROM all_entities
                    GROUP BY entity_name
                """)
                pol_ent_count = conn.execute("SELECT COUNT(*) FROM main.graph_pol_entities").fetchone()[0]
                multi_role = conn.execute(
                    "SELECT COUNT(*) FROM main.graph_pol_entities WHERE roles LIKE '%,%'"
                ).fetchone()[0]
                print(f"Political entities built: {pol_ent_count:,} ({multi_role:,} with multiple roles)", flush=True)

                # Donation edges: donor → candidate
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_pol_donations AS
                    SELECT UPPER(TRIM(name)) AS donor_name,
                           UPPER(TRIM(recipname)) AS candidate_name,
                           SUM(TRY_CAST(amnt AS DOUBLE)) AS total_donated,
                           COUNT(*) AS donation_count,
                           MAX(date) AS latest_date,
                           FIRST(empname) AS employer
                    FROM lake.city_government.campaign_contributions
                    WHERE name IS NOT NULL AND recipname IS NOT NULL
                      AND UPPER(TRIM(name)) IN (SELECT entity_name FROM main.graph_pol_entities)
                    GROUP BY UPPER(TRIM(name)), UPPER(TRIM(recipname))
                """)
                pol_don_count = conn.execute("SELECT COUNT(*) FROM main.graph_pol_donations").fetchone()[0]
                print(f"Political donations built: {pol_don_count:,} donor→candidate edges", flush=True)

                # Contract edges: vendor ← agency
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_pol_contracts AS
                    SELECT UPPER(TRIM(vendor_name)) AS vendor_name,
                           UPPER(TRIM(agency_name)) AS agency_name,
                           SUM(TRY_CAST(contract_amount AS DOUBLE)) AS total_amount,
                           COUNT(*) AS contract_count,
                           MAX(start_date) AS latest_date
                    FROM lake.city_government.contract_awards
                    WHERE vendor_name IS NOT NULL AND agency_name IS NOT NULL
                    GROUP BY UPPER(TRIM(vendor_name)), UPPER(TRIM(agency_name))
                """)
                pol_con_count = conn.execute("SELECT COUNT(*) FROM main.graph_pol_contracts").fetchone()[0]
                print(f"Political contracts built: {pol_con_count:,} vendor→agency edges", flush=True)

                # Lobbying edges: lobbyist → client
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_pol_lobbying AS
                    SELECT UPPER(TRIM(lobbyist_name)) AS lobbyist_name,
                           UPPER(TRIM(client_name)) AS client_name,
                           SUM(TRY_CAST(compensation_total AS DOUBLE)) AS total_compensation,
                           SUM(TRY_CAST(lobbying_expenses_total AS DOUBLE)) AS total_expenses,
                           COUNT(*) AS filing_count,
                           MAX(report_year) AS latest_year,
                           FIRST(client_industry) AS industry
                    FROM lake.city_government.elobbyist
                    WHERE lobbyist_name IS NOT NULL AND client_name IS NOT NULL
                    GROUP BY UPPER(TRIM(lobbyist_name)), UPPER(TRIM(client_name))
                """)
                pol_lob_count = conn.execute("SELECT COUNT(*) FROM main.graph_pol_lobbying").fetchone()[0]
                print(f"Political lobbying built: {pol_lob_count:,} lobbyist→client edges", flush=True)

                # FEC federal contributions — extends influence graph to federal level
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_fec_contributions AS
                    SELECT UPPER(TRIM(name)) AS donor_name,
                           cmte_id AS committee_id,
                           SUM(transaction_amt) AS total_donated,
                           COUNT(*) AS donation_count,
                           FIRST(employer) AS employer,
                           FIRST(occupation) AS occupation,
                           FIRST(city) AS city,
                           FIRST(zip_code) AS zip_code,
                           MAX(election_cycle) AS latest_cycle
                    FROM lake.federal.fec_contributions
                    WHERE name IS NOT NULL AND LENGTH(name) > 3
                      AND transaction_amt > 0
                    GROUP BY UPPER(TRIM(name)), cmte_id
                """)
                fec_count = conn.execute("SELECT COUNT(*) FROM main.graph_fec_contributions").fetchone()[0]
                fec_donors = conn.execute("SELECT COUNT(DISTINCT donor_name) FROM main.graph_fec_contributions").fetchone()[0]
                # Count overlap with existing political entities
                fec_overlap = conn.execute("""
                    SELECT COUNT(DISTINCT f.donor_name)
                    FROM main.graph_fec_contributions f
                    JOIN main.graph_pol_entities p ON f.donor_name = p.entity_name
                """).fetchone()[0]
                print(f"FEC contributions built: {fec_count:,} donor→committee edges, "
                      f"{fec_donors:,} unique donors, {fec_overlap:,} overlap with city politics", flush=True)

                # HPD litigation respondents — court-named bad actors
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_litigation_respondents AS
                    SELECT UPPER(TRIM(respondent)) AS respondent_name,
                           bbl,
                           casetype,
                           casestatus,
                           TRY_CAST(caseopendate AS DATE) AS case_open_date,
                           findingofharassment,
                           penalty
                    FROM lake.housing.hpd_litigations
                    WHERE respondent IS NOT NULL AND LENGTH(respondent) > 3
                      AND bbl IS NOT NULL
                """)
                lit_count = conn.execute("SELECT COUNT(*) FROM main.graph_litigation_respondents").fetchone()[0]
                lit_harassment = conn.execute(
                    "SELECT COUNT(*) FROM main.graph_litigation_respondents WHERE findingofharassment IS NOT NULL AND findingofharassment != ''"
                ).fetchone()[0]
                lit_respondents = conn.execute("SELECT COUNT(DISTINCT respondent_name) FROM main.graph_litigation_respondents").fetchone()[0]
                print(f"Litigation respondents built: {lit_count:,} cases, "
                      f"{lit_respondents:,} named respondents, {lit_harassment:,} harassment findings", flush=True)
            except Exception as e:
                print(f"Warning: Influence network build failed: {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_pol_entities AS SELECT NULL AS entity_name WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_pol_donations AS SELECT NULL AS donor_name, NULL AS candidate_name WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_pol_contracts AS SELECT NULL AS vendor_name, NULL AS agency_name WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_pol_lobbying AS SELECT NULL AS lobbyist_name, NULL AS client_name WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_fec_contributions AS SELECT NULL::VARCHAR AS donor_name, NULL::VARCHAR AS committee_id WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_litigation_respondents AS SELECT NULL::VARCHAR AS respondent_name, NULL::VARCHAR AS bbl WHERE FALSE")

            # =================================================================
            # GRAPH 4: Contractor Network (DOB permits)
            # =================================================================
            try:
                # Contractors: unique by license number
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_contractors AS
                    SELECT permittee_s_license AS license,
                           FIRST(UPPER(TRIM(permittee_s_business_name))) AS business_name,
                           COUNT(*) AS permit_count,
                           COUNT(DISTINCT (
                               CASE WHEN borough = 'MANHATTAN' THEN '1'
                                    WHEN borough = 'BRONX' THEN '2'
                                    WHEN borough = 'BROOKLYN' THEN '3'
                                    WHEN borough = 'QUEENS' THEN '4'
                                    WHEN borough = 'STATEN ISLAND' THEN '5'
                                    ELSE borough END
                               || LPAD(block::VARCHAR, 5, '0')
                               || LPAD(lot::VARCHAR, 4, '0')
                           )) AS building_count,
                           MIN(TRY_CAST(issuance_date AS DATE)) AS first_permit,
                           MAX(TRY_CAST(issuance_date AS DATE)) AS last_permit
                    FROM lake.housing.dob_permit_issuance
                    WHERE permittee_s_license IS NOT NULL AND LENGTH(permittee_s_license) > 2
                    GROUP BY permittee_s_license
                """)
                contractor_count = conn.execute("SELECT COUNT(*) FROM main.graph_contractors").fetchone()[0]
                print(f"Contractors built: {contractor_count:,} unique licenses", flush=True)

                # Edges: contractor → building (via BBL)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_permit_edges AS
                    SELECT DISTINCT
                        permittee_s_license AS license,
                        (CASE WHEN borough = 'MANHATTAN' THEN '1'
                              WHEN borough = 'BRONX' THEN '2'
                              WHEN borough = 'BROOKLYN' THEN '3'
                              WHEN borough = 'QUEENS' THEN '4'
                              WHEN borough = 'STATEN ISLAND' THEN '5'
                              ELSE borough END
                         || LPAD(block::VARCHAR, 5, '0')
                         || LPAD(lot::VARCHAR, 4, '0')) AS bbl,
                        FIRST(job_type) AS job_type,
                        FIRST(work_type) AS work_type,
                        COUNT(*) AS permit_count,
                        MIN(TRY_CAST(issuance_date AS DATE)) AS first_date,
                        MAX(TRY_CAST(issuance_date AS DATE)) AS last_date,
                        FIRST(UPPER(TRIM(owner_s_business_name))) AS owner_name
                    FROM lake.housing.dob_permit_issuance
                    WHERE permittee_s_license IS NOT NULL AND LENGTH(permittee_s_license) > 2
                      AND borough IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
                    GROUP BY permittee_s_license,
                        (CASE WHEN borough = 'MANHATTAN' THEN '1'
                              WHEN borough = 'BRONX' THEN '2'
                              WHEN borough = 'BROOKLYN' THEN '3'
                              WHEN borough = 'QUEENS' THEN '4'
                              WHEN borough = 'STATEN ISLAND' THEN '5'
                              ELSE borough END
                         || LPAD(block::VARCHAR, 5, '0')
                         || LPAD(lot::VARCHAR, 4, '0'))
                """)
                permit_edge_count = conn.execute("SELECT COUNT(*) FROM main.graph_permit_edges").fetchone()[0]
                print(f"Permit edges built: {permit_edge_count:,} contractor→building edges", flush=True)

                # Derived: contractor↔contractor via shared buildings (co-workers)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_contractor_shared AS
                    WITH contractor_buildings AS (
                        SELECT license, bbl FROM main.graph_permit_edges
                    )
                    SELECT a.license AS license1, b.license AS license2,
                           COUNT(DISTINCT a.bbl) AS shared_buildings
                    FROM contractor_buildings a
                    JOIN contractor_buildings b ON a.bbl = b.bbl AND a.license < b.license
                    GROUP BY a.license, b.license
                    HAVING COUNT(DISTINCT a.bbl) >= 3
                """)
                contractor_shared_count = conn.execute("SELECT COUNT(*) FROM main.graph_contractor_shared").fetchone()[0]
                print(f"Contractor shared edges built: {contractor_shared_count:,} "
                      f"(3+ shared buildings)", flush=True)
            except Exception as e:
                print(f"Warning: Contractor network build failed: {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_contractors AS SELECT NULL AS license WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_permit_edges AS SELECT NULL AS license, NULL AS bbl WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_contractor_shared AS SELECT NULL AS license1, NULL AS license2 WHERE FALSE")

            # =================================================================
            # GRAPH 5: Officer Misconduct Network (CCRB + NYPD profiles)
            # =================================================================
            try:
                # Officers: merge CCRB officers (tax_id key) with NYPD profiles (shield key)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_officers AS
                    WITH ccrb AS (
                        SELECT tax_id,
                               UPPER(TRIM(officer_first_name)) AS first_name,
                               UPPER(TRIM(officer_last_name)) AS last_name,
                               shield_no AS shield,
                               current_rank, current_command,
                               TRY_CAST(total_complaints AS INTEGER) AS total_complaints,
                               TRY_CAST(total_substantiated_complaints AS INTEGER) AS substantiated,
                               active_per_last_reported_status AS active_status
                        FROM lake.public_safety.ccrb_officers
                        WHERE tax_id IS NOT NULL
                    ),
                    profiles AS (
                        SELECT profile_id, UPPER(TRIM(name)) AS full_name, shield
                        FROM lake.public_safety.nypd_officer_profile
                        WHERE name IS NOT NULL
                    )
                    SELECT c.tax_id AS officer_id,
                           c.first_name, c.last_name,
                           c.first_name || ' ' || c.last_name AS full_name,
                           c.shield, c.current_rank, c.current_command,
                           c.total_complaints, c.substantiated, c.active_status,
                           p.profile_id
                    FROM ccrb c
                    LEFT JOIN profiles p ON c.shield = p.shield
                """)
                officer_count = conn.execute("SELECT COUNT(*) FROM main.graph_officers").fetchone()[0]
                print(f"Officers built: {officer_count:,} (CCRB+profile merged)", flush=True)

                # Officer → CCRB complaint edges
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_officer_complaints AS
                    SELECT a.tax_id AS officer_id,
                           a.complaint_id,
                           a.fado_type AS allegation_type,
                           a.allegation,
                           a.ccrb_allegation_disposition AS disposition,
                           a.officer_rank_abbreviation_at_incident AS rank_at_incident,
                           a.officer_command_at_incident AS command_at_incident
                    FROM lake.public_safety.ccrb_allegations a
                    WHERE a.tax_id IS NOT NULL
                """)
                complaint_edge_count = conn.execute("SELECT COUNT(*) FROM main.graph_officer_complaints").fetchone()[0]
                print(f"Officer complaint edges: {complaint_edge_count:,}", flush=True)

                # Officer ↔ officer via shared command (co-location network)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_officer_shared_command AS
                    SELECT a.officer_id AS officer1, b.officer_id AS officer2,
                           a.current_command AS command,
                           a.total_complaints + b.total_complaints AS combined_complaints
                    FROM main.graph_officers a
                    JOIN main.graph_officers b
                        ON a.current_command = b.current_command
                        AND a.officer_id < b.officer_id
                    WHERE a.current_command IS NOT NULL
                      AND (a.substantiated > 0 OR b.substantiated > 0)
                """)
                shared_cmd_count = conn.execute("SELECT COUNT(*) FROM main.graph_officer_shared_command").fetchone()[0]
                print(f"Officer shared-command edges: {shared_cmd_count:,} "
                      f"(pairs with substantiated complaints)", flush=True)
            except Exception as e:
                print(f"Warning: Officer network build failed: {e}", flush=True)
                if conn.execute("SELECT COUNT(*) FROM main.graph_officers").fetchone()[0] == 0:
                    conn.execute("CREATE OR REPLACE TABLE main.graph_officers AS SELECT NULL::VARCHAR AS officer_id WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_officer_complaints AS SELECT NULL::VARCHAR AS officer_id, NULL::VARCHAR AS complaint_id WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_officer_shared_command AS SELECT NULL::VARCHAR AS officer1, NULL::VARCHAR AS officer2 WHERE FALSE")

            # =================================================================
            # GRAPH 6: COIB Pay-to-Play Network
            # =================================================================
            try:
                # COIB donors: merge legal defense trust + elected NFP + agency donations
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_coib_donors AS
                    WITH ldt AS (
                        SELECT UPPER(TRIM(first_name || ' ' || last_name)) AS donor_name,
                               trust_name AS recipient,
                               'LEGAL_DEFENSE_TRUST' AS donation_type,
                               TRY_CAST(amount AS DOUBLE) AS amount,
                               city, state, zip_code
                        FROM lake.city_government.coib_legal_defense_trust
                        WHERE first_name IS NOT NULL AND last_name IS NOT NULL
                    ),
                    nfp AS (
                        SELECT UPPER(TRIM(donor_name)) AS donor_name,
                               organization_name AS recipient,
                               'ELECTED_OFFICIAL_NFP' AS donation_type,
                               TRY_CAST(donation_value AS DOUBLE) AS amount,
                               donor_s_city_of_residence AS city,
                               donor_s_state_of_residence AS state,
                               NULL AS zip_code
                        FROM lake.city_government.coib_elected_nfp_donations
                        WHERE donor_name IS NOT NULL
                    ),
                    agency AS (
                        SELECT UPPER(TRIM(name_of_donor_individual)) AS donor_name,
                               agency_name AS recipient,
                               'AGENCY_DONATION' AS donation_type,
                               TRY_CAST(REPLACE(REPLACE(value_of_donation, '$', ''), ',', '') AS DOUBLE) AS amount,
                               NULL AS city, NULL AS state, NULL AS zip_code
                        FROM lake.city_government.coib_agency_donations
                        WHERE name_of_donor_individual IS NOT NULL
                    ),
                    all_donors AS (
                        SELECT * FROM ldt UNION ALL
                        SELECT * FROM nfp UNION ALL
                        SELECT * FROM agency
                    )
                    SELECT donor_name,
                           LISTAGG(DISTINCT donation_type, ',') AS donation_types,
                           SUM(amount) AS total_donated,
                           COUNT(*) AS donation_count,
                           FIRST(city) AS city,
                           FIRST(state) AS state,
                           FIRST(zip_code) AS zip_code
                    FROM all_donors
                    WHERE donor_name IS NOT NULL AND LENGTH(donor_name) > 2
                    GROUP BY donor_name
                """)
                coib_donor_count = conn.execute("SELECT COUNT(*) FROM main.graph_coib_donors").fetchone()[0]
                multi_type = conn.execute(
                    "SELECT COUNT(*) FROM main.graph_coib_donors WHERE donation_types LIKE '%,%'"
                ).fetchone()[0]
                print(f"COIB donors built: {coib_donor_count:,} ({multi_type:,} multi-type)", flush=True)

                # Policymakers: people with city decision-making power
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_coib_policymakers AS
                    SELECT UPPER(TRIM(name_of_employee)) AS policymaker_name,
                           FIRST(agency_name) AS agency,
                           FIRST(title) AS title,
                           MAX(calendar_year) AS latest_year,
                           COUNT(DISTINCT calendar_year) AS years_active
                    FROM lake.city_government.coib_policymakers
                    WHERE name_of_employee IS NOT NULL
                    GROUP BY UPPER(TRIM(name_of_employee))
                """)
                pm_count = conn.execute("SELECT COUNT(*) FROM main.graph_coib_policymakers").fetchone()[0]
                print(f"Policymakers built: {pm_count:,}", flush=True)

                # Edges: donor → recipient (legal defense trust, NFP, agency)
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_coib_donor_edges AS
                    WITH ldt AS (
                        SELECT UPPER(TRIM(first_name || ' ' || last_name)) AS donor_name,
                               trust_name AS recipient, 'LEGAL_DEFENSE_TRUST' AS edge_type,
                               TRY_CAST(amount AS DOUBLE) AS amount
                        FROM lake.city_government.coib_legal_defense_trust
                        WHERE first_name IS NOT NULL
                    ),
                    nfp AS (
                        SELECT UPPER(TRIM(donor_name)) AS donor_name,
                               organization_name || ' (' || elected_official || ')' AS recipient,
                               'ELECTED_NFP' AS edge_type,
                               TRY_CAST(donation_value AS DOUBLE) AS amount
                        FROM lake.city_government.coib_elected_nfp_donations
                        WHERE donor_name IS NOT NULL
                    ),
                    agency AS (
                        SELECT UPPER(TRIM(name_of_donor_individual)) AS donor_name,
                               agency_name AS recipient, 'AGENCY' AS edge_type,
                               TRY_CAST(REPLACE(REPLACE(value_of_donation, '$', ''), ',', '') AS DOUBLE) AS amount
                        FROM lake.city_government.coib_agency_donations
                        WHERE name_of_donor_individual IS NOT NULL
                    )
                    SELECT * FROM ldt UNION ALL SELECT * FROM nfp UNION ALL SELECT * FROM agency
                """)
                coib_edge_count = conn.execute("SELECT COUNT(*) FROM main.graph_coib_donor_edges").fetchone()[0]
                print(f"COIB donor edges: {coib_edge_count:,}", flush=True)
            except Exception as e:
                print(f"Warning: COIB pay-to-play network build failed: {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_coib_donors AS SELECT NULL::VARCHAR AS donor_name WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_coib_policymakers AS SELECT NULL::VARCHAR AS policymaker_name WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_coib_donor_edges AS SELECT NULL::VARCHAR AS donor_name, NULL::VARCHAR AS recipient WHERE FALSE")

            # =================================================================
            # GRAPH 7: BIC Trade Waste Network
            # =================================================================
            try:
                # Companies: unique by BIC number
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_bic_companies AS
                    SELECT bic_number,
                           FIRST(UPPER(TRIM(account_name))) AS company_name,
                           FIRST(trade_name) AS trade_name,
                           FIRST(address) AS address,
                           COUNT(*) AS record_count
                    FROM lake.business.bic_trade_waste
                    WHERE bic_number IS NOT NULL
                    GROUP BY bic_number
                """)
                bic_count = conn.execute("SELECT COUNT(*) FROM main.graph_bic_companies").fetchone()[0]

                # Violation edges: company → violation
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_bic_violation_edges AS
                    SELECT violation_number,
                           UPPER(TRIM(account_name)) AS company_name,
                           type_of_violation, violation_disposition,
                           TRY_CAST(maximum_fine AS DOUBLE) AS max_fine,
                           TRY_CAST(fine_amount AS DOUBLE) AS fine_amount,
                           bbl
                    FROM lake.business.bic_violations
                    WHERE account_name IS NOT NULL
                """)
                bic_viol_count = conn.execute("SELECT COUNT(*) FROM main.graph_bic_violation_edges").fetchone()[0]

                # Shared BBL edges: companies operating at same property
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_bic_shared_bbl AS
                    WITH company_bbls AS (
                        SELECT DISTINCT UPPER(TRIM(account_name)) AS company_name, bbl
                        FROM lake.business.bic_trade_waste
                        WHERE bbl IS NOT NULL AND LENGTH(bbl) >= 10
                    )
                    SELECT a.company_name AS company1, b.company_name AS company2,
                           COUNT(DISTINCT a.bbl) AS shared_properties
                    FROM company_bbls a
                    JOIN company_bbls b ON a.bbl = b.bbl AND a.company_name < b.company_name
                    GROUP BY a.company_name, b.company_name
                """)
                bic_shared_count = conn.execute("SELECT COUNT(*) FROM main.graph_bic_shared_bbl").fetchone()[0]
                print(f"BIC network built: {bic_count:,} companies, {bic_viol_count:,} violations, "
                      f"{bic_shared_count:,} shared-BBL edges", flush=True)
            except Exception as e:
                print(f"Warning: BIC trade waste network build failed: {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_bic_companies AS SELECT NULL::VARCHAR AS bic_number WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_bic_violation_edges AS SELECT NULL::VARCHAR AS violation_number WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_bic_shared_bbl AS SELECT NULL::VARCHAR AS company1, NULL::VARCHAR AS company2 WHERE FALSE")

            # =================================================================
            # GRAPH 8: DOB Violation Respondents (named individuals/entities)
            # =================================================================
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_dob_respondents AS
                    SELECT UPPER(TRIM(respondent_name)) AS respondent_name,
                           COUNT(*) AS violation_count,
                           SUM(TRY_CAST(penality_imposed AS DOUBLE)) AS total_penalties,
                           SUM(TRY_CAST(amount_paid AS DOUBLE)) AS total_paid,
                           SUM(TRY_CAST(balance_due AS DOUBLE)) AS total_balance_due,
                           COUNT(DISTINCT (boro || LPAD(block, 5, '0') || LPAD(lot, 4, '0'))) AS building_count,
                           COUNT(CASE WHEN UPPER(severity) = 'HAZARDOUS' THEN 1 END) AS hazardous_count
                    FROM lake.housing.dob_ecb_violations
                    WHERE respondent_name IS NOT NULL AND LENGTH(respondent_name) > 2
                    GROUP BY UPPER(TRIM(respondent_name))
                """)
                dob_resp_count = conn.execute("SELECT COUNT(*) FROM main.graph_dob_respondents").fetchone()[0]

                # Respondent → BBL edges
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_dob_respondent_bbl AS
                    SELECT DISTINCT
                        UPPER(TRIM(respondent_name)) AS respondent_name,
                        (boro || LPAD(block, 5, '0') || LPAD(lot, 4, '0')) AS bbl,
                        COUNT(*) AS violation_count,
                        SUM(TRY_CAST(penality_imposed AS DOUBLE)) AS penalties
                    FROM lake.housing.dob_ecb_violations
                    WHERE respondent_name IS NOT NULL AND LENGTH(respondent_name) > 2
                      AND boro IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
                    GROUP BY UPPER(TRIM(respondent_name)),
                             (boro || LPAD(block, 5, '0') || LPAD(lot, 4, '0'))
                """)
                dob_bbl_count = conn.execute("SELECT COUNT(*) FROM main.graph_dob_respondent_bbl").fetchone()[0]
                print(f"DOB respondents built: {dob_resp_count:,} named entities, "
                      f"{dob_bbl_count:,} respondent→BBL edges", flush=True)
            except Exception as e:
                print(f"Warning: DOB respondent network build failed: {e}", flush=True)
                conn.execute("CREATE OR REPLACE TABLE main.graph_dob_respondents AS SELECT NULL::VARCHAR AS respondent_name WHERE FALSE")
                conn.execute("CREATE OR REPLACE TABLE main.graph_dob_respondent_bbl AS SELECT NULL::VARCHAR AS respondent_name, NULL::VARCHAR AS bbl WHERE FALSE")

            # Save to Parquet cache for fast restart
            _save_graph_to_cache(conn)
            print("Graph tables saved to Parquet cache", flush=True)

            print(f"Property graph built: {owner_count:,} owners, {building_count:,} buildings, "
                  f"{violation_count:,} violations, {owns_count:,} ownership edges, "
                  f"{shared_count:,} shared-owner edges", flush=True)

        # Create property graph definitions (lightweight — just metadata pointers)
        conn.execute("""
            CREATE OR REPLACE PROPERTY GRAPH nyc_housing
            VERTEX TABLES (
                main.graph_owners PROPERTIES (owner_id, registrationid, owner_name) LABEL Owner,
                main.graph_buildings PROPERTIES (bbl, housenumber, streetname, zip, boroid, total_units, stories) LABEL Building,
                main.graph_violations PROPERTIES (violation_id, bbl, severity, status, issued_date) LABEL Violation
            )
            EDGE TABLES (
                main.graph_owns
                    SOURCE KEY (owner_id) REFERENCES main.graph_owners (owner_id)
                    DESTINATION KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    LABEL Owns,
                main.graph_has_violation
                    SOURCE KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    DESTINATION KEY (violation_id) REFERENCES main.graph_violations (violation_id)
                    LABEL HasViolation
            )
        """)
        conn.execute("""
            CREATE OR REPLACE PROPERTY GRAPH nyc_building_network
            VERTEX TABLES (
                main.graph_buildings PROPERTIES (bbl, housenumber, streetname, zip, boroid, total_units, stories) LABEL Building
            )
            EDGE TABLES (
                main.graph_shared_owner
                    SOURCE KEY (bbl1) REFERENCES main.graph_buildings (bbl)
                    DESTINATION KEY (bbl2) REFERENCES main.graph_buildings (bbl)
                    LABEL SharedOwner
            )
        """)

        # Transaction network graph: entities connected through shared property transactions
        try:
            conn.execute("""
                CREATE OR REPLACE PROPERTY GRAPH nyc_transaction_network
                VERTEX TABLES (
                    main.graph_tx_entities
                        PROPERTIES (entity_name, tx_count, property_count, as_seller, as_buyer, total_amount)
                        LABEL TxEntity
                )
                EDGE TABLES (
                    main.graph_tx_shared
                        SOURCE KEY (entity1) REFERENCES main.graph_tx_entities (entity_name)
                        DESTINATION KEY (entity2) REFERENCES main.graph_tx_entities (entity_name)
                        LABEL SharedTransaction
                )
            """)
            print("Property graph nyc_transaction_network created", flush=True)
        except Exception as e:
            print(f"Warning: nyc_transaction_network graph definition failed: {e}", flush=True)

        # Corporate web graph: corps connected through shared officers
        try:
            conn.execute("""
                CREATE OR REPLACE PROPERTY GRAPH nyc_corporate_web
                VERTEX TABLES (
                    main.graph_corps
                        PROPERTIES (dos_id, current_entity_name, entity_type, county,
                                    dos_process_name, chairman_name)
                        LABEL Corp,
                    main.graph_corp_people
                        PROPERTIES (person_name, corp_count, primary_address)
                        LABEL Person
                )
                EDGE TABLES (
                    main.graph_corp_officer_edges
                        SOURCE KEY (person_name) REFERENCES main.graph_corp_people (person_name)
                        DESTINATION KEY (dos_id) REFERENCES main.graph_corps (dos_id)
                        LABEL OfficerOf,
                    main.graph_corp_shared_officer
                        SOURCE KEY (corp1) REFERENCES main.graph_corps (dos_id)
                        DESTINATION KEY (corp2) REFERENCES main.graph_corps (dos_id)
                        LABEL SharedOfficer
                )
            """)
            print("Property graph nyc_corporate_web created", flush=True)
        except Exception as e:
            print(f"Warning: nyc_corporate_web graph definition failed: {e}", flush=True)

        # Political influence graph: donors ↔ candidates via donations
        try:
            conn.execute("""
                CREATE OR REPLACE PROPERTY GRAPH nyc_influence_network
                VERTEX TABLES (
                    main.graph_pol_entities
                        PROPERTIES (entity_name, roles, total_amount, total_transactions)
                        LABEL PoliticalEntity
                )
                EDGE TABLES (
                    main.graph_pol_donations
                        SOURCE KEY (donor_name) REFERENCES main.graph_pol_entities (entity_name)
                        DESTINATION KEY (candidate_name) REFERENCES main.graph_pol_entities (entity_name)
                        LABEL DonatesTo,
                    main.graph_pol_lobbying
                        SOURCE KEY (lobbyist_name) REFERENCES main.graph_pol_entities (entity_name)
                        DESTINATION KEY (client_name) REFERENCES main.graph_pol_entities (entity_name)
                        LABEL LobbiesFor
                )
            """)
            print("Property graph nyc_influence_network created", flush=True)
        except Exception as e:
            print(f"Warning: nyc_influence_network graph definition failed: {e}", flush=True)

        # Contractor network graph: contractors connected through shared buildings
        try:
            conn.execute("""
                CREATE OR REPLACE PROPERTY GRAPH nyc_contractor_network
                VERTEX TABLES (
                    main.graph_contractors
                        PROPERTIES (license, business_name, permit_count, building_count)
                        LABEL Contractor
                )
                EDGE TABLES (
                    main.graph_contractor_shared
                        SOURCE KEY (license1) REFERENCES main.graph_contractors (license)
                        DESTINATION KEY (license2) REFERENCES main.graph_contractors (license)
                        LABEL SharedBuilding
                )
            """)
            print("Property graph nyc_contractor_network created", flush=True)
        except Exception as e:
            print(f"Warning: nyc_contractor_network graph definition failed: {e}", flush=True)

        # Officer misconduct graph
        try:
            conn.execute("""
                CREATE OR REPLACE PROPERTY GRAPH nyc_officer_network
                VERTEX TABLES (
                    main.graph_officers
                        PROPERTIES (officer_id, first_name, last_name, full_name, shield,
                                    current_rank, current_command, total_complaints, substantiated)
                        LABEL Officer
                )
                EDGE TABLES (
                    main.graph_officer_shared_command
                        SOURCE KEY (officer1) REFERENCES main.graph_officers (officer_id)
                        DESTINATION KEY (officer2) REFERENCES main.graph_officers (officer_id)
                        LABEL SharedCommand
                )
            """)
            print("Property graph nyc_officer_network created", flush=True)
        except Exception as e:
            print(f"Warning: nyc_officer_network graph definition failed: {e}", flush=True)

        # BIC trade waste graph
        try:
            conn.execute("""
                CREATE OR REPLACE PROPERTY GRAPH nyc_tradewaste_network
                VERTEX TABLES (
                    main.graph_bic_companies
                        PROPERTIES (bic_number, company_name, trade_name, address, record_count)
                        LABEL TradeWasteCompany
                )
                EDGE TABLES (
                    main.graph_bic_shared_bbl
                        SOURCE KEY (company1) REFERENCES main.graph_bic_companies (company_name)
                        DESTINATION KEY (company2) REFERENCES main.graph_bic_companies (company_name)
                        LABEL SharedProperty
                )
            """)
            print("Property graph nyc_tradewaste_network created", flush=True)
        except Exception as e:
            print(f"Warning: nyc_tradewaste_network graph definition failed: {e}", flush=True)

        graph_ready = True
    except Exception as e:
        print(f"Warning: DuckPGQ property graph build failed: {e}", flush=True)

    # PostHog analytics — track MCP tool usage
    ph_key = os.environ.get("POSTHOG_API_KEY", "")
    if ph_key:
        posthog.project_api_key = ph_key
        posthog.host = os.environ.get("POSTHOG_HOST", "https://us.i.posthog.com")
        posthog.debug = False
        print("PostHog analytics enabled", flush=True)
    else:
        print("PostHog analytics disabled (no POSTHOG_API_KEY)", flush=True)

    # Explorations use graph_tables (loaded from cache) not DuckPGQ, so check tables exist
    try:
        conn.execute("SELECT 1 FROM main.graph_owners LIMIT 1")
        explorations = _build_explorations(conn)
    except Exception:
        explorations = []

    # Build percentile ranking tables
    percentiles_ready = False
    try:
        build_percentile_tables(conn, lock=_db_lock)
        print("Percentile tables built (owners + buildings)", flush=True)
        percentiles_ready = True
    except Exception as e:
        print(f"Warning: Percentile table build failed: {e}", flush=True)

    try:
        build_lake_percentile_tables(conn, lock=_db_lock)
        print("Lake percentile tables built (restaurants + ZIPs + precincts)", flush=True)
    except Exception as e:
        print(f"Warning: Lake percentile tables failed: {e}", flush=True)

    # --- Vector embeddings for semantic search (persistent + incremental) ---
    embed_fn = None
    embed_dims = 768  # default for OpenRouter Gemini (reduced via Matryoshka), 384 for ONNX
    try:
        from embedder import create_embedder
        embed_fn, embed_batch_fn, embed_dims = create_embedder("/app/model")
        print(f"Embedding model loaded ({embed_dims} dims)", flush=True)
    except Exception as e:
        print(f"Warning: embedding model unavailable: {e}", flush=True)

    if embed_fn is not None:
        def _background_embed():
            try:
                import pathlib, duckdb as _duckdb
                pathlib.Path(LANCE_DIR).mkdir(parents=True, exist_ok=True)

                # Separate connection for Lance writes — avoids racing the main conn
                bg_conn = _duckdb.connect(config={"allow_unsigned_extensions": "true"})
                try:
                    bg_conn.execute("LOAD lance")
                except Exception:
                    pass

                print("Background: building Lance embeddings...", flush=True)
                _build_catalog_embeddings(bg_conn, embed_batch_fn, embed_dims)
                _build_description_embeddings(conn, bg_conn, embed_batch_fn, embed_dims)
                if graph_ready:
                    _build_entity_name_embeddings(conn, bg_conn, embed_batch_fn, embed_dims)
                    _build_building_vectors(conn, bg_conn)
                _create_lance_indexes(bg_conn)
                bg_conn.close()
                print("Background: Lance embedding pipeline complete", flush=True)
            except Exception as e:
                print(f"Background: embedding error: {e}", flush=True)

        import threading
        embed_thread = threading.Thread(target=_background_embed, daemon=True)
        embed_thread.start()
        print("Embedding pipeline started in background", flush=True)

    # Pre-compute resource category embeddings for semantic matching in resource_finder
    resource_category_vecs = {}
    if embed_fn:
        try:
            for cat, desc in RESOURCE_CATEGORIES.items():
                resource_category_vecs[cat] = embed_fn(desc)
            print(f"Resource category embeddings: {len(resource_category_vecs)} categories", flush=True)
        except Exception as e:
            print(f"Warning: resource category embeddings failed: {e}", flush=True)

    try:
        yield {
            "db": conn, "catalog": catalog,
            "graph_ready": graph_ready,
            "marriage_parquet": MARRIAGE_PARQUET if marriage_available else None,
            "posthog_enabled": bool(ph_key),
            "explorations": explorations,
            "embed_fn": embed_fn,
            "percentiles_ready": percentiles_ready,
            "lock": _db_lock,
            "resource_category_vecs": resource_category_vecs,
        }
    finally:
        if ph_key:
            posthog.flush()
            posthog.shutdown()
        conn.close()


# ---------------------------------------------------------------------------
# Instructions
# ---------------------------------------------------------------------------

INSTRUCTIONS = """\
NYC open data lake — 294 tables, 12 schemas, 60M+ rows of public records.

WHAT'S HERE: Every public dataset about NYC — housing violations, property sales, landlord networks, restaurant inspections, crime stats, school performance, 311 complaints, campaign donations, corporate filings, and more. Data goes back to 1966 for property transactions.

ROUTING — pick the FIRST match:
* BUILDING by BBL → building_profile, landlord_watchdog, building_story
* PERSON/COMPANY by name → entity_xray, person_crossref
* LANDLORD/OWNER → landlord_network, worst_landlords, llc_piercer
* SCHOOL by name/ZIP → school_search, then school_report
* DISTRICT by number → district_report
* NEIGHBORHOOD by ZIP → neighborhood_portrait, neighborhood_compare
* OFFICER/COP by name → cop_sheet
* JUDGE by name → judge_profile
* CRIME/SAFETY by precinct → safety_report
* ENVIRONMENT/CLIMATE by ZIP → climate_risk
* BACKGROUND CHECK by name → due_diligence
* POLITICAL MONEY by name → money_trail (broader), pay_to_play (city-level)
* VITAL RECORDS/GENEALOGY → vital_records, marriage_search
* CORRUPTION/INFLUENCE → pay_to_play, shell_detector, flipper_detector
* FIND DATA by keyword → data_catalog, then list_tables
* CUSTOM QUERY → sql_query (read-only, all tables in 'lake' database)
* EXPORT/DOWNLOAD/SPREADSHEET → export_data (branded XLSX with hyperlinks and percentile heatmap)
* DON'T KNOW WHAT TO EXPLORE → suggest_explorations
* SEMANTIC SEARCH by concept → semantic_search (finds similar descriptions even with different words)

POWER FEATURES:
* Graph tools trace ownership networks across LLCs, property transactions, and corporate filings
* text_search does full-text search across CFPB complaint narratives and restaurant violations
* semantic_search does vector similarity search across 311 complaints, restaurant violations, HPD violations, OATH hearings
* person_crossref links a name across every dataset (property, donations, businesses, violations)
* export_data generates branded XLSX with Common Ground styling, source hyperlinks, and percentile color scales

SUGGEST INTERESTING THINGS: When the user is exploring or says "what's interesting", call suggest_explorations to get pre-computed highlights — worst landlords, biggest property flips, corporate shell networks, neighborhood comparisons. Use these as conversation starters.

Too many tools? Use search_tools(query) to find domain-specific tools.
BBL: 10 digits = borough(1) + block(5) + lot(4). Example: 1000670001
ZIP: 5 digits. Manhattan 100xx, Brooklyn 112xx, Bronx 104xx.
"""

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

ALWAYS_VISIBLE = [
    # Data discovery (always needed)
    "sql_query",
    "data_catalog",
    "list_schemas",
    "suggest_explorations",
    "semantic_search",
    "export_data",
    # Building-centric (most common user intent)
    "building_profile",
    "landlord_watchdog",
    # Person/entity investigation
    "entity_xray",
    "person_crossref",
    # Neighborhood/area
    "neighborhood_portrait",
    "safety_report",
    # Graph power tools
    "landlord_network",
]

mcp = FastMCP(
    "Common Ground",
    instructions=INSTRUCTIONS,
    lifespan=app_lifespan,
    transforms=[
        BM25SearchTransform(
            max_results=5,
            always_visible=ALWAYS_VISIBLE,
        ),
    ],
)

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READONLY, tags={"discovery"})
def sql_query(sql: Annotated[str, Field(description="Read-only SQL. Tables in 'lake' database. Example: SELECT * FROM lake.housing.hpd_violations LIMIT 10")], ctx: Context) -> ToolResult:
    """Execute a read-only SQL query against the NYC data lake (294 tables, 60M+ rows). Use this for custom queries when no domain tool fits. All tables in 'lake' database. Only SELECT/WITH/EXPLAIN/DESCRIBE allowed. Example: SELECT * FROM lake.housing.hpd_violations LIMIT 10. Start with data_catalog(keyword) or list_schemas() to discover tables."""
    _validate_sql(sql)
    db = ctx.lifespan_context["db"]
    t0 = time.time()
    cols, rows = _execute(db, sql.strip().rstrip(";"))
    elapsed = round((time.time() - t0) * 1000)
    return make_result(
        f"Query returned {len(rows)} rows ({elapsed}ms).",
        cols,
        rows,
        {"query_time_ms": elapsed},
    )


@mcp.tool(annotations=ADMIN, tags={"discovery"})
def sql_admin(sql: Annotated[str, Field(description="DDL statement. Only CREATE OR REPLACE VIEW allowed")], ctx: Context) -> str:
    """Create SQL views on the NYC data lake (CREATE OR REPLACE VIEW only). Use this to parse JSON columns, add computed columns, or create reusable derived tables. No other DDL allowed. Example: CREATE OR REPLACE VIEW lake.housing.v_designated_buildings AS SELECT *, json_extract_string(the_geom, '$.coordinates[0]')::DOUBLE AS lon FROM lake.housing.designated_buildings. For read-only queries, use sql_query() instead."""
    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    stripped = re.sub(r"--[^\n]*", " ", stripped)
    stripped = stripped.strip().rstrip(";")

    if ";" in stripped:
        raise ToolError("Only single SQL statements are allowed.")

    if not _SAFE_DDL.match(stripped):
        raise ToolError(
            "Only CREATE OR REPLACE VIEW statements are allowed via sql_admin."
        )

    db = ctx.lifespan_context["db"]
    with _db_lock:
        try:
            db.execute(sql.strip().rstrip(";"))
            # Refresh catalog after DDL
            ctx.lifespan_context["catalog"] = _build_catalog(db)
            return "OK — view created successfully."
        except duckdb.Error as e:
            raise ToolError(f"DDL error: {e}. Only CREATE OR REPLACE VIEW is allowed.")


@mcp.tool(annotations=READONLY, tags={"discovery"})
def list_schemas(ctx: Context) -> str:
    """List all 12 schemas in the NYC data lake with table counts and total rows. Schemas: housing, public_safety, health, social_services, financial, environment, recreation, education, business, transportation, city_government, census. Use list_tables(schema) next to see individual tables in a schema."""
    catalog = ctx.lifespan_context["catalog"]
    lines = []
    for schema in sorted(catalog):
        if schema in _HIDDEN_SCHEMAS:
            continue
        tables = catalog[schema]
        total_rows = sum(t["row_count"] for t in tables.values())
        lines.append(f"{schema}: {len(tables)} tables, ~{total_rows:,} rows")
    return "\n".join(lines)


@mcp.tool(annotations=READONLY, tags={"discovery"})
def list_tables(schema: Annotated[str, Field(description="Schema name. Use list_schemas() to see available schemas")], ctx: Context) -> ToolResult:
    """List all tables in a specific schema with row counts and column counts. Use after list_schemas() to drill into a domain. Fuzzy-matches schema names. For column-level detail, follow up with describe_table(schema, table). For keyword search across all schemas, use data_catalog(keyword) instead."""
    catalog = ctx.lifespan_context["catalog"]
    db = ctx.lifespan_context["db"]
    schema = _fuzzy_match_schema(db, schema, catalog)
    tables = catalog[schema]
    cols = ["table_name", "row_count", "column_count"]
    rows = sorted(
        [(name, t["row_count"], t["column_count"]) for name, t in tables.items()],
        key=lambda r: -r[1],
    )
    return make_result(f"Schema '{schema}': {len(tables)} tables.", cols, rows)


@mcp.tool(annotations=READONLY, tags={"discovery"})
def describe_table(schema: Annotated[str, Field(description="Schema name")], table: Annotated[str, Field(description="Table name. Use list_tables(schema) to see available tables")], ctx: Context) -> ToolResult:
    """Show column names, types, and nullability for a specific table in the NYC data lake. Use after data_catalog() or list_tables() to understand a table's structure before querying with sql_query(). Fuzzy-matches both schema and table names. Parameters: schema (e.g. 'housing'), table (e.g. 'hpd_violations')."""
    db = ctx.lifespan_context["db"]
    catalog = ctx.lifespan_context["catalog"]
    schema = _fuzzy_match_schema(db, schema, catalog)
    table = _fuzzy_match_table(db, table, schema, catalog)
    qualified = f"lake.{schema}.{table}"
    cols, rows = _execute(
        db,
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_catalog = 'lake'
          AND table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    )
    if not rows:
        raise ToolError(
            f"Table '{qualified}' not found. Use list_tables('{schema}') to see available tables."
        )

    # Row count from catalog cache
    catalog = ctx.lifespan_context["catalog"]
    row_count = catalog.get(schema, {}).get(table, {}).get("row_count")
    row_count_str = f"{row_count:,}" if isinstance(row_count, int) else "unknown"

    summary = f"{qualified}: {row_count_str} rows, {len(rows)} columns"
    return make_result(summary, cols, rows)


# ---------------------------------------------------------------------------
# Domain tools
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READONLY, tags={"building", "housing"})
def building_profile(bbl: BBL, ctx: Context) -> ToolResult:
    """Quick building dossier by BBL — address, units, stories, plus violation and complaint counts. Use this for a fast overview. For deeper investigation, follow up with: landlord_watchdog(bbl) for the owner's full portfolio and slumlord score, enforcement_web(bbl) for multi-agency enforcement actions, property_history(bbl) for ACRIS transaction chain since 1966, building_story(bbl) for historical narrative. BBL is 10 digits: borough(1) + block(5) + lot(4). Example: 1000670001"""
    if not re.match(r"^\d{10}$", bbl):
        raise ToolError(
            "BBL must be exactly 10 digits. Format: borough(1) + block(5) + lot(4)"
        )

    db = ctx.lifespan_context["db"]
    cols, rows = _execute(db, BUILDING_PROFILE_SQL, [bbl, bbl, bbl, bbl])

    if not rows:
        raise ToolError(f"No building found for BBL {bbl}")

    row = dict(zip(cols, rows[0]))

    # City-wide violation percentile context
    try:
        _, pct_rows = _execute(db, """
            WITH bbl_counts AS (
                SELECT bbl, COUNT(*) AS cnt FROM lake.housing.hpd_violations GROUP BY bbl
            )
            SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE cnt <= (
                SELECT cnt FROM bbl_counts WHERE bbl = ?
            )) / COUNT(*), 1) AS percentile
            FROM bbl_counts
        """, [bbl])
        if pct_rows and pct_rows[0][0] is not None:
            row["violation_percentile"] = pct_rows[0][0]
    except Exception:
        pass

    # Additional enrichments — landmark, tax exemptions, valuation, SRO, facades
    borough = bbl[0]
    block = bbl[1:6].lstrip("0") or "0"
    lot = bbl[6:10].lstrip("0") or "0"

    try:
        _, landmark_rows = _execute(db, """
            SELECT lm_name, lm_type, hist_distr, status, desdate
            FROM lake.housing.designated_buildings
            WHERE bbl = ?
            LIMIT 1
        """, [bbl])
    except Exception:
        landmark_rows = []

    try:
        _, tax_rows = _execute(db, """
            SELECT exmp_code, exname, year, curexmptot, benftstart
            FROM lake.housing.tax_exemptions
            WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
            ORDER BY year DESC
            LIMIT 5
        """, [bbl])
    except Exception:
        tax_rows = []

    try:
        _, val_rows = _execute(db, """
            SELECT year, curmkttot, curacttot, curtxbtot, zoning, owner,
                   bldg_class, yrbuilt, units, gross_sqft
            FROM lake.housing.property_valuation
            WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
            ORDER BY year DESC
            LIMIT 1
        """, [bbl])
    except Exception:
        val_rows = []

    try:
        _, sro_rows = _execute(db, """
            SELECT dobbuildingclass, legalclassa, legalclassb, managementprogram
            FROM lake.housing.sro_buildings
            WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
            LIMIT 1
        """, [bbl])
    except Exception:
        sro_rows = []

    try:
        _, facade_rows = _execute(db, """
            SELECT current_status, filing_status, cycle, exterior_wall_type_sx
            FROM lake.housing.dob_safety_facades
            WHERE borough = ? AND block = ? AND lot = ?
            ORDER BY TRY_CAST(submitted_on AS DATE) DESC NULLS LAST
            LIMIT 1
        """, [borough, block, lot])
    except Exception:
        facade_rows = []

    summary = (
        f"BBL {row['bbl']}: {row['address']}, {row['zip']}\n"
        f"  {row['stories']} stories, {row['total_units']} units"
        f" ({row['managementprogram'] or 'N/A'})\n"
        f"  HPD Violations: {row['total_violations']} total,"
        f" {row['open_violations']} open"
        f" (latest: {row['latest_violation_date'] or 'N/A'})\n"
        f"  HPD Complaints: {row['total_complaints']} total,"
        f" {row['open_complaints']} open"
        f" (latest: {row['latest_complaint_date'] or 'N/A'})\n"
        f"  DOB/ECB Violations: {row['total_dob_violations']}"
        f" (latest: {row['latest_dob_date'] or 'N/A'})"
    )

    if landmark_rows:
        r = landmark_rows[0]
        summary += f"\n  Landmark: {r[0] or '?'} ({r[1] or '?'}) | {r[2] or 'N/A'} | {r[3] or '?'} | designated {r[4] or '?'}"
        row["landmark"] = {"name": r[0], "type": r[1], "district": r[2], "status": r[3], "date": r[4]}

    if tax_rows:
        exemptions = [f"{r[1] or r[0]} ({r[2] or '?'})" for r in tax_rows[:3]]
        summary += f"\n  Tax Exemptions: {', '.join(exemptions)}"
        row["tax_exemptions"] = [{"code": r[0], "name": r[1], "year": r[2], "amount": r[3], "start": r[4]} for r in tax_rows]

    if val_rows:
        r = val_rows[0]
        mkt = f"${float(r[1] or 0):,.0f}" if r[1] else "?"
        summary += f"\n  Valuation ({r[0] or '?'}): Market {mkt} | Zoning {r[4] or '?'} | Owner: {r[5] or '?'} | Built {r[7] or '?'}"
        row["valuation"] = {"year": r[0], "market_total": r[1], "actual_total": r[2],
                            "taxable_total": r[3], "zoning": r[4], "owner": r[5],
                            "bldg_class": r[6], "year_built": r[7], "units": r[8], "gross_sqft": r[9]}

    if sro_rows:
        summary += f"\n  SRO: Yes — Class A: {sro_rows[0][1] or 0}, Class B: {sro_rows[0][2] or 0}"
        row["is_sro"] = True

    if facade_rows:
        r = facade_rows[0]
        summary += f"\n  Facade (FISP): {r[0] or '?'} | Cycle {r[2] or '?'} | Wall: {r[3] or '?'}"
        row["facade"] = {"status": r[0], "filing": r[1], "cycle": r[2], "wall_type": r[3]}

    if "violation_percentile" in row:
        summary += f"\n  Violation percentile: {row['violation_percentile']}% (city-wide, by building)"

    return ToolResult(
        content=summary,
        structured_content={"building": row},
        meta={"bbl": bbl},
    )


@mcp.tool(annotations=READONLY, tags={"housing"})
def complaints_by_zip(zip_code: ZIP, ctx: Context, days: Annotated[int, Field(description="Look-back period in days. Default: 365", ge=1, le=3650)] = 365) -> ToolResult:
    """Aggregate HPD housing complaints and 311 service requests for a NYC ZIP code over a time period. Use this for complaint volume and category breakdowns by ZIP. Parameters: zip_code (5 digits), days (look-back period, default 365). For building-specific violations, use owner_violations(bbl). For full neighborhood context, use neighborhood_portrait(zipcode)."""
    if not re.match(r"^\d{5}$", zip_code):
        raise ToolError("ZIP code must be exactly 5 digits")

    db = ctx.lifespan_context["db"]
    cols, rows = _execute(
        db, COMPLAINTS_BY_ZIP_SQL, [zip_code, days, zip_code, days]
    )

    total = sum(r[2] for r in rows) if rows else 0
    sources = sorted(set(r[0] for r in rows)) if rows else []
    top5 = rows[:5]
    top5_text = ", ".join(f"{r[1]} ({r[2]})" for r in top5) if top5 else "none"

    summary = (
        f"ZIP {zip_code} last {days} days: {total:,} complaints"
        f" from {', '.join(sources) if sources else 'no sources'}\n"
        f"Top: {top5_text}"
    )

    return make_result(
        summary,
        cols,
        rows,
        {"total_complaints": total, "days": days, "sources": sources},
    )


@mcp.tool(annotations=READONLY, tags={"housing"})
def owner_violations(bbl: BBL, ctx: Context) -> ToolResult:
    """Get HPD housing violations and DOB/ECB building violations for a specific NYC property by BBL. Use this for the violation list at one building. For the full landlord portfolio analysis, use landlord_watchdog(bbl). For multi-agency enforcement (FDNY, OATH, restaurants, facades), use enforcement_web(bbl). BBL is 10 digits: borough(1) + block(5) + lot(4). Example: 1000670001"""
    if not re.match(r"^\d{10}$", bbl):
        raise ToolError("BBL must be exactly 10 digits")

    db = ctx.lifespan_context["db"]
    cols, rows = _execute(db, OWNER_VIOLATIONS_SQL, [bbl, bbl])

    total = len(rows)
    open_count = sum(
        1 for r in rows if r[4] and "open" in str(r[4]).lower()
    )
    sources = sorted(set(r[0] for r in rows)) if rows else []

    summary = (
        f"BBL {bbl}: {total} violations ({open_count} open)"
        f" from {', '.join(sources) if sources else 'no sources'}"
    )

    return make_result(
        summary,
        cols,
        rows,
        {"total_violations": total, "open_count": open_count, "sources": sources},
    )


@mcp.tool(annotations=READONLY, tags={"discovery"})
def data_catalog(ctx: Context, keyword: Annotated[str, Field(description="Search term — fuzzy matched against table names, descriptions, column names. Examples: 'eviction', 'restaurant', 'arrest'")] = "") -> ToolResult:
    """Search the NYC data lake for tables matching a keyword — fuzzy matching, typos OK. START HERE if you don't know which tool to use. Searches table names, descriptions, and column names. Call with no keyword for a schema overview. Examples: 'eviction', 'restaurant', 'arrest', 'school', 'rat'. After finding a table, use describe_table(schema, table) for columns, then sql_query() to query it."""
    if not keyword.strip():
        catalog = ctx.lifespan_context["catalog"]
        lines = []
        for schema in sorted(catalog):
            if schema in _HIDDEN_SCHEMAS:
                continue
            tables = catalog[schema]
            total = sum(t["row_count"] for t in tables.values())
            desc = SCHEMA_DESCRIPTIONS.get(schema, "")
            lines.append(
                f"{schema} ({len(tables)} tables, ~{total:,} rows): {desc}"
            )
        return ToolResult(content="\n".join(lines))

    db = ctx.lifespan_context["db"]
    kw = keyword.strip()
    cols, rows = _execute(db, DATA_CATALOG_SQL, [kw, kw, kw])

    if rows:
        schema_idx = cols.index("schema_name")
        rows = [r for r in rows if r[schema_idx] not in _HIDDEN_SCHEMAS]

    # Semantic fallback: when rapidfuzz finds < 3 tables, try vector similarity on descriptions
    embed_fn = ctx.lifespan_context.get("embed_fn")
    semantic_note = ""
    if embed_fn and len(rows) < 3:
        try:
            from embedder import vec_to_sql
            query_vec = embed_fn(kw)
            vec_literal = vec_to_sql(query_vec)
            sem_sql = f"""
                SELECT schema_name, table_name, description, _distance AS distance
                FROM lance_vector_search('{LANCE_DIR}/catalog_embeddings.lance', 'embedding', {vec_literal}, k=5)
                WHERE table_name != '__schema__'
                ORDER BY _distance ASC
            """
            with _db_lock:
                sem_rows = db.execute(sem_sql).fetchall()
            # Only include if similarity > 0.3 (distance < 0.7)
            good_matches = [(s, t, d) for s, t, d, dist in sem_rows if dist < LANCE_CATALOG_DISTANCE]
            if good_matches:
                semantic_note = "\n\nSemantic matches (by description similarity):\n"
                for s, t, d in good_matches:
                    semantic_note += f"  {s}.{t}: {d}\n"
        except Exception:
            pass

    if not rows and semantic_note:
        return ToolResult(content=f"No exact matches for '{kw}', but found related tables:{semantic_note}")

    if not rows:
        return ToolResult(
            content=f"No tables found matching '{kw}'. Try a broader term."
        )

    summary = f"Found {len(rows)} tables matching '{kw}':"
    if semantic_note:
        result = make_result(summary, cols, rows)
        result = ToolResult(content=result.content + semantic_note)
        return result
    return make_result(summary, cols, rows)


@mcp.tool(annotations=READONLY, tags={"discovery"})
def suggest_explorations(ctx: Context) -> str:
    """Get pre-computed interesting findings from the NYC data lake — worst landlords, property flips, shell company networks, restaurant violations, and more. Call this when the user wants to explore, says 'what's interesting', 'surprise me', or needs conversation starters. Returns highlights with follow-up tool suggestions."""
    explorations = ctx.lifespan_context.get("explorations", [])
    if not explorations:
        return "No pre-computed explorations available. Try data_catalog() to browse schemas."

    lines = ["Here are some interesting things in the NYC data lake:\n"]
    for i, exp in enumerate(explorations, 1):
        lines.append(f"**{i}. {exp['title']}**")
        lines.append(exp["description"])
        if exp.get("sample"):
            lines.append(f"Preview: {exp['sample']}")
        if exp.get("follow_up"):
            lines.append(f"Dig deeper: {exp['follow_up']}")
        lines.append("")
    return "\n".join(lines)


@mcp.tool(annotations=READONLY, tags={"discovery"})
def export_data(
    sql: Annotated[str, Field(description="SQL query to export. All tables in 'lake' database. Example: SELECT * FROM lake.housing.hpd_violations WHERE bbl = '1000670001'")],
    ctx: Context,
    name: Annotated[str, Field(description="Short name for the file. Example: 'violations_10003', 'worst_landlords'")] = "export",
    sources: Annotated[str, Field(description="Comma-separated data sources. Example: 'HPD violations, DOB permits'")] = "",
    table_name: Annotated[str, Field(description="Source table name for deep-links. Example: 'hpd_violations', 'restaurant_inspections'. Leave empty if unknown.")] = "",
) -> str:
    """Export query results as a branded XLSX spreadsheet with Common Ground styling, clickable hyperlinks to source records, and percentile color scales. Use when the user wants to download data, save results, get a spreadsheet, or export. BBL columns link to WhoOwnsWhat. Source column links to original Socrata records. Maximum 100,000 rows."""
    _validate_sql(sql)
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    with _db_lock:
        try:
            cur = db.execute(sql.strip().rstrip(";"))
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(100_000)
        except duckdb.Error as e:
            raise ToolError(f"Export query failed: {e}. Use data_catalog(keyword) to find table names.")

    if not cols:
        raise ToolError("Query returned no columns. Check your SQL.")

    elapsed = round((time.time() - t0) * 1000)
    source_list = [s.strip() for s in sources.split(",") if s.strip()] if sources else []

    from xlsx_export import generate_branded_xlsx

    xlsx_bytes = generate_branded_xlsx(
        cols, rows,
        table_name=table_name,
        sql=sql,
        sources=source_list,
    )

    safe_name = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)[:100]
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    export_dir = "/data/common-ground/exports"
    os.makedirs(export_dir, exist_ok=True)
    path = os.path.join(export_dir, f"{safe_name}_{timestamp}.xlsx")
    with open(path, "wb") as f:
        f.write(xlsx_bytes)

    preview_rows = rows[:5]
    preview_lines = [" | ".join(str(c) for c in cols)]
    preview_lines.append("-" * len(preview_lines[0]))
    for row in preview_rows:
        preview_lines.append(" | ".join(str(v) if v is not None else "" for v in row))

    return (
        f"Exported {len(rows):,} rows to XLSX ({elapsed}ms).\n\n"
        f"File: {path}\n"
        f"Size: {len(xlsx_bytes):,} bytes\n\n"
        f"Features: branded header, frozen columns, "
        + ("BBL hyperlinks to WhoOwnsWhat, " if any(c.lower() == "bbl" for c in cols) else "")
        + (f"Source links to {table_name} on Socrata, " if table_name else "")
        + "percentile color scales\n\n"
        f"Preview (first {len(preview_rows)} rows):\n"
        + "\n".join(preview_lines)
        + (f"\n\n... and {len(rows) - 5:,} more rows" if len(rows) > 5 else "")
    )


@mcp.tool(annotations=READONLY, tags={"discovery"})
def text_search(query: Annotated[str, Field(description="Search keywords. Examples: 'identity theft credit report', 'mice droppings kitchen'")], ctx: Context, corpus: Annotated[str, Field(description="Which data to search: 'financial' (CFPB complaints), 'restaurants' (inspection violations), or 'all' (both)")] = "all", limit: Annotated[int, Field(description="Max results (1-100). Default: 20", ge=1, le=100)] = 20) -> ToolResult:
    """Full-text keyword search across NYC open data records (ILIKE on lake tables). Searches CFPB consumer financial complaints and restaurant inspection violation narratives. Use this when you need to find specific text in records rather than aggregate data. Parameters: query (search text), corpus ('financial', 'restaurants', or 'all'). Examples: 'identity theft credit report', 'mice droppings kitchen'. For table discovery, use data_catalog() instead."""
    if not query.strip():
        raise ToolError("Search query cannot be empty")
    if limit < 1 or limit > 100:
        raise ToolError("Limit must be between 1 and 100")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    all_results = []
    pattern = f"%{query.strip()}%"

    # Search CFPB narratives directly from lake
    if corpus in ("all", "financial"):
        try:
            cols_c, rows_c = _execute(
                db,
                """
                SELECT 'CFPB' AS source, complaint_id AS id, product AS category,
                       company AS entity, date_received AS date,
                       LEFT(complaint_what_happened, 250) AS excerpt
                FROM lake.financial.cfpb_complaints_ny
                WHERE complaint_what_happened ILIKE ?
                   OR product ILIKE ?
                   OR issue ILIKE ?
                ORDER BY date_received DESC
                LIMIT ?
                """,
                [pattern, pattern, pattern, min(limit, 100)],
            )
            all_results.extend(rows_c)
        except Exception:
            pass

    # Search restaurant violations directly from lake
    if corpus in ("all", "restaurants"):
        try:
            cols_r, rows_r = _execute(
                db,
                """
                SELECT 'Restaurant' AS source,
                       camis AS id,
                       cuisine_description AS category,
                       dba || ' (' || boro || ' ' || zipcode || ')' AS entity,
                       inspection_date AS date,
                       LEFT(violation_description, 250) AS excerpt
                FROM lake.health.restaurant_inspections
                WHERE violation_description ILIKE ?
                   OR dba ILIKE ?
                   OR cuisine_description ILIKE ?
                ORDER BY inspection_date DESC
                LIMIT ?
                """,
                [pattern, pattern, pattern, min(limit, 100)],
            )
            all_results.extend(rows_r)
        except Exception:
            pass

    # Semantic enhancement: find conceptually similar description categories
    embed_fn = ctx.lifespan_context.get("embed_fn")
    semantic_suggestions = []
    if embed_fn:
        try:
            from embedder import vec_to_sql
            query_vec = embed_fn(query.strip())
            vec_literal = vec_to_sql(query_vec)
            source_filter = ""
            if corpus == "financial":
                source_filter = "WHERE source IN ('cfpb')"
            elif corpus == "restaurants":
                source_filter = "WHERE source = 'restaurant'"
            sem_sql = f"""
                SELECT source, description, _distance AS distance
                FROM lance_vector_search('{LANCE_DIR}/description_embeddings.lance', 'embedding', {vec_literal}, k=5)
                {source_filter}
                ORDER BY _distance ASC
            """
            with _db_lock:
                sem_rows = db.execute(sem_sql).fetchall()
            semantic_suggestions = [
                (src, desc, round(max(0, 1.0 / (1.0 + dist)) * 100))
                for src, desc, dist in sem_rows
                if dist < LANCE_CATALOG_DISTANCE
            ]
        except Exception:
            pass

    elapsed = round((time.time() - t0) * 1000)

    if not all_results:
        return ToolResult(
            content=f"No results for '{query}'. Try broader terms.",
            meta={"query_time_ms": elapsed},
        )

    all_results = all_results[:limit]

    out_cols = ["source", "id", "category", "entity", "date", "excerpt"]
    corpora_searched = []
    if corpus in ("all", "financial"):
        corpora_searched.append("cfpb_complaints_ny")
    if corpus in ("all", "restaurants"):
        corpora_searched.append("restaurant_inspections")

    lines = [
        f"Found {len(all_results)} results for '{query}' ({elapsed}ms)",
        f"Searched: {', '.join(corpora_searched)}",
    ]

    if semantic_suggestions:
        lines.append("\nRelated categories (semantic match):")
        for src, desc, sim in semantic_suggestions:
            lines.append(f"  [{src}] ({sim}% similar) {desc}")

    summary = "\n".join(lines)
    return make_result(summary, out_cols, all_results, {"query_time_ms": elapsed})


VALID_DESC_SOURCES = frozenset({"311", "restaurant", "hpd", "oath"})


@mcp.tool(annotations=READONLY, tags={"discovery"})
def semantic_search(
    query: Annotated[str, Field(description="Natural language search query. Examples: 'pest problems in restaurants', 'landlord neglect complaints', 'noise from construction'")],
    ctx: Context,
    source: Annotated[str, Field(description="Filter by data source: '311', 'restaurant', 'hpd', 'oath', or 'all'")] = "all",
    limit: Annotated[int, Field(description="Max results (1-50). Default: 15", ge=1, le=50)] = 15,
) -> ToolResult:
    """Semantic similarity search across NYC complaint and violation descriptions. Unlike text_search (keyword matching), this finds conceptually similar records even when different words are used. 'pest problems' finds 'mice', 'roaches', 'flies'. 'building neglect' finds 'peeling paint', 'broken elevator', 'no heat'. Uses vector embeddings + HNSW index. Sources: 311 complaints, restaurant violations, HPD violations, OATH hearings."""
    if not query.strip():
        raise ToolError("Search query cannot be empty")

    if source != "all" and source not in VALID_DESC_SOURCES:
        raise ToolError(
            f"Invalid source '{source}'. Must be one of: {', '.join(sorted(VALID_DESC_SOURCES))}, or 'all'"
        )

    embed_fn = ctx.lifespan_context.get("embed_fn")
    if embed_fn is None:
        raise ToolError(
            "Semantic search is unavailable — embedding model not loaded. Use text_search() for keyword matching instead."
        )

    from embedder import vec_to_sql

    t0 = time.time()
    vec = embed_fn(query.strip())
    vec_literal = vec_to_sql(vec)

    source_filter = f"WHERE source = '{source}'" if source != "all" else ""

    sql = f"""
        SELECT source, description,
               max(0, 1.0 / (1.0 + _distance)) AS similarity
        FROM lance_vector_search('{LANCE_DIR}/description_embeddings.lance', 'embedding', {vec_literal}, k={limit})
        {source_filter}
        ORDER BY _distance ASC
    """

    with _db_lock:
        try:
            db = ctx.lifespan_context["db"]
            cur = db.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        except Exception as e:
            raise ToolError(f"Semantic search failed: {e}")

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(
            content=f"No results found for '{query}'. Try broader or different phrasing.",
            meta={"query_time_ms": elapsed},
        )

    lines = [f"Top {len(rows)} semantic matches for '{query}' ({elapsed}ms)\n"]
    for i, row in enumerate(rows, 1):
        rec = dict(zip(cols, row))
        pct = round(rec["similarity"] * 100, 1)
        lines.append(f"{i}. [{rec['source']}] {pct}% — {rec['description']}")

    lines.append(
        "\nTo retrieve full records, use sql_query() on the source table. "
        "Example: sql_query(\"SELECT * FROM lake.housing.hpd_violations WHERE novdescription ILIKE '%no heat%' LIMIT 20\")"
    )

    content = "\n".join(lines)
    structured = [dict(zip(cols, row)) for row in rows]

    return ToolResult(
        content=content,
        structured_content=structured,
        meta={"query_time_ms": elapsed, "result_count": len(rows), "source_filter": source},
    )


@mcp.tool(annotations=READONLY, tags={"services"})
def restaurant_lookup(name: Annotated[str, Field(description="Restaurant name — fuzzy matched, typos OK. Example: 'Wo Hop', 'joes pizza'")], ctx: Context, zipcode: Annotated[str, Field(description="Optional ZIP to narrow results")] = "", borough: Annotated[str, Field(description="Optional borough filter: Manhattan, Brooklyn, Bronx, Queens, Staten Island")] = "") -> ToolResult:
    """Look up a NYC restaurant by name to see its health department grade, inspection history, and food safety violations. Fuzzy matching — typos OK. Use this when someone asks about a specific restaurant. Parameters: name (restaurant name), zipcode (optional), borough (optional). Examples: restaurant_lookup("Wo Hop"), restaurant_lookup("joes pizza", borough="Manhattan"). For area-wide restaurant stats, use neighborhood_portrait(zipcode)."""
    if len(name.strip()) < 2:
        raise ToolError("Restaurant name must be at least 2 characters")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Build optional filters
    zip_filter = ""
    params = [name.strip(), name.strip()]
    if zipcode.strip():
        zip_filter += " AND zipcode = ?"
        params.append(zipcode.strip())
    if borough.strip():
        zip_filter += " AND LOWER(boro) = LOWER(?)"
        params.append(borough.strip())

    sql = RESTAURANT_FUZZY_MATCH_SQL.format(zip_filter=zip_filter)
    cols, matches = _execute(db, sql, params)

    if not matches:
        raise ToolError(
            f"No restaurant found matching '{name}'"
            + (f" in ZIP {zipcode}" if zipcode else "")
            + (f" in {borough}" if borough else "")
            + ". Try a shorter name or remove location filters."
        )

    # Take best match
    best = dict(zip(cols, matches[0]))
    camis = best["camis"]

    # Get inspection history
    insp_cols, inspections = _execute(db, RESTAURANT_INSPECTIONS_SQL, [camis])

    # Build summary
    current_grade = "Not yet graded"
    for insp in inspections:
        d = dict(zip(insp_cols, insp))
        if d.get("grade") and d["grade"] in ("A", "B", "C"):
            current_grade = d["grade"]
            break

    # Grade history timeline
    grades = []
    for insp in inspections:
        d = dict(zip(insp_cols, insp))
        if d.get("grade") and d["grade"] in ("A", "B", "C"):
            date_str = str(d["inspection_date"])[:10]
            grades.append(f"{d['grade']}({date_str})")

    # Recent violations (from most recent inspection)
    recent_violations = []
    if inspections:
        latest = dict(zip(insp_cols, inspections[0]))
        if latest.get("violations"):
            for v in latest["violations"].split(" | ")[:5]:
                recent_violations.append(v[:120])

    lines = [
        f"{best['dba']} — {best['cuisine_description']}",
        f"  {best['address']}, {best['boro']} {best['zipcode']}"
        + (f" | {best['phone']}" if best.get("phone") else ""),
        f"  Current grade: {current_grade}"
        + (f" | Score: {inspections[0][2]}" if inspections else ""),
        f"  Match confidence: {best['name_score']:.0f}%",
    ]

    if grades:
        lines.append(f"  Grade history: {' → '.join(grades[:8])}")

    if recent_violations:
        lines.append(f"  Recent violations ({str(inspections[0][0])[:10]}):")
        for v in recent_violations:
            lines.append(f"    - {v}")

    # Other matches
    if len(matches) > 1:
        lines.append("\n  Other matches:")
        for m in matches[1:5]:
            md = dict(zip(cols, m))
            lines.append(f"    {md['dba']} ({md['cuisine_description']}) — {md['boro']} {md['zipcode']} [{md['name_score']:.0f}%]")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)
    if inspections:
        text += "\n\nINSPECTION HISTORY:\n" + format_text_table(insp_cols, inspections)

    structured = {
        "restaurant": best,
        "inspections": [dict(zip(insp_cols, r)) for r in inspections],
        "other_matches": [dict(zip(cols, m)) for m in matches[1:5]],
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"camis": camis, "query_time_ms": elapsed, "match_score": best["name_score"]},
    )


@mcp.tool(annotations=READONLY, tags={"neighborhood"})
def area_snapshot(lat: Annotated[float, Field(description="Latitude within NYC (40.4-41.0). Example: 40.7128", ge=40.4, le=41.0)], lng: Annotated[float, Field(description="Longitude within NYC (-74.3 to -73.6). Example: -74.0060", ge=-74.3, le=-73.6)], ctx: Context, radius_m: Annotated[int, Field(description="Search radius in meters (50-2000). Default: 500", ge=50, le=2000)] = 500) -> ToolResult:
    """What's nearby a NYC location — spatial radius query across crimes, restaurants, subway stops, 311 complaints, rats, and trees. Use this for location-based questions with coordinates. Parameters: lat, lng, radius_m (default 500). Examples: area_snapshot(40.7128, -74.0060) for Lower Manhattan. For ZIP-based analysis, use neighborhood_portrait(zipcode). For comparing neighborhoods, use neighborhood_compare([zips])."""
    if not (40.4 <= lat <= 41.0 and -74.3 <= lng <= -73.6):
        raise ToolError("Coordinates must be within NYC bounds (lat 40.4-41.0, lng -74.3 to -73.6)")
    if radius_m < 50 or radius_m > 2000:
        raise ToolError("Radius must be between 50 and 2000 meters")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # ST_DWithin uses degrees; approximate conversion: 1 degree ≈ 111,139m at NYC latitude
    radius_deg = radius_m / 111139.0

    # Main summary query
    params = [lng, lat] + [radius_deg] * 7  # 7 ST_DWithin calls (crimes historic + YTD + rest/subway/311/rats/trees)
    cols, rows = _execute(db, AREA_SNAPSHOT_SQL, params)

    # Subway stops (separate query for detail)
    sub_cols, sub_rows = _execute(db, """
        WITH pt AS (SELECT ST_Point(?, ?) AS geom)
        SELECT s.name, s.line,
               ROUND(ST_Distance(s.geom, pt.geom)::NUMERIC * 111139, 0) AS dist_m
        FROM lake.spatial.subway_stops s, pt
        WHERE s.geom IS NOT NULL AND ST_DWithin(s.geom, pt.geom, ?)
        ORDER BY dist_m LIMIT 5
    """, [lng, lat, radius_deg])

    # 311 breakdown (separate query)
    svc_cols, svc_rows = _execute(db, """
        WITH pt AS (SELECT ST_Point(?, ?) AS geom)
        SELECT agency, COUNT(*) AS cnt
        FROM lake.spatial.n311_complaints c, pt
        WHERE c.geom IS NOT NULL AND ST_DWithin(c.geom, pt.geom, ?)
          AND TRY_CAST(c.created_date AS DATE) >= CURRENT_DATE - INTERVAL 730 DAY
        GROUP BY agency ORDER BY cnt DESC LIMIT 8
    """, [lng, lat, radius_deg])

    # Closest restaurants (separate query for names)
    rest_cols, rest_rows = _execute(db, """
        WITH pt AS (SELECT ST_Point(?, ?) AS geom)
        SELECT DISTINCT ON (r.camis) r.dba, r.cuisine_description, r.grade,
               ROUND(ST_Distance(r.geom, pt.geom)::NUMERIC * 111139, 0) AS dist_m
        FROM lake.spatial.restaurant_inspections r, pt
        WHERE r.geom IS NOT NULL AND ST_DWithin(r.geom, pt.geom, ?)
          AND r.grade IN ('A', 'B', 'C')
        ORDER BY r.camis, r.inspection_date DESC
    """, [lng, lat, radius_deg])
    # Sort by distance
    rest_rows = sorted(rest_rows, key=lambda r: r[3])[:10]

    elapsed = round((time.time() - t0) * 1000)

    # Build summary
    d = dict(zip(cols, rows[0])) if rows else {}

    lines = [
        f"AREA SNAPSHOT — {lat:.4f}, {lng:.4f} ({radius_m}m radius)",
        "=" * 45,
    ]

    # Safety
    total_crimes = d.get("total_crimes", 0) or 0
    felonies = d.get("felonies", 0) or 0
    misdemeanors = d.get("misdemeanors", 0) or 0
    lines.append(f"\nSAFETY (past year):")
    lines.append(f"  {total_crimes} crimes ({felonies} felonies, {misdemeanors} misdemeanors)")
    if d.get("top_crimes"):
        lines.append(f"  Top: {d['top_crimes']}")

    # Restaurants
    total_rest = d.get("total_restaurants", 0) or 0
    pct_a = d.get("pct_a", 0) or 0
    grade_c = d.get("grade_c_count", 0) or 0
    lines.append(f"\nFOOD:")
    lines.append(f"  {total_rest} restaurants ({pct_a:.0f}% grade A, {grade_c} grade C)")
    if rest_rows:
        lines.append("  Nearest:")
        for r in rest_rows[:5]:
            lines.append(f"    {r[0]} ({r[1]}) — grade {r[2]}, {r[3]:.0f}m")

    # Transit
    lines.append(f"\nTRANSIT:")
    if sub_rows:
        for s in sub_rows[:5]:
            lines.append(f"  {s[0]} [{s[1]}] — {s[2]:.0f}m")
    else:
        lines.append("  No subway stops within radius")

    # Environment
    rats = d.get("rat_inspections", 0) or 0
    active = d.get("active_rats", 0) or 0
    trees = d.get("tree_count", 0) or 0
    healthy = d.get("healthy_trees", 0) or 0
    lines.append(f"\nENVIRONMENT:")
    lines.append(f"  Trees: {trees} ({healthy} healthy)")
    lines.append(f"  Rat inspections: {rats} ({active} with active signs)")

    # 311 activity
    if svc_rows:
        total_311 = sum(r[1] for r in svc_rows)
        top_agencies = ", ".join(f"{r[0]}({r[1]})" for r in svc_rows[:5])
        lines.append(f"\n311 ACTIVITY (past 2 years):")
        lines.append(f"  {total_311} complaints — {top_agencies}")

    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)

    structured = {
        "summary": d,
        "subway_stops": [dict(zip(sub_cols, r)) for r in sub_rows] if sub_rows else [],
        "nearest_restaurants": [dict(zip(rest_cols, r)) for r in rest_rows[:10]] if rest_rows else [],
        "complaints_311": [dict(zip(svc_cols, r)) for r in svc_rows] if svc_rows else [],
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"lat": lat, "lng": lng, "radius_m": radius_m, "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"neighborhood"})
def neighborhood_compare(zip_codes: Annotated[list[str], Field(description="2-7 NYC ZIP codes to compare. Example: ['10003', '11201', '11215']")], ctx: Context) -> ToolResult:
    """Compare 2-7 NYC neighborhoods side-by-side across quality-of-life dimensions: crime, restaurants, housing complaints, noise, income, commute time, parks, and education. Use this when comparing ZIPs for living, working, or investing. Parameters: zip_codes (list of 2-7 ZIP strings). Example: neighborhood_compare(["10003", "11201", "11215"]). For a single neighborhood deep-dive, use neighborhood_portrait(zipcode)."""
    if not zip_codes or len(zip_codes) < 2:
        raise ToolError("Provide at least 2 ZIP codes to compare")
    if len(zip_codes) > 7:
        raise ToolError("Maximum 7 ZIP codes per comparison")
    for z in zip_codes:
        if not re.match(r"^\d{5}$", z):
            raise ToolError(f"Invalid ZIP code: {z}. Must be exactly 5 digits.")

    db = ctx.lifespan_context["db"]

    # Try H3-based comparison first
    try:
        from spatial import h3_zip_centroid_sql, h3_neighborhood_stats_sql
        results = []
        for z in zip_codes:
            centroid_cols, centroid_rows = _execute(db, *h3_zip_centroid_sql(z))
            if not centroid_rows or centroid_rows[0][0] is None:
                continue
            center = dict(zip(centroid_cols, centroid_rows[0]))
            stats_cols, stats_rows = _execute(db,
                *h3_neighborhood_stats_sql(center["center_lat"], center["center_lng"], radius_rings=8))
            if stats_rows:
                s = dict(zip(stats_cols, stats_rows[0]))
                s["zip"] = z
                results.append(s)

        if len(results) >= 2:
            # Supplement with ZIP-based income + housing (no H3 equivalent)
            for r in results:
                z = r["zip"]
                try:
                    _, inc = _execute(db, """
                        SELECT ROUND(1000.0 * SUM(TRY_CAST(agi_amount AS DOUBLE))
                               / NULLIF(SUM(TRY_CAST(num_returns AS DOUBLE)), 0), 0)
                        FROM lake.economics.irs_soi_zip_income
                        WHERE zipcode = ? AND TRY_CAST(tax_year AS INTEGER) = (
                            SELECT MAX(TRY_CAST(tax_year AS INTEGER)) FROM lake.economics.irs_soi_zip_income)
                    """, [z])
                    if inc and inc[0][0]:
                        r["avg_income"] = inc[0][0]
                except Exception:
                    pass
                try:
                    _, hpd = _execute(db, """
                        SELECT COUNT(DISTINCT complaint_id)
                        FROM lake.housing.hpd_complaints
                        WHERE post_code = ? AND TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
                    """, [z])
                    if hpd and hpd[0][0]:
                        r["housing_complaints"] = hpd[0][0]
                except Exception:
                    pass

            lines = ["Neighborhood Comparison (H3 hex-based):"]
            for r in results:
                lines.append(f"\nZIP {r['zip']}:")
                lines.append(f"  Crime: {r.get('total_crimes', 0):,} incidents nearby")
                lines.append(f"  Arrests: {r.get('total_arrests', 0):,} nearby")
                lines.append(f"  Food: {r.get('restaurants', 0):,} restaurants")
                lines.append(f"  311 calls: {r.get('n311_calls', 0):,}")
                lines.append(f"  Shootings: {r.get('shootings', 0):,}")
                lines.append(f"  Trees: {r.get('street_trees', 0):,}")
                if r.get("avg_income"):
                    lines.append(f"  Income: ${r['avg_income']:,.0f} avg AGI")
                if r.get("housing_complaints"):
                    lines.append(f"  Housing: {r['housing_complaints']:,} HPD complaints/yr")

            return ToolResult(
                content="\n".join(lines),
                structured_content={"neighborhoods": results},
                meta={"zip_codes": zip_codes, "method": "h3"},
            )
    except Exception:
        pass  # Fall back to original SQL

    cols, rows = _execute(db, NEIGHBORHOOD_COMPARE_SQL, [zip_codes])

    if not rows:
        raise ToolError("No data found for the provided ZIP codes")

    # Build a readable summary
    lines = ["Neighborhood Comparison:"]
    for row in rows:
        d = dict(zip(cols, row))
        z = d["zip"]
        parts = [f"\nZIP {z}:"]
        if d.get("felonies") is not None:
            parts.append(f"  Crime: {d['felonies']} felonies, {d['assaults']} assaults/yr")
        if d.get("noise_calls") is not None:
            parts.append(f"  Noise: {d['noise_calls']:,} NYPD 311 calls/yr")
        if d.get("restaurants") is not None:
            parts.append(f"  Food: {d['restaurants']} restaurants ({d['pct_grade_a']}% A-grade)")
        if d.get("housing_complaints") is not None:
            parts.append(f"  Housing: {d['housing_complaints']:,} HPD complaints/yr")
        if d.get("avg_income") is not None:
            parts.append(f"  Income: ${d['avg_income']:,.0f} avg AGI")
        if d.get("commute_min") is not None:
            parts.append(f"  Commute: {d['commute_min']:.0f} min avg")
        if d.get("poverty_rate") is not None:
            parts.append(f"  Poverty: {d['poverty_rate']:.1f}%")
        if d.get("park_access_pct") is not None:
            parts.append(f"  Park access: {d['park_access_pct']:.0f}%")
        if d.get("bachelors_pct") is not None:
            parts.append(f"  College educated: {d['bachelors_pct']:.0f}%")
        lines.extend(parts)

    summary = "\n".join(lines)
    return ToolResult(
        content=summary + "\n\n" + format_text_table(cols, rows),
        structured_content={"neighborhoods": [dict(zip(cols, r)) for r in rows]},
        meta={"zip_codes": zip_codes, "dimensions": len(cols) - 1},
    )


RESTAURANT_FUZZY_MATCH_SQL = """
WITH candidates AS (
    SELECT DISTINCT camis, dba, cuisine_description, boro,
           building || ' ' || street AS address, zipcode, phone,
           rapidfuzz_token_set_ratio(LOWER(?), LOWER(dba)) AS name_score
    FROM lake.health.restaurant_inspections
    WHERE rapidfuzz_token_set_ratio(LOWER(?), LOWER(dba)) >= 65
      {zip_filter}
    ORDER BY name_score DESC
    LIMIT 10
)
SELECT * FROM candidates
"""

RESTAURANT_INSPECTIONS_SQL = """
SELECT inspection_date, grade, score, inspection_type,
       STRING_AGG(DISTINCT violation_description, ' | ') AS violations
FROM lake.health.restaurant_inspections
WHERE camis = ?
  AND inspection_date IS NOT NULL
GROUP BY inspection_date, grade, score, inspection_type
ORDER BY inspection_date DESC
LIMIT 20
"""

AREA_SNAPSHOT_SQL = """
WITH pt AS (
    SELECT ST_Point(?, ?) AS geom  -- lng, lat
),
-- Crimes within radius (historic + YTD, last 365 days with 730-day lookback on historic)
all_nearby_crimes AS (
    SELECT law_cat_cd, ofns_desc, rpt_dt
    FROM lake.spatial.nypd_crimes c, pt
    WHERE c.geom IS NOT NULL AND ST_DWithin(c.geom, pt.geom, ?)
      AND TRY_CAST(c.rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 730 DAY
    UNION ALL
    SELECT law_cat_cd, ofns_desc, rpt_dt
    FROM lake.spatial.nypd_crimes_ytd c, pt
    WHERE c.geom IS NOT NULL AND ST_DWithin(c.geom, pt.geom, ?)
),
nearby_crimes AS (
    SELECT law_cat_cd, ofns_desc, COUNT(*) AS cnt
    FROM all_nearby_crimes
    WHERE TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY law_cat_cd, ofns_desc
),
crime_summary AS (
    SELECT
        SUM(cnt) AS total_crimes,
        SUM(cnt) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies,
        SUM(cnt) FILTER (WHERE law_cat_cd = 'MISDEMEANOR') AS misdemeanors
    FROM nearby_crimes
),
crime_top AS (
    SELECT STRING_AGG(ofns_desc || ' (' || cnt || ')', ', ' ORDER BY cnt DESC) AS top_crimes
    FROM (SELECT ofns_desc, cnt FROM nearby_crimes ORDER BY cnt DESC LIMIT 5)
),
-- Restaurants within radius (latest inspection per restaurant)
nearby_restaurants AS (
    SELECT DISTINCT ON (r.camis) r.dba, r.cuisine_description, r.grade, r.score,
           ROUND(ST_Distance(r.geom, pt.geom)::NUMERIC * 111139, 0) AS dist_m
    FROM lake.spatial.restaurant_inspections r, pt
    WHERE r.geom IS NOT NULL
      AND ST_DWithin(r.geom, pt.geom, ?)
      AND r.grade IN ('A', 'B', 'C')
    ORDER BY r.camis, r.inspection_date DESC
),
restaurant_summary AS (
    SELECT COUNT(*) AS total_restaurants,
           ROUND(100.0 * COUNT(*) FILTER (WHERE grade = 'A') / NULLIF(COUNT(*), 0), 0) AS pct_a,
           COUNT(*) FILTER (WHERE grade = 'C') AS grade_c_count
    FROM nearby_restaurants
),
-- Subway stops within radius
nearby_subway AS (
    SELECT s.name, s.line,
           ROUND(ST_Distance(s.geom, pt.geom)::NUMERIC * 111139, 0) AS dist_m
    FROM lake.spatial.subway_stops s, pt
    WHERE s.geom IS NOT NULL
      AND ST_DWithin(s.geom, pt.geom, ?)
    ORDER BY dist_m
),
-- 311 complaints within radius (last 180 days, top categories)
nearby_311 AS (
    SELECT agency, COUNT(*) AS cnt
    FROM lake.spatial.n311_complaints c, pt
    WHERE c.geom IS NOT NULL
      AND ST_DWithin(c.geom, pt.geom, ?)
      AND TRY_CAST(c.created_date AS DATE) >= CURRENT_DATE - INTERVAL 730 DAY
    GROUP BY agency
),
-- Rat inspections within radius (geom is lat/lng swapped in this table)
nearby_rats AS (
    SELECT COUNT(*) AS total_inspections,
           COUNT(*) FILTER (WHERE "Result" ILIKE '%Rat Activity%') AS active_rats
    FROM lake.spatial.rat_inspections r, pt
    WHERE r.geom IS NOT NULL
      AND ST_DWithin(ST_Point(ST_Y(r.geom), ST_X(r.geom)), pt.geom, ?)
),
-- Trees within radius
nearby_trees AS (
    SELECT COUNT(*) AS tree_count,
           COUNT(*) FILTER (WHERE health = 'Good') AS healthy_trees
    FROM lake.spatial.street_trees t, pt
    WHERE t.geom IS NOT NULL
      AND ST_DWithin(t.geom, pt.geom, ?)
)
SELECT 'summary' AS section,
       cs.total_crimes, cs.felonies, cs.misdemeanors, ct.top_crimes,
       rs.total_restaurants, rs.pct_a, rs.grade_c_count,
       nr.total_inspections AS rat_inspections, nr.active_rats,
       nt.tree_count, nt.healthy_trees
FROM crime_summary cs, crime_top ct, restaurant_summary rs, nearby_rats nr, nearby_trees nt
"""

GENTRIFICATION_SIGNALS_SQL = """
WITH quarters AS (
    SELECT UNNEST(GENERATE_SERIES(
        DATE_TRUNC('quarter', DATE '2020-01-01'),
        DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL 90 DAY),
        INTERVAL 1 QUARTER
    )) AS q
),
-- New restaurants appearing per quarter (first inspection = proxy for opening)
new_restaurants AS (
    SELECT zipcode AS zip, DATE_TRUNC('quarter', MIN(TRY_CAST(inspection_date AS DATE))) AS q,
           COUNT(*) AS cnt
    FROM lake.health.restaurant_inspections
    WHERE zipcode IN (SELECT UNNEST(?::VARCHAR[]))
    GROUP BY zipcode, camis
),
new_rest_q AS (
    SELECT zip, q, COUNT(*) AS new_restaurants
    FROM new_restaurants
    WHERE q >= DATE '2020-01-01'
    GROUP BY zip, q
),
-- HPD complaints per quarter (tenant turnover signal)
hpd_q AS (
    SELECT post_code AS zip,
           DATE_TRUNC('quarter', TRY_CAST(received_date AS DATE)) AS q,
           COUNT(DISTINCT complaint_id) AS hpd_complaints
    FROM lake.housing.hpd_complaints
    WHERE post_code IN (SELECT UNNEST(?::VARCHAR[]))
      AND TRY_CAST(received_date AS DATE) >= DATE '2020-01-01'
    GROUP BY 1, 2
),
-- 311 NYPD calls per quarter (nightlife/noise proxy)
noise_q AS (
    SELECT incident_zip AS zip,
           DATE_TRUNC('quarter', TRY_CAST(created_date AS DATE)) AS q,
           COUNT(*) AS noise_calls
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT UNNEST(?::VARCHAR[]))
      AND agency = 'NYPD'
      AND TRY_CAST(created_date AS DATE) >= DATE '2020-01-01'
    GROUP BY 1, 2
),
-- DOB ECB violations per quarter (construction boom proxy)
dob_q AS (
    SELECT respondent_zip AS zip,
           DATE_TRUNC('quarter', TRY_STRPTIME(issue_date, '%Y%m%d')) AS q,
           COUNT(*) AS dob_violations
    FROM lake.housing.dob_ecb_violations
    WHERE respondent_zip IN (SELECT UNNEST(?::VARCHAR[]))
      AND TRY_STRPTIME(issue_date, '%Y%m%d') >= DATE '2020-01-01'
    GROUP BY 1, 2
),
-- 311 DOB calls per quarter (construction complaint proxy)
dob_311_q AS (
    SELECT incident_zip AS zip,
           DATE_TRUNC('quarter', TRY_CAST(created_date AS DATE)) AS q,
           COUNT(*) AS construction_complaints
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT UNNEST(?::VARCHAR[]))
      AND agency = 'DOB'
      AND TRY_CAST(created_date AS DATE) >= DATE '2020-01-01'
    GROUP BY 1, 2
),
-- Cross join all ZIPs with all quarters to fill gaps
zip_list AS (SELECT UNNEST(?::VARCHAR[]) AS zip),
grid AS (
    SELECT z.zip, q.q FROM zip_list z CROSS JOIN quarters q
)
SELECT g.zip, g.q,
       COALESCE(r.new_restaurants, 0) AS new_restaurants,
       COALESCE(h.hpd_complaints, 0) AS hpd_complaints,
       COALESCE(n.noise_calls, 0) AS noise_calls,
       COALESCE(d.dob_violations, 0) AS dob_violations,
       COALESCE(c.construction_complaints, 0) AS construction_complaints
FROM grid g
LEFT JOIN new_rest_q r ON g.zip = r.zip AND g.q = r.q
LEFT JOIN hpd_q h ON g.zip = h.zip AND g.q = h.q
LEFT JOIN noise_q n ON g.zip = n.zip AND g.q = n.q
LEFT JOIN dob_q d ON g.zip = d.zip AND g.q = d.q
LEFT JOIN dob_311_q c ON g.zip = c.zip AND g.q = c.q
ORDER BY g.zip, g.q
"""

IRS_INCOME_TREND_SQL = """
SELECT zipcode AS zip, tax_year,
       ROUND(1000.0 * SUM(TRY_CAST(agi_amount AS DOUBLE))
           / NULLIF(SUM(TRY_CAST(num_returns AS DOUBLE)), 0), 0) AS avg_agi
FROM lake.economics.irs_soi_zip_income
WHERE zipcode IN (SELECT UNNEST(?::VARCHAR[]))
GROUP BY zipcode, tax_year
ORDER BY zipcode, tax_year
"""

GENT_HMDA_INVESTOR_SQL = """
SELECT activity_year,
       COUNT(*) AS total_loans,
       COUNT(*) FILTER (WHERE occupancy_type IN ('2','3')) AS investor_loans,
       ROUND(100.0 * COUNT(*) FILTER (WHERE occupancy_type IN ('2','3')) / NULLIF(COUNT(*), 0), 1) AS investor_pct,
       COUNT(*) FILTER (WHERE derived_race = 'White') AS white_borrowers,
       COUNT(*) FILTER (WHERE derived_race = 'Black or African American') AS black_borrowers,
       COUNT(*) FILTER (WHERE action_taken = '3') AS denials,
       ROUND(100.0 * COUNT(*) FILTER (WHERE action_taken = '3' AND derived_race = 'Black or African American')
           / NULLIF(COUNT(*) FILTER (WHERE derived_race = 'Black or African American'), 0), 1) AS black_denial_pct,
       ROUND(100.0 * COUNT(*) FILTER (WHERE action_taken = '3' AND derived_race = 'White')
           / NULLIF(COUNT(*) FILTER (WHERE derived_race = 'White'), 0), 1) AS white_denial_pct
FROM lake.housing.hmda_nyc_loans
WHERE borough = ?
GROUP BY activity_year
ORDER BY activity_year
"""

GENT_EVICTION_SQL = """
SELECT EXTRACT(YEAR FROM TRY_CAST(executed_date AS DATE)) AS yr,
       COUNT(*) AS evictions
FROM lake.housing.evictions
WHERE UPPER(borough) = UPPER(?)
  AND TRY_CAST(executed_date AS DATE) >= DATE '2018-01-01'
GROUP BY yr ORDER BY yr
"""

GENT_REZONING_SQL = """
SELECT project_na, status, effective, ulurpno
FROM lake.city_government.zola_zoning_amendments
WHERE TRY_CAST(effective AS DATE) >= DATE '2015-01-01'
  AND status = 'Adopted'
ORDER BY effective DESC
LIMIT 15
"""

GENT_AFFORDABLE_LOSS_SQL = """
SELECT COUNT(*) AS total_buildings,
       SUM(TRY_CAST(total_units AS INT)) AS total_units,
       SUM(TRY_CAST(counted_rental_units AS INT)) AS rental_units
FROM lake.housing.affordable_housing
WHERE postcode IN (SELECT UNNEST(?::VARCHAR[]))
"""


@mcp.tool(annotations=READONLY, tags={"neighborhood"})
def gentrification_tracker(zip_codes: Annotated[list[str], Field(description="1-5 NYC ZIP codes to track. Example: ['10009', '11216']")], ctx: Context) -> ToolResult:
    """Track gentrification and displacement pressure for NYC neighborhoods using quarterly signals since 2020 plus 4-quarter forecasts. Covers: new restaurant openings, HPD housing complaints, noise/NYPD calls, DOB construction violations, IRS income trends, HMDA investor/lending disparities, evictions, rezonings, and affordable housing loss. Use this for gentrification, displacement, rent pressure, or development questions. Parameters: zip_codes (1-5 ZIPs). Example: gentrification_tracker(["11237", "11221"]). For a broader neighborhood profile, use neighborhood_portrait(zipcode)."""
    if not zip_codes or len(zip_codes) > 5:
        raise ToolError("Provide 1-5 ZIP codes")
    for z in zip_codes:
        if not re.match(r"^\d{5}$", z):
            raise ToolError(f"Invalid ZIP: {z}")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # 1. Get quarterly signals
    params = [zip_codes] * 6  # 6 references to zip list in SQL
    cols, rows = _execute(db, GENTRIFICATION_SIGNALS_SQL, params)
    if not rows:
        raise ToolError("No data found for these ZIP codes")

    # 2. Forecast each signal 4 quarters ahead using anofox_forecast
    signals = ["new_restaurants", "hpd_complaints", "noise_calls", "dob_violations", "construction_complaints"]
    forecasts = {}

    try:
        # Build temp table for forecasting
        with _db_lock:
            tbl = "_gent_tmp"
            db.execute(f"DROP TABLE IF EXISTS {tbl}")
            db.execute(f"""CREATE TEMP TABLE {tbl} (
                zip VARCHAR, q TIMESTAMP,
                new_restaurants INTEGER, hpd_complaints INTEGER,
                noise_calls INTEGER, dob_violations INTEGER,
                construction_complaints INTEGER
            )""")
            if rows:
                db.executemany(f"INSERT INTO {tbl} VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

            for signal in signals:
                try:
                    # Pivot to id/ds/y format for each signal
                    pivot_tbl = f"_gent_{signal}"
                    db.execute(f"DROP TABLE IF EXISTS {pivot_tbl}")
                    db.execute(f"""
                        CREATE TEMP TABLE {pivot_tbl} AS
                        SELECT zip AS id, q AS ds, {signal} AS y
                        FROM {tbl}
                        ORDER BY zip, q
                    """)

                    fcst = db.execute(f"""
                        SELECT * FROM ts_forecast_by('{pivot_tbl}', id, ds, y,
                            'AutoETS', 4, '1q', MAP{{}})
                    """).fetchall()
                    forecasts[signal] = fcst
                    db.execute(f"DROP TABLE IF EXISTS {pivot_tbl}")
                except Exception as e:
                    print(f"Forecast failed for {signal}: {e}", flush=True)

            db.execute(f"DROP TABLE IF EXISTS {tbl}")
    except Exception as e:
        print(f"Forecast setup failed: {e}", flush=True)

    # 3. Get IRS income trend
    inc_cols, inc_rows = _execute(db, IRS_INCOME_TREND_SQL, [zip_codes])

    # 4. Build readable summary per ZIP
    # Organize historical data by zip
    by_zip = {}
    for r in rows:
        by_zip.setdefault(r[0], []).append(r)

    signal_labels = {
        "new_restaurants": "Restaurants opened",
        "hpd_complaints": "HPD complaints",
        "noise_calls": "Noise/NYPD 311 calls",
        "dob_violations": "DOB violations",
        "construction_complaints": "Construction 311 calls",
    }

    lines = ["GENTRIFICATION PRESSURE TRACKER", "=" * 40]

    for zip_code in sorted(by_zip):
        zrows = by_zip[zip_code]
        lines.append(f"\nZIP {zip_code}:")
        lines.append("  TREND (2020-21 avg → 2024-25 avg per quarter):")

        # Compare first year avg vs last year avg
        early = [r for r in zrows if str(r[1])[:4] in ("2020", "2021")]
        recent = [r for r in zrows if str(r[1])[:4] in ("2024", "2025")]

        if early and recent:
            for i, signal in enumerate(signals, 2):
                label = signal_labels.get(signal, signal)
                early_avg = sum(r[i] for r in early) / len(early)
                recent_avg = sum(r[i] for r in recent) / len(recent)
                if early_avg > 0:
                    pct = ((recent_avg - early_avg) / early_avg) * 100
                    arrow = "+" if pct > 0 else ""
                    lines.append(f"    {label}: {early_avg:.0f} → {recent_avg:.0f}/qtr ({arrow}{pct:.0f}%)")
                else:
                    lines.append(f"    {label}: 0 → {recent_avg:.0f}/qtr")

        # Forecasts
        fcst_lines = []
        for signal in signals:
            if signal in forecasts:
                label = signal_labels.get(signal, signal)
                zip_fcsts = [f for f in forecasts[signal] if f[0] == zip_code]
                if zip_fcsts:
                    last_fcst = zip_fcsts[-1]
                    fcst_lines.append(
                        f"    {label}: {last_fcst[3]:.0f} [{last_fcst[4]:.0f}–{last_fcst[5]:.0f}]"
                    )
        if fcst_lines:
            lines.append("  FORECAST (Q+4, 95% CI):")
            lines.extend(fcst_lines)

        # Income trend
        zip_inc = [r for r in inc_rows if r[0] == zip_code]
        if zip_inc:
            first_inc = zip_inc[0]
            last_inc = zip_inc[-1]
            if first_inc[2] and last_inc[2]:
                inc_change = ((last_inc[2] - first_inc[2]) / first_inc[2]) * 100
                lines.append(
                    f"  Income: ${first_inc[2]:,.0f} ({first_inc[1]})"
                    f" → ${last_inc[2]:,.0f} ({last_inc[1]})"
                    f" ({'+' if inc_change > 0 else ''}{inc_change:.0f}%)"
                )

    # HMDA investor & lending activity (borough-level)
    boroughs_seen = set()
    for z in zip_codes:
        pfx = z[:3]
        if pfx in ("100", "101", "102"):
            boro = "Manhattan"
        elif pfx in ("104", "105"):
            boro = "Bronx"
        elif pfx in ("112", "111"):
            boro = "Brooklyn"
        elif pfx in ("113", "114", "116"):
            boro = "Queens"
        elif pfx == "103":
            boro = "Staten Island"
        else:
            boro = None
        if boro and boro not in boroughs_seen:
            boroughs_seen.add(boro)
            _, hmda_rows = _safe_query(db, GENT_HMDA_INVESTOR_SQL, [boro])
            if hmda_rows:
                lines.append(f"\nHMDA LENDING — {boro}:")
                for r in hmda_rows:
                    inv_pct = f"{r[3]}%" if r[3] else "0%"
                    lines.append(f"  {r[0]}: {r[1]:,} loans ({inv_pct} investor)")
                # Racial disparity
                latest = hmda_rows[-1]
                if latest[7] and latest[8]:
                    lines.append(f"  Denial rates (latest): Black {latest[7]}% vs White {latest[8]}%")
                    if latest[7] > latest[8]:
                        ratio = latest[7] / latest[8] if latest[8] > 0 else 0
                        lines.append(f"  Black applicants denied at {ratio:.1f}x the white rate")

            # Evictions
            _, evict_rows = _safe_query(db, GENT_EVICTION_SQL, [boro])
            if evict_rows:
                lines.append(f"\nEVICTIONS — {boro}:")
                for r in evict_rows:
                    if r[0]:
                        lines.append(f"  {int(r[0])}: {r[1]:,}")

    # Affordable housing in these ZIPs
    _, afford_rows = _safe_query(db, GENT_AFFORDABLE_LOSS_SQL, [zip_codes])
    if afford_rows and afford_rows[0][0]:
        total_bldg = int(afford_rows[0][0] or 0)
        total_units = int(afford_rows[0][1] or 0)
        rental_units = int(afford_rows[0][2] or 0)
        if total_bldg > 0:
            lines.append(f"\nAFFORDABLE HOUSING: {total_bldg} buildings, {total_units:,} units ({rental_units:,} rental)")

    # Recent rezonings (citywide, most recent)
    _, rezone_rows = _safe_query(db, GENT_REZONING_SQL, [])
    if rezone_rows:
        lines.append(f"\nRECENT REZONINGS (adopted, citywide):")
        for r in rezone_rows[:8]:
            eff = str(r[2])[:10] if r[2] else "N/A"
            lines.append(f"  {r[0]} — {eff} ({r[3]})")

    # Actionable steps
    lines.append("\nWHAT YOU CAN DO:")
    lines.append("  1. Right to Counsel: FREE lawyer for eviction if eligible — 718-557-1379")
    lines.append("  2. Rent stabilization lookup: hcr.ny.gov/building-search")
    lines.append("  3. Report illegal rent increases: hcr.ny.gov or 718-739-6400")
    lines.append("  4. Attend Community Board land use hearings (rezonings must go through ULURP)")
    lines.append("  5. Churches United for Fair Housing: cuffh.org (anti-displacement organizing)")
    lines.append("  6. Right to the City Alliance: righttothecity.org")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    # Historical table
    text = "\n".join(lines)
    text += "\n\nQUARTERLY DETAIL:\n" + format_text_table(cols, rows, max_rows=40)

    structured = {
        "historical": [dict(zip(cols, r)) for r in rows],
        "income_trend": [dict(zip(inc_cols, r)) for r in inc_rows] if inc_rows else [],
        "forecasts": {
            signal: [{"zip": f[0], "horizon": f[1], "date": str(f[2]),
                       "forecast": f[3], "lo": f[4], "hi": f[5], "model": f[6]}
                      for f in flist]
            for signal, flist in forecasts.items()
        },
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"zip_codes": zip_codes, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Safety Report
# ---------------------------------------------------------------------------

SAFETY_CRIME_SQL = """
WITH crimes_hist AS (
    SELECT ofns_desc, law_cat_cd,
           EXTRACT(YEAR FROM TRY_CAST(rpt_dt AS DATE)) AS yr
    FROM lake.public_safety.nypd_complaints_historic
    WHERE addr_pct_cd = ?
      AND TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 3 YEAR
),
crimes_ytd AS (
    SELECT ofns_desc, law_cat_cd,
           EXTRACT(YEAR FROM TRY_CAST(rpt_dt AS DATE)) AS yr
    FROM lake.public_safety.nypd_complaints_ytd
    WHERE addr_pct_cd = ?
),
crimes AS (SELECT * FROM crimes_hist UNION ALL SELECT * FROM crimes_ytd),
by_year AS (
    SELECT yr, COUNT(*) AS total,
           COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies,
           COUNT(*) FILTER (WHERE law_cat_cd = 'MISDEMEANOR') AS misdemeanors,
           COUNT(*) FILTER (WHERE law_cat_cd = 'VIOLATION') AS violations
    FROM crimes WHERE yr IS NOT NULL GROUP BY yr ORDER BY yr
),
top_crimes AS (
    SELECT ofns_desc, COUNT(*) AS cnt
    FROM crimes WHERE yr = EXTRACT(YEAR FROM CURRENT_DATE) OR yr = EXTRACT(YEAR FROM CURRENT_DATE) - 1
    GROUP BY ofns_desc ORDER BY cnt DESC LIMIT 8
)
SELECT 'crime_by_year' AS section, yr, total, felonies, misdemeanors, violations, NULL AS ofns_desc
FROM by_year
UNION ALL
SELECT 'top_crimes', NULL, cnt, NULL, NULL, NULL, ofns_desc FROM top_crimes
"""

SAFETY_ARRESTS_SQL = """
WITH arrests_hist AS (
    SELECT ofns_desc, law_cat_cd, perp_race, perp_sex, age_group,
           EXTRACT(YEAR FROM TRY_CAST(arrest_date AS DATE)) AS yr
    FROM lake.public_safety.nypd_arrests_historic
    WHERE arrest_precinct = ?
      AND TRY_CAST(arrest_date AS DATE) >= CURRENT_DATE - INTERVAL 3 YEAR
),
arrests_ytd AS (
    SELECT ofns_desc, law_cat_cd, perp_race, perp_sex, age_group,
           EXTRACT(YEAR FROM TRY_CAST(arrest_date AS DATE)) AS yr
    FROM lake.public_safety.nypd_arrests_ytd
    WHERE arrest_precinct = ?
),
arrests AS (SELECT * FROM arrests_hist UNION ALL SELECT * FROM arrests_ytd),
by_year AS (
    SELECT yr, COUNT(*) AS total,
           COUNT(*) FILTER (WHERE law_cat_cd = 'F') AS felonies,
           COUNT(*) FILTER (WHERE law_cat_cd = 'M') AS misdemeanors
    FROM arrests WHERE yr IS NOT NULL GROUP BY yr ORDER BY yr
),
top_charges AS (
    SELECT ofns_desc, COUNT(*) AS cnt
    FROM arrests WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
    GROUP BY ofns_desc ORDER BY cnt DESC LIMIT 8
),
demographics AS (
    SELECT perp_race, COUNT(*) AS cnt
    FROM arrests WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
      AND perp_race IS NOT NULL AND perp_race NOT IN ('UNKNOWN', 'OTHER', '(null)')
    GROUP BY perp_race ORDER BY cnt DESC
),
sex_breakdown AS (
    SELECT perp_sex, COUNT(*) AS cnt
    FROM arrests WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
      AND perp_sex IS NOT NULL
    GROUP BY perp_sex ORDER BY cnt DESC
),
age_breakdown AS (
    SELECT age_group, COUNT(*) AS cnt
    FROM arrests WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
      AND age_group IS NOT NULL
    GROUP BY age_group ORDER BY cnt DESC
)
SELECT 'arrest_by_year' AS section, yr::VARCHAR AS key1, total::VARCHAR AS val1,
       felonies::VARCHAR AS val2, misdemeanors::VARCHAR AS val3
FROM by_year
UNION ALL SELECT 'top_charges', ofns_desc, cnt::VARCHAR, NULL, NULL FROM top_charges
UNION ALL SELECT 'race', perp_race, cnt::VARCHAR, NULL, NULL FROM demographics
UNION ALL SELECT 'sex', perp_sex, cnt::VARCHAR, NULL, NULL FROM sex_breakdown
UNION ALL SELECT 'age', age_group, cnt::VARCHAR, NULL, NULL FROM age_breakdown
"""

SAFETY_SHOOTINGS_SQL = """
SELECT EXTRACT(YEAR FROM TRY_CAST(occur_date AS DATE)) AS yr,
       COUNT(*) AS shootings
FROM lake.public_safety.shootings
WHERE precinct = ?
  AND TRY_CAST(occur_date AS DATE) >= CURRENT_DATE - INTERVAL 5 YEAR
GROUP BY yr ORDER BY yr
"""

SAFETY_SUMMONS_SQL = """
WITH summons AS (
    SELECT offense_description,
           EXTRACT(YEAR FROM TRY_CAST(summons_date AS DATE)) AS yr
    FROM lake.public_safety.criminal_court_summons
    WHERE precinct_of_occur = ?
      AND TRY_CAST(summons_date AS DATE) >= CURRENT_DATE - INTERVAL 3 YEAR
),
by_year AS (
    SELECT yr, COUNT(*) AS total FROM summons WHERE yr IS NOT NULL GROUP BY yr ORDER BY yr
),
top_types AS (
    SELECT offense_description, COUNT(*) AS cnt
    FROM summons WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
    GROUP BY offense_description ORDER BY cnt DESC LIMIT 8
)
SELECT 'summons_by_year' AS section, yr::VARCHAR AS key1, total::VARCHAR AS val1
FROM by_year
UNION ALL SELECT 'top_summons', offense_description, cnt::VARCHAR FROM top_types
"""

SAFETY_HATE_SQL = """
SELECT bias_motive_description, offense_category, COUNT(*) AS cnt
FROM lake.public_safety.hate_crimes
WHERE complaint_precinct_code = ?
  AND TRY_CAST(complaint_year_number AS INT) >= EXTRACT(YEAR FROM CURRENT_DATE) - 2
GROUP BY bias_motive_description, offense_category
ORDER BY cnt DESC LIMIT 10
"""

SAFETY_CITY_AVERAGES_SQL = """
WITH arrest_totals AS (
    SELECT arrest_precinct AS pct, COUNT(*) AS arrests
    FROM lake.public_safety.nypd_arrests_historic
    WHERE TRY_CAST(arrest_date AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR
    GROUP BY arrest_precinct
),
crime_totals AS (
    SELECT addr_pct_cd AS pct, COUNT(*) AS crimes
    FROM lake.public_safety.nypd_complaints_historic
    WHERE TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR
    GROUP BY addr_pct_cd
)
SELECT
    AVG(a.arrests) AS avg_arrests,
    AVG(c.crimes) AS avg_crimes,
    AVG(CASE WHEN c.crimes > 0 THEN a.arrests * 1000.0 / c.crimes END) AS avg_arrest_rate_per_1k
FROM arrest_totals a
JOIN crime_totals c ON a.pct = c.pct
"""


SAFETY_CCRB_SQL = """
WITH complaints AS (
    SELECT complaint_id, ccrb_complaint_disposition, reason_for_police_contact,
           EXTRACT(YEAR FROM TRY_CAST(incident_date AS DATE)) AS yr
    FROM lake.public_safety.ccrb_complaints
    WHERE precinct_of_incident_occurrence = ?
),
by_year AS (
    SELECT yr, COUNT(DISTINCT complaint_id) AS cnt
    FROM complaints WHERE yr IS NOT NULL GROUP BY yr ORDER BY yr DESC LIMIT 5
),
by_disposition AS (
    SELECT ccrb_complaint_disposition AS disposition, COUNT(DISTINCT complaint_id) AS cnt
    FROM complaints WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 3
    GROUP BY ccrb_complaint_disposition ORDER BY cnt DESC
),
by_reason AS (
    SELECT reason_for_police_contact AS reason, COUNT(DISTINCT complaint_id) AS cnt
    FROM complaints WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 3
    GROUP BY reason_for_police_contact ORDER BY cnt DESC LIMIT 5
)
SELECT 'ccrb_by_year' AS section, yr::VARCHAR AS k1, cnt::VARCHAR AS k2, NULL AS k3
FROM by_year
UNION ALL SELECT 'ccrb_disposition', disposition, cnt::VARCHAR, NULL FROM by_disposition
UNION ALL SELECT 'ccrb_reason', reason, cnt::VARCHAR, NULL FROM by_reason
"""

SAFETY_CCRB_ALLEGATIONS_SQL = """
SELECT fado_type, COUNT(*) AS total,
       COUNT(*) FILTER (WHERE ccrb_allegation_disposition = 'Substantiated') AS substantiated,
       COUNT(*) FILTER (WHERE ccrb_allegation_disposition = 'Exonerated') AS exonerated,
       COUNT(*) FILTER (WHERE ccrb_allegation_disposition = 'Unsubstantiated') AS unsubstantiated
FROM lake.public_safety.ccrb_allegations a
JOIN lake.public_safety.ccrb_complaints c
  ON a.complaint_id = c.complaint_id
WHERE c.precinct_of_incident_occurrence = ?
GROUP BY fado_type ORDER BY total DESC
"""

SAFETY_CCRB_VICTIM_RACE_SQL = """
SELECT COALESCE(a.victim_allegedvictim_race_legacy, 'Unknown') AS race, COUNT(*) AS cnt
FROM lake.public_safety.ccrb_allegations a
JOIN lake.public_safety.ccrb_complaints c
  ON a.complaint_id = c.complaint_id
WHERE c.precinct_of_incident_occurrence = ?
  AND a.victim_allegedvictim_race_legacy IS NOT NULL
  AND a.victim_allegedvictim_race_legacy NOT IN ('Unknown', 'Refused', '')
GROUP BY race ORDER BY cnt DESC
"""

SAFETY_UOF_SQL = """
SELECT forcetype, COUNT(*) AS cnt,
       COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM TRY_CAST(occurrence_date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE)) AS this_year,
       COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM TRY_CAST(occurrence_date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE) - 1) AS last_year
FROM lake.public_safety.use_of_force_incidents
WHERE incident_pct = ?
GROUP BY forcetype ORDER BY cnt DESC
"""

SAFETY_UOF_SUBJECT_SQL = """
SELECT s.subject_race, COUNT(*) AS cnt,
       COUNT(*) FILTER (WHERE s.subject_injured = 'Y') AS injured,
       COUNT(*) FILTER (WHERE s.subject_injury_level NOT IN ('No Injury', 'N')) AS serious_injury
FROM lake.public_safety.use_of_force_subjects s
JOIN lake.public_safety.use_of_force_incidents i
  ON s.tri_incident_number = i.tri_incident_number
WHERE i.incident_pct = ?
  AND s.subject_race IS NOT NULL AND s.subject_race != ''
GROUP BY s.subject_race ORDER BY cnt DESC
"""

SAFETY_UOF_CITY_AVG_SQL = """
SELECT AVG(pct_cnt) AS avg_uof_per_pct
FROM (
    SELECT incident_pct, COUNT(*) AS pct_cnt
    FROM lake.public_safety.use_of_force_incidents
    WHERE incident_pct IS NOT NULL
    GROUP BY incident_pct
)
"""

SAFETY_DISCIPLINE_SQL = """
SELECT charge_description, disposition, penalty_and_quantity
FROM lake.public_safety.discipline_charges
ORDER BY TRY_CAST(date AS DATE) DESC
LIMIT 500
"""


@mcp.tool(annotations=READONLY, tags={"safety"})
def safety_report(precinct: Annotated[int, Field(description="NYPD precinct number 1-123. Example: 75 (East New York), 14 (Midtown South)", ge=1, le=123)], ctx: Context) -> ToolResult:
    """Comprehensive safety & policing report for an NYPD precinct (1-123). Crime trends, arrest demographics, enforcement intensity, shootings, summons, hate crimes, CCRB misconduct, Use of Force, and officer discipline. Use this for crime/safety/policing questions. Need a ZIP instead of precinct? Use neighborhood_portrait(zip) which includes safety context. Examples: safety_report(75) for East New York, safety_report(14) for Midtown South."""
    if precinct < 1 or precinct > 123:
        raise ToolError("Precinct must be between 1 and 123")

    pct = str(precinct)
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Run all queries
    crime_cols, crime_rows = _execute(db, SAFETY_CRIME_SQL, [pct, pct])
    arrest_cols, arrest_rows = _execute(db, SAFETY_ARRESTS_SQL, [pct, pct])
    shoot_cols, shoot_rows = _execute(db, SAFETY_SHOOTINGS_SQL, [pct])
    summ_cols, summ_rows = _execute(db, SAFETY_SUMMONS_SQL, [pct])
    hate_cols, hate_rows = _execute(db, SAFETY_HATE_SQL, [pct])
    avg_cols, avg_rows = _execute(db, SAFETY_CITY_AVERAGES_SQL, [])

    # Parse crime data
    crime_years = [(r[1], r[2], r[3], r[4], r[5]) for r in crime_rows if r[0] == "crime_by_year"]
    top_crimes = [(r[6], r[2]) for r in crime_rows if r[0] == "top_crimes"]

    # Parse arrest data (all values are VARCHAR from UNION ALL)
    arrest_years = [(r[1], int(r[2]) if r[2] else 0, int(r[3]) if r[3] else 0,
                     int(r[4]) if r[4] else 0) for r in arrest_rows if r[0] == "arrest_by_year"]
    top_charges = [(r[1], int(r[2]) if r[2] else 0) for r in arrest_rows if r[0] == "top_charges"]
    race_data = [(r[1], int(r[2])) for r in arrest_rows if r[0] == "race"]
    sex_data = [(r[1], int(r[2])) for r in arrest_rows if r[0] == "sex"]
    age_data = [(r[1], int(r[2])) for r in arrest_rows if r[0] == "age"]

    lines = [
        f"SAFETY REPORT — Precinct {precinct}",
        "=" * 45,
    ]

    # Crime overview
    lines.append("\nCRIME (3-year trend):")
    for yr, total, fel, misd, viol in crime_years:
        yr_int = int(yr) if yr else 0
        lines.append(f"  {yr_int}: {total:,} total ({fel:,} felony, {misd:,} misdemeanor)")
    if top_crimes:
        lines.append("  Top offenses (recent):")
        for desc, cnt in top_crimes[:6]:
            lines.append(f"    {desc}: {cnt:,}")

    # Shootings
    lines.append("\nGUN VIOLENCE (5-year):")
    if shoot_rows:
        for r in shoot_rows:
            lines.append(f"  {int(r[0])}: {r[1]} shootings")
    else:
        lines.append("  No shootings recorded")

    # Arrests
    lines.append("\nARRESTS (3-year trend):")
    for yr, total, fel, misd in arrest_years:
        lines.append(f"  {yr}: {total:,} total ({fel:,} felony, {misd:,} misdemeanor)")

    if top_charges:
        lines.append("  Top charges (recent):")
        for desc, cnt in top_charges[:6]:
            lines.append(f"    {desc}: {cnt:,}")

    # Enforcement intensity — arrest-to-crime ratio
    lines.append("\nENFORCEMENT INTENSITY:")
    # Most recent full year
    if crime_years and arrest_years:
        # Use last full year (not current partial year)
        last_crime_yr = crime_years[-2] if len(crime_years) > 1 else crime_years[-1]
        last_arrest_yr = arrest_years[-2] if len(arrest_years) > 1 else arrest_years[-1]
        if last_crime_yr[1] and last_crime_yr[1] > 0:
            total_arrests = int(last_arrest_yr[1])
            total_crimes = int(last_crime_yr[1])
            ratio = total_arrests * 1000.0 / total_crimes
            lines.append(f"  Arrests per 1,000 crimes: {ratio:.0f}")
            if avg_rows and avg_rows[0][2]:
                city_avg = float(avg_rows[0][2])
                pct_diff = ((ratio - city_avg) / city_avg) * 100
                qualifier = "above" if pct_diff > 0 else "below"
                lines.append(f"  City average: {city_avg:.0f} per 1,000 ({abs(pct_diff):.0f}% {qualifier})")
                if pct_diff > 30:
                    lines.append("  ⚠ HIGH enforcement intensity — significantly above city average")
                elif pct_diff < -30:
                    lines.append("  LOW enforcement intensity — significantly below city average")

    # Summons
    summ_years = [(r[1], r[2]) for r in summ_rows if r[0] == "summons_by_year"]
    top_summons = [(r[1], r[2]) for r in summ_rows if r[0] == "top_summons"]
    if summ_years:
        lines.append("\nSUMMONS (3-year trend):")
        for yr, total in summ_years:
            lines.append(f"  {yr}: {total}")
        if top_summons:
            lines.append("  Top types:")
            for desc, cnt in top_summons[:5]:
                lines.append(f"    {desc}: {cnt}")

    # Arrest demographics
    if race_data:
        total_race = sum(cnt for _, cnt in race_data)
        lines.append("\nARREST DEMOGRAPHICS (recent):")
        lines.append("  Race:")
        for race, cnt in race_data:
            pct_val = cnt * 100.0 / total_race
            lines.append(f"    {race}: {cnt:,} ({pct_val:.1f}%)")
    if sex_data:
        total_sex = sum(cnt for _, cnt in sex_data)
        lines.append("  Sex:")
        for sex, cnt in sex_data:
            label = {"M": "Male", "F": "Female"}.get(sex, sex)
            lines.append(f"    {label}: {cnt:,} ({cnt * 100.0 / total_sex:.1f}%)")
    if age_data:
        lines.append("  Age:")
        for age, cnt in age_data:
            lines.append(f"    {age}: {cnt:,}")

    # Hate crimes
    if hate_rows:
        lines.append("\nHATE CRIMES (recent):")
        for r in hate_rows:
            lines.append(f"  {r[0]} — {r[1]} ({r[2]} incidents)")
    else:
        lines.append("\nHATE CRIMES: None reported in this precinct recently")

    # CCRB misconduct complaints
    _, ccrb_rows = _safe_query(db, SAFETY_CCRB_SQL, [pct])
    ccrb_years = [(r[1], int(r[2])) for r in ccrb_rows if r[0] == "ccrb_by_year"]
    ccrb_dispositions = [(r[1], int(r[2])) for r in ccrb_rows if r[0] == "ccrb_disposition"]
    ccrb_reasons = [(r[1], int(r[2])) for r in ccrb_rows if r[0] == "ccrb_reason"]

    _, fado_rows = _safe_query(db, SAFETY_CCRB_ALLEGATIONS_SQL, [pct])
    _, victim_race_rows = _safe_query(db, SAFETY_CCRB_VICTIM_RACE_SQL, [pct])

    if ccrb_years:
        lines.append("\nCCRB MISCONDUCT COMPLAINTS:")
        for yr, cnt in ccrb_years:
            lines.append(f"  {yr}: {cnt} complaints")
        if fado_rows:
            lines.append("  By type (FADO):")
            for r in fado_rows:
                sub_rate = f"{r[2] * 100 / r[1]:.0f}%" if r[1] > 0 else "0%"
                lines.append(f"    {r[0]}: {r[1]} allegations ({sub_rate} substantiated)")
        if ccrb_dispositions:
            lines.append("  Outcomes:")
            for disp, cnt in ccrb_dispositions[:5]:
                lines.append(f"    {disp}: {cnt}")
        if victim_race_rows:
            total_victims = sum(r[1] for r in victim_race_rows)
            lines.append("  Complainant race:")
            for race, cnt in victim_race_rows:
                lines.append(f"    {race}: {cnt} ({cnt * 100 / total_victims:.1f}%)")
        if ccrb_reasons:
            lines.append("  Reasons for contact:")
            for reason, cnt in ccrb_reasons:
                lines.append(f"    {reason}: {cnt}")

    # Use of Force
    _, uof_rows = _safe_query(db, SAFETY_UOF_SQL, [pct])
    _, uof_subj_rows = _safe_query(db, SAFETY_UOF_SUBJECT_SQL, [pct])
    _, uof_avg_rows = _safe_query(db, SAFETY_UOF_CITY_AVG_SQL, [])

    if uof_rows:
        total_uof = sum(int(r[1]) for r in uof_rows)
        lines.append(f"\nUSE OF FORCE: {total_uof:,} incidents")
        city_avg_uof = float(uof_avg_rows[0][0]) if uof_avg_rows and uof_avg_rows[0][0] else 0
        if city_avg_uof > 0:
            ratio = total_uof / city_avg_uof
            lines.append(f"  {ratio:.1f}x city precinct average ({city_avg_uof:.0f})")
        for r in uof_rows:
            lines.append(f"  {r[0]}: {r[1]:,} total ({r[2]} this year, {r[3]} last year)")
        if uof_subj_rows:
            total_subj = sum(int(r[1]) for r in uof_subj_rows)
            total_injured = sum(int(r[2]) for r in uof_subj_rows)
            lines.append(f"  Subject demographics ({total_subj} subjects, {total_injured} injured):")
            for r in uof_subj_rows:
                pct_val = int(r[1]) * 100 / total_subj if total_subj > 0 else 0
                lines.append(f"    {r[0]}: {r[1]} ({pct_val:.1f}%)")

    # Know your rights
    lines.append("\nKNOW YOUR RIGHTS:")
    lines.append("  File CCRB complaint: ccrb.nyc.gov or 800-341-2272")
    lines.append("  You have the right to record police encounters")
    lines.append("  Request officer name, badge number, and command")
    lines.append("  Legal aid (misconduct): 212-577-3300 (Legal Aid Society)")
    lines.append("  Cop Accountability Project: communityresource.io")

    # H3 crime hotspot analysis
    try:
        from spatial import h3_heatmap_sql
        _, pct_center = _execute(db, f"""
            SELECT AVG(TRY_CAST(latitude AS DOUBLE)), AVG(TRY_CAST(longitude AS DOUBLE))
            FROM lake.city_government.pluto
            WHERE policeprct = '{precinct}'
              AND TRY_CAST(latitude AS DOUBLE) BETWEEN 40.4 AND 41.0
        """)
        if pct_center and pct_center[0][0]:
            lat, lng = pct_center[0][0], pct_center[0][1]
            hm_cols, hm_rows = _execute(db, *h3_heatmap_sql(
                source_table="lake.foundation.h3_index",
                filter_table="public_safety.nypd_complaints_ytd",
                lat=lat, lng=lng, radius_rings=10))
            if hm_rows:
                hotspots = [dict(zip(hm_cols, r)) for r in hm_rows[:5]]
                lines.append(f"\nCRIME HOTSPOT CELLS (top 5 H3 hexes):")
                for h in hotspots:
                    lines.append(f"  ({h['cell_lat']:.4f}, {h['cell_lng']:.4f}): {h['count']:,} incidents")
    except Exception:
        pass

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)

    structured = {
        "precinct": precinct,
        "crime_by_year": [{"year": int(r[0]) if r[0] else None, "total": r[1],
                           "felonies": r[2], "misdemeanors": r[3]} for r in crime_years],
        "top_crimes": [{"offense": r[0], "count": r[1]} for r in top_crimes],
        "shootings_by_year": [{"year": int(r[0]), "count": r[1]} for r in shoot_rows] if shoot_rows else [],
        "arrests_by_year": [{"year": r[0], "total": r[1], "felonies": r[2],
                             "misdemeanors": r[3]} for r in arrest_years],
        "top_charges": [{"charge": r[0], "count": r[1]} for r in top_charges],
        "arrest_demographics": {
            "race": [{"group": r[0], "count": r[1]} for r in race_data],
            "sex": [{"group": r[0], "count": r[1]} for r in sex_data],
            "age": [{"group": r[0], "count": r[1]} for r in age_data],
        },
        "summons_by_year": [{"year": r[0], "total": r[1]} for r in summ_years],
        "hate_crimes": [{"bias": r[0], "category": r[1], "count": r[2]} for r in hate_rows] if hate_rows else [],
        "ccrb": {
            "by_year": [{"year": r[0], "count": r[1]} for r in ccrb_years],
            "fado": [{"type": r[0], "total": r[1], "substantiated": r[2]} for r in (fado_rows or [])],
            "victim_race": [{"race": r[0], "count": r[1]} for r in (victim_race_rows or [])],
            "dispositions": [{"outcome": r[0], "count": r[1]} for r in ccrb_dispositions],
        },
        "use_of_force": {
            "total": sum(int(r[1]) for r in uof_rows) if uof_rows else 0,
            "by_type": [{"type": r[0], "count": r[1]} for r in (uof_rows or [])],
            "subject_race": [{"race": r[0], "count": r[1], "injured": r[2]} for r in (uof_subj_rows or [])],
        },
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"precinct": precinct, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# SQL constants — landlord_watchdog
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
  AND "residential/commercial" = 'Residential'
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


def _safe_query(db, sql, params=None):
    """Execute SQL, return (cols, rows) or ([], []) if table doesn't exist."""
    try:
        return _execute(db, sql, params)
    except ToolError:
        return [], []


def _fill_placeholders(sql_template, bbls):
    """Replace {placeholders} with ?,?,? for the BBL list."""
    ph = ",".join(["?"] * len(bbls))
    return sql_template.replace("{placeholders}", ph)


@mcp.tool(annotations=READONLY, tags={"housing"})
def landlord_watchdog(bbl: BBL, ctx: Context) -> ToolResult:
    """Investigate a landlord's FULL portfolio — all buildings, violations, complaints, evictions, enforcement flags (AEP, CONH, Underlying Conditions), and slumlord score. Use this when a user asks about a landlord, building owner, or tenant rights. For ownership NETWORK analysis (graph traversal, shell companies), use landlord_network(bbl). For LLC piercing, use llc_piercer(entity_name). BBL is 10 digits: borough(1) + block(5) + lot(4). Example: 1000670001. Returns actionable next steps: hotlines, tenant rights, legal aid contacts."""
    if not re.match(r"^\d{10}$", bbl):
        raise ToolError(
            "BBL must be exactly 10 digits. Format: borough(1) + block(5) + lot(4)"
        )

    t0 = time.time()
    db = ctx.lifespan_context["db"]

    # Step 1: Look up the target building
    _, rows = _execute(db, WATCHDOG_BUILDING_SQL, [bbl])
    if not rows:
        raise ToolError(f"No building found for BBL {bbl}")

    bldg = rows[0]
    reg_id = bldg[4]  # registrationid
    address = f"{bldg[6]} {bldg[7]}"
    zipcode = bldg[8]
    stories = bldg[9]
    units_a = int(bldg[10] or 0)
    units_b = int(bldg[11] or 0)
    total_units_target = units_a + units_b
    mgmt = bldg[12] or "N/A"

    # Step 2: Find all buildings in portfolio
    portfolio_bbls = [bbl]
    portfolio_buildings = []
    if reg_id and reg_id != "0" and not reg_id.endswith("995"):
        _, port_rows = _execute(db, WATCHDOG_PORTFOLIO_SQL, [reg_id])
        portfolio_buildings = port_rows
        portfolio_bbls = list(set(r[10] for r in port_rows))  # bbl column
        if bbl not in portfolio_bbls:
            portfolio_bbls.append(bbl)

    portfolio_size = len(portfolio_bbls)
    total_portfolio_units = sum(
        int(r[8] or 0) + int(r[9] or 0)  # legalclassa + legalclassb
        for r in portfolio_buildings
    ) if portfolio_buildings else total_units_target

    # Step 3: Aggregate violations across portfolio
    v_sql = _fill_placeholders(WATCHDOG_VIOLATIONS_SQL, portfolio_bbls)
    _, v_rows = _execute(db, v_sql, portfolio_bbls)

    total_violations = sum(int(r[1] or 0) for r in v_rows)
    total_open = sum(int(r[2] or 0) for r in v_rows)
    total_class_c = sum(int(r[3] or 0) for r in v_rows)
    total_class_b = sum(int(r[4] or 0) for r in v_rows)
    total_class_a = sum(int(r[5] or 0) for r in v_rows)
    total_open_c = sum(int(r[6] or 0) for r in v_rows)

    # Target building violations
    target_v = next((r for r in v_rows if r[0] == bbl), None)
    target_violations = int(target_v[1] or 0) if target_v else 0
    target_open = int(target_v[2] or 0) if target_v else 0
    target_class_c = int(target_v[3] or 0) if target_v else 0
    target_open_c = int(target_v[6] or 0) if target_v else 0

    # Step 4: Complaints
    c_sql = _fill_placeholders(WATCHDOG_COMPLAINTS_SQL, portfolio_bbls)
    _, c_rows = _execute(db, c_sql, portfolio_bbls)
    total_complaints = sum(int(r[1] or 0) for r in c_rows)
    total_open_complaints = sum(int(r[2] or 0) for r in c_rows)

    # Top complaint categories
    tc_sql = _fill_placeholders(WATCHDOG_TOP_COMPLAINTS_SQL, portfolio_bbls)
    _, tc_rows = _execute(db, tc_sql, portfolio_bbls)

    # Step 5: Evictions
    e_sql = _fill_placeholders(WATCHDOG_EVICTIONS_SQL, portfolio_bbls)
    _, e_rows = _execute(db, e_sql, portfolio_bbls)
    eviction_count = int(e_rows[0][0] or 0) if e_rows else 0

    # Step 6: DOB violations
    d_sql = _fill_placeholders(WATCHDOG_DOB_SQL, portfolio_bbls)
    _, d_rows = _execute(db, d_sql, portfolio_bbls)
    dob_violations = int(d_rows[0][0] or 0) if d_rows else 0
    dob_unpaid = int(d_rows[0][1] or 0) if d_rows else 0

    # Step 7: Enforcement flags (AEP, CONH, Underlying Conditions)
    aep_buildings = []
    conh_buildings = []
    underlying_buildings = []

    aep_sql = _fill_placeholders(WATCHDOG_AEP_SQL, portfolio_bbls)
    _, aep_rows = _safe_query(db, aep_sql, portfolio_bbls)
    aep_buildings = [(r[0], r[1], r[2]) for r in aep_rows] if aep_rows else []

    conh_sql = _fill_placeholders(WATCHDOG_CONH_SQL, portfolio_bbls)
    _, conh_rows = _safe_query(db, conh_sql, portfolio_bbls)
    conh_buildings = [(r[0], r[1]) for r in conh_rows] if conh_rows else []

    und_sql = _fill_placeholders(WATCHDOG_UNDERLYING_SQL, portfolio_bbls)
    _, und_rows = _safe_query(db, und_sql, portfolio_bbls)
    underlying_buildings = [(r[0], r[1]) for r in und_rows] if und_rows else []

    # Step 8: City averages for comparison
    _, avg_rows = _execute(db, WATCHDOG_CITY_AVERAGES_SQL, [])
    city_avg_violations = float(avg_rows[0][0] or 0) if avg_rows else 0
    city_avg_open = float(avg_rows[0][1] or 0) if avg_rows else 0

    # Percentile ranking is injected automatically by PercentileMiddleware
    violations_per_bldg = total_violations / portfolio_size if portfolio_size else 0
    vs_city = (
        f"{violations_per_bldg / city_avg_violations:.1f}x city average"
        if city_avg_violations > 0
        else "N/A"
    )

    elapsed = int((time.time() - t0) * 1000)

    # Build output
    lines = []
    lines.append(f"LANDLORD WATCHDOG REPORT — BBL {bbl}")
    lines.append("=" * 50)

    lines.append(f"\nTARGET BUILDING:")
    lines.append(f"  {address}, {zipcode}")
    lines.append(f"  {stories} stories, {total_units_target} units, program: {mgmt}")
    lines.append(f"  Violations: {target_violations:,} total, {target_open:,} open, {target_class_c:,} Class C ({target_open_c:,} open)")

    lines.append(f"\nOWNER PORTFOLIO: {portfolio_size} buildings, {total_portfolio_units:,} units")
    lines.append(f"  Registration ID: {reg_id}")

    if portfolio_size > 1:
        # Show worst buildings
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

    # Enforcement flags
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

    lines.append(f"\n(Percentile ranking added automatically)")

    lines.append(f"\nWHAT YOU CAN DO:")
    lines.append(f"  1. File HPD complaint: call 311 or nyc.gov/311")
    lines.append(f"  2. Report emergency (no heat/hot water, gas leak): call 311, press 2")
    lines.append(f"  3. Request HPD inspection: 311 complaint triggers automatic inspection")
    lines.append(f"  4. Contact tenant hotline: (212) 979-0611 (Met Council on Housing)")
    lines.append(f"  5. Free legal help: (718) 557-1379 (Legal Aid Society Housing)")
    lines.append(f"  6. File CCRB complaint if harassed by landlord's agents: ccrb.nyc.gov")
    lines.append(f"  7. Check rent stabilization status: hcr.ny.gov/building-search")
    lines.append(f"  8. Contact your City Council member — this portfolio needs oversight")
    lines.append(f"  9. JustFix.org — free tools to document conditions and send demand letters")

    text = "\n".join(lines)

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
        "owner_id": reg_id,
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"bbl": bbl, "portfolio_size": portfolio_size, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# SQL constants — environmental_justice
# ---------------------------------------------------------------------------

EJ_AIR_QUALITY_SQL = """
SELECT name, data_value, time_period, measure
FROM lake.environment.air_quality
WHERE geo_type_name = 'CD'
  AND geo_place_name ILIKE '%' || ? || '%'
  AND name IN ('Fine particles (PM 2.5)', 'Nitrogen dioxide (NO2)',
               'Ozone (O3)', 'Deaths due to PM2.5',
               'Asthma emergency department visits due to PM2.5')
ORDER BY name, time_period DESC
"""

EJ_AIR_CITY_AVG_SQL = """
SELECT name, AVG(TRY_CAST(data_value AS DOUBLE)) AS avg_val
FROM lake.environment.air_quality
WHERE geo_type_name = 'CD'
  AND name IN ('Fine particles (PM 2.5)', 'Nitrogen dioxide (NO2)')
  AND time_period = (
      SELECT MAX(time_period) FROM lake.environment.air_quality
      WHERE geo_type_name = 'CD' AND name = 'Fine particles (PM 2.5)'
  )
GROUP BY name
"""

EJ_AREAS_SQL = """
SELECT NTAName, EJDesignat, MinorityPCT, BelowPovPCT, TotPop
FROM lake.environment.arcgis_ej_areas
WHERE BoroName ILIKE ? OR NTAName ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(MinorityPCT AS DOUBLE) DESC
LIMIT 20
"""

EJ_WASTE_SQL = """
SELECT name, type, street_address, borough_city, zipcode
FROM lake.environment.waste_transfer
WHERE zipcode = ? OR borough_city ILIKE '%' || ? || '%'
"""

EJ_EPA_SQL = """
SELECT facility_name, registry_id
FROM lake.environment.epa_facilities_nyc
WHERE zip_code = ?
"""

EJ_E_DESIGNATIONS_SQL = """
SELECT bbl, enumber, description
FROM lake.environment.e_designations
WHERE borocode = ?
LIMIT 50
"""

EJ_HEALTH_ASTHMA_SQL = """
SELECT m.indicator_name, h.value, h.display_value,
       g.geo_name, g.geo_type
FROM lake.health.health_indicators h
JOIN lake.health.health_metadata m ON h.indicator_id = m.indicator_id
JOIN lake.health.health_geo_lookup g ON h.geo_id = g.geo_id AND h.geo_type = g.geo_type
WHERE g.geo_name ILIKE '%' || ? || '%'
  AND m.indicator_name IN (
      'Asthma emergency department visits (adults)',
      'Asthma hospitalizations (adults)',
      'Premature mortality',
      'Self-reported health'
  )
ORDER BY m.indicator_name
"""

EJ_HEALTH_CITY_AVG_SQL = """
SELECT m.indicator_name, AVG(TRY_CAST(h.value AS DOUBLE)) AS avg_val
FROM lake.health.health_indicators h
JOIN lake.health.health_metadata m ON h.indicator_id = m.indicator_id
WHERE h.geo_type = 'CD'
  AND m.indicator_name IN (
      'Asthma emergency department visits (adults)',
      'Asthma hospitalizations (adults)',
      'Premature mortality'
  )
GROUP BY m.indicator_name
"""

EJ_RATS_SQL = """
SELECT result, COUNT(*) AS cnt
FROM lake.health.rats_inspections
WHERE zip_code = ?
GROUP BY result ORDER BY cnt DESC
"""

EJ_TREES_SQL = """
SELECT COUNT(*) AS tree_count
FROM lake.environment.street_trees
WHERE zipcode = ?
"""

EJ_311_ENV_SQL = """
SELECT complaint_type, COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ?
  AND complaint_type IN ('Air Quality', 'Asbestos', 'Lead', 'Mold',
                          'Indoor Air Quality', 'Water Quality',
                          'Noise - Residential', 'Rodent', 'Unsanitary Condition',
                          'HEAT/HOT WATER', 'Industrial Waste')
GROUP BY complaint_type ORDER BY cnt DESC
"""

EJ_FLOOD_SQL = """
SELECT fvi_score, population, pct_nonwhite, pct_poverty
FROM lake.environment.flood_vulnerability
WHERE zip_code = ?
LIMIT 1
"""

EJ_HVI_SQL = """
SELECT hvi, zcta20
FROM lake.environment.heat_vulnerability
WHERE zcta20 = ?
LIMIT 1
"""

EJ_DSNY_SQL = """
SELECT communitydistrict,
       SUM(TRY_CAST(refusetonscollected AS DOUBLE)) AS refuse,
       SUM(TRY_CAST(papertonscollected AS DOUBLE)) AS paper,
       SUM(TRY_CAST(mgptonscollected AS DOUBLE)) AS mgp
FROM lake.environment.dsny_tonnage
WHERE communitydistrict ILIKE ?
GROUP BY communitydistrict
"""


@mcp.tool(annotations=READONLY, tags={"environment"})
def environmental_justice(zipcode: ZIP, ctx: Context) -> ToolResult:
    """Environmental justice analysis for a NYC ZIP code — pollution burden, air quality (PM2.5, NO2), asthma rates, environmental racism indicators, waste facility proximity, EPA-regulated sites, toxic exposure, flood and heat vulnerability, and 311 environmental complaints. Use this for pollution, air quality, asthma, environmental racism, EJ, waste, or toxic exposure questions. Parameters: zipcode (5 digits). Example: 10456 (South Bronx). For general neighborhood info, use neighborhood_portrait(zipcode)."""
    if not re.match(r"^\d{5}$", zipcode):
        raise ToolError("ZIP code must be exactly 5 digits")

    t0 = time.time()
    db = ctx.lifespan_context["db"]

    # Determine borough and CD from ZIP (approximate mapping)
    zip_prefix = zipcode[:3]
    if zip_prefix in ("100", "101", "102"):
        borough = "Manhattan"
        boro_code = "1"
    elif zip_prefix in ("104", "105"):
        borough = "Bronx"
        boro_code = "2"
    elif zip_prefix in ("112", "111", "110", "113", "114"):
        borough = "Brooklyn"
        boro_code = "3"
    elif zip_prefix in ("110", "111", "113", "114", "116"):
        borough = "Queens"
        boro_code = "4"
    elif zip_prefix == "103":
        borough = "Staten Island"
        boro_code = "5"
    else:
        borough = "NYC"
        boro_code = "1"

    lines = [
        f"ENVIRONMENTAL JUSTICE REPORT — ZIP {zipcode} ({borough})",
        "=" * 55,
    ]

    # Air quality (by CD name — we search using borough)
    _, aq_rows = _safe_query(db, EJ_AIR_QUALITY_SQL, [borough])
    _, aq_avg_rows = _safe_query(db, EJ_AIR_CITY_AVG_SQL, [])
    city_avgs = {r[0]: float(r[1]) for r in aq_avg_rows} if aq_avg_rows else {}

    if aq_rows:
        # Group by indicator, take most recent
        seen = {}
        for r in aq_rows:
            if r[0] not in seen:
                seen[r[0]] = r
        lines.append("\nAIR QUALITY:")
        for name, row in seen.items():
            val = row[1]
            period = row[2]
            avg = city_avgs.get(name)
            compare = f" (city avg: {avg:.1f})" if avg else ""
            lines.append(f"  {name}: {val} [{period}]{compare}")

    # EJ areas
    _, ej_rows = _safe_query(db, EJ_AREAS_SQL, [borough, zipcode])
    ej_count = sum(1 for r in ej_rows if r[1] and "EJ Area" in str(r[1]))
    uncertain = sum(1 for r in ej_rows if r[1] and "Uncertain" in str(r[1]))
    if ej_rows:
        lines.append(f"\nENVIRONMENTAL JUSTICE AREAS: {ej_count} EJ tracts, {uncertain} uncertain")
        for r in ej_rows[:5]:
            lines.append(f"  {r[0]}: {r[1]} (minority {r[2]}%, poverty {r[3]}%, pop {r[4]})")

    # Waste transfer stations
    _, waste_rows = _safe_query(db, EJ_WASTE_SQL, [zipcode, borough])
    if waste_rows:
        lines.append(f"\nWASTE FACILITIES: {len(waste_rows)} in area")
        for r in waste_rows[:8]:
            lines.append(f"  {r[0]} ({r[1]}) — {r[2]}")
    else:
        lines.append("\nWASTE FACILITIES: None in this ZIP")

    # EPA facilities
    _, epa_rows = _safe_query(db, EJ_EPA_SQL, [zipcode])
    if epa_rows:
        lines.append(f"\nEPA-REGULATED FACILITIES: {len(epa_rows)}")
        for r in epa_rows[:8]:
            lines.append(f"  {r[0]} (ID: {r[1]})")

    # E-designations (hazardous lots)
    _, edesig_rows = _safe_query(db, EJ_E_DESIGNATIONS_SQL, [boro_code])
    if edesig_rows:
        lines.append(f"\nE-DESIGNATIONS (known hazardous lots): {len(edesig_rows)} in borough")

    # Health outcomes
    _, health_rows = _safe_query(db, EJ_HEALTH_ASTHMA_SQL, [borough])
    _, health_avg_rows = _safe_query(db, EJ_HEALTH_CITY_AVG_SQL, [])
    health_avgs = {r[0]: float(r[1]) for r in health_avg_rows} if health_avg_rows else {}

    if health_rows:
        lines.append("\nHEALTH OUTCOMES:")
        seen_health = {}
        for r in health_rows:
            if r[0] not in seen_health:
                seen_health[r[0]] = r
        for name, row in seen_health.items():
            val = row[2] or row[1]
            avg = health_avgs.get(name)
            compare = ""
            if avg and row[1]:
                try:
                    ratio = float(row[1]) / avg
                    compare = f" ({ratio:.1f}x city avg)"
                except (ValueError, ZeroDivisionError):
                    pass
            lines.append(f"  {name}: {val}{compare}")

    # Rats
    _, rat_rows = _safe_query(db, EJ_RATS_SQL, [zipcode])
    if rat_rows:
        total_inspections = sum(int(r[1]) for r in rat_rows)
        active = sum(int(r[1]) for r in rat_rows if r[0] and "active" in str(r[0]).lower())
        lines.append(f"\nRAT INSPECTIONS: {total_inspections:,} total, {active:,} active signs")

    # Trees
    _, tree_rows = _safe_query(db, EJ_TREES_SQL, [zipcode])
    if tree_rows and tree_rows[0][0]:
        lines.append(f"\nSTREET TREES: {int(tree_rows[0][0]):,}")

    # 311 environmental complaints
    _, env311_rows = _safe_query(db, EJ_311_ENV_SQL, [zipcode])
    if env311_rows:
        total_311 = sum(int(r[1]) for r in env311_rows)
        lines.append(f"\n311 ENVIRONMENTAL COMPLAINTS: {total_311:,}")
        for r in env311_rows[:6]:
            lines.append(f"  {r[0]}: {r[1]:,}")

    # Flood vulnerability
    _, flood_rows = _safe_query(db, EJ_FLOOD_SQL, [zipcode])
    if flood_rows and flood_rows[0][0]:
        lines.append(f"\nFLOOD VULNERABILITY: score {flood_rows[0][0]}")
        if flood_rows[0][2]:
            lines.append(f"  Non-white: {flood_rows[0][2]}%, Poverty: {flood_rows[0][3]}%")

    # Heat vulnerability
    _, hvi_rows = _safe_query(db, EJ_HVI_SQL, [zipcode])
    if hvi_rows and hvi_rows[0][0]:
        hvi = hvi_rows[0][0]
        lines.append(f"\nHEAT VULNERABILITY INDEX: {hvi}/5")
        if int(hvi) >= 4:
            lines.append("  HIGH RISK — prioritize cooling center access")

    # DSNY waste tonnage
    _, dsny_rows = _safe_query(db, EJ_DSNY_SQL, [f"%{boro_code}%"])
    if dsny_rows:
        total_refuse = sum(float(r[1] or 0) for r in dsny_rows)
        lines.append(f"\nWASTE BURDEN: {total_refuse:,.0f} tons refuse collected")

    # Compute burden score
    burden_score = 0
    if waste_rows:
        burden_score += min(20, len(waste_rows) * 5)
    if epa_rows:
        burden_score += min(15, len(epa_rows) * 3)
    if ej_count > 0:
        burden_score += min(15, ej_count * 3)
    if rat_rows:
        active_pct = active * 100 / total_inspections if total_inspections > 0 else 0
        if active_pct > 30:
            burden_score += 10
        elif active_pct > 15:
            burden_score += 5
    if env311_rows:
        if total_311 > 5000:
            burden_score += 15
        elif total_311 > 1000:
            burden_score += 10
        elif total_311 > 200:
            burden_score += 5
    if hvi_rows and hvi_rows[0][0]:
        burden_score += int(hvi_rows[0][0]) * 3
    if flood_rows and flood_rows[0][0]:
        try:
            burden_score += min(10, int(float(flood_rows[0][0]) * 2))
        except (ValueError, TypeError):
            pass

    if burden_score >= 50:
        risk = "SEVERE — environmental racism pattern"
    elif burden_score >= 35:
        risk = "HIGH — disproportionate environmental burden"
    elif burden_score >= 20:
        risk = "ELEVATED — above-average exposure"
    else:
        risk = "MODERATE — typical urban levels"

    lines.append(f"\nENVIRONMENTAL BURDEN SCORE: {burden_score}/100 — {risk}")

    lines.append("\nWHAT YOU CAN DO:")
    lines.append("  1. Report environmental hazards: call 311 (Air/Water/Lead/Asbestos)")
    lines.append("  2. NYC DEP complaints: nyc.gov/dep (water, sewer, noise)")
    lines.append("  3. EPA tip line: 1-888-372-7341 (illegal dumping, hazardous waste)")
    lines.append("  4. NYS DEC EJ community grants: dec.ny.gov/public/333.html")
    lines.append("  5. WE ACT for Environmental Justice: weact.org (Northern Manhattan)")
    lines.append("  6. UPROSE: uprose.org (Sunset Park, citywide EJ advocacy)")
    lines.append("  7. South Bronx Unite: southbronxunite.org")
    if burden_score >= 35:
        lines.append("  8. Contact your City Council member — demand EJ designation review")
        lines.append("  9. NYC Environmental Justice Advisory Board: nyc.gov/ej")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)

    structured = {
        "zipcode": zipcode,
        "borough": borough,
        "air_quality": {r[0]: r[1] for r in aq_rows[:10]} if aq_rows else {},
        "ej_areas": {"ej_count": ej_count, "uncertain": uncertain},
        "waste_facilities": len(waste_rows) if waste_rows else 0,
        "epa_facilities": len(epa_rows) if epa_rows else 0,
        "e_designations": len(edesig_rows) if edesig_rows else 0,
        "rats": {"total": total_inspections, "active": active} if rat_rows else {},
        "trees": int(tree_rows[0][0]) if tree_rows and tree_rows[0][0] else 0,
        "env_311": total_311 if env311_rows else 0,
        "flood_vulnerability": str(flood_rows[0][0]) if flood_rows and flood_rows[0][0] else None,
        "heat_vulnerability": str(hvi_rows[0][0]) if hvi_rows and hvi_rows[0][0] else None,
        "burden_score": burden_score,
        "risk_level": risk,
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Semantic category descriptions — resource_finder
# ---------------------------------------------------------------------------

RESOURCE_CATEGORIES = {
    "food": "food assistance, groceries, meals, SNAP benefits, food pantry, soup kitchen, hunger",
    "health": "health clinic, doctor, hospital, mental health, therapy, counseling, medical care",
    "childcare": "childcare, daycare, babysitting, preschool, early childhood, after school care",
    "youth": "youth programs, teen activities, summer camp, mentoring, after school, tutoring",
    "benefits": "government benefits, welfare, public assistance, SSI, disability, unemployment",
    "legal": "legal aid, lawyer, tenant rights, immigration help, court, eviction defense",
    "shelter": "shelter, housing assistance, emergency housing, homeless services, transitional housing",
    "wifi": "free wifi, internet access, computer lab, digital access, library, technology center",
    "senior": "senior services, elder care, meals on wheels, senior center, aging, retirement",
}

# ---------------------------------------------------------------------------
# SQL constants — resource_finder
# ---------------------------------------------------------------------------

RESOURCE_FOOD_SQL = """
SELECT program, distadd, org_phone, program_type
FROM lake.social_services.finder_food_help
WHERE distzip = ?
ORDER BY program
LIMIT 15
"""

RESOURCE_MENTAL_HEALTH_SQL = """
SELECT FACNAME, ADDRESS, NULL AS phone, FACTYPE
FROM lake.social_services.finder_mental_health
WHERE ZIPCODE = ?
ORDER BY FACNAME
LIMIT 15
"""

RESOURCE_CHILDCARE_SQL = """
SELECT CenterName, Address, NULL AS phone, FacilityPrg_Type
FROM lake.social_services.arcgis_childcare
WHERE ZipCode = ?
ORDER BY CenterName
LIMIT 15
"""

RESOURCE_DYCD_SQL = """
SELECT program_type, program_site_name, street_address, borough, zipcode, provider
FROM lake.social_services.dycd_program_sites
WHERE zipcode = ?
ORDER BY program_type, program_site_name
LIMIT 20
"""

RESOURCE_SNAP_SQL = """
SELECT facility_name, street_address, city, state, zip_code, phone_number_s_
FROM lake.social_services.snap_centers
LIMIT 20
"""

RESOURCE_BENEFITS_SQL = """
SELECT facility_name, street_address, city, phone_number_s, comments
FROM lake.social_services.benefits_centers
LIMIT 20
"""

RESOURCE_FAMILY_JUSTICE_SQL = """
SELECT borough, street_address, telephone_number, comments
FROM lake.social_services.family_justice_centers
"""

RESOURCE_WIFI_SQL = """
SELECT 'Free Wi-Fi' AS name, 'See nyc.gov/wifi' AS location, 'LinkNYC' AS type
FROM (SELECT 1) WHERE ? IS NOT NULL
LIMIT 0
"""

RESOURCE_311_WAIT_SQL = """
SELECT language, connect_time_sec, call_time
FROM lake.social_services.n311_interpreter_wait
ORDER BY call_time DESC
LIMIT 10
"""

RESOURCE_BROADBAND_SQL = """
SELECT *
FROM lake.social_services.broadband_adoption
WHERE zipcode = ?
LIMIT 1
"""

RESOURCE_KYR_SQL = """
SELECT borough, zip_code, startdate, primary_language, total_attendees, list_of_languages
FROM lake.social_services.know_your_rights
ORDER BY TRY_CAST(startdate AS DATE) DESC
LIMIT 10
"""


@mcp.tool(annotations=READONLY, tags={"services"})
def resource_finder(zipcode: ZIP, need: Annotated[str, Field(description="What you're looking for: 'food', 'health', 'childcare', 'youth', 'benefits', 'legal', 'shelter', 'wifi', 'senior', or 'all'")], ctx: Context) -> ToolResult:
    """Find food pantries, SNAP benefits, health clinics, shelters, legal aid, childcare, youth programs, and other social services near a NYC ZIP code. Use this when someone needs help finding resources, benefits, or community services. Parameters: zipcode (5 digits), need (category: 'food', 'health', 'childcare', 'youth', 'benefits', 'legal', 'shelter', 'wifi', 'senior', 'immigration', or 'all'). Example: resource_finder("10456", "food")."""
    if not re.match(r"^\d{5}$", zipcode):
        raise ToolError("ZIP code must be exactly 5 digits")

    t0 = time.time()
    db = ctx.lifespan_context["db"]
    need_lower = need.lower().strip()

    embed_fn = ctx.lifespan_context.get("embed_fn")
    cat_vecs = ctx.lifespan_context.get("resource_category_vecs", {})
    matched_categories = set()
    if need_lower == "all":
        matched_categories = set(RESOURCE_CATEGORIES.keys())
    elif need_lower in RESOURCE_CATEGORIES:
        matched_categories.add(need_lower)
    elif embed_fn and cat_vecs:
        try:
            import numpy as np
            query_vec = embed_fn(need_lower)
            q_norm = query_vec / (np.linalg.norm(query_vec) + 1e-9)
            for cat, cat_vec in cat_vecs.items():
                c_norm = cat_vec / (np.linalg.norm(cat_vec) + 1e-9)
                sim = float(np.dot(q_norm, c_norm))
                if sim > LANCE_CATEGORY_SIM:
                    matched_categories.add(cat)
        except Exception:
            pass
    # Fallback: if nothing matched semantically, try old keyword approach
    if not matched_categories and need_lower != "all":
        matched_categories = {need_lower}  # pass through to existing keyword logic
    show_all = not matched_categories or "all" in matched_categories

    lines = [
        f"RESOURCE FINDER — ZIP {zipcode}",
        f"Looking for: {need}",
        "=" * 45,
    ]

    results_found = 0

    # Food  # keywords: food, hungry, eat, snap, pantry, market
    if show_all or "food" in matched_categories:
        _, food_rows = _safe_query(db, RESOURCE_FOOD_SQL, [zipcode])
        if food_rows:
            lines.append(f"\nFOOD ASSISTANCE ({len(food_rows)} locations):")
            for r in food_rows[:8]:
                lines.append(f"  {r[0]}")
                if r[1]:
                    lines.append(f"    {r[1]}")
                if r[2]:
                    lines.append(f"    Phone: {r[2]}")
            results_found += len(food_rows)
        # SNAP centers
        _, snap_rows = _safe_query(db, RESOURCE_SNAP_SQL, [])
        if snap_rows:
            lines.append(f"\n  SNAP CENTERS (citywide):")
            for r in snap_rows[:5]:
                lines.append(f"    {r[0]}, {r[2]} — {r[5]}")
            results_found += len(snap_rows)

    # Health  # keywords: health, clinic, doctor, mental, hospital
    if show_all or "health" in matched_categories:
        _, mh_rows = _safe_query(db, RESOURCE_MENTAL_HEALTH_SQL, [zipcode])
        if mh_rows:
            lines.append(f"\nHEALTH & MENTAL HEALTH ({len(mh_rows)} locations):")
            for r in mh_rows[:8]:
                lines.append(f"  {r[0]} ({r[3]})")
                if r[1]:
                    lines.append(f"    {r[1]}")
                if r[2]:
                    lines.append(f"    Phone: {r[2]}")
            results_found += len(mh_rows)

    # Childcare  # keywords: child, daycare, childcare, baby, toddler
    if show_all or "childcare" in matched_categories:
        _, cc_rows = _safe_query(db, RESOURCE_CHILDCARE_SQL, [zipcode])
        if cc_rows:
            lines.append(f"\nCHILDCARE ({len(cc_rows)} providers):")
            for r in cc_rows[:8]:
                lines.append(f"  {r[0]} ({r[3]})")
                if r[1]:
                    lines.append(f"    {r[1]}")
                if r[2]:
                    lines.append(f"    Phone: {r[2]}")
            results_found += len(cc_rows)

    # Youth / DYCD  # keywords: youth, teen, job, dycd, program, after
    if show_all or "youth" in matched_categories:
        _, dycd_rows = _safe_query(db, RESOURCE_DYCD_SQL, [zipcode])
        if dycd_rows:
            lines.append(f"\nYOUTH & COMMUNITY PROGRAMS ({len(dycd_rows)} sites):")
            for r in dycd_rows[:10]:
                lines.append(f"  [{r[0]}] {r[1]}")
                if r[2]:
                    lines.append(f"    {r[2]}")
                if r[5]:
                    lines.append(f"    Phone: {r[5]}")
            results_found += len(dycd_rows)

    # Benefits  # keywords: benefit, medicaid, cash, assistance, snap
    if show_all or "benefits" in matched_categories:
        _, ben_rows = _safe_query(db, RESOURCE_BENEFITS_SQL, [])
        if ben_rows:
            lines.append(f"\nBENEFITS ACCESS CENTERS:")
            for r in ben_rows[:8]:
                lines.append(f"  {r[0]} — {r[1]}, {r[2]}")
                if r[3]:
                    lines.append(f"    Phone: {r[3]}")
            results_found += len(ben_rows)

    # DV / Family Justice  # keywords: domestic, violence, dv, abuse, family justice, legal
    if show_all or "legal" in matched_categories:
        _, fj_rows = _safe_query(db, RESOURCE_FAMILY_JUSTICE_SQL, [])
        if fj_rows:
            lines.append(f"\nFAMILY JUSTICE CENTERS (domestic violence):")
            for r in fj_rows:
                lines.append(f"  {r[0]}: {r[1]} — {r[2]}")
            results_found += len(fj_rows)

    # Shelter  # keywords: shelter, homeless, housing
    if show_all or "shelter" in matched_categories:
        lines.append("\nSHELTER & HOUSING:")
        lines.append("  DHS Intake: 400 E 30th St, Manhattan — 212-361-8000")
        lines.append("  Families: PATH Center, 151 E 151st St, Bronx — 718-503-6400")
        lines.append("  Safe Haven: 311 for nearest location")
        lines.append("  HomeBase (eviction prevention): 311, ask for HomeBase")
        results_found += 1

    # WiFi  # keywords: wifi, internet, broadband, connect
    if show_all or "wifi" in matched_categories:
        lines.append("\nFREE WI-FI & INTERNET:")
        lines.append("  LinkNYC kiosks: free gigabit Wi-Fi at 1,800+ locations")
        lines.append("  NYC Mesh: nycmesh.net (community internet)")
        lines.append("  Big Apple Connect: free internet in NYCHA")
        lines.append("  NYPL/BPL/QPL: free Wi-Fi at all public libraries")
        _, bb_rows = _safe_query(db, RESOURCE_BROADBAND_SQL, [zipcode])
        if bb_rows:
            lines.append(f"  Broadband data for ZIP {zipcode} available")
        results_found += 1

    # Senior  # keywords: senior, elder, aging, dfta, meal
    if show_all or "senior" in matched_categories:
        lines.append("\nSENIOR SERVICES:")
        lines.append("  DFTA Aging Connect: 212-244-6469")
        lines.append("  Meals on Wheels: mowaa.org or 311")
        lines.append("  Senior centers: 311 for nearest location")
        results_found += 1

    # Immigration  # keywords: immigra, legal, asylum, idnyc, undocumented
    if show_all or "legal" in matched_categories:
        lines.append("\nIMMIGRATION SERVICES:")
        lines.append("  ActionNYC (free legal): 800-354-0365")
        lines.append("  IDNYC: 311 for locations (municipal ID for all residents)")
        lines.append("  New York Immigration Coalition: nyic.org")
        lines.append("  Catholic Charities: catholiccharitiesny.org")
        lines.append("  Know Your Rights: 311 or MOIA hotline")
        _, kyr_rows = _safe_query(db, RESOURCE_KYR_SQL, [])
        if kyr_rows:
            lines.append(f"  Recent Know Your Rights events:")
            for r in kyr_rows[:3]:
                lines.append(f"    {r[0]} {r[1]} — {r[4]} participants ({r[5]})")
        results_found += 1

    if results_found == 0:
        lines.append(f"\nNo specific results for '{need}' in ZIP {zipcode}.")
        lines.append("Try: food, health, childcare, youth, benefits, shelter, wifi, senior, immigration, all")

    lines.append("\nUNIVERSAL HOTLINES:")
    lines.append("  311: NYC services (any language)")
    lines.append("  988: Suicide & Crisis Lifeline")
    lines.append("  911: Emergency")
    lines.append("  Safe Horizon: 1-800-621-4673 (crime victims)")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)

    structured = {
        "zipcode": zipcode,
        "need": need,
        "results_found": results_found,
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"zipcode": zipcode, "need": need, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# School Report
# ---------------------------------------------------------------------------

SCHOOL_DIRECTORY_SQL = """
SELECT dbn, school, district, principal, admission_method, community_school,
       gifted_and_talented, cte, dual_language_or_transitional,
       federal_accountability_status, report_type
FROM lake.education.schools_directory
WHERE dbn = ?
LIMIT 1
"""

SCHOOL_PERFORMANCE_SQL = """
SELECT year, performance, impact, school_type, report_type
FROM lake.education.schools_performance
WHERE dbn = ?
ORDER BY year DESC
LIMIT 5
"""

SCHOOL_ELA_SQL = """
SELECT year, grade, number_tested, mean_scale_score,
       pct_level_3_and_4, level_3_4, level_3_4_1
FROM lake.education.ela_results
WHERE geographic_subdivision = ? AND report_category = 'School'
  AND grade = 'All Grades'
ORDER BY year DESC
LIMIT 5
"""

SCHOOL_MATH_SQL = """
SELECT year, grade, number_tested, mean_scale_score,
       pct_level_3_and_4, num_level_3_and_4, num_level_3_and_4
FROM lake.education.math_results
WHERE geographic_division = ? AND report_category = 'School'
  AND grade = 'All Grades'
ORDER BY year DESC
LIMIT 5
"""

SCHOOL_SAFETY_SQL = """
SELECT *
FROM lake.education.school_safety
WHERE dbn = ?
ORDER BY 1 DESC
LIMIT 3
"""

SCHOOL_QUALITY_SQL = """
SELECT *
FROM lake.education.quality_reports
WHERE dbn = ?
LIMIT 1
"""

SCHOOL_DEMOGRAPHICS_SQL = """
SELECT year, total_enrollment, female, female_1, male, male_1,
       asian, asian_1, black, black_1, hispanic, hispanic_1,
       multi_racial, multi_racial_1, white, white_1
FROM lake.education.demographics_2020
WHERE dbn = ?
ORDER BY year DESC
LIMIT 1
"""

SCHOOL_ATTENDANCE_SQL = """
SELECT year, school_name, grade, attendance, chronically_absent_1
FROM lake.education.chronic_absenteeism
WHERE dbn = ?
  AND grade = 'All Grades'
ORDER BY year DESC
LIMIT 3
"""

SCHOOL_CLASS_SIZE_SQL = """
SELECT grade_level, program_type,
       ROUND(AVG(TRY_CAST(average_class_size AS DOUBLE)), 1) AS avg_size,
       MIN(TRY_CAST(minimum_class_size AS INT)) AS min_size,
       MAX(TRY_CAST(maximum_class_size AS INT)) AS max_size,
       SUM(TRY_CAST(number_of_students AS INT)) AS students,
       SUM(TRY_CAST(number_of_classes AS INT)) AS classes
FROM lake.education.class_size
WHERE dbn = ?
GROUP BY grade_level, program_type
ORDER BY grade_level
"""

SCHOOL_SURVEY_SQL = """
SELECT
    p.school_name,
    p.collaborative_teachers_score AS p_teachers,
    p.effective_school_leadership AS p_leadership,
    p.rigorous_instruction_score AS p_rigor,
    p.supportive_environment_score AS p_environment,
    p.trust_score AS p_trust,
    p.total_parent_response_rate AS parent_rate,
    t.collaborative_teachers_score AS t_teachers,
    t.effective_school_leadership AS t_leadership,
    t.rigorous_instruction_score AS t_rigor,
    t.supportive_environment_score AS t_environment,
    t.trust_score AS t_trust,
    t.total_teacher_response_rate AS teacher_rate,
    s.collaborative_teachers_score AS s_teachers,
    s.effective_school_leadership AS s_leadership,
    s.rigorous_instruction_score AS s_rigor,
    s.supportive_environment_score AS s_environment,
    s.trust_score AS s_trust,
    s.total_student_response_rate AS student_rate
FROM lake.education.survey_parents p
LEFT JOIN lake.education.survey_teachers t ON p.dbn = t.dbn
LEFT JOIN lake.education.survey_students s ON p.dbn = s.dbn
WHERE p.dbn = ?
LIMIT 1
"""

SCHOOL_SAFETY_DETAIL_SQL = """
SELECT school_year, location_name, address, borough_name, postcode,
       register, major_n, oth_n, nocrim_n, prop_n, vio_n,
       engroupa, bbl, latitude, longitude
FROM lake.education.school_safety
WHERE dbn = ?
ORDER BY school_year DESC
LIMIT 3
"""

SCHOOL_REGENTS_SQL = """
SELECT regents_exam, year, total_tested, mean_score,
       percent_scoring_65_or_above, percent_scoring_80_or_above
FROM lake.education.regents_results
WHERE school_dbn = ?
  AND category = 'All Students'
ORDER BY year DESC, regents_exam
LIMIT 20
"""

SCHOOL_CAFETERIA_SQL = """
SELECT inspectiondate, level, code, violationdescription
FROM lake.health.school_cafeteria_inspections
WHERE entityid = ?
ORDER BY TRY_CAST(inspectiondate AS DATE) DESC NULLS LAST
LIMIT 10
"""

SCHOOL_SPECIALIZED_HS_SQL = """
SELECT year, count_of_testers, number_of_offers, number_of_discovery_participants
FROM lake.education.specialized_hs_tests
WHERE feeder_school_dbn = ?
ORDER BY year DESC
LIMIT 5
"""

SCHOOL_DISCHARGE_SQL = """
SELECT year, discharge_category, discharge_description,
       SUM(TRY_CAST(count_of_students AS INT)) AS students
FROM lake.education.discharge_reporting
WHERE report_category = 'School'
  AND geographic_unit = ?
  AND student_category = 'All Students'
GROUP BY year, discharge_category, discharge_description
ORDER BY year DESC, students DESC
LIMIT 15
"""


@mcp.tool(annotations=READONLY, tags={"education", "services"})
def school_report(dbn: Annotated[str, Field(description="School DBN: district(2) + borough letter(1) + number(3). Example: 02M001. Borough: M=Manhattan, X=Bronx, K=Brooklyn, Q=Queens, R=Staten Island")], ctx: Context) -> ToolResult:
    """Comprehensive school report for a NYC public school by DBN — test scores, demographics, attendance, safety, class size, surveys, cafeteria inspections, and Regents results. Use school_search(query) first to find DBNs if you don't know them. For comparing schools, use school_compare([dbns]). For district-level stats, use district_report(district). DBN format: district(2) + borough letter + number(3). Examples: 02M001 (PS 1, Manhattan), 13K330 (Brooklyn)."""
    dbn = dbn.strip().upper()
    if len(dbn) < 5 or len(dbn) > 6:
        raise ToolError("DBN must be 5-6 characters. Format: district(2) + borough(1) + number(3). Example: 02M001")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # --- Existing queries ---
    _, dir_rows = _safe_query(db, SCHOOL_DIRECTORY_SQL, [dbn])
    _, perf_rows = _safe_query(db, SCHOOL_PERFORMANCE_SQL, [dbn])
    _, ela_rows = _safe_query(db, SCHOOL_ELA_SQL, [dbn])
    _, math_rows = _safe_query(db, SCHOOL_MATH_SQL, [dbn])
    _, qual_rows = _safe_query(db, SCHOOL_QUALITY_SQL, [dbn])

    # --- New queries ---
    _, demo_rows = _safe_query(db, SCHOOL_DEMOGRAPHICS_SQL, [dbn])
    _, attend_rows = _safe_query(db, SCHOOL_ATTENDANCE_SQL, [dbn])
    _, class_rows = _safe_query(db, SCHOOL_CLASS_SIZE_SQL, [dbn])
    _, survey_rows = _safe_query(db, SCHOOL_SURVEY_SQL, [dbn])
    _, safety_rows = _safe_query(db, SCHOOL_SAFETY_DETAIL_SQL, [dbn])
    _, regents_rows = _safe_query(db, SCHOOL_REGENTS_SQL, [dbn])
    _, cafe_rows = _safe_query(db, SCHOOL_CAFETERIA_SQL, [dbn])
    _, shsat_rows = _safe_query(db, SCHOOL_SPECIALIZED_HS_SQL, [dbn])
    _, discharge_rows = _safe_query(db, SCHOOL_DISCHARGE_SQL, [dbn])

    if not dir_rows and not perf_rows and not ela_rows and not demo_rows and not safety_rows:
        raise ToolError(f"No school found for DBN {dbn}. Use school_search(name) to find the correct DBN.")

    lines = [f"SCHOOL REPORT — {dbn}", "=" * 55]

    # --- Directory (existing, from schools_directory if it exists) ---
    if dir_rows:
        d = dir_rows[0]
        lines.append(f"\n{d[1]}")
        lines.append(f"  District {d[2]} | Principal: {d[3] or 'N/A'}")
        lines.append(f"  Admission: {d[4] or 'N/A'}")
        flags = []
        if d[5] == '1': flags.append("Community School")
        if d[6] == '1': flags.append("Gifted & Talented")
        if d[7] == '1': flags.append("CTE")
        if d[8] and d[8] != '0': flags.append("Dual Language/Transitional")
        if d[9]: flags.append(f"Federal: {d[9]}")
        if flags:
            lines.append(f"  Programs: {', '.join(flags)}")

    # --- Safety/Location (fallback directory if schools_directory missing) ---
    if safety_rows:
        s = safety_rows[0]
        if not dir_rows:
            lines.append(f"\n{s[1]}")
            lines.append(f"  {s[2]}, {s[3]} {s[4]}")
        else:
            lines.append(f"\n  Address: {s[2]}, {s[3]} {s[4]}")
        if s[5]:
            lines.append(f"  Register: {s[5]} students")

    # --- Demographics ---
    if demo_rows:
        d = demo_rows[0]
        enrollment = d[1] or '?'
        lines.append(f"\nDEMOGRAPHICS ({d[0]})")
        lines.append(f"  Enrollment: {enrollment}")
        if d[2] and d[3]:
            lines.append(f"  Gender: {d[3]}% female, {d[5]}% male")
        race_parts = []
        for label, pct_idx in [("Asian", 7), ("Black", 9), ("Hispanic", 11), ("Multi-racial", 13), ("White", 15)]:
            pct = d[pct_idx] if len(d) > pct_idx and d[pct_idx] else None
            if pct:
                race_parts.append(f"{label} {pct}%")
        if race_parts:
            lines.append(f"  Race: {', '.join(race_parts)}")

    # --- Performance (existing) ---
    if perf_rows:
        lines.append("\nPERFORMANCE (DOE Framework):")
        for r in perf_rows:
            perf = float(r[1]) if r[1] else 0
            impact = float(r[2]) if r[2] else 0
            lines.append(f"  {r[0]}: Performance {perf:.2f} | Impact {impact:.2f} ({r[3]})")

    # --- ELA (existing) ---
    if ela_rows:
        lines.append("\nELA TEST RESULTS:")
        for r in ela_rows:
            tested = r[2] or '?'
            score = r[3] or 'N/A'
            proficient = r[4] or r[5] or r[6] or 'N/A'
            lines.append(f"  {r[0]}: {tested} tested, mean {score}, proficient {proficient}")

    # --- Math (existing) ---
    if math_rows:
        lines.append("\nMATH TEST RESULTS:")
        for r in math_rows:
            tested = r[2] or '?'
            score = r[3] or 'N/A'
            proficient = r[4] or r[5] or r[6] or 'N/A'
            lines.append(f"  {r[0]}: {tested} tested, mean {score}, proficient {proficient}")

    # --- Regents (high schools) ---
    if regents_rows:
        lines.append("\nREGENTS EXAMS:")
        current_year = None
        for r in regents_rows:
            exam, year, tested, mean, pct65, pct80 = r
            if year != current_year:
                current_year = year
                lines.append(f"  {year}:")
            lines.append(f"    {exam}: {tested} tested, mean {mean}, ≥65: {pct65 or '?'}%, ≥80: {pct80 or '?'}%")

    # --- Attendance ---
    if attend_rows:
        lines.append("\nATTENDANCE:")
        for r in attend_rows:
            year, name, grade, attend_rate, chronic_pct = r
            lines.append(f"  {year}: {attend_rate}% attendance, {chronic_pct}% chronically absent")

    # --- Class Size ---
    if class_rows:
        lines.append("\nCLASS SIZE:")
        for r in class_rows:
            grade, prog, avg, mn, mx, students, classes = r
            avg_str = f"{avg}" if avg else "?"
            lines.append(f"  {grade} ({prog}): avg {avg_str} students, {classes or '?'} classes")

    # --- Surveys ---
    if survey_rows:
        s = survey_rows[0]
        lines.append("\nSURVEY SCORES:")
        lines.append(f"  Parents (response rate: {s[6] or '?'}%):")
        lines.append(f"    Teachers: {s[1] or '?'} | Leadership: {s[2] or '?'} | Rigor: {s[3] or '?'} | Environment: {s[4] or '?'} | Trust: {s[5] or '?'}")
        if s[7]:
            lines.append(f"  Teachers (response rate: {s[12] or '?'}%):")
            lines.append(f"    Teachers: {s[7] or '?'} | Leadership: {s[8] or '?'} | Rigor: {s[9] or '?'} | Environment: {s[10] or '?'} | Trust: {s[11] or '?'}")
        if s[13]:
            lines.append(f"  Students (response rate: {s[18] or '?'}%):")
            lines.append(f"    Teachers: {s[13] or '?'} | Leadership: {s[14] or '?'} | Rigor: {s[15] or '?'} | Environment: {s[16] or '?'} | Trust: {s[17] or '?'}")

    # --- Safety incidents ---
    if safety_rows:
        s = safety_rows[0]
        major = s[6] or 0
        other = s[7] or 0
        nocrim = s[8] or 0
        prop = s[9] or 0
        violent = s[10] or 0
        total = int(major or 0) + int(other or 0) + int(nocrim or 0) + int(prop or 0) + int(violent or 0)
        if total > 0:
            lines.append(f"\nSAFETY ({s[0]}):")
            lines.append(f"  Total incidents: {total}")
            if int(major or 0) > 0: lines.append(f"    Major criminal: {major}")
            if int(violent or 0) > 0: lines.append(f"    Violent: {violent}")
            if int(prop or 0) > 0: lines.append(f"    Property: {prop}")
            if int(other or 0) > 0: lines.append(f"    Other: {other}")
            if int(nocrim or 0) > 0: lines.append(f"    Non-criminal: {nocrim}")

    # --- Cafeteria inspections ---
    if cafe_rows:
        lines.append("\nCAFETERIA INSPECTIONS:")
        for r in cafe_rows[:5]:
            date, level, code, desc = r
            lines.append(f"  {date or '?'}: [{level or '?'}] {desc or code or 'N/A'}")

    # --- SHSAT feeder data ---
    if shsat_rows:
        lines.append("\nSPECIALIZED HS TEST (SHSAT) RESULTS:")
        for r in shsat_rows:
            year, testers, offers, discovery = r
            lines.append(f"  {year}: {testers} testers, {offers} offers" + (f", {discovery} discovery" if discovery and discovery != '0' else ""))

    # --- Composite quality (existing) ---
    quality_score = None
    if perf_rows:
        latest = perf_rows[0]
        perf_val = float(latest[1]) if latest[1] else 0
        impact_val = float(latest[2]) if latest[2] else 0
        quality_score = round((perf_val * 60 + impact_val * 40) * 100)
        if quality_score >= 70: rating = "STRONG"
        elif quality_score >= 50: rating = "GOOD"
        elif quality_score >= 30: rating = "FAIR"
        else: rating = "NEEDS IMPROVEMENT"
        lines.append(f"\nCOMPOSITE QUALITY: {quality_score}/100 — {rating}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "dbn": dbn,
        "directory": dir_rows[0] if dir_rows else None,
        "performance": [list(r) for r in perf_rows] if perf_rows else [],
        "quality_score": quality_score,
        "demographics": demo_rows[0] if demo_rows else None,
        "safety_incidents": safety_rows[0] if safety_rows else None,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"dbn": dbn, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# School Search
# ---------------------------------------------------------------------------

SCHOOL_SEARCH_BY_NAME_SQL = """
SELECT DISTINCT
    d.dbn,
    d.school_name,
    d.year,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment,
    s.address,
    s.borough_name AS borough,
    s.postcode AS zipcode,
    s.geographical_district_code AS district
FROM lake.education.demographics_2020 d
LEFT JOIN lake.education.school_safety s ON d.dbn = s.dbn
WHERE d.school_name ILIKE '%' || ? || '%' ESCAPE '\\'
ORDER BY TRY_CAST(d.total_enrollment AS INT) DESC NULLS LAST
LIMIT 20
"""

SCHOOL_SEARCH_BY_ZIP_SQL = """
SELECT DISTINCT
    d.dbn,
    d.school_name,
    d.year,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment,
    s.address,
    s.borough_name AS borough,
    s.postcode AS zipcode,
    s.geographical_district_code AS district
FROM lake.education.demographics_2020 d
JOIN lake.education.school_safety s ON d.dbn = s.dbn
WHERE s.postcode = ?
ORDER BY TRY_CAST(d.total_enrollment AS INT) DESC NULLS LAST
LIMIT 30
"""

SCHOOL_SEARCH_BY_DISTRICT_SQL = """
SELECT DISTINCT
    d.dbn,
    d.school_name,
    d.year,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment,
    s.address,
    s.borough_name AS borough,
    s.postcode AS zipcode,
    s.geographical_district_code AS district
FROM lake.education.demographics_2020 d
LEFT JOIN lake.education.school_safety s ON d.dbn = s.dbn
WHERE LPAD(CAST(s.geographical_district_code AS VARCHAR), 2, '0') = LPAD(?, 2, '0')
ORDER BY TRY_CAST(d.total_enrollment AS INT) DESC NULLS LAST
LIMIT 40
"""


@mcp.tool(annotations=READONLY, tags={"education"})
def school_search(
    query: Annotated[str, Field(description="School name, 5-digit ZIP code, or 2-digit district number. Examples: 'Stuyvesant', '10003', '02'")],
    ctx: Context,
) -> ToolResult:
    """Find NYC public schools by name, ZIP code, or district number. Use this as the entry point when you don't know a school's DBN. Returns school names, DBNs, addresses, enrollment, and district. Follow up with school_report(dbn) for detailed performance data or school_compare([dbns]) for side-by-side comparison. Examples: school_search('Stuyvesant'), school_search('10003'), school_search('02')."""
    query = query.strip()
    if not query or len(query) < 2:
        raise ToolError("Search query must be at least 2 characters.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Detect query type: ZIP (5 digits), district (1-2 digits), or name
    if re.match(r"^\d{5}$", query):
        _, rows = _safe_query(db, SCHOOL_SEARCH_BY_ZIP_SQL, [query])
        search_type = f"ZIP {query}"
    elif re.match(r"^\d{1,2}$", query):
        _, rows = _safe_query(db, SCHOOL_SEARCH_BY_DISTRICT_SQL, [query])
        search_type = f"District {int(query)}"
    else:
        name_query = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        _, rows = _safe_query(db, SCHOOL_SEARCH_BY_NAME_SQL, [name_query])
        search_type = f"name matching '{query}'"

    if not rows:
        raise ToolError(f"No schools found for {search_type}. Try a broader search term.")

    lines = [f"SCHOOL SEARCH — {search_type}", f"Found {len(rows)} schools", "=" * 50]

    for r in rows:
        dbn, name, year, enrollment, address, borough, zipcode, district = r[:8]
        try:
            enrollment_str = f"{int(enrollment):,}" if enrollment else "?"
        except (ValueError, TypeError):
            enrollment_str = str(enrollment) if enrollment else "?"
        addr_str = f"{address}, {borough}" if address else borough or ""
        zip_str = f" {zipcode}" if zipcode else ""
        dist_str = f"D{district}" if district else ""

        lines.append(f"\n  {name}")
        lines.append(f"    DBN: {dbn} | {dist_str} | {enrollment_str} students")
        if addr_str:
            lines.append(f"    {addr_str}{zip_str}")

    lines.append(f"\nUse school_report(dbn) for full details on any school.")
    lines.append(f"Use school_compare([dbn1, dbn2, ...]) to compare schools side-by-side.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"search_type": search_type, "results": [{"dbn": r[0], "name": r[1], "enrollment": r[3], "zipcode": r[6], "district": r[7]} for r in rows]},
        meta={"query": query, "result_count": len(rows), "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# School Compare
# ---------------------------------------------------------------------------

SCHOOL_COMPARE_SCORES_SQL = """
SELECT
    d.dbn,
    d.school_name,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment,
    (SELECT COALESCE(level_3_4_1, level_3_4)
     FROM lake.education.ela_results
     WHERE geographic_subdivision = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS ela_proficient_pct,
    (SELECT mean_scale_score
     FROM lake.education.ela_results
     WHERE geographic_subdivision = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS ela_mean,
    (SELECT COALESCE(pct_level_3_and_4, num_level_3_and_4)
     FROM lake.education.math_results
     WHERE geographic_division = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS math_proficient_pct,
    (SELECT mean_scale_score
     FROM lake.education.math_results
     WHERE geographic_division = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     -- NOTE: math_results uses geographic_division (not geographic_subdivision like ela_results)
     ORDER BY year DESC LIMIT 1) AS math_mean,
    (SELECT attendance
     FROM lake.education.chronic_absenteeism
     WHERE dbn = d.dbn AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS attendance_pct,
    (SELECT chronically_absent_1
     FROM lake.education.chronic_absenteeism
     WHERE dbn = d.dbn AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS chronic_absent_pct,
    (SELECT trust_score
     FROM lake.education.survey_parents
     WHERE dbn = d.dbn LIMIT 1) AS parent_trust,
    (SELECT COALESCE(TRY_CAST(major_n AS INT), 0) + COALESCE(TRY_CAST(vio_n AS INT), 0)
     FROM lake.education.school_safety
     WHERE dbn = d.dbn
     ORDER BY school_year DESC LIMIT 1) AS safety_incidents,
    (SELECT ROUND(AVG(TRY_CAST(average_class_size AS DOUBLE)), 1)
     FROM lake.education.class_size
     WHERE dbn = d.dbn) AS avg_class_size,
    d.asian_1 AS asian_pct,
    d.black_1 AS black_pct,
    d.hispanic_1 AS hispanic_pct,
    d.white_1 AS white_pct,
    s.postcode AS zipcode
FROM lake.education.demographics_2020 d
LEFT JOIN lake.education.school_safety s ON d.dbn = s.dbn
WHERE d.dbn IN ({placeholders})
ORDER BY d.dbn
"""


@mcp.tool(annotations=READONLY, tags={"education"})
def school_compare(
    dbns: Annotated[list[str], Field(description="2-7 school DBNs to compare. Example: ['02M475', '02M545', '13K330']")],
    ctx: Context,
) -> ToolResult:
    """Compare 2-7 NYC public schools side-by-side across test scores, attendance, safety, class size, demographics, and parent trust. Use school_search(query) first to find DBNs. Example: school_compare(['02M475', '02M545']). For a single school deep-dive, use school_report(dbn). For district-level comparison, use district_report(district)."""
    if len(dbns) < 2:
        raise ToolError("Provide at least 2 DBNs to compare. Use school_search(query) to find DBNs.")
    if len(dbns) > 7:
        raise ToolError("Maximum 7 schools per comparison.")

    dbns = [d.strip().upper() for d in dbns]
    for d in dbns:
        if not re.match(r'^[A-Z0-9]{5,6}$', d):
            raise ToolError(f"Invalid DBN format: '{d}'. Expected 5-6 alphanumeric chars like 02M001.")
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    sql = _fill_placeholders(SCHOOL_COMPARE_SCORES_SQL, dbns)
    _, rows = _safe_query(db, sql, dbns)

    if not rows:
        raise ToolError(f"No data found for DBNs: {', '.join(dbns)}. Verify with school_search().")

    lines = [f"SCHOOL COMPARISON — {len(rows)} schools", "=" * 60]

    names = [r[1][:25] for r in rows]
    lines.append(f"\n{'Metric':<25} | " + " | ".join(f"{n:<25}" for n in names))
    lines.append("-" * (27 + 28 * len(rows)))

    metrics = [
        ("Enrollment", 2, "{:,}"),
        ("ELA Proficient %", 3, "{}%"),
        ("ELA Mean Score", 4, "{}"),
        ("Math Proficient %", 5, "{}%"),
        ("Math Mean Score", 6, "{}"),
        ("Attendance %", 7, "{}%"),
        ("Chronic Absent %", 8, "{}%"),
        ("Parent Trust", 9, "{}"),
        ("Safety Incidents", 10, "{}"),
        ("Avg Class Size", 11, "{}"),
        ("Asian %", 12, "{}%"),
        ("Black %", 13, "{}%"),
        ("Hispanic %", 14, "{}%"),
        ("White %", 15, "{}%"),
    ]

    for label, idx, fmt in metrics:
        vals = []
        for r in rows:
            v = r[idx] if idx < len(r) and r[idx] is not None else "—"
            try:
                vals.append(fmt.format(v))
            except (ValueError, TypeError):
                vals.append(str(v) if v != "—" else "—")
        lines.append(f"{label:<25} | " + " | ".join(f"{v:<25}" for v in vals))

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"dbns": dbns, "schools": [{"dbn": r[0], "name": r[1], "enrollment": r[2]} for r in rows]},
        meta={"dbn_count": len(dbns), "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# District Report
# ---------------------------------------------------------------------------

DISTRICT_SCHOOLS_SQL = """
SELECT
    d.dbn,
    d.school_name,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment
FROM lake.education.demographics_2020 d
WHERE SUBSTRING(d.dbn, 1, 2) = LPAD(?, 2, '0')
ORDER BY enrollment DESC NULLS LAST
"""

DISTRICT_AGGREGATE_SQL = """
SELECT
    COUNT(DISTINCT d.dbn) AS school_count,
    SUM(TRY_CAST(d.total_enrollment AS INT)) AS total_students,
    ROUND(AVG(TRY_CAST(d.total_enrollment AS INT)), 0) AS avg_enrollment,
    ROUND(AVG(TRY_CAST(d.asian_1 AS DOUBLE)), 1) AS avg_asian_pct,
    ROUND(AVG(TRY_CAST(d.black_1 AS DOUBLE)), 1) AS avg_black_pct,
    ROUND(AVG(TRY_CAST(d.hispanic_1 AS DOUBLE)), 1) AS avg_hispanic_pct,
    ROUND(AVG(TRY_CAST(d.white_1 AS DOUBLE)), 1) AS avg_white_pct
FROM lake.education.demographics_2020 d
WHERE SUBSTRING(d.dbn, 1, 2) = LPAD(?, 2, '0')
"""

DISTRICT_TEST_SCORES_SQL = """
SELECT
    year,
    'ELA' AS subject,
    COUNT(*) AS schools_tested,
    ROUND(AVG(TRY_CAST(mean_scale_score AS DOUBLE)), 1) AS avg_mean_score
FROM lake.education.ela_results
WHERE report_category = 'School'
  AND grade = 'All Grades'
  AND SUBSTRING(geographic_subdivision, 1, 2) = LPAD(?, 2, '0')
GROUP BY year
UNION ALL
SELECT
    year,
    'Math' AS subject,
    COUNT(*) AS schools_tested,
    ROUND(AVG(TRY_CAST(mean_scale_score AS DOUBLE)), 1) AS avg_mean_score
FROM lake.education.math_results
WHERE report_category = 'School'
  AND grade = 'All Grades'
  AND SUBSTRING(geographic_division, 1, 2) = LPAD(?, 2, '0')
GROUP BY year
ORDER BY year DESC, subject
"""

DISTRICT_ATTENDANCE_SQL = """
SELECT
    year,
    COUNT(DISTINCT dbn) AS schools,
    ROUND(AVG(TRY_CAST(attendance AS DOUBLE)), 1) AS avg_attendance,
    ROUND(AVG(TRY_CAST(chronically_absent_1 AS DOUBLE)), 1) AS avg_chronic_absent
FROM lake.education.chronic_absenteeism
WHERE grade = 'All Grades'
  AND SUBSTRING(dbn, 1, 2) = LPAD(?, 2, '0')
GROUP BY year
ORDER BY year DESC
LIMIT 3
"""

DISTRICT_SAFETY_SQL = """
SELECT
    school_year,
    COUNT(DISTINCT dbn) AS schools,
    SUM(TRY_CAST(major_n AS INT)) AS major_incidents,
    SUM(TRY_CAST(vio_n AS INT)) AS violent_incidents,
    SUM(TRY_CAST(prop_n AS INT)) AS property_incidents,
    SUM(TRY_CAST(register AS INT)) AS total_register
FROM lake.education.school_safety
WHERE CAST(geographical_district_code AS INT) = CAST(? AS INT)
GROUP BY school_year
ORDER BY school_year DESC
LIMIT 3
"""


@mcp.tool(annotations=READONLY, tags={"education"})
def district_report(
    district: Annotated[str, Field(description="NYC school district number (1-32, 75, 79). Example: '2' or '02'")],
    ctx: Context,
) -> ToolResult:
    """Aggregate education report for a NYC school district — school count, enrollment, demographics, average test scores, attendance, and safety across all schools in the district. Districts 1-32 are geographic, 75 is citywide special education, 79 is alternative schools. Example: district_report('2'). For individual school detail, use school_report(dbn). To find schools in a district, use school_search(district_number)."""
    district = district.strip().lstrip('0') or '0'
    if not district.isdigit() or int(district) < 1 or int(district) > 79:
        raise ToolError("District must be 1-32, 75, or 79.")

    padded = district.zfill(2)
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    _, agg_rows = _safe_query(db, DISTRICT_AGGREGATE_SQL, [padded])
    _, test_rows = _safe_query(db, DISTRICT_TEST_SCORES_SQL, [padded, padded])
    _, attend_rows = _safe_query(db, DISTRICT_ATTENDANCE_SQL, [padded])
    _, safety_rows = _safe_query(db, DISTRICT_SAFETY_SQL, [padded])
    _, school_rows = _safe_query(db, DISTRICT_SCHOOLS_SQL, [padded])

    if not agg_rows or not agg_rows[0][0]:
        raise ToolError(f"No data found for District {district}. Valid districts: 1-32, 75, 79.")

    a = agg_rows[0]
    lines = [f"DISTRICT {district} REPORT", "=" * 50]

    lines.append(f"\nOVERVIEW")
    lines.append(f"  Schools: {a[0]}")
    lines.append(f"  Total students: {int(a[1] or 0):,}")
    lines.append(f"  Average enrollment: {int(a[2] or 0):,}")

    lines.append(f"\nDEMOGRAPHICS (district average)")
    for label, val in [("Asian", a[3]), ("Black", a[4]), ("Hispanic", a[5]), ("White", a[6])]:
        if val:
            lines.append(f"  {label}: {val}%")

    if test_rows:
        lines.append(f"\nTEST SCORES (district average):")
        for r in test_rows[:6]:
            year, subject, count, avg_score = r
            lines.append(f"  {year} {subject}: avg mean score {avg_score} ({count} schools)")

    if attend_rows:
        lines.append(f"\nATTENDANCE:")
        for r in attend_rows:
            year, schools, avg_attend, avg_chronic = r
            lines.append(f"  {year}: {avg_attend}% avg attendance, {avg_chronic}% chronic absent ({schools} schools)")

    if safety_rows:
        lines.append(f"\nSAFETY:")
        for r in safety_rows:
            sy, schools, major, violent, prop, register = r
            total = int(major or 0) + int(violent or 0) + int(prop or 0)
            rate = round(total / int(register) * 1000, 1) if register and int(register) > 0 else 0
            lines.append(f"  {sy}: {total} incidents across {schools} schools ({rate} per 1000 students)")

    if school_rows:
        lines.append(f"\nLARGEST SCHOOLS:")
        for r in school_rows[:10]:
            dbn, name, enrollment = r
            enr_str = f"{enrollment:,}" if enrollment else "?"
            lines.append(f"  {dbn} — {name} ({enr_str} students)")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"district": district, "school_count": a[0], "total_students": a[1]},
        meta={"district": district, "query_time_ms": elapsed},
    )


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

    extra_names = _vector_expand_names(ctx, name.strip().upper())

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

    # Phonetic fallback for professional databases
    phonetic_pro = []
    try:
        from entity import phonetic_search_sql
        ph_sql, ph_params = phonetic_search_sql(
            first_name=first_guess if len(parts) >= 2 else None,
            last_name=last_guess, min_score=0.8, limit=10)
        ph_cols, ph_rows = _execute(db, ph_sql, ph_params)
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

    extra_names = _vector_expand_names(ctx, name.strip().upper())

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
# NOTE: marriage_licenses_1908_1949 has NO name columns (only license_number, license_year, county).
# This query is a placeholder that returns 0 rows. Cannot search by name.

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

    # Phonetic enhancement for historical records
    phonetic_deaths = []
    try:
        from entity import phonetic_vital_search_sql
        ph_sql, ph_params = phonetic_vital_search_sql(
            first_name=first if first != "%" else None, last_name=last,
            table="lake.federal.nys_death_index",
            first_col="first_name", last_col="last_name",
            extra_cols="age, date_of_death", limit=20)
        _, phonetic_deaths = _execute(db, ph_sql, ph_params)
    except Exception:
        pass

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

    if phonetic_deaths:
        lines.append(f"\nPHONETIC DEATH MATCHES ({len(phonetic_deaths)} — spelling variants):")
        for r in phonetic_deaths[:10]:
            lines.append(f"  {r[0] or '?'} {r[1] or '?'} — score: {r[-2]:.2f}/{r[-1]:.2f}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"name": name, "deaths": len(death_rows or []), "marriages": len(all_marriages), "births": len(birth_rows or [])},
        meta={"name": name, "total_records": total, "query_time_ms": elapsed},
    )


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

    extra_names = _vector_expand_names(ctx, name.strip().upper())

    parts = name.split()
    first = parts[0] if len(parts) >= 2 else "%"
    last = parts[-1] if len(parts) >= 2 else name

    _, nys_rows = _safe_query(db, MONEY_NYS_DONATIONS_SQL, [f"{last}%", f"{first}%", name])
    _, fec_rows = _safe_query(db, MONEY_FEC_SQL, [name])
    _, nyc_rows = _safe_query(db, MONEY_NYC_DONATIONS_SQL, [name, name])
    _, contract_rows = _safe_query(db, MONEY_CONTRACTS_SQL, [name])
    _, procurement_rows = _safe_query(db, MONEY_PROCUREMENT_SQL, [name])

    # Fuzzy enhancement for name variants
    fuzzy_fec = []
    try:
        from entity import fuzzy_money_search_sql
        fec_sql, fec_params = fuzzy_money_search_sql(name=name,
            table="lake.federal.fec_contributions", name_col="contributor_name",
            extra_cols="committee_id, contribution_receipt_amount, contribution_receipt_date",
            min_score=75, limit=15)
        _, fuzzy_fec = _execute(db, fec_sql, fec_params)
    except Exception:
        pass

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

    if fuzzy_fec and not fec_rows:
        lines.append(f"\nFUZZY FEC MATCHES ({len(fuzzy_fec)} — name variants):")
        for r in fuzzy_fec[:5]:
            lines.append(f"  {r[0]} — ${float(r[2] or 0):,.0f} (score: {r[-1]:.0f})")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"name": name, "nys_donations": len(nys_rows), "fec_donations": len(fec_rows), "nyc_donations": len(nyc_rows), "contracts": len(contract_rows)},
        meta={"name": name, "query_time_ms": elapsed},
    )


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



# ---------------------------------------------------------------------------
# Commercial Vitality
# ---------------------------------------------------------------------------

COMMERCIAL_LICENSES_SQL = """
SELECT
    COUNT(*) AS total_licenses,
    COUNT(*) FILTER (WHERE TRY_CAST(license_expiration_date AS DATE) >= CURRENT_DATE) AS active_licenses,
    COUNT(DISTINCT industry) AS industries
FROM lake.business.issued_licenses
WHERE COALESCE(business_zip, contact_phone_zip) = ?
"""

COMMERCIAL_SIDEWALK_CAFES_SQL = """
SELECT COUNT(*) AS cafes
FROM lake.business.sidewalk_cafe
WHERE zip = ?
"""

COMMERCIAL_DOB_PERMITS_SQL = """
SELECT
    COUNT(*) AS total_permits,
    COUNT(*) FILTER (WHERE TRY_CAST(issuance_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY) AS recent_permits
FROM lake.housing.dob_permit_issuance
WHERE COALESCE(zip, postcode) = ?
"""

COMMERCIAL_RESTAURANTS_SQL = """
SELECT
    COUNT(DISTINCT camis) AS restaurants,
    ROUND(100.0 * COUNT(DISTINCT camis) FILTER (WHERE grade = 'A')
        / NULLIF(COUNT(DISTINCT camis) FILTER (WHERE grade IN ('A','B','C')), 0), 0) AS pct_a
FROM lake.health.restaurant_inspections
WHERE zipcode = ?
"""

COMMERCIAL_CERTIFIED_BIZ_SQL = """
SELECT COUNT(*) AS certified_businesses
FROM lake.business.sbs_certified
WHERE zip = ?
"""

COMMERCIAL_311_SQL = """
SELECT problem_formerly_complaint_type AS type, COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ?
  AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
  AND (agency = 'DCA' OR problem_formerly_complaint_type ILIKE '%commercial%'
       OR problem_formerly_complaint_type ILIKE '%store%'
       OR problem_formerly_complaint_type ILIKE '%sidewalk%')
GROUP BY problem_formerly_complaint_type
ORDER BY cnt DESC LIMIT 5
"""


@mcp.tool(annotations=READONLY, tags={"services"})
def commercial_vitality(zipcode: ZIP, ctx: Context) -> ToolResult:
    """Measure business licenses, permits, commercial economic activity, and storefront health for a NYC ZIP code. Covers active licenses, restaurant count, DOB construction permits, sidewalk cafes, SBS certified businesses, and commercial 311 complaints. Use this for business vitality, economic activity, or storefront vacancy questions. Parameters: zipcode (5 digits). Example: 10003 (East Village). For neighborhood lifestyle context, use neighborhood_portrait(zipcode)."""
    if not re.match(r"^\d{5}$", zipcode):
        raise ToolError("ZIP code must be exactly 5 digits")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    _, lic_rows = _safe_query(db, COMMERCIAL_LICENSES_SQL, [zipcode])
    _, cafe_rows = _safe_query(db, COMMERCIAL_SIDEWALK_CAFES_SQL, [zipcode])
    _, permit_rows = _safe_query(db, COMMERCIAL_DOB_PERMITS_SQL, [zipcode])
    _, rest_rows = _safe_query(db, COMMERCIAL_RESTAURANTS_SQL, [zipcode])
    _, cert_rows = _safe_query(db, COMMERCIAL_CERTIFIED_BIZ_SQL, [zipcode])
    _, svc311_rows = _safe_query(db, COMMERCIAL_311_SQL, [zipcode])

    lines = [f"COMMERCIAL VITALITY — ZIP {zipcode}", "=" * 45]

    total_licenses = int(lic_rows[0][0] or 0) if lic_rows else 0
    active_licenses = int(lic_rows[0][1] or 0) if lic_rows else 0
    industries = int(lic_rows[0][2] or 0) if lic_rows else 0
    cafes = int(cafe_rows[0][0] or 0) if cafe_rows else 0
    total_permits = int(permit_rows[0][0] or 0) if permit_rows else 0
    recent_permits = int(permit_rows[0][1] or 0) if permit_rows else 0
    restaurants = int(rest_rows[0][0] or 0) if rest_rows else 0
    pct_a = rest_rows[0][1] if rest_rows and rest_rows[0][1] else 0
    certified = int(cert_rows[0][0] or 0) if cert_rows else 0

    lines.append(f"\nBUSINESS LICENSES: {total_licenses:,} total ({active_licenses:,} active)")
    lines.append(f"  {industries} distinct industries")
    lines.append(f"\nRESTAURANTS: {restaurants} ({pct_a:.0f}% grade A)")
    lines.append(f"SIDEWALK CAFES: {cafes}")
    lines.append(f"SBS CERTIFIED BUSINESSES: {certified}")
    lines.append(f"\nDOB PERMITS: {total_permits:,} total ({recent_permits:,} in last year)")

    if svc311_rows:
        lines.append(f"\nCOMMERCIAL 311 COMPLAINTS (past year):")
        for r in svc311_rows:
            lines.append(f"  {r[0]}: {r[1]}")

    # Vitality score
    score = 0
    score += min(25, active_licenses // 10)       # Up to 25 pts for active licenses
    score += min(20, restaurants // 5)             # Up to 20 pts for restaurants
    score += min(15, recent_permits // 20)         # Up to 15 pts for recent construction
    score += min(10, cafes)                        # Up to 10 pts for sidewalk cafes
    score += min(10, certified)                    # Up to 10 pts for certified businesses
    score += min(10, industries)                   # Up to 10 pts for diversity
    if pct_a and pct_a >= 80:
        score += 10                                # 10 pts for food safety quality

    if score >= 70:
        rating = "THRIVING — strong commercial ecosystem"
    elif score >= 50:
        rating = "HEALTHY — active business district"
    elif score >= 30:
        rating = "DEVELOPING — moderate activity"
    else:
        rating = "QUIET — limited commercial presence"

    lines.append(f"\nVITALITY SCORE: {score}/100 — {rating}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "zipcode": zipcode,
        "active_licenses": active_licenses,
        "restaurants": restaurants,
        "pct_grade_a": pct_a,
        "sidewalk_cafes": cafes,
        "recent_permits": recent_permits,
        "certified_businesses": certified,
        "vitality_score": score,
        "rating": rating,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# NYC Oracle — tasteful, data-driven stories about buildings & neighborhoods
# ---------------------------------------------------------------------------

# Notable NYC events by decade for building_story context
_NYC_ERAS = {
    range(1800, 1860): ("Antebellum New York", "when Manhattan was still farmland above 14th Street"),
    range(1860, 1900): ("the Gilded Age", "as immigrants poured through Castle Garden and the Brooklyn Bridge opened"),
    range(1900, 1920): ("the Progressive Era", "when tenement reform reshaped housing and the subway arrived"),
    range(1920, 1930): ("the Roaring Twenties", "during Prohibition, jazz clubs, and the Harlem Renaissance"),
    range(1930, 1946): ("the Depression & War years", "when WPA workers built parks and public housing across the city"),
    range(1946, 1960): ("the postwar boom", "as GIs returned home and Robert Moses reshaped the boroughs"),
    range(1960, 1980): ("the urban crisis", "through fiscal collapse, the '77 blackout, and the South Bronx fires"),
    range(1980, 2000): ("the renewal era", "as crack receded, crime fell, and neighborhoods rebuilt"),
    range(2000, 2010): ("the post-9/11 era", "through rebuilding, rezoning, and the 2008 financial crisis"),
    range(2010, 2020): ("the Bloomberg-de Blasio years", "amid record construction, Hurricane Sandy, and rising rents"),
    range(2020, 2030): ("the pandemic era", "through COVID, remote work, and a transformed city"),
}

_NYC_MILESTONES = [
    (1883, "Brooklyn Bridge opened"),
    (1886, "Statue of Liberty dedicated"),
    (1904, "First subway line opened"),
    (1929, "Stock market crash"),
    (1931, "Empire State Building completed"),
    (1939, "World's Fair in Flushing"),
    (1964, "World's Fair returns"),
    (1969, "Stonewall uprising"),
    (1977, "Blackout"),
    (2001, "September 11th"),
    (2012, "Hurricane Sandy"),
    (2020, "COVID-19 pandemic"),
]

STORY_PLUTO_SQL = """
SELECT yearbuilt, numfloors, unitsres, unitstotal, bldgclass, bldgarea, lotarea,
       address, ownername, landmark, histdist, yearalter1, yearalter2,
       zonedist1, assesstot, borough, postcode
FROM lake.city_government.pluto
WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
LIMIT 1
"""

STORY_VIOLATIONS_SQL = """
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_ct,
       MIN(TRY_CAST(novissueddate AS DATE)) AS first_violation,
       MAX(TRY_CAST(novissueddate AS DATE)) AS latest_violation
FROM lake.housing.hpd_violations
WHERE boroid = ? AND block = ? AND lot = ?
"""

STORY_COMPLAINTS_SQL = """
SELECT majorcategory, COUNT(*) AS cnt
FROM lake.housing.hpd_complaints
WHERE boroid = ? AND block = ? AND lot = ?
GROUP BY majorcategory ORDER BY cnt DESC LIMIT 8
"""

STORY_311_SQL = """
SELECT complaint_type, COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ? AND bbl = ?
GROUP BY complaint_type ORDER BY cnt DESC LIMIT 8
"""

STORY_PERMITS_SQL = """
SELECT job_type, COUNT(*) AS cnt,
       MIN(TRY_CAST(filing_date AS DATE)) AS earliest,
       MAX(TRY_CAST(filing_date AS DATE)) AS latest
FROM lake.housing.dob_permit_issuance
WHERE borough = ? AND block = ? AND lot = ?
GROUP BY job_type ORDER BY cnt DESC LIMIT 5
"""

STORY_NEIGHBORS_SQL = """
SELECT
    COUNT(*) AS total_buildings,
    AVG(TRY_CAST(yearbuilt AS INT)) FILTER (WHERE TRY_CAST(yearbuilt AS INT) > 1800) AS avg_year,
    AVG(TRY_CAST(numfloors AS DOUBLE)) AS avg_floors,
    AVG(TRY_CAST(unitsres AS DOUBLE)) AS avg_units
FROM lake.city_government.pluto
WHERE postcode = ? AND TRY_CAST(yearbuilt AS INT) > 1800
"""

STORY_RESTAURANTS_SQL = """
SELECT dba, cuisine_description, grade
FROM lake.health.restaurant_inspections
WHERE zipcode = ?
  AND grade IN ('A', 'B', 'C')
  AND grade_date = (
      SELECT MAX(grade_date)
      FROM lake.health.restaurant_inspections r2
      WHERE r2.camis = restaurant_inspections.camis
  )
GROUP BY dba, cuisine_description, grade
ORDER BY grade, dba
LIMIT 200
"""


@mcp.tool(annotations=READONLY, tags={"building", "housing"})
def building_story(bbl: BBL, ctx: Context) -> ToolResult:
    """The narrative history of a NYC building — its era, what it's survived, complaint character, neighborhood comparison, and milestones witnessed. Use this for a storytelling view of a building's life. Differs from building_profile (quick stats) and building_context (era/contemporaries). Parameters: bbl (10 digits: borough(1) + block(5) + lot(4)). Example: 1000670001. For quick stats, use building_profile(bbl). For era context, use building_context(bbl)."""

    t0 = time.time()
    db = ctx.lifespan_context["db"]

    # --- PLUTO lookup ---
    cols, rows = _safe_query(db, STORY_PLUTO_SQL, [bbl])
    if not rows:
        raise ToolError(f"No building found for BBL {bbl} in PLUTO.")

    p = dict(zip(cols, rows[0]))
    year = int(float(p.get("yearbuilt") or 0))
    floors = int(float(p.get("numfloors") or 0))
    units = int(float(p.get("unitsres") or 0))
    bldg_class = p.get("bldgclass") or "Unknown"
    address = p.get("address") or "Unknown"
    owner = p.get("ownername") or "Unknown"
    borough = p.get("borough") or ""
    zipcode = p.get("postcode") or ""
    landmark = p.get("landmark")
    hist_dist = p.get("histdist")
    lot_area = int(float(p.get("lotarea") or 0))
    bldg_area = int(float(p.get("bldgarea") or 0))
    assessed = float(p.get("assesstot") or 0)
    alter1 = int(float(p.get("yearalter1") or 0))
    alter2 = int(float(p.get("yearalter2") or 0))
    zone = p.get("zonedist1") or ""

    boro_digit = bbl[0]
    block = str(int(bbl[1:6]))
    lot = str(int(bbl[6:10]))

    # --- Era and age ---
    current_year = 2026
    age = current_year - year if year > 1800 else None
    era_name, era_desc = "an unknown era", ""
    for yr_range, (name, desc) in _NYC_ERAS.items():
        if year in yr_range:
            era_name, era_desc = name, desc
            break

    # --- Milestones witnessed ---
    witnessed = [(y, e) for y, e in _NYC_MILESTONES if y >= year] if year > 1800 else []

    # --- Families estimate (avg NYC tenancy ~5 years) ---
    families_est = (units * age // 5) if age and units else None

    lines = []
    lines.append(f"THE LIFE OF {address}, {borough}")
    lines.append("=" * 60)

    if age and year > 1800:
        lines.append(f"\nBuilt in {year}, during {era_name} — {era_desc}.")
        lines.append(f"Your building is {age} years old.")
        if families_est:
            lines.append(f"Approximately {families_est:,} families have called it home.")
    else:
        lines.append(f"\nYear built: {year if year > 0 else 'Unknown'}")

    lines.append(f"\nTHE FACTS")
    lines.append(f"  Floors: {floors}")
    lines.append(f"  Residential units: {units}")
    lines.append(f"  Building class: {bldg_class}")
    lines.append(f"  Lot area: {lot_area:,} sq ft")
    lines.append(f"  Building area: {bldg_area:,} sq ft")
    lines.append(f"  Zoning: {zone}")
    lines.append(f"  Assessed value: ${assessed:,.0f}")
    lines.append(f"  Owner: {owner}")

    if landmark:
        lines.append(f"  Landmark: {landmark}")
    if hist_dist:
        lines.append(f"  Historic district: {hist_dist}")
    if alter1 and alter1 > 1800:
        lines.append(f"  Major alteration: {alter1}")
    if alter2 and alter2 > 1800:
        lines.append(f"  Second alteration: {alter2}")

    # --- Violations ---
    cols_v, rows_v = _safe_query(db, STORY_VIOLATIONS_SQL, [boro_digit, block, lot])
    if rows_v:
        v = dict(zip(cols_v, rows_v[0]))
        total_v = int(v.get("total") or 0)
        open_v = int(v.get("open_ct") or 0)
        first_v = v.get("first_violation")
        latest_v = v.get("latest_violation")
        if total_v:
            lines.append(f"\nVIOLATION HISTORY")
            lines.append(f"  {total_v:,} total violations on record ({open_v:,} currently open)")
            if first_v:
                lines.append(f"  First recorded: {first_v}")
            if latest_v:
                lines.append(f"  Most recent: {latest_v}")

    # --- Complaint character ---
    cols_c, rows_c = _safe_query(db, STORY_COMPLAINTS_SQL, [boro_digit, block, lot])
    if rows_c:
        lines.append(f"\nWHAT RESIDENTS TALK ABOUT")
        for row in rows_c[:6]:
            r = dict(zip(cols_c, row))
            cat = r.get("majorcategory") or "Other"
            cnt = int(r.get("cnt") or 0)
            lines.append(f"  {cat}: {cnt:,} complaints")

    # --- 311 ---
    if zipcode:
        cols_311, rows_311 = _safe_query(db, STORY_311_SQL, [zipcode, bbl])
        if rows_311:
            lines.append(f"\n311 CALLS (since 2020)")
            for row in rows_311[:5]:
                r = dict(zip(cols_311, row))
                lines.append(f"  {r.get('complaint_type')}: {int(r.get('cnt') or 0):,}")

    # --- Construction history ---
    cols_p, rows_p = _safe_query(db, STORY_PERMITS_SQL, [boro_digit, block, lot])
    if rows_p:
        lines.append(f"\nCONSTRUCTION HISTORY")
        for row in rows_p:
            r = dict(zip(cols_p, row))
            lines.append(f"  {r.get('job_type')}: {int(r.get('cnt') or 0)} permits ({r.get('earliest')} — {r.get('latest')})")

    # --- Milestones ---
    if witnessed:
        lines.append(f"\nWHAT THIS BUILDING HAS WITNESSED")
        for yr, event in witnessed:
            lines.append(f"  {yr}: {event}")

    # --- Neighborhood comparison ---
    if zipcode:
        cols_n, rows_n = _safe_query(db, STORY_NEIGHBORS_SQL, [zipcode])
        if rows_n:
            n = dict(zip(cols_n, rows_n[0]))
            total_n = int(n.get("total_buildings") or 0)
            avg_yr = int(float(n.get("avg_year") or 0))
            avg_fl = round(float(n.get("avg_floors") or 0), 1)
            avg_un = round(float(n.get("avg_units") or 0), 1)
            if total_n:
                lines.append(f"\nIN THE NEIGHBORHOOD (ZIP {zipcode})")
                lines.append(f"  {total_n:,} buildings in your ZIP code")
                if avg_yr > 1800 and year > 1800:
                    diff = year - avg_yr
                    if diff < -5:
                        lines.append(f"  Your building is {abs(diff)} years older than average (built {avg_yr})")
                    elif diff > 5:
                        lines.append(f"  Your building is {diff} years younger than average (built {avg_yr})")
                    else:
                        lines.append(f"  Your building is typical age for the area (avg built {avg_yr})")
                if floors:
                    if floors > avg_fl + 2:
                        lines.append(f"  Taller than neighbors ({floors} floors vs avg {avg_fl})")
                    elif floors < avg_fl - 2:
                        lines.append(f"  Shorter than neighbors ({floors} floors vs avg {avg_fl})")
                    else:
                        lines.append(f"  Similar height to neighbors ({floors} floors, avg {avg_fl})")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "bbl": bbl,
        "address": address,
        "borough": borough,
        "year_built": year,
        "age": age,
        "era": era_name,
        "floors": floors,
        "units": units,
        "building_class": bldg_class,
        "assessed_value": assessed,
        "owner": owner,
        "landmark": landmark,
        "historic_district": hist_dist,
        "families_estimate": families_est,
        "milestones_witnessed": len(witnessed),
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"bbl": bbl, "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"housing"})
def similar_buildings(
    bbl: BBL,
    ctx: Context,
    limit: Annotated[int, Field(description="Number of similar buildings (1-30). Default: 10", ge=1, le=30)] = 10,
) -> ToolResult:
    """Find buildings most similar to the given BBL based on physical characteristics and violation patterns — stories, units, age, violation rate, open violations. Useful for comparison, risk assessment, and pattern detection. Returns similar buildings with BBLs for follow-up with building_profile() or landlord_watchdog()."""

    if not re.match(r"^\d{10}$", bbl):
        raise ToolError(f"Invalid BBL '{bbl}': must be exactly 10 digits.")

    t0 = time.time()
    db = ctx.lifespan_context["db"]

    with _db_lock:
        target_rows = db.execute(
            f"SELECT features FROM '{LANCE_DIR}/building_vectors.lance' WHERE bbl = '{bbl}'"
        ).fetchall()

    if not target_rows:
        raise ToolError(f"No feature vector found for BBL {bbl}. Building may not be in the vector index.")

    target_vec = target_rows[0][0]
    vec_literal = "[" + ",".join(f"{v:.6g}" for v in target_vec) + "]::FLOAT[]"

    knn_sql = f"""
SELECT
    bv.bbl,
    gb.housenumber,
    gb.streetname,
    gb.zip,
    gb.stories,
    gb.units,
    gb.year_built,
    gb.borough,
    bv._distance AS distance
FROM lance_vector_search('{LANCE_DIR}/building_vectors.lance', 'features', {vec_literal}, k={limit+1}) bv
LEFT JOIN main.graph_buildings gb ON gb.bbl = bv.bbl
WHERE bv.bbl != '{bbl}'
ORDER BY bv._distance ASC
LIMIT {limit}
"""

    with _db_lock:
        result = db.execute(knn_sql).fetchall()
        cols = [d[0] for d in db.description]

    elapsed = round((time.time() - t0) * 1000)

    if not result:
        return ToolResult(
            content=f"No similar buildings found for BBL {bbl}.",
            meta={"bbl": bbl, "query_time_ms": elapsed, "count": 0},
        )

    lines = [f"BUILDINGS SIMILAR TO {bbl}", "=" * 60, ""]
    records = []
    for i, row in enumerate(result, 1):
        r = dict(zip(cols, row))
        sim_bbl = r.get("bbl") or ""
        housenumber = r.get("housenumber") or ""
        streetname = r.get("streetname") or ""
        zip_code = r.get("zip") or ""
        stories = r.get("stories") or "?"
        units = r.get("units") or "?"
        year_built = r.get("year_built") or "?"
        borough = r.get("borough") or ""
        address = f"{housenumber} {streetname}".strip() or "Unknown address"
        addr_full = f"{address}, {borough} {zip_code}".strip(", ")
        lines.append(f"{i}. BBL {sim_bbl} — {addr_full}")
        lines.append(f"   {stories} stories | {units} units | built {year_built}")
        records.append({
            "bbl": sim_bbl,
            "address": addr_full,
            "stories": stories,
            "units": units,
            "year_built": year_built,
            "borough": borough,
        })

    lines.append("")
    lines.append("Use building_profile(bbl) or landlord_watchdog(bbl) for any building above.")
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"target_bbl": bbl, "similar_buildings": records},
        meta={"bbl": bbl, "query_time_ms": elapsed, "count": len(records)},
    )


# --- Neighborhood Portrait ---

PORTRAIT_CUISINE_SQL = """
SELECT cuisine_description, COUNT(DISTINCT camis) AS restaurants
FROM lake.health.restaurant_inspections
WHERE zipcode = ?
  AND grade IS NOT NULL
GROUP BY cuisine_description
ORDER BY restaurants DESC
LIMIT 10
"""

PORTRAIT_CUISINE_CITY_SQL = """
SELECT cuisine_description, COUNT(DISTINCT camis) AS restaurants
FROM lake.health.restaurant_inspections
WHERE grade IS NOT NULL
GROUP BY cuisine_description
ORDER BY restaurants DESC
LIMIT 20
"""

PORTRAIT_GRADES_SQL = """
WITH latest AS (
    SELECT camis, grade,
           ROW_NUMBER() OVER (PARTITION BY camis ORDER BY grade_date DESC) AS rn
    FROM lake.health.restaurant_inspections
    WHERE zipcode = ? AND grade IN ('A','B','C')
)
SELECT grade, COUNT(*) AS cnt FROM latest WHERE rn = 1 GROUP BY grade ORDER BY grade
LIMIT 3
"""

PORTRAIT_BUILDINGS_SQL = """
SELECT
    COUNT(*) AS total,
    AVG(TRY_CAST(yearbuilt AS INT)) FILTER (WHERE TRY_CAST(yearbuilt AS INT) > 1800) AS avg_year,
    AVG(TRY_CAST(numfloors AS DOUBLE)) AS avg_floors,
    MEDIAN(TRY_CAST(assesstot AS DOUBLE)) AS median_assessed,
    COUNT(*) FILTER (WHERE landmark IS NOT NULL AND landmark != '') AS landmarks,
    COUNT(*) FILTER (WHERE histdist IS NOT NULL AND histdist != '') AS in_hist_district
FROM lake.city_government.pluto
WHERE postcode = ?
"""

PORTRAIT_311_SQL = """
SELECT complaint_type, COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ?
GROUP BY complaint_type ORDER BY cnt DESC LIMIT 10
"""

PORTRAIT_311_NOISE_SQL = """
SELECT COUNT(*) AS noise_total
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ?
  AND complaint_type ILIKE '%noise%'
LIMIT 1
"""

PORTRAIT_311_CITY_NOISE_SQL = """
SELECT
    COUNT(*) FILTER (WHERE complaint_type ILIKE '%noise%') * 1.0 / NULLIF(COUNT(DISTINCT incident_zip), 0) AS avg_noise_per_zip
FROM (
    SELECT complaint_type, incident_zip
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IS NOT NULL AND LENGTH(incident_zip) = 5
      AND created_date >= CURRENT_DATE - INTERVAL '3 years'
) sub
LIMIT 1
"""

PORTRAIT_CRIME_SQL = """
WITH precinct AS (
    SELECT LPAD(CAST(policeprct AS VARCHAR), 3, '0') AS pct
    FROM lake.city_government.pluto
    WHERE postcode = ?
      AND policeprct IS NOT NULL
    LIMIT 1
)
SELECT
    COUNT(*) FILTER (WHERE TRY_CAST(SUBSTRING(cmplnt_fr_dt, -4) AS INT) = 2024) AS crimes_2024,
    COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY' AND TRY_CAST(SUBSTRING(cmplnt_fr_dt, -4) AS INT) = 2024) AS felonies_2024
FROM lake.public_safety.nypd_complaints_ytd
WHERE SUBSTRING(CAST(cmplnt_num AS VARCHAR), 1, 3) = (SELECT pct FROM precinct)
LIMIT 1
"""

PORTRAIT_BIZ_SQL = """
SELECT industry, COUNT(*) AS cnt
FROM lake.business.issued_licenses
WHERE zip = ?
  AND license_status = 'Active'
GROUP BY industry ORDER BY cnt DESC LIMIT 8
"""


@mcp.tool(annotations=READONLY, tags={"neighborhood"})
def neighborhood_portrait(zipcode: ZIP, ctx: Context) -> ToolResult:
    """What makes a NYC neighborhood distinctive — cuisine fingerprint, building stock, noise level, business mix, and how it compares to the city. Use this for neighborhood questions by ZIP. For comparing multiple ZIPs side-by-side, use neighborhood_compare([zips]). For environmental justice analysis, use environmental_justice(zipcode). For gentrification tracking, use gentrification_tracker([zips]). For schools in this ZIP, use school_search(zipcode). ZIP: 5 digits. Example: 10003 (East Village), 11201 (Downtown Brooklyn)."""

    t0 = time.time()
    db = ctx.lifespan_context["db"]

    # Validate
    if not zipcode or len(zipcode) != 5 or not zipcode.isdigit():
        raise ToolError("Please provide a valid 5-digit NYC ZIP code.")

    # Borough from ZIP prefix
    prefix = int(zipcode[:3])
    if 100 <= prefix <= 102:
        boro_name = "Manhattan"
    elif 103 <= prefix <= 103:
        boro_name = "Staten Island"
    elif 104 <= prefix <= 105:
        boro_name = "The Bronx"
    elif 112 <= prefix <= 112:
        boro_name = "Brooklyn"
    elif 110 <= prefix <= 116:
        boro_name = "Queens"
    else:
        boro_name = "NYC"

    lines = []
    lines.append(f"NEIGHBORHOOD PORTRAIT: {zipcode}, {boro_name}")
    lines.append("=" * 60)

    # --- Cuisine fingerprint ---
    cols_c, rows_c = _safe_query(db, PORTRAIT_CUISINE_SQL, [zipcode])
    cols_city, rows_city = _safe_query(db, PORTRAIT_CUISINE_CITY_SQL)
    city_totals = {}
    city_restaurants = 0
    city_zips = 200  # approximate number of NYC ZIPs
    if rows_city:
        for row in rows_city:
            r = dict(zip(cols_city, row))
            city_totals[r["cuisine_description"]] = int(r["restaurants"])
            city_restaurants += int(r["restaurants"])

    if rows_c:
        lines.append(f"\nCUISINE FINGERPRINT")
        total_local = sum(int(dict(zip(cols_c, r))["restaurants"]) for r in rows_c)
        standout = None
        standout_ratio = 0
        for row in rows_c[:8]:
            r = dict(zip(cols_c, row))
            cuisine = r["cuisine_description"]
            count = int(r["restaurants"])
            pct = round(count / total_local * 100) if total_local else 0
            city_avg = city_totals.get(cuisine, 0) / city_zips if city_zips else 0
            ratio = count / city_avg if city_avg > 1 else 0
            marker = ""
            if ratio > 2:
                marker = f"  ({ratio:.1f}x city average)"
                if ratio > standout_ratio:
                    standout_ratio = ratio
                    standout = (cuisine, ratio)
            lines.append(f"  {cuisine}: {count} restaurants ({pct}%){marker}")

        if standout:
            lines.append(f"\n  Signature: {standout[0]} — {standout[1]:.1f}x the city average")

    # --- Restaurant grades ---
    cols_g, rows_g = _safe_query(db, PORTRAIT_GRADES_SQL, [zipcode])
    if rows_g:
        grades = {dict(zip(cols_g, r))["grade"]: int(dict(zip(cols_g, r))["cnt"]) for r in rows_g}
        total_graded = sum(grades.values())
        pct_a = round(grades.get("A", 0) / total_graded * 100) if total_graded else 0
        lines.append(f"\nFOOD SAFETY")
        lines.append(f"  {total_graded} graded restaurants: {pct_a}% Grade A")
        for g in ["A", "B", "C"]:
            if g in grades:
                lines.append(f"    {g}: {grades[g]}")

    # --- Building stock ---
    cols_b, rows_b = _safe_query(db, PORTRAIT_BUILDINGS_SQL, [zipcode])
    if rows_b:
        b = dict(zip(cols_b, rows_b[0]))
        total_bldg = int(b.get("total") or 0)
        avg_year = int(float(b.get("avg_year") or 0))
        avg_floors = round(float(b.get("avg_floors") or 0), 1)
        median_val = float(b.get("median_assessed") or 0)
        landmarks_ct = int(b.get("landmarks") or 0)
        hist_ct = int(b.get("in_hist_district") or 0)

        lines.append(f"\nBUILDING STOCK")
        lines.append(f"  {total_bldg:,} buildings")
        if avg_year > 1800:
            lines.append(f"  Average built: {avg_year}")
        lines.append(f"  Average height: {avg_floors} floors")
        lines.append(f"  Median assessed value: ${median_val:,.0f}")
        if landmarks_ct:
            lines.append(f"  Landmarks: {landmarks_ct}")
        if hist_ct:
            lines.append(f"  In historic district: {hist_ct}")

    # --- 311 character ---
    cols_311, rows_311 = _safe_query(db, PORTRAIT_311_SQL, [zipcode])
    if rows_311:
        lines.append(f"\n311 CHARACTER (since 2020)")
        total_311 = 0
        for row in rows_311[:8]:
            r = dict(zip(cols_311, row))
            cnt = int(r.get("cnt") or 0)
            total_311 += cnt
            lines.append(f"  {r.get('complaint_type')}: {cnt:,}")

    # Noise comparison
    cols_noise, rows_noise = _safe_query(db, PORTRAIT_311_NOISE_SQL, [zipcode])
    cols_cn, rows_cn = _safe_query(db, PORTRAIT_311_CITY_NOISE_SQL)
    if rows_noise and rows_cn:
        local_noise = int(dict(zip(cols_noise, rows_noise[0])).get("noise_total") or 0)
        city_avg_noise = float(dict(zip(cols_cn, rows_cn[0])).get("avg_noise_per_zip") or 1)
        if city_avg_noise > 0:
            noise_ratio = local_noise / city_avg_noise
            if noise_ratio > 1.5:
                lines.append(f"\n  Noise level: {noise_ratio:.1f}x the city average — a loud neighborhood")
            elif noise_ratio < 0.5:
                lines.append(f"\n  Noise level: {noise_ratio:.1f}x the city average — notably quiet")
            else:
                lines.append(f"\n  Noise level: about average for NYC")

    # --- Business mix ---
    cols_biz, rows_biz = _safe_query(db, PORTRAIT_BIZ_SQL, [zipcode])
    if rows_biz:
        lines.append(f"\nBUSINESS MIX (active licenses)")
        for row in rows_biz[:6]:
            r = dict(zip(cols_biz, row))
            lines.append(f"  {r.get('industry')}: {int(r.get('cnt') or 0):,}")

    # H3-based safety snapshot
    try:
        from spatial import h3_zip_centroid_sql, h3_neighborhood_stats_sql
        _, centroid = _execute(db, *h3_zip_centroid_sql(zipcode))
        if centroid and centroid[0][0] is not None:
            c_lat, c_lng = centroid[0][1], centroid[0][2]
            _, stats = _execute(db, *h3_neighborhood_stats_sql(c_lat, c_lng, radius_rings=6))
            if stats:
                s = dict(zip(["total_crimes", "total_arrests", "restaurants_h3", "n311_calls", "shootings", "street_trees"], stats[0]))
                lines.append(f"\nSAFETY SNAPSHOT (H3 hex radius)")
                lines.append(f"  Crimes nearby: {s.get('total_crimes', 0):,}")
                lines.append(f"  Arrests nearby: {s.get('total_arrests', 0):,}")
                lines.append(f"  Shootings nearby: {s.get('shootings', 0):,}")
                lines.append(f"  311 calls nearby: {s.get('n311_calls', 0):,}")
                lines.append(f"  Street trees: {s.get('street_trees', 0):,}")
    except Exception:
        pass

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "zipcode": zipcode,
        "borough": boro_name,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


# --- NYC Twins ---

TWINS_FIND_SQL = """
WITH target AS (
    SELECT bbl, yearbuilt, numfloors, unitsres, bldgclass, bldgarea, lotarea,
           address, borough, postcode, assesstot
    FROM lake.city_government.pluto
    WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    LIMIT 1
)
SELECT
    REPLACE(CAST(p.bbl AS VARCHAR), '.00000000', '') AS twin_bbl,
    p.address, p.borough, p.postcode,
    TRY_CAST(p.yearbuilt AS INT) AS yearbuilt,
    TRY_CAST(p.numfloors AS DOUBLE) AS numfloors,
    TRY_CAST(p.unitsres AS INT) AS unitsres,
    p.bldgclass,
    TRY_CAST(p.assesstot AS DOUBLE) AS assesstot,
    TRY_CAST(p.bldgarea AS INT) AS bldgarea,
    ABS(TRY_CAST(p.yearbuilt AS INT) - TRY_CAST(t.yearbuilt AS INT)) AS year_diff,
    ABS(TRY_CAST(p.numfloors AS DOUBLE) - TRY_CAST(t.numfloors AS DOUBLE)) AS floor_diff,
    ABS(TRY_CAST(p.unitsres AS INT) - TRY_CAST(t.unitsres AS INT)) AS unit_diff
FROM lake.city_government.pluto p
CROSS JOIN target t
WHERE p.bldgclass = t.bldgclass
  AND REPLACE(CAST(p.bbl AS VARCHAR), '.00000000', '') != ?
  AND p.borough != t.borough
  AND TRY_CAST(p.yearbuilt AS INT) BETWEEN TRY_CAST(t.yearbuilt AS INT) - 5 AND TRY_CAST(t.yearbuilt AS INT) + 5
  AND TRY_CAST(p.numfloors AS DOUBLE) BETWEEN TRY_CAST(t.numfloors AS DOUBLE) - 2 AND TRY_CAST(t.numfloors AS DOUBLE) + 2
  AND TRY_CAST(p.yearbuilt AS INT) > 1800
ORDER BY (year_diff + floor_diff * 2 + unit_diff * 0.5) ASC
LIMIT 5
"""

TWINS_VIOLATIONS_SQL = """
SELECT
    (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl,
    COUNT(*) AS total_violations,
    COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_violations
FROM lake.housing.hpd_violations
WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) IN ({placeholders})
GROUP BY 1
"""

TWINS_COUNT_SQL = """
SELECT COUNT(*) AS total_similar
FROM lake.city_government.pluto p
WHERE bldgclass = ?
  AND TRY_CAST(yearbuilt AS INT) BETWEEN ? AND ?
  AND TRY_CAST(yearbuilt AS INT) > 1800
"""


@mcp.tool(annotations=READONLY, tags={"building"})
def nyc_twins(bbl: BBL, ctx: Context) -> ToolResult:
    """Find statistically similar buildings (twins) across NYC — same era, same building class, similar size — and compare violation rates and assessed values. Use this to benchmark one building against its peers in other boroughs. Parameters: bbl (10 digits). Example: 1000670001. For the building's own history, use building_story(bbl). For block-level trends, use block_timeline(bbl)."""

    t0 = time.time()
    db = ctx.lifespan_context["db"]

    # Get target building
    cols_t, rows_t = _safe_query(db, STORY_PLUTO_SQL, [bbl])
    if not rows_t:
        raise ToolError(f"No building found for BBL {bbl} in PLUTO.")

    target = dict(zip(cols_t, rows_t[0]))
    year = int(float(target.get("yearbuilt") or 0))
    floors = int(float(target.get("numfloors") or 0))
    units = int(float(target.get("unitsres") or 0))
    bldg_class = target.get("bldgclass") or "Unknown"
    address = target.get("address") or "Unknown"
    borough = target.get("borough") or ""
    assessed = float(target.get("assesstot") or 0)

    if year < 1800:
        raise ToolError(f"Building has no valid year built ({year}). Cannot find twins.")

    # Find twins in OTHER boroughs
    cols_tw, rows_tw = _safe_query(db, TWINS_FIND_SQL, [bbl, bbl])

    # Count total similar citywide
    cols_ct, rows_ct = _safe_query(db, TWINS_COUNT_SQL, [bldg_class, year - 5, year + 5])
    total_similar = 0
    if rows_ct:
        total_similar = int(dict(zip(cols_ct, rows_ct[0])).get("total_similar") or 0)

    lines = []
    lines.append(f"BUILDINGS LIKE YOURS")
    lines.append("=" * 60)
    lines.append(f"\nYOUR BUILDING: {address}, {borough}")
    lines.append(f"  Built {year} · {floors} floors · {units} units · Class {bldg_class}")
    lines.append(f"  Assessed: ${assessed:,.0f}")

    if total_similar:
        lines.append(f"\nThere are {total_similar:,} buildings in NYC with the same class ({bldg_class})")
        lines.append(f"built within 5 years of yours ({year-5}–{year+5}).")

    if not rows_tw:
        lines.append(f"\nNo close twins found in other boroughs with matching class and era.")
    else:
        # Get violation counts for twins
        twin_bbls = [dict(zip(cols_tw, r))["twin_bbl"] for r in rows_tw]
        all_bbls = [bbl] + twin_bbls
        viol_sql = _fill_placeholders(TWINS_VIOLATIONS_SQL, all_bbls)
        cols_v, rows_v = _safe_query(db, viol_sql, all_bbls)
        viol_map = {}
        if rows_v:
            for row in rows_v:
                v = dict(zip(cols_v, row))
                viol_map[v["bbl"]] = (int(v["total_violations"]), int(v["open_violations"]))

        my_viols = viol_map.get(bbl, (0, 0))
        lines.append(f"\n  Your violations: {my_viols[0]:,} total, {my_viols[1]:,} open")

        lines.append(f"\nTWINS IN OTHER BOROUGHS")
        lines.append("-" * 40)

        for row in rows_tw:
            tw = dict(zip(cols_tw, row))
            tw_bbl = tw["twin_bbl"]
            tw_viols = viol_map.get(tw_bbl, (0, 0))
            tw_assessed = float(tw.get("assesstot") or 0)
            val_diff = ""
            if assessed > 0 and tw_assessed > 0:
                ratio = tw_assessed / assessed
                if ratio > 1.2:
                    val_diff = f" ({ratio:.1f}x your assessed value)"
                elif ratio < 0.8:
                    val_diff = f" ({ratio:.1f}x your assessed value)"

            lines.append(f"\n  {tw.get('address')}, {tw.get('borough')} (ZIP {tw.get('postcode')})")
            lines.append(f"    Built {tw.get('yearbuilt')} · {int(float(tw.get('numfloors') or 0))} floors · {tw.get('unitsres')} units")
            lines.append(f"    Assessed: ${tw_assessed:,.0f}{val_diff}")
            lines.append(f"    Violations: {tw_viols[0]:,} total, {tw_viols[1]:,} open")

            # Compare
            if tw_viols[0] > my_viols[0] * 1.5 and my_viols[0] > 0:
                lines.append(f"    ↑ {tw_viols[0] / my_viols[0]:.1f}x more violations than yours")
            elif my_viols[0] > tw_viols[0] * 1.5 and tw_viols[0] > 0:
                lines.append(f"    ↓ {my_viols[0] / tw_viols[0]:.1f}x fewer violations than yours")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "bbl": bbl,
        "address": address,
        "borough": borough,
        "year_built": year,
        "building_class": bldg_class,
        "total_similar_citywide": total_similar,
        "twins_found": len(rows_tw) if rows_tw else 0,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"bbl": bbl, "query_time_ms": elapsed},
    )


# --- Block Timeline ---

BLOCK_PLUTO_SQL = """
SELECT REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') AS bbl,
       address, borough, postcode, TRY_CAST(yearbuilt AS INT) AS yearbuilt,
       TRY_CAST(numfloors AS DOUBLE) AS numfloors, bldgclass
FROM lake.city_government.pluto
WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
LIMIT 1
"""

BLOCK_NEIGHBORS_SQL = """
SELECT REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') AS bbl,
       address, TRY_CAST(yearbuilt AS INT) AS yearbuilt,
       TRY_CAST(numfloors AS DOUBLE) AS numfloors, bldgclass,
       TRY_CAST(unitsres AS INT) AS unitsres
FROM lake.city_government.pluto
WHERE SUBSTRING(REPLACE(CAST(bbl AS VARCHAR), '.00000000', ''), 1, 6) = ?
  AND TRY_CAST(yearbuilt AS INT) > 1800
ORDER BY address
LIMIT 50
"""

BLOCK_PERMITS_SQL = """
SELECT EXTRACT(YEAR FROM TRY_CAST(issuance_date AS DATE)) AS yr,
       job_type, permit_type, COUNT(*) AS cnt
FROM lake.housing.dob_permit_issuance
WHERE borough = ? AND block = ?
  AND TRY_CAST(issuance_date AS DATE) >= CURRENT_DATE - INTERVAL '10 years'
GROUP BY yr, job_type, permit_type
ORDER BY yr DESC, cnt DESC
"""

BLOCK_RESTAURANTS_SQL = """
WITH inspections AS (
    SELECT camis, dba, cuisine_description, grade, grade_date,
           building || ' ' || street AS addr,
           ROW_NUMBER() OVER (PARTITION BY camis ORDER BY grade_date DESC) AS rn
    FROM lake.health.restaurant_inspections
    WHERE zipcode = ? AND grade IN ('A','B','C')
)
SELECT camis, dba, cuisine_description, grade, grade_date, addr
FROM inspections WHERE rn = 1
ORDER BY grade_date DESC
LIMIT 30
"""

BLOCK_311_SQL = """
SELECT
    EXTRACT(YEAR FROM TRY_CAST(created_date AS DATE)) AS yr,
    complaint_type,
    COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ? AND bbl LIKE ?
GROUP BY yr, complaint_type
ORDER BY yr DESC, cnt DESC
"""

BLOCK_HPD_SQL = """
SELECT
    EXTRACT(YEAR FROM TRY_CAST(receiveddate AS DATE)) AS yr,
    majorcategory,
    COUNT(*) AS cnt
FROM lake.housing.hpd_complaints
WHERE boroid = ? AND block = ?
GROUP BY yr, majorcategory
ORDER BY yr DESC, cnt DESC
"""


@mcp.tool(annotations=READONLY, tags={"building"})
def block_timeline(bbl: BBL, ctx: Context) -> ToolResult:
    """A NYC block through time — all buildings, construction permits by year, restaurant scene, 311 complaint trends, and HPD complaint patterns over the last decade. Uses the block portion of the BBL to find all buildings on the block. Parameters: bbl (10 digits). Example: 1000670001. For a single building's story, use building_story(bbl). For neighborhood-wide trends, use neighborhood_portrait(zipcode)."""

    t0 = time.time()
    db = ctx.lifespan_context["db"]

    # Look up the target building
    cols_b, rows_b = _safe_query(db, BLOCK_PLUTO_SQL, [bbl])
    if not rows_b:
        raise ToolError(f"No building found for BBL {bbl}.")

    bldg = dict(zip(cols_b, rows_b[0]))
    address = bldg.get("address") or "Unknown"
    borough = bldg.get("borough") or ""
    zipcode = bldg.get("postcode") or ""
    boro_digit = bbl[0]
    block = str(int(bbl[1:6]))
    block_prefix = bbl[:6]  # borough + block for PLUTO matching

    lines = []
    lines.append(f"YOUR BLOCK THROUGH TIME")
    lines.append(f"Block {block}, {borough} (from {address})")
    lines.append("=" * 60)

    # --- All buildings on the block ---
    cols_n, rows_n = _safe_query(db, BLOCK_NEIGHBORS_SQL, [block_prefix])
    if rows_n:
        buildings = [dict(zip(cols_n, r)) for r in rows_n]
        years = [b["yearbuilt"] for b in buildings if b.get("yearbuilt")]
        lines.append(f"\nBUILDINGS ON YOUR BLOCK: {len(buildings)}")
        if years:
            lines.append(f"  Oldest: {min(years)} · Newest: {max(years)} · Span: {max(years) - min(years)} years")
        # Group by decade
        decades = {}
        for b in buildings:
            y = b.get("yearbuilt")
            if y:
                decade = (y // 10) * 10
                decades[decade] = decades.get(decade, 0) + 1
        if decades:
            lines.append(f"  Built by decade:")
            for d in sorted(decades):
                lines.append(f"    {d}s: {decades[d]} building{'s' if decades[d] > 1 else ''}")

        # List a few notable ones
        lines.append(f"\n  Notable addresses:")
        for b in buildings[:10]:
            y = b.get("yearbuilt") or "?"
            fl = int(float(b.get("numfloors") or 0))
            cls = b.get("bldgclass") or ""
            marker = " ← YOUR BUILDING" if b.get("bbl") == bbl else ""
            lines.append(f"    {b.get('address')}: {y}, {fl}fl, {cls}{marker}")

    # --- Construction permits (last 10 years) ---
    cols_p, rows_p = _safe_query(db, BLOCK_PERMITS_SQL, [boro_digit, block])
    if rows_p:
        lines.append(f"\nCONSTRUCTION ACTIVITY (last 10 years)")
        by_year = {}
        for row in rows_p:
            r = dict(zip(cols_p, row))
            yr = int(r.get("yr") or 0)
            if yr > 0:
                if yr not in by_year:
                    by_year[yr] = []
                by_year[yr].append((r.get("job_type") or "?", int(r.get("cnt") or 0)))

        for yr in sorted(by_year, reverse=True)[:8]:
            total = sum(c for _, c in by_year[yr])
            types = ", ".join(f"{t}: {c}" for t, c in by_year[yr][:3])
            lines.append(f"  {yr}: {total} permits ({types})")

        # Trend
        years_list = sorted(by_year.keys())
        if len(years_list) >= 3:
            recent = sum(sum(c for _, c in by_year[y]) for y in years_list[-3:]) / 3
            earlier = sum(sum(c for _, c in by_year[y]) for y in years_list[:3]) / 3
            if earlier > 0:
                change = (recent - earlier) / earlier * 100
                if change > 20:
                    lines.append(f"\n  Trend: construction activity up {change:.0f}% vs. early period")
                elif change < -20:
                    lines.append(f"\n  Trend: construction activity down {abs(change):.0f}% vs. early period")

    # --- Restaurant scene ---
    if zipcode:
        cols_r, rows_r = _safe_query(db, BLOCK_RESTAURANTS_SQL, [zipcode])
        if rows_r:
            restaurants = [dict(zip(cols_r, r)) for r in rows_r]
            cuisines = {}
            for rest in restaurants:
                c = rest.get("cuisine_description") or "Other"
                cuisines[c] = cuisines.get(c, 0) + 1

            lines.append(f"\nRESTAURANT SCENE (ZIP {zipcode})")
            lines.append(f"  {len(restaurants)} restaurants with recent grades")
            top_cuisines = sorted(cuisines.items(), key=lambda x: -x[1])[:5]
            lines.append(f"  Top cuisines: {', '.join(f'{c} ({n})' for c, n in top_cuisines)}")

    # --- 311 trends ---
    if zipcode:
        bbl_prefix = bbl[:6] + "%"  # match all lots on this block
        cols_311, rows_311 = _safe_query(db, BLOCK_311_SQL, [zipcode, bbl_prefix])
        if rows_311:
            lines.append(f"\n311 COMPLAINTS ON YOUR BLOCK (since 2020)")
            by_year_311 = {}
            for row in rows_311:
                r = dict(zip(cols_311, row))
                yr = int(r.get("yr") or 0)
                if yr > 0:
                    if yr not in by_year_311:
                        by_year_311[yr] = {}
                    by_year_311[yr][r.get("complaint_type")] = int(r.get("cnt") or 0)

            for yr in sorted(by_year_311, reverse=True)[:4]:
                total = sum(by_year_311[yr].values())
                top = sorted(by_year_311[yr].items(), key=lambda x: -x[1])[:3]
                top_str = ", ".join(f"{t}: {c}" for t, c in top)
                lines.append(f"  {yr}: {total} calls ({top_str})")

    # --- HPD complaint trends ---
    cols_h, rows_h = _safe_query(db, BLOCK_HPD_SQL, [boro_digit, block])
    if rows_h:
        lines.append(f"\nHPD COMPLAINTS ON YOUR BLOCK")
        by_year_hpd = {}
        for row in rows_h:
            r = dict(zip(cols_h, row))
            yr = int(r.get("yr") or 0)
            if yr >= 2018:
                if yr not in by_year_hpd:
                    by_year_hpd[yr] = {}
                by_year_hpd[yr][r.get("majorcategory")] = int(r.get("cnt") or 0)

        for yr in sorted(by_year_hpd, reverse=True)[:5]:
            total = sum(by_year_hpd[yr].values())
            top = sorted(by_year_hpd[yr].items(), key=lambda x: -x[1])[:2]
            top_str = ", ".join(f"{t}: {c}" for t, c in top)
            lines.append(f"  {yr}: {total} complaints ({top_str})")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "bbl": bbl,
        "address": address,
        "borough": borough,
        "block": block,
        "buildings_on_block": len(rows_n) if rows_n else 0,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"bbl": bbl, "query_time_ms": elapsed},
    )


# --- Building Context ---

_FAMOUS_BUILDINGS = [
    (1883, "Brooklyn Bridge"),
    (1885, "The Dakota"),
    (1902, "Flatiron Building"),
    (1903, "Williamsburg Bridge"),
    (1904, "New York Times Tower (original)"),
    (1910, "Penn Station (original)"),
    (1913, "Woolworth Building"),
    (1913, "Grand Central Terminal"),
    (1927, "Holland Tunnel"),
    (1930, "Chrysler Building"),
    (1931, "Empire State Building"),
    (1931, "George Washington Bridge"),
    (1932, "Radio City Music Hall"),
    (1937, "Lincoln Tunnel"),
    (1939, "Rockefeller Center"),
    (1950, "United Nations HQ"),
    (1959, "Guggenheim Museum"),
    (1962, "Lincoln Center"),
    (1964, "Verrazano-Narrows Bridge"),
    (1966, "Madison Square Garden (current)"),
    (1973, "World Trade Center (original)"),
    (1977, "Citicorp Center (now 601 Lex)"),
    (2004, "Time Warner Center"),
    (2014, "One World Trade Center"),
    (2019, "Hudson Yards"),
]

CONTEXT_ERA_BUILDINGS_SQL = """
SELECT COUNT(*) AS total_same_year,
       COUNT(DISTINCT postcode) AS zips_with_same_year
FROM lake.city_government.pluto
WHERE TRY_CAST(yearbuilt AS INT) = ?
"""

CONTEXT_ERA_BOROUGH_SQL = """
SELECT borough,
       COUNT(*) AS cnt
FROM lake.city_government.pluto
WHERE TRY_CAST(yearbuilt AS INT) = ?
GROUP BY borough ORDER BY cnt DESC
"""

CONTEXT_DECADE_SQL = """
SELECT
    COUNT(*) AS total_decade,
    AVG(TRY_CAST(numfloors AS DOUBLE)) AS avg_floors,
    COUNT(DISTINCT bldgclass) AS class_variety
FROM lake.city_government.pluto
WHERE TRY_CAST(yearbuilt AS INT) BETWEEN ? AND ?
"""

CONTEXT_ZIP_ERA_SQL = """
SELECT
    COUNT(*) AS buildings_in_zip,
    COUNT(*) FILTER (WHERE TRY_CAST(yearbuilt AS INT) = ?) AS same_year_in_zip,
    MIN(TRY_CAST(yearbuilt AS INT)) FILTER (WHERE TRY_CAST(yearbuilt AS INT) > 1800) AS oldest_in_zip,
    MAX(TRY_CAST(yearbuilt AS INT)) AS newest_in_zip
FROM lake.city_government.pluto
WHERE postcode = ?
"""


@mcp.tool(annotations=READONLY, tags={"building", "housing"})
def building_context(bbl: BBL, ctx: Context) -> ToolResult:
    """What was happening when a NYC building was born — famous building contemporaries, citywide construction activity that year, and historical era context. Differs from building_story (narrative history) and building_profile (quick stats). Use this for historical/architectural curiosity about a building's era. Parameters: bbl (10 digits). Example: 1000670001. For the building's full life narrative, use building_story(bbl)."""

    t0 = time.time()
    db = ctx.lifespan_context["db"]

    # Look up building
    cols_b, rows_b = _safe_query(db, STORY_PLUTO_SQL, [bbl])
    if not rows_b:
        raise ToolError(f"No building found for BBL {bbl} in PLUTO.")

    p = dict(zip(cols_b, rows_b[0]))
    year = int(float(p.get("yearbuilt") or 0))
    address = p.get("address") or "Unknown"
    borough = p.get("borough") or ""
    zipcode = p.get("postcode") or ""

    if year < 1800:
        raise ToolError(f"Building has no valid construction year. Cannot provide context.")

    # Era
    era_name, era_desc = "an unknown era", ""
    for yr_range, (name, desc) in _NYC_ERAS.items():
        if year in yr_range:
            era_name, era_desc = name, desc
            break

    lines = []
    lines.append(f"THE YEAR YOUR BUILDING WAS BORN: {year}")
    lines.append(f"{address}, {borough}")
    lines.append("=" * 60)

    lines.append(f"\n{era_name.upper()}")
    lines.append(f"Your building was constructed {era_desc}.")

    # --- Famous contemporaries ---
    contemporaries = [(y, name) for y, name in _FAMOUS_BUILDINGS if abs(y - year) <= 3]
    exact_match = [(y, name) for y, name in _FAMOUS_BUILDINGS if y == year]

    if exact_match:
        lines.append(f"\nFAMOUS CONTEMPORARIES (same year)")
        for y, name in exact_match:
            lines.append(f"  {name} ({y})")
    if contemporaries and not exact_match:
        lines.append(f"\nFAMOUS CONTEMPORARIES (within 3 years)")
        for y, name in contemporaries[:5]:
            lines.append(f"  {name} ({y})")

    # Closest famous building if none within 3 years
    if not contemporaries:
        closest = min(_FAMOUS_BUILDINGS, key=lambda x: abs(x[0] - year))
        diff = abs(closest[0] - year)
        direction = "after" if closest[0] > year else "before"
        lines.append(f"\nNEAREST FAMOUS CONTEMPORARY")
        lines.append(f"  {closest[1]} ({closest[0]}) — built {diff} years {direction}")

    # --- How many buildings that year ---
    cols_c, rows_c = _safe_query(db, CONTEXT_ERA_BUILDINGS_SQL, [year])
    if rows_c:
        c = dict(zip(cols_c, rows_c[0]))
        total = int(c.get("total_same_year") or 0)
        zips = int(c.get("zips_with_same_year") or 0)
        lines.append(f"\nCITYWIDE IN {year}")
        lines.append(f"  {total:,} buildings were constructed across {zips} ZIP codes")

    # Borough breakdown
    cols_boro, rows_boro = _safe_query(db, CONTEXT_ERA_BOROUGH_SQL, [year])
    if rows_boro:
        lines.append(f"  By borough:")
        boro_names = {"MN": "Manhattan", "BK": "Brooklyn", "BX": "Bronx", "QN": "Queens", "SI": "Staten Island"}
        for row in rows_boro:
            r = dict(zip(cols_boro, row))
            bname = boro_names.get(r.get("borough"), r.get("borough") or "?")
            lines.append(f"    {bname}: {int(r.get('cnt') or 0):,}")

    # --- The decade ---
    decade_start = (year // 10) * 10
    decade_end = decade_start + 9
    cols_d, rows_d = _safe_query(db, CONTEXT_DECADE_SQL, [decade_start, decade_end])
    if rows_d:
        d = dict(zip(cols_d, rows_d[0]))
        total_decade = int(d.get("total_decade") or 0)
        avg_fl = round(float(d.get("avg_floors") or 0), 1)
        lines.append(f"\nTHE {decade_start}s")
        lines.append(f"  {total_decade:,} buildings constructed citywide that decade")
        lines.append(f"  Average height: {avg_fl} floors")

    # --- Your neighborhood at the time ---
    if zipcode:
        cols_z, rows_z = _safe_query(db, CONTEXT_ZIP_ERA_SQL, [year, zipcode])
        if rows_z:
            z = dict(zip(cols_z, rows_z[0]))
            total_zip = int(z.get("buildings_in_zip") or 0)
            same_yr = int(z.get("same_year_in_zip") or 0)
            oldest = int(z.get("oldest_in_zip") or 0)
            newest = int(z.get("newest_in_zip") or 0)
            lines.append(f"\nYOUR NEIGHBORHOOD (ZIP {zipcode})")
            lines.append(f"  {total_zip:,} buildings exist in your ZIP today")
            if same_yr > 1:
                lines.append(f"  {same_yr} of them were built the same year as yours")
            elif same_yr == 1:
                lines.append(f"  Yours is the only one built in {year}")
            if oldest > 1800:
                lines.append(f"  Oldest building in ZIP: {oldest} ({year - oldest} years before yours)")
            if newest:
                lines.append(f"  Newest building in ZIP: {newest}")

    # --- What it means ---
    current_year = 2026
    age = current_year - year
    pct_older = 0
    if rows_c:
        total_city = int(dict(zip(cols_c, rows_c[0])).get("total_same_year") or 0)
        # Rough estimate: there are ~858K buildings in PLUTO
        # Buildings built before this year
    lines.append(f"\nPERSPECTIVE")
    lines.append(f"  Your building has been standing for {age} years.")
    milestones = [(y, e) for y, e in _NYC_MILESTONES if y >= year]
    lines.append(f"  It has witnessed {len(milestones)} defining moments in NYC history.")
    if year <= 1931:
        lines.append(f"  It predates the Empire State Building.")
    if year <= 1904:
        lines.append(f"  It predates the NYC subway system.")
    if year <= 1883:
        lines.append(f"  It predates the Brooklyn Bridge.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "bbl": bbl,
        "address": address,
        "borough": borough,
        "year_built": year,
        "era": era_name,
        "same_year_citywide": int(dict(zip(cols_c, rows_c[0])).get("total_same_year") or 0) if rows_c else 0,
        "famous_contemporaries": [name for _, name in contemporaries[:5]],
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"bbl": bbl, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Graph tools (DuckPGQ)
# ---------------------------------------------------------------------------


def _require_graph(ctx):
    if not ctx.lifespan_context.get("graph_ready"):
        raise ToolError("Property graph unavailable — DuckPGQ extension not loaded.")


@mcp.tool(annotations=READONLY, tags={"graph", "housing"})
def landlord_network(bbl: BBL, ctx: Context) -> ToolResult:
    """Discover the full ownership NETWORK around a building using DuckPGQ graph traversal. Finds the owner, ALL their other buildings, and aggregated violation stats across the portfolio. Use this for ownership graph analysis. For a simpler portfolio view, use landlord_watchdog(bbl). For hidden ownership clusters (WCC algorithm), use ownership_clusters(). For worst landlords ranking, use worst_landlords(). BBL is 10 digits: borough(1) + block(5) + lot(4). Example: 1000670001"""
    _require_graph(ctx)
    if not re.match(r"^\d{10}$", bbl):
        raise ToolError("BBL must be exactly 10 digits")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Traverse: Building <- Owns - Owner - Owns -> Building -> HasViolation -> Violation
    # Note: DuckPGQ MATCH does not support parameterized queries — bbl is regex-validated above
    cols, rows = _execute(db, f"""
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
        return ToolResult(content=f"BBL {bbl}: no ownership graph found (building may not be HPD-registered).")

    owner_id = rows[0][0]
    owner_name = rows[0][1] or f"Registration #{owner_id}"
    portfolio_bbls = list({r[2] for r in rows})

    # Get violation aggregates for the portfolio
    placeholders = ", ".join(["?"] * len(portfolio_bbls))
    v_cols, v_rows = _execute(db, f"""
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

    # Get building flags (AEP, litigations, tax liens, DOB violations)
    f_cols, f_rows = _execute(db, f"""
        SELECT bbl, is_aep, litigation_count, harassment_findings, lien_count, dob_violation_count
        FROM main.graph_building_flags
        WHERE bbl IN ({placeholders})
          AND (is_aep = 1 OR litigation_count > 0 OR lien_count > 0 OR dob_violation_count > 0)
    """, portfolio_bbls)
    flag_map = {r[0]: {"aep": r[1], "litigations": r[2], "harassment": r[3],
                       "liens": r[4], "dob": r[5]} for r in f_rows}

    # Get ACRIS sale data
    s_cols, s_rows = _execute(db, f"""
        SELECT bbl, last_sale_price, last_sale_date
        FROM main.graph_acris_sales
        WHERE bbl IN ({placeholders})
    """, portfolio_bbls)
    sale_map = {r[0]: {"price": r[1], "date": r[2]} for r in s_rows}

    # Get rent stabilization data
    rs_cols, rs_rows = _execute(db, f"""
        SELECT bbl, earliest_stab_units, latest_stab_units
        FROM main.graph_rent_stabilization
        WHERE bbl IN ({placeholders})
    """, portfolio_bbls)
    stab_map = {r[0]: {"earliest": r[1], "latest": r[2]} for r in rs_rows}

    # Get NYS corp contacts for this owner
    corp_cols, corp_rows = _execute(db, """
        SELECT dos_id, current_entity_name, dos_process_name,
               dos_process_address_1, dos_process_city,
               registered_agent_name, chairman_name
        FROM main.graph_corp_contacts
        WHERE owner_id = ?
    """, [owner_id])

    # Get businesses at portfolio buildings
    biz_cols, biz_rows = _execute(db, f"""
        SELECT bbl, business_name, business_category, license_status
        FROM main.graph_business_at_building
        WHERE bbl IN ({placeholders})
    """, portfolio_bbls)
    biz_map = {}
    for r in biz_rows:
        biz_map.setdefault(r[0], []).append({"name": r[1], "category": r[2], "status": r[3]})

    # Get ACRIS ownership chain (recent buyers/sellers)
    chain_cols, chain_rows = _execute(db, f"""
        SELECT bbl, party_name, role, amount, doc_date
        FROM main.graph_acris_chain
        WHERE bbl IN ({placeholders})
        ORDER BY doc_date DESC
    """, portfolio_bbls)

    # Get eviction records for portfolio buildings
    try:
        evict_cols, evict_rows = _execute(db, f"""
            SELECT bbl, COUNT(*) AS eviction_count,
                   MAX(executed_date) AS latest_eviction
            FROM main.graph_eviction_petitioners
            WHERE bbl IN ({placeholders})
            GROUP BY bbl
        """, portfolio_bbls)
        evict_map = {r[0]: {"count": r[1], "latest": r[2]} for r in evict_rows}
    except Exception:
        evict_map = {}

    elapsed = round((time.time() - t0) * 1000)

    # Build text output
    lines = [f"OWNERSHIP NETWORK for BBL {bbl}"]
    lines.append(f"Owner: {owner_name} (reg #{owner_id})")
    lines.append(f"Portfolio: {len(portfolio_bbls)} buildings")

    # Show corp contacts if found
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
    # Rent stabilization summary
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

    # Dedup buildings (multiple registration contacts can cause duplicates)
    seen_bbls = set()
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

    # Show businesses at portfolio buildings
    if biz_map:
        lines.append("\nBUSINESSES AT PORTFOLIO:")
        for b_bbl, businesses in list(biz_map.items())[:10]:
            for biz in businesses[:3]:
                lines.append(f"  {b_bbl} | {biz['name']} | {biz['category']} | {biz['status']}")

    # Show recent ownership chain
    if chain_rows:
        lines.append("\nOWNERSHIP CHAIN (recent deeds):")
        seen_chain = set()
        for r in chain_rows[:10]:
            key = (r[0], r[1], r[2])
            if key not in seen_chain:
                seen_chain.add(key)
                amt_str = f"${r[3]:,.0f}" if r[3] else "?"
                lines.append(f"  {r[0]} | {r[2]}: {r[1]} | {amt_str} | {r[4] or '?'}")

    lines.append(f"\n({elapsed}ms)")

    structured = {
        "owner_id": owner_id,
        "owner_name": owner_name,
        "query_bbl": bbl,
        "portfolio_size": len(portfolio_bbls),
        "total_violations": total_v,
        "total_open": total_open,
        "total_class_c": total_c,
        "total_litigations": total_litigations,
        "total_aep": total_aep,
        "total_tax_liens": total_liens,
        "total_evictions": total_evictions,
        "corp_contacts": [dict(zip(corp_cols, r)) for r in corp_rows] if corp_rows else [],
        "businesses": biz_map,
        "ownership_chain": [dict(zip(["bbl", "party_name", "role", "amount", "doc_date"], r[:5])) for r in chain_rows],
        "buildings": [
            {
                "bbl": r[2], "address": r[3], "zip": r[4],
                "total_units": r[5], "stories": r[6],
                **viol_map.get(r[2], {}),
                **{f"flag_{k}": v for k, v in flag_map.get(r[2], {}).items()},
                **{f"sale_{k}": v for k, v in sale_map.get(r[2], {}).items()},
                **{f"stab_{k}": v for k, v in stab_map.get(r[2], {}).items()},
            }
            for r in deduped_rows
        ],
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"owner_id": owner_id, "portfolio_size": len(portfolio_bbls), "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"graph"})
def ownership_graph(bbl: BBL, depth: Annotated[int, Field(description="Hops through ownership network (1-6). Default: 2", ge=1, le=6)] = 2, ctx: Context = None) -> ToolResult:
    """BFS graph traversal from one BBL through shared-owner links to find connected buildings at increasing depths. Depth 1 = buildings sharing a direct owner. Depth 2+ = indirect connections through shared ownership chains. Use this for ownership graph exploration from a specific building. Differs from ownership_clusters (WCC across all buildings) and ownership_cliques (clustering coefficient). Parameters: bbl (10 digits), depth (1-6, default 2). For portfolio-level analysis, use landlord_network(bbl)."""
    _require_graph(ctx)
    if not re.match(r"^\d{10}$", bbl):
        raise ToolError("BBL must be exactly 10 digits")
    depth = min(max(depth, 1), 6)  # clamp 1-6

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Use building network graph (same vertex type → supports shortest path properly)
    cols, rows = _execute(db, f"""
        FROM GRAPH_TABLE (nyc_building_network
            MATCH p = ANY SHORTEST
                (start:Building WHERE start.bbl = '{bbl}')
                -[e:SharedOwner]-{{1,{depth}}}
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

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content=f"BBL {bbl}: no connected buildings found within {depth} hops.")

    lines = [f"OWNERSHIP GRAPH from BBL {bbl} (max {depth} hops)"]
    lines.append(f"Connected buildings: {len(rows)}\n")

    for r in rows:
        lines.append(f"  {r[1]} | {r[2] or '?'} | {r[3] or '?'} | {r[4] or '?'} units | {r[5]} hop(s)")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"from_bbl": bbl, "depth": depth, "connected": [dict(zip(cols, r)) for r in rows]},
        meta={"connected_count": len(rows), "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"graph"})
def ownership_clusters(ctx: Context, min_buildings: Annotated[int, Field(description="Min buildings per cluster. Default: 5", ge=2, le=100)] = 5, borough: Annotated[str, Field(description="Borough filter. Empty for all boroughs")] = "") -> ToolResult:
    """Find hidden ownership networks using Weakly Connected Components (WCC) across ALL NYC buildings — groups of buildings linked through shared landlords, even across different corporate names. Differs from ownership_graph (BFS from one BBL) and ownership_cliques (clustering coefficient). Use this to discover large ownership empires. Parameters: min_buildings (default 5), borough (optional filter). For worst landlords ranked by violations, use worst_landlords()."""
    _require_graph(ctx)
    db = ctx.lifespan_context["db"]
    t0 = time.time()
    min_buildings = min(max(min_buildings, 2), 100)

    boro_map = {"manhattan": "1", "bronx": "2", "brooklyn": "3", "queens": "4", "staten island": "5"}
    boro_code = borough.strip()
    if boro_code.lower() in boro_map:
        boro_code = boro_map[boro_code.lower()]

    cols, rows = _execute(db, f"""
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
        {"WHERE b.boroid = '" + boro_code + "'" if boro_code in ('1','2','3','4','5') else ""}
        ORDER BY c.cluster_size DESC, c.componentid, b.bbl
        LIMIT 500
    """)

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content="No ownership clusters found matching criteria.")

    # Group by cluster
    clusters = {}
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


@mcp.tool(annotations=READONLY, tags={"graph"})
def ownership_cliques(ctx: Context, top_n: Annotated[int, Field(description="Number of results. Default: 20", ge=1, le=100)] = 20) -> ToolResult:
    """Find buildings at the center of tight-knit ownership cliques using Local Clustering Coefficient (LCC). High LCC means a building's co-owned neighbors are also co-owned with each other — indicating concentrated ownership (potential shell company networks or portfolio landlords). Differs from ownership_graph (BFS from one BBL) and ownership_clusters (WCC across all buildings). Parameters: top_n (default 20). For shell company detection, use shell_detector()."""
    _require_graph(ctx)
    db = ctx.lifespan_context["db"]
    t0 = time.time()
    top_n = min(max(top_n, 1), 100)

    cols, rows = _execute(db, f"""
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
        LIMIT {top_n}
    """)

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content="No ownership cliques found (all buildings have LCC = 0).")

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


@mcp.tool(annotations=READONLY, tags={"graph", "housing"})
def worst_landlords(ctx: Context, borough: Annotated[str, Field(description="Borough name or code 1-5. Empty for all boroughs")] = "", top_n: Annotated[int, Field(description="Number of results. Default: 25", ge=1, le=100)] = 25) -> ToolResult:
    """Rank the worst landlords in NYC by a composite slumlord score across violations, litigations, harassment findings, AEP buildings, tax liens, evictions, and complaints. Uses DuckPGQ ownership graph to aggregate across entire portfolios. Use this for 'who are the worst landlords' questions. Parameters: borough (optional filter), top_n (default 25). For a specific landlord's portfolio, use landlord_watchdog(bbl) or landlord_network(bbl)."""
    _require_graph(ctx)
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Map borough names to codes
    boro_map = {"manhattan": "1", "bronx": "2", "brooklyn": "3", "queens": "4", "staten island": "5"}
    boro_code = borough.strip()
    if boro_code.lower() in boro_map:
        boro_code = boro_map[boro_code.lower()]
    borough_filter = f"AND b.boroid = '{boro_code}'" if boro_code in ("1", "2", "3", "4", "5") else ""
    top_n = min(max(top_n, 1), 100)

    cols, rows = _execute(db, f"""
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
        LIMIT {top_n}
    """)

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content="No results found.")

    # IQR anomaly flagging
    outlier_owners = set()
    if rows:
        violations_list = [r[4] for r in rows if r[4]]  # violations column
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
        name = (r[1] or f"Reg#{r[0]}")[:28]
        outlier_tag = " [STATISTICAL OUTLIER]" if r[0] in outlier_owners else ""
        lines.append(f"{i:<5} {name:<30} {r[2]:<6} {r[3] or 0:<6} "
                     f"{r[4]:<7} {r[5]:<6} {r[6]:<6} {r[7]:<6} {r[8]:<6} "
                     f"{r[9]:<6} {r[10]:<7} {r[11]:<5} {r[12]}{outlier_tag}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"borough": borough.strip() or "all", "results": [dict(zip(cols, r)) for r in rows]},
        meta={"result_count": len(rows), "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"investigation"})
def llc_piercer(entity_name: NAME, ctx: Context) -> ToolResult:
    """Pierce the LLC veil — find the people and addresses behind a shell company or LLC using NYS Department of State corporation filings, ACRIS property transaction records, and HPD registration contacts. Use this for LLC piercing, corporate ownership, or shell company investigation. Parameters: entity_name (company or person name, min 3 chars). Example: 'GREENPOINT HOLDINGS LLC'. For corporate network graph traversal, use corporate_web(entity_name)."""
    if len(entity_name.strip()) < 3:
        raise ToolError("Entity name must be at least 3 characters")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    search_term = entity_name.strip().upper()

    extra_names = _vector_expand_names(ctx, search_term)

    # 1. NYS corporations — registered agents, chairmen, process contacts
    corp_cols, corp_rows = _execute(db, """
        SELECT current_entity_name, entity_type, initial_dos_filing_date,
               dos_process_name, dos_process_address_1, dos_process_city, dos_process_state,
               registered_agent_name, registered_agent_address_1,
               chairman_name, chairman_address_1, county
        FROM lake.business.nys_corporations
        WHERE UPPER(current_entity_name) LIKE ?
        LIMIT 20
    """, [f"%{search_term}%"])

    # 2. ACRIS parties — buyers/sellers on property transactions
    acris_cols, acris_rows = _execute(db, """
        SELECT p.name, p.party_type, p.address_1, p.city, p.state, p.zip,
               m.doc_type, TRY_CAST(m.document_amt AS DOUBLE) AS amount,
               TRY_CAST(m.document_date AS DATE) AS doc_date,
               m.document_id
        FROM lake.housing.acris_parties p
        JOIN lake.housing.acris_master m ON p.document_id = m.document_id
        WHERE UPPER(p.name) LIKE ?
        ORDER BY m.document_date DESC
        LIMIT 30
    """, [f"%{search_term}%"])

    # 3. HPD registration contacts — who's registered as owner
    hpd_cols, hpd_rows = _execute(db, """
        SELECT registrationid, type, corporationname, firstname, lastname,
               businesshousenumber, businessstreetname, businesszip
        FROM lake.housing.hpd_registration_contacts
        WHERE UPPER(corporationname) LIKE ?
           OR UPPER(firstname || ' ' || lastname) LIKE ?
        LIMIT 20
    """, [f"%{search_term}%", f"%{search_term}%"])

    elapsed = round((time.time() - t0) * 1000)

    if not corp_rows and not acris_rows and not hpd_rows:
        return ToolResult(content=f"No records found for '{entity_name}' across NYS corps, ACRIS, or HPD.")

    lines = [f"LLC PIERCER — '{entity_name}'\n"]

    if corp_rows:
        lines.append(f"--- NYS CORPORATIONS ({len(corp_rows)} matches) ---")
        for r in corp_rows:
            lines.append(f"  Entity: {r[0]} ({r[1] or '?'}) filed {r[2] or '?'}")
            people = []
            if r[3]:
                people.append(f"Process: {r[3]}, {r[4] or ''} {r[5] or ''} {r[6] or ''}")
            if r[7]:
                people.append(f"Agent: {r[7]}, {r[8] or ''}")
            if r[9]:
                people.append(f"Chairman: {r[9]}, {r[10] or ''}")
            for p in people:
                lines.append(f"    {p}")
        lines.append("")

    if acris_rows:
        lines.append(f"--- ACRIS TRANSACTIONS ({len(acris_rows)} matches) ---")
        for r in acris_rows:
            party_label = "BUYER" if str(r[1]) == "2" else "SELLER" if str(r[1]) == "1" else f"TYPE-{r[1]}"
            amt_str = f"${r[7]:,.0f}" if r[7] else "?"
            lines.append(f"  {party_label}: {r[0]} | {r[6] or '?'} | {amt_str} | {r[8] or '?'}")
            if r[2]:
                lines.append(f"    Address: {r[2]}, {r[3] or ''} {r[4] or ''} {r[5] or ''}")
        lines.append("")

    if hpd_rows:
        lines.append(f"--- HPD REGISTRATIONS ({len(hpd_rows)} matches) ---")
        for r in hpd_rows:
            name = r[2] or f"{r[3] or ''} {r[4] or ''}".strip()
            addr = f"{r[5] or ''} {r[6] or ''} {r[7] or ''}".strip()
            lines.append(f"  Reg#{r[0]} | {r[1]} | {name} | {addr}")
        lines.append("")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")

    lines.append(f"({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "entity_name": entity_name,
            "nys_corps": [dict(zip(corp_cols, r)) for r in corp_rows],
            "acris_transactions": [dict(zip(acris_cols, r)) for r in acris_rows],
            "hpd_registrations": [dict(zip(hpd_cols, r)) for r in hpd_rows],
        },
        meta={"total_matches": len(corp_rows) + len(acris_rows) + len(hpd_rows), "query_time_ms": elapsed},
    )


def _resolve_name_variants(db, name: str) -> list[tuple[str, str]]:
    """Look up Splink resolved_entities to find all name variants in the same cluster.

    Parses the input name into words and tries both word orderings as
    (last_name, first_name). Returns a list of (last_name, first_name) tuples
    for all records sharing a cluster_id with the input name.
    Returns empty list if no cluster found or table doesn't exist.
    """
    try:
        search = name.strip().upper()
        words = [w for w in search.split() if len(w) >= 2]
        if not words:
            return []

        # Build candidate (last_name, first_name) pairs to search
        candidates = []
        if len(words) >= 2:
            # Try both orderings: "Barton Perlbinder" → (PERLBINDER, BARTON) and (BARTON, PERLBINDER)
            candidates.append((words[-1], words[0]))
            candidates.append((words[0], words[-1]))
        else:
            # Single word — try as last name only
            candidates.append((words[0], ""))

        # Find cluster_ids for any candidate match
        cluster_ids = set()
        for last, first in candidates:
            if first:
                cols, rows = _execute(db, """
                    SELECT DISTINCT cluster_id
                    FROM lake.federal.resolved_entities
                    WHERE last_name = ? AND first_name = ?
                    LIMIT 5
                """, [last, first])
            else:
                cols, rows = _execute(db, """
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

        # Get all name variants in these clusters
        placeholders = ", ".join(["?"] * len(cluster_ids))
        cols, rows = _execute(db, f"""
            SELECT DISTINCT last_name, first_name
            FROM lake.federal.resolved_entities
            WHERE cluster_id IN ({placeholders})
              AND last_name IS NOT NULL AND first_name IS NOT NULL
            LIMIT 20
        """, list(cluster_ids))

        return [(r[0], r[1]) for r in rows]

    except Exception:
        # Table doesn't exist or query failed — graceful fallback
        return []


@mcp.tool(annotations=READONLY, tags={"investigation"})
def entity_xray(name: NAME, ctx: Context) -> ToolResult:
    """Full X-ray of a person or entity across ALL NYC datasets — corporations, businesses, restaurants, campaign donations, expenditures, OATH hearings, ACRIS transactions, property, DOB permits, payroll, marriage records, attorney registrations, and more. USE THIS when someone asks about a person or company by name. Handles name variations — 'Barton Perlbinder' matches 'PERLBINDER, BARTON M'. For probabilistic cross-referencing with Splink matching, use person_crossref(name). For political influence specifically, use pay_to_play(name)."""
    if len(name.strip()) < 3:
        raise ToolError("Name must be at least 3 characters")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    search = name.strip().upper()
    # Split into words for cross-column matching (handles "BARTON PERLBINDER"
    # matching "Perlbinder, Barton M" or last_name=PERLBINDER first_name=BARTON)
    words = [w for w in search.split() if len(w) >= 2]

    extra_names = _vector_expand_names(ctx, search)

    # Splink probabilistic name resolution — find all name variants in the same cluster
    name_variants = _resolve_name_variants(db, name)

    # 1. NYS corps — find ALL entities this person/name is associated with
    corp_cols, corp_rows = _execute(db, """
        SELECT current_entity_name, entity_type, initial_dos_filing_date,
               dos_process_name, registered_agent_name, chairman_name, county
        FROM lake.business.nys_corporations
        WHERE UPPER(current_entity_name) LIKE ?
           OR UPPER(dos_process_name) LIKE ?
           OR UPPER(registered_agent_name) LIKE ?
           OR UPPER(chairman_name) LIKE ?
        LIMIT 30
    """, [f"%{search}%"] * 4)

    # 2. Business licenses
    biz_cols, biz_rows = _execute(db, """
        SELECT business_name, dba_trade_name, business_category,
               license_type, license_status, bbl,
               address_building || ' ' || address_street_name AS address,
               address_zip
        FROM lake.business.issued_licenses
        WHERE UPPER(business_name) LIKE ? OR UPPER(dba_trade_name) LIKE ?
        LIMIT 20
    """, [f"%{search}%"] * 2)

    # 3. Restaurant inspections
    rest_cols, rest_rows = _execute(db, """
        SELECT DISTINCT dba, cuisine_description, building || ' ' || street AS address,
               boro, zipcode, grade, bbl
        FROM lake.health.restaurant_inspections
        WHERE UPPER(dba) LIKE ?
        LIMIT 20
    """, [f"%{search}%"])

    # 4. Campaign contributions — search full string AND individual words
    #    to catch "Perlbinder, Barton M" when user searches "Barton Perlbinder"
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
    camp_cols, camp_rows = _execute(db, camp_sql, camp_params)

    # 5. OATH hearings — search both first and last name with word splitting
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
        # Full-string match on last name, full-string match on concat, then
        # each word tried as (last LIKE word AND first LIKE other_word)
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
    oath_cols, oath_rows = _execute(db, oath_sql, oath_params)

    # 6. ACRIS property transactions — who's buying/selling real estate
    acris_cols, acris_rows = _execute(db, """
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
    """, [f"%{search}%"])

    # 7. PLUTO — property portfolio by owner name (assessed value, zoning, units)
    pluto_cols, pluto_rows = _execute(db, """
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
    """, [f"%{search}%"])

    # 8. Campaign expenditures — where politicians spend (vendors, consultants)
    expend_cols, expend_rows = _execute(db, """
        SELECT name, candlast || ', ' || candfirst AS candidate,
               TRY_CAST(amnt AS DOUBLE) AS amount,
               purpose, explain, date, city
        FROM lake.city_government.campaign_expenditures
        WHERE UPPER(name) LIKE ?
        ORDER BY TRY_CAST(amnt AS DOUBLE) DESC NULLS LAST
        LIMIT 20
    """, [f"%{search}%"])

    # 9. DOB permits — building permits with owner name
    if len(words) > 1:
        dob_word_clauses = " AND ".join(
            [f"(UPPER(owner_s_business_name || ' ' || COALESCE(owner_s_first_name,'') || ' ' || COALESCE(owner_s_last_name,'')) LIKE ?)"] * 1
        )
        dob_sql = f"""
            SELECT owner_s_business_name, owner_s_first_name, owner_s_last_name,
                   permit_type, job_type, issuance_date,
                   house || ' ' || street_name AS address,
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
                   house || ' ' || street_name AS address,
                   (borough || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
            FROM lake.housing.dob_permit_issuance
            WHERE UPPER(owner_s_business_name) LIKE ?
               OR UPPER(owner_s_last_name) LIKE ?
            ORDER BY issuance_date DESC
            LIMIT 15
        """
        dob_params = [f"%{search}%"] * 2
    dob_cols, dob_rows = _execute(db, dob_sql, dob_params)

    # 10. DCWP consumer protection charges
    dcwp_cols, dcwp_rows = _execute(db, """
        SELECT business_name, dba_trade_name, business_category,
               charge, violation_date, outcome, charge_count
        FROM lake.business.dcwp_charges
        WHERE UPPER(business_name) LIKE ? OR UPPER(dba_trade_name) LIKE ?
        ORDER BY violation_date DESC
        LIMIT 15
    """, [f"%{search}%"] * 2)

    # 11. SBS M/WBE certified businesses
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
    sbs_cols, sbs_rows = _execute(db, sbs_sql, sbs_params)

    # 12. Citywide payroll — city employees
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
    payroll_cols, payroll_rows = _execute(db, payroll_sql, payroll_params)

    # 13. DOB Application Owners — who's on permit applications (different from HPD owner)
    try:
        dob_app_cols, dob_app_rows = _execute(db, """
            SELECT owner_name, business_name,
                   owner_address, owner_city, owner_state, owner_zip
            FROM main.graph_dob_owners
            WHERE UPPER(owner_name) LIKE ? OR UPPER(business_name) LIKE ?
            LIMIT 15
        """, [f"%{search}%"] * 2)
    except Exception:
        dob_app_cols, dob_app_rows = [], []

    # 14. Doing Business disclosures — city contractor principals
    try:
        doing_biz_cols, doing_biz_rows = _execute(db, """
            SELECT entity_name, person_name, title, transaction_type,
                   entity_address, entity_city, entity_state
            FROM main.graph_doing_business
            WHERE UPPER(entity_name) LIKE ? OR UPPER(person_name) LIKE ?
            LIMIT 15
        """, [f"%{search}%"] * 2)
    except Exception:
        doing_biz_cols, doing_biz_rows = [], []

    # 15. EPA ECHO facilities — environmental compliance
    try:
        epa_cols, epa_rows = _execute(db, """
            SELECT facility_name, address, city, zip, county,
                   current_violation, total_penalties, inspection_count,
                   formal_action_count, last_penalty_amount
            FROM main.graph_epa_facilities
            WHERE UPPER(facility_name) LIKE ?
            LIMIT 15
        """, [f"%{search}%"])
    except Exception:
        epa_cols, epa_rows = [], []

    # 17. NYS Attorney Registrations — bar admissions
    try:
        atty_cols, atty_rows = _execute(db, """
            SELECT first_name, last_name, registration_number, law_school,
                   company_name, status, year_admitted, city, state
            FROM lake.financial.nys_attorney_registrations
            WHERE UPPER(last_name) LIKE ? OR UPPER(company_name) LIKE ?
            LIMIT 20
        """, [f"%{search}%"] * 2)
    except Exception:
        atty_cols, atty_rows = [], []

    # 18. ACRIS Personal Property (UCC filings)
    try:
        pp_cols, pp_rows = _execute(db, """
            SELECT name, party_type, document_id, address_1, city, state, zip
            FROM lake.business.acris_pp_parties
            WHERE UPPER(name) LIKE ?
            LIMIT 20
        """, [f"%{search}%"])
    except Exception:
        pp_cols, pp_rows = [], []

    # 19. Civil Service Active Lists — city exam results
    try:
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
        civil_cols, civil_rows = _execute(db, civil_sql, civil_params)
    except Exception:
        civil_cols, civil_rows = [], []

    # 20. NYS Lobbyist Registration
    try:
        lobby_cols, lobby_rows = _execute(db, """
            SELECT principal_lobbyist_name, contractual_client_name,
                   lobbying_subjects, compensation_amount, reporting_year,
                   level_of_government, individual_lobbyist_s
            FROM lake.city_government.nys_lobbyist_registration
            WHERE UPPER(principal_lobbyist_name) LIKE ?
               OR UPPER(contractual_client_name) LIKE ?
               OR UPPER(individual_lobbyist_s) LIKE ?
            LIMIT 20
        """, [f"%{search}%"] * 3)
    except Exception:
        lobby_cols, lobby_rows = [], []

    # 21. NYS Death Index
    try:
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
        death_cols, death_rows = _execute(db, death_sql, death_params)
    except Exception:
        death_cols, death_rows = [], []

    # 22. NYS Real Estate Brokers
    try:
        broker_cols, broker_rows = _execute(db, """
            SELECT license_holder_name, business_name, license_type,
                   license_number, license_expiration_date, county,
                   business_city, business_state
            FROM lake.financial.nys_re_brokers
            WHERE UPPER(license_holder_name) LIKE ? OR UPPER(business_name) LIKE ?
            LIMIT 20
        """, [f"%{search}%"] * 2)
    except Exception:
        broker_cols, broker_rows = [], []

    # 23. NYS Notaries
    try:
        notary_cols, notary_rows = _execute(db, """
            SELECT commission_holder_name, commissioned_county,
                   commission_type_traditional_or_electronic,
                   term_issue_date, term_expiration_date,
                   business_name_if_available
            FROM lake.financial.nys_notaries
            WHERE UPPER(commission_holder_name) LIKE ?
               OR UPPER(business_name_if_available) LIKE ?
            LIMIT 20
        """, [f"%{search}%"] * 2)
    except Exception:
        notary_cols, notary_rows = [], []

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
            lines.append(f"  {r[0]}{dba} | {r[2] or '?'} | {r[3]} | {r[4]} | {r[6] or '?'} {r[7] or ''}")
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
        total_penalties = sum(float(str(r[6] or 0).replace('$', '').replace(',', '') or 0) for r in epa_rows)
        lines.append(f"--- EPA ECHO FACILITIES ({len(epa_rows)} facilities, ${total_penalties:,.0f} total penalties) ---")
        for r in epa_rows:
            viol = "VIOLATION" if r[5] and r[5] == "Y" else "compliant"
            pen_str = f"${float(str(r[9] or 0).replace('$', '').replace(',', '') or 0):,.0f}" if r[9] else "$0"
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
            comp = f"${float(str(r[3] or 0).replace('$', '').replace(',', '') or 0):,.0f}" if r[3] else "?"
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

    # 16. Marriage records (1866-2017) — confirms family relationships
    marriage_cols, marriage_rows = [], []
    try:
        if len(words) > 1:
            mar_where = " OR ".join([
                f"(UPPER(groom_surname) = '{words[-1]}' AND UPPER(groom_first_name) LIKE '{words[0]}%')",
                f"(UPPER(bride_surname) = '{words[-1]}' AND UPPER(bride_first_name) LIKE '{words[0]}%')",
                f"(UPPER(groom_surname) = '{words[0]}' AND UPPER(groom_first_name) LIKE '{words[-1]}%')",
                f"(UPPER(bride_surname) = '{words[0]}' AND UPPER(bride_first_name) LIKE '{words[-1]}%')",
            ])
        else:
            mar_where = f"UPPER(groom_surname) = '{search}' OR UPPER(bride_surname) = '{search}'"
        marriage_cols, marriage_rows = _execute(db, f"""
            SELECT groom_first_name, groom_middle_name, groom_surname,
                   bride_first_name, bride_middle_name, bride_surname,
                   LICENSE_BOROUGH_ID AS license_borough, LICENSE_YEAR AS license_year
            FROM lake.city_government.marriage_licenses_1950_2017
            WHERE {mar_where}
            ORDER BY license_year
            LIMIT 20
        """)
    except Exception:
        pass

    # 16b. Marriage certificates 1866-1937
    hist_marriage_rows = []
    try:
        if len(words) > 1:
            _, hist_marriage_rows = _execute(db, f"""
                SELECT first_name, NULL, last_name, NULL, NULL, NULL, county, year
                FROM lake.city_government.marriage_certificates_1866_1937
                WHERE (UPPER(last_name) = '{words[-1]}' AND UPPER(first_name) LIKE '{words[0]}%')
                   OR (UPPER(last_name) = '{words[0]}' AND UPPER(first_name) LIKE '{words[-1]}%')
                ORDER BY year
                LIMIT 10
            """)
        else:
            _, hist_marriage_rows = _execute(db, f"""
                SELECT first_name, NULL, last_name, NULL, NULL, NULL, county, year
                FROM lake.city_government.marriage_certificates_1866_1937
                WHERE UPPER(last_name) = '{search}'
                ORDER BY year
                LIMIT 10
            """)
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

    # === SPLINK CLUSTER: Probabilistic name matches from resolved_entities ===
    splink_cluster_rows = []
    if name_variants:
        try:
            # Find the cluster_id(s) for the searched name
            variant_last = name_variants[0][0]
            variant_first = name_variants[0][1]
            # Use the first variant to find cluster_ids, then get all records
            _, cid_rows = _execute(db, """
                SELECT DISTINCT cluster_id
                FROM lake.federal.resolved_entities
                WHERE last_name = ? AND first_name = ?
                LIMIT 5
            """, [variant_last, variant_first])
            cluster_ids = [r[0] for r in cid_rows if r[0] is not None]

            if cluster_ids:
                placeholders = ", ".join(["?"] * len(cluster_ids))
                splink_cols, splink_cluster_rows = _execute(db, f"""
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
        # Group by source table for readable output
        variant_names = sorted({f"{r[1] or ''} {r[2] or ''}".strip() for r in splink_cluster_rows})
        lines.append(f"--- SPLINK CLUSTER ({len(splink_cluster_rows)} records, {len(variant_names)} name variants) ---")
        lines.append(f"  Name variants: {', '.join(variant_names[:10])}")
        current_table = None
        for r in splink_cluster_rows:
            table = r[0] or "unknown"
            if table != current_table:
                current_table = table
                lines.append(f"  [{table}]")
            loc = f"{r[3] or ''} {r[4] or ''}".strip() or "?"
            lines.append(f"    {r[1] or ''}, {r[2] or ''} | {loc} | cluster={r[5]}")
        lines.append("")

    # Phonetic cross-reference (catches typos, spelling variants)
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
        ph_cols, ph_rows = _execute(db, phonetic_sql, phonetic_params)
        phonetic_matches = [dict(zip(ph_cols, r)) for r in ph_rows]
    except Exception:
        pass  # Phonetic index may not exist yet

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
        lines.append("Run entity_xray(name) on any of these for their full X-ray.")

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
                                "city": r[3], "zip": r[4], "cluster_id": r[5]}
                               for r in splink_cluster_rows],
            "name_variants": [{"last_name": ln, "first_name": fn} for ln, fn in name_variants],
            "phonetic_matches": phonetic_matches,
        },
        meta={"total_matches": total, "query_time_ms": elapsed},
    )


VALID_ENTITY_SOURCES = frozenset({"owner", "corp_officer", "donor", "tx_party"})


@mcp.tool(annotations=READONLY, tags={"entity"})
def fuzzy_entity_search(
    name: NAME,
    ctx: Context,
    source: Annotated[str, Field(description="Filter by source: 'owner', 'corp_officer', 'donor', 'tx_party', or 'all'")] = "all",
    limit: Annotated[int, Field(description="Max results (1-50). Default: 20", ge=1, le=50)] = 20,
) -> ToolResult:
    """Find entities with similar names using vector similarity. Catches variations that exact search misses: 'Bob Smith' finds 'Robert Smith', 'R. Smith', 'Smith, Robert J.' Use this when entity_xray returns too few results. Sources: property owners, corporate officers, campaign donors, ACRIS transaction parties. Follow up with entity_xray(name) for full X-ray."""
    if source != "all" and source not in VALID_ENTITY_SOURCES:
        raise ToolError(
            f"Invalid source '{source}'. Must be one of: {', '.join(sorted(VALID_ENTITY_SOURCES))}, or 'all'"
        )

    embed_fn = ctx.lifespan_context.get("embed_fn")
    if embed_fn is None:
        raise ToolError(
            "Fuzzy entity search is unavailable — embedding model not loaded. Use entity_xray() for exact name matching instead."
        )

    from embedder import vec_to_sql

    t0 = time.time()
    vec = embed_fn(name.strip())
    vec_literal = vec_to_sql(vec)

    source_filter = f"WHERE source = '{source}'" if source != "all" else ""

    sql = f"""
        SELECT source, name,
               max(0, 1.0 / (1.0 + _distance)) AS similarity
        FROM lance_vector_search('{LANCE_DIR}/entity_name_embeddings.lance', 'embedding', {vec_literal}, k={limit})
        {source_filter}
        ORDER BY _distance ASC
    """

    with _db_lock:
        try:
            db = ctx.lifespan_context["db"]
            cur = db.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        except Exception as e:
            raise ToolError(f"Fuzzy entity search failed: {e}")

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(
            content=f"No similar entities found for '{name}'. Try a shorter name or check spelling.",
            meta={"query_time_ms": elapsed},
        )

    lines = [f"Top {len(rows)} similar entities for '{name}' ({elapsed}ms)\n"]
    for i, row in enumerate(rows, 1):
        rec = dict(zip(cols, row))
        pct = round(rec["similarity"] * 100, 1)
        lines.append(f"{i}. [{rec['source']}] {rec['name']} — {pct}% similarity")

    lines.append(
        "\nUse entity_xray(name) to get the full X-ray for any matched entity."
    )

    content = "\n".join(lines)
    structured = [dict(zip(cols, row)) for row in rows]

    return ToolResult(
        content=content,
        structured_content=structured,
        meta={"query_time_ms": elapsed, "result_count": len(rows), "source_filter": source},
    )


@mcp.tool(annotations=READONLY, tags={"investigation"})
def marriage_search(surname: Annotated[str, Field(description="Last name to search. Matches both bride and groom. Example: 'SMITH'")], first_name: Annotated[str, Field(description="Optional first name filter. Example: 'JOHN'")] = "", ctx: Context = None) -> ToolResult:
    """Search NYC marriage records (1866-2017, ~10M records) by bride or groom surname. Combines marriage certificates 1866-1937 and marriage index 1950-2017. Use this to find marriage record matches, confirm family relationships, or trace wedding and family history. Parameters: surname (required), first_name (optional filter). Example: marriage_search("PERLBINDER", "BARTON"). For broader person search, use entity_xray(name) or person_crossref(name)."""
    if len(surname.strip()) < 2:
        raise ToolError("Surname must be at least 2 characters")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    search_last = surname.strip().upper()
    search_first = first_name.strip().upper() if first_name else ""

    extra_names = _vector_expand_names(ctx, f"{search_first} {search_last}".strip())

    # Phonetic search for spelling variants
    phonetic_results = []
    try:
        from entity import phonetic_vital_search_sql
        ph_sql, ph_params = phonetic_vital_search_sql(
            first_name=search_first or None, last_name=search_last,
            table="lake.city_government.marriage_licenses_1950_2017",
            first_col="groom_first_name", last_col="groom_surname",
            extra_cols="bride_first_name, bride_surname, LICENSE_BOROUGH_ID, LICENSE_YEAR",
            limit=30)
        _, ph_rows = _execute(db, ph_sql, ph_params)
        if ph_rows:
            phonetic_results = list(ph_rows)
    except Exception:
        pass

    # Source 1: Marriage index parquet (1950-2017)
    if search_first:
        cols, rows = _execute(db, """
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
        cols, rows = _execute(db, """
            SELECT groom_first_name, groom_middle_name, groom_surname,
                   bride_first_name, bride_middle_name, bride_surname,
                   LICENSE_BOROUGH_ID AS license_borough, LICENSE_YEAR AS license_year
            FROM lake.city_government.marriage_licenses_1950_2017
            WHERE UPPER(groom_surname) = ? OR UPPER(bride_surname) = ?
            ORDER BY license_year
            LIMIT 50
        """, [search_last, search_last])

    # Source 2: Marriage certificates 1866-1937
    # Schema: first_name, last_name, county, number, year, month, day
    # These are individual records (one per person), not paired bride+groom
    try:
        if search_first:
            _, hist_rows = _execute(db, """
                SELECT first_name, NULL AS middle_name, last_name,
                       NULL AS spouse_first, NULL AS spouse_middle, NULL AS spouse_last,
                       county, year
                FROM lake.city_government.marriage_certificates_1866_1937
                WHERE UPPER(last_name) = ? AND UPPER(first_name) LIKE ?
                ORDER BY year
                LIMIT 30
            """, [search_last, f"{search_first}%"])
        else:
            _, hist_rows = _execute(db, """
                SELECT first_name, NULL AS middle_name, last_name,
                       NULL AS spouse_first, NULL AS spouse_middle, NULL AS spouse_last,
                       county, year
                FROM lake.city_government.marriage_certificates_1866_1937
                WHERE UPPER(last_name) = ?
                ORDER BY year
                LIMIT 30
            """, [search_last])
    except Exception:
        hist_rows = []

    # Merge all results and sort by year
    all_rows = list(rows) + hist_rows
    all_rows.sort(key=lambda r: int(r[7] or 0) if r[7] else 0)

    elapsed = int((time.time() - t0) * 1000)
    lines = [f"NYC Marriage Records (1866-2017) — '{surname}' matches: {len(all_rows)}", ""]

    if hist_rows:
        lines.append(f"  [{len(hist_rows)} from certificates 1866-1937, {len(rows)} from index 1950-2017]")
        lines.append("")

    for r in all_rows:
        groom = f"{r[0] or ''} {r[1] or ''} {r[2] or ''}".strip()
        bride = f"{r[3] or ''} {r[4] or ''} {r[5] or ''}".strip()
        if bride:
            lines.append(f"  {r[7] or '?'} | {r[6] or '?'} | {groom}  ×  {bride}")
        else:
            lines.append(f"  {r[7] or '?'} | {r[6] or '?'} | {groom} (certificate)")

    if phonetic_results:
        lines.append(f"\nPHONETIC MATCHES ({len(phonetic_results)} — spelling variants):")
        for r in phonetic_results[:10]:
            lines.append(f"  {' | '.join(str(v) for v in r[:6] if v)}")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")

    lines.append(f"\n({elapsed}ms)")

    all_matches = [dict(zip(cols, r)) for r in rows]
    for r in hist_rows:
        all_matches.append({
            "first_name": r[0], "last_name": r[2],
            "county": r[6], "year": r[7], "source": "certificates_1866_1937"
        })

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "surname": surname,
            "first_name": first_name or None,
            "matches": all_matches,
        },
        meta={"total_matches": len(all_rows), "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Phase 1 tools — ACRIS transaction graph + multi-agency enforcement + flippers
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READONLY, tags={"finance"})
def property_history(bbl: BBL, ctx: Context) -> ToolResult:
    """Full ACRIS transaction chain for a NYC property since 1966 — every sale, mortgage, satisfaction, and assignment. Shows who bought from whom, when, for how much, plus mortgage and flip detection. Use this for property ownership history, sale price, or deed questions. Parameters: bbl (10 digits). Example: 1000670001. For multi-agency enforcement, use enforcement_web(bbl). For landlord portfolio, use landlord_watchdog(bbl)."""
    if not re.match(r"^\d{10}$", bbl):
        raise ToolError("BBL must be exactly 10 digits")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    borough = bbl[0]
    block = bbl[1:6].lstrip("0") or "0"
    lot = bbl[6:10].lstrip("0") or "0"

    # Get all documents for this BBL via acris_legals, joined to master + parties
    cols, rows = _execute(db, """
        WITH docs AS (
            SELECT DISTINCT m.document_id, m.doc_type,
                   TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                   TRY_CAST(m.document_date AS DATE) AS doc_date,
                   TRY_CAST(m.recorded_datetime AS DATE) AS recorded_date
            FROM lake.housing.acris_legals l
            JOIN lake.housing.acris_master m ON l.document_id = m.document_id
            WHERE l.borough = ? AND l.block = ? AND l.lot = ?
        ),
        parties AS (
            SELECT p.document_id,
                   p.name,
                   CASE WHEN p.party_type = '1' THEN 'GRANTOR'
                        WHEN p.party_type = '2' THEN 'GRANTEE'
                        ELSE 'OTHER' END AS role,
                   p.address_1,
                   p.city,
                   p.state,
                   p.zip
            FROM lake.housing.acris_parties p
            WHERE p.document_id IN (SELECT document_id FROM docs)
              AND p.name IS NOT NULL AND TRIM(p.name) != ''
        )
        SELECT d.doc_date, d.doc_type, d.amount, d.recorded_date,
               p.role, p.name, p.address_1, p.city, p.state,
               d.document_id
        FROM docs d
        LEFT JOIN parties p ON d.document_id = p.document_id
        ORDER BY d.doc_date DESC NULLS LAST, d.document_id, p.role
    """, [borough, block, lot])

    elapsed = int((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content=f"BBL {bbl}: no ACRIS transaction records found.")

    # Group by document
    doc_map = {}
    for r in rows:
        did = r[9]
        if did not in doc_map:
            doc_map[did] = {
                "date": r[0], "type": r[1], "amount": r[2],
                "recorded": r[3], "parties": [],
            }
        if r[5]:
            doc_map[did]["parties"].append({
                "role": r[4], "name": r[5],
                "address": f"{r[6] or ''} {r[7] or ''} {r[8] or ''}".strip(),
            })

    # Classify transactions
    deeds = [d for d in doc_map.values() if d["type"] in ("DEED", "DEEDO", "DEED, RP")]
    mortgages = [d for d in doc_map.values() if d["type"] in ("MTGE", "M&CON", "ASPM")]
    satisfactions = [d for d in doc_map.values() if d["type"] in ("SAT", "SATIS", "PSAT")]
    other_docs = [d for d in doc_map.values()
                  if d["type"] not in ("DEED", "DEEDO", "DEED, RP", "MTGE", "M&CON", "ASPM", "SAT", "SATIS", "PSAT")]

    lines = [f"PROPERTY HISTORY — BBL {bbl}"]
    lines.append(f"Total documents: {len(doc_map)} ({len(deeds)} deeds, "
                 f"{len(mortgages)} mortgages, {len(satisfactions)} satisfactions, "
                 f"{len(other_docs)} other)\n")

    # Show deeds (ownership transfers)
    if deeds:
        lines.append("--- OWNERSHIP TRANSFERS (DEEDS) ---")
        for d in sorted(deeds, key=lambda x: x["date"] or "", reverse=True):
            amt_str = f"${d['amount']:,.0f}" if d['amount'] else "N/A"
            date_str = str(d["date"]) if d["date"] else "?"
            sellers = [p for p in d["parties"] if p["role"] == "GRANTOR"]
            buyers = [p for p in d["parties"] if p["role"] == "GRANTEE"]
            lines.append(f"  {date_str} | {d['type']} | {amt_str}")
            for s in sellers[:3]:
                lines.append(f"    SELLER: {s['name']}" + (f" ({s['address']})" if s["address"] else ""))
            for b in buyers[:3]:
                lines.append(f"    BUYER:  {b['name']}" + (f" ({b['address']})" if b["address"] else ""))
        lines.append("")

    # Show mortgages
    if mortgages:
        lines.append(f"--- MORTGAGES ({len(mortgages)}) ---")
        for d in sorted(mortgages, key=lambda x: x["date"] or "", reverse=True)[:10]:
            amt_str = f"${d['amount']:,.0f}" if d['amount'] else "N/A"
            date_str = str(d["date"]) if d["date"] else "?"
            lenders = [p["name"] for p in d["parties"] if p["role"] == "GRANTEE"]
            borrowers = [p["name"] for p in d["parties"] if p["role"] == "GRANTOR"]
            lender_str = ", ".join(lenders[:2]) if lenders else "?"
            borrower_str = ", ".join(borrowers[:2]) if borrowers else "?"
            lines.append(f"  {date_str} | {amt_str} | Lender: {lender_str} | Borrower: {borrower_str}")
        if len(mortgages) > 10:
            lines.append(f"  ... and {len(mortgages) - 10} more mortgages")
        lines.append("")

    # Flip detection: consecutive deeds with price increases
    deed_prices = [(d["date"], d["amount"]) for d in sorted(deeds, key=lambda x: x["date"] or "")
                   if d["amount"] and d["amount"] > 10000]
    if len(deed_prices) >= 2:
        lines.append("--- PRICE HISTORY ---")
        for i, (date, price) in enumerate(deed_prices):
            delta = ""
            if i > 0 and deed_prices[i - 1][1]:
                change = price - deed_prices[i - 1][1]
                pct = (change / deed_prices[i - 1][1]) * 100
                delta = f"  ({'+' if change >= 0 else ''}{pct:.0f}%)"
            lines.append(f"  {date or '?'} — ${price:,.0f}{delta}")
        lines.append("")

    lines.append(f"({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "bbl": bbl,
            "total_documents": len(doc_map),
            "deeds": len(deeds),
            "mortgages": len(mortgages),
            "satisfactions": len(satisfactions),
            "transactions": [
                {
                    "date": str(d["date"]) if d["date"] else None,
                    "type": d["type"],
                    "amount": d["amount"],
                    "parties": d["parties"],
                }
                for d in sorted(doc_map.values(), key=lambda x: x["date"] or "", reverse=True)
            ],
        },
        meta={"total_documents": len(doc_map), "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"finance"})
def enforcement_web(bbl: BBL, ctx: Context) -> ToolResult:
    """Multi-agency enforcement web for a NYC property — HPD violations, DOB ECB violations, FDNY violations, OATH hearings, restaurant inspections, DOB complaints, facade safety (FISP), boiler safety, and SRO status. Shows every agency that has taken action at this address with severity scoring. Use this for enforcement or inspection history questions. Parameters: bbl (10 digits). Example: 1000670001. For building-level violations only, use owner_violations(bbl). For landlord portfolio, use landlord_watchdog(bbl)."""
    if not re.match(r"^\d{10}$", bbl):
        raise ToolError("BBL must be exactly 10 digits")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    borough = bbl[0]
    block = bbl[1:6].lstrip("0") or "0"
    lot = bbl[6:10].lstrip("0") or "0"

    # 1. HPD violations
    hpd_cols, hpd_rows = _execute(db, """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_v,
            COUNT(*) FILTER (WHERE UPPER(class) = 'C') AS class_c,
            COUNT(*) FILTER (WHERE UPPER(class) = 'B') AS class_b,
            COUNT(*) FILTER (WHERE UPPER(class) = 'A') AS class_a,
            MAX(TRY_CAST(novissueddate AS DATE)) AS latest,
            MIN(TRY_CAST(novissueddate AS DATE)) AS earliest
        FROM lake.housing.hpd_violations
        WHERE bbl = ?
    """, [bbl])
    hpd = dict(zip(hpd_cols, hpd_rows[0])) if hpd_rows else {}

    # 2. DOB ECB violations
    dob_cols, dob_rows = _execute(db, """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE UPPER(violation_type) LIKE '%HAZARD%'
                OR UPPER(severity) = 'HAZARDOUS') AS hazardous,
            SUM(TRY_CAST(balance_due AS DOUBLE)) AS total_penalties_due,
            SUM(TRY_CAST(penality_imposed AS DOUBLE)) AS total_paid,
            MAX(TRY_CAST(issue_date AS DATE)) AS latest,
            MIN(TRY_CAST(issue_date AS DATE)) AS earliest
        FROM lake.housing.dob_ecb_violations
        WHERE boro = ? AND block = ? AND lot = ?
    """, [borough, block, lot])
    dob = dict(zip(dob_cols, dob_rows[0])) if dob_rows else {}

    # 3. FDNY violations
    fdny_cols, fdny_rows = _execute(db, """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE UPPER(action) NOT IN ('CLOSED', 'DISMISSED')) AS open_v,
            MAX(TRY_CAST(vio_date AS DATE)) AS latest,
            MIN(TRY_CAST(vio_date AS DATE)) AS earliest
        FROM lake.housing.fdny_violations
        WHERE bbl = ?
    """, [bbl])
    fdny = dict(zip(fdny_cols, fdny_rows[0])) if fdny_rows else {}

    # 4. OATH hearings (enforcement hub — DOB, DOHMH, FDNY, DCWP, etc.)
    oath_cols, oath_rows = _execute(db, """
        SELECT
            issuing_agency,
            COUNT(*) AS total,
            SUM(TRY_CAST(total_violation_amount AS DOUBLE)) AS total_penalty,
            SUM(TRY_CAST(paid_amount AS DOUBLE)) AS total_paid,
            SUM(TRY_CAST(balance_due AS DOUBLE)) AS amount_due,
            COUNT(*) FILTER (WHERE UPPER(hearing_status) = 'DEFAULT') AS defaults,
            MAX(TRY_CAST(hearing_date AS DATE)) AS latest
        FROM lake.city_government.oath_hearings
        WHERE violation_location_block_no = ? AND violation_location_lot_no = ?
            AND violation_location_borough IS NOT NULL
        GROUP BY issuing_agency
        ORDER BY total_penalty DESC NULLS LAST
    """, [block, lot])

    # 5. Restaurant inspections (if applicable)
    rest_cols, rest_rows = _execute(db, """
        SELECT dba, cuisine_description, grade,
               inspection_date, violation_code, violation_description,
               critical_flag, score
        FROM lake.health.restaurant_inspections
        WHERE bbl = ?
        ORDER BY inspection_date DESC
        LIMIT 20
    """, [bbl])

    # 6. HPD complaints summary
    complaint_cols, complaint_rows = _execute(db, """
        SELECT
            COUNT(DISTINCT complaint_id) AS total_complaints,
            COUNT(DISTINCT complaint_id) FILTER (WHERE UPPER(complaint_status) = 'OPEN') AS open_complaints,
            MAX(TRY_CAST(received_date AS DATE)) AS latest
        FROM lake.housing.hpd_complaints
        WHERE bbl = ?
    """, [bbl])
    complaints = dict(zip(complaint_cols, complaint_rows[0])) if complaint_rows else {}

    # 7. DOB Complaints (construction complaints)
    try:
        dob_comp_cols, dob_comp_rows = _execute(db, """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE UPPER(status) = 'ACTIVE') AS active,
                COUNT(DISTINCT complaint_category) AS categories,
                MAX(TRY_CAST(date_entered AS DATE)) AS latest,
                MIN(TRY_CAST(date_entered AS DATE)) AS earliest
            FROM lake.housing.dob_complaints
            WHERE bin = (
                SELECT bin FROM lake.housing.hpd_jurisdiction
                WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
                LIMIT 1
            )
        """, [bbl])
        dob_comp = dict(zip(dob_comp_cols, dob_comp_rows[0])) if dob_comp_rows else {}
    except Exception:
        dob_comp = {}

    # 8. DOB Safety Facades
    try:
        facade_cols, facade_rows = _execute(db, """
            SELECT current_status, filing_status, cycle,
                   owner_name, exterior_wall_type_sx, comments
            FROM lake.housing.dob_safety_facades
            WHERE borough = ? AND block = ? AND lot = ?
            ORDER BY TRY_CAST(submitted_on AS DATE) DESC NULLS LAST
            LIMIT 5
        """, [borough, block, lot])
    except Exception:
        facade_cols, facade_rows = [], []

    # 9. DOB Safety Boilers
    try:
        boiler_cols, boiler_rows = _execute(db, """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE UPPER(defects_exist) = 'YES') AS with_defects,
                MAX(TRY_CAST(inspection_date AS DATE)) AS latest_inspection,
                owner_first_name || ' ' || owner_last_name AS owner
            FROM lake.housing.dob_safety_boiler
            WHERE bin_number = (
                SELECT bin FROM lake.housing.hpd_jurisdiction
                WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
                LIMIT 1
            )
            GROUP BY owner
        """, [bbl])
    except Exception:
        boiler_cols, boiler_rows = [], []

    # 10. SRO Buildings
    try:
        sro_cols, sro_rows = _execute(db, """
            SELECT buildingid, managementprogram, legalclassa, legalclassb,
                   dobbuildingclass, legalstories
            FROM lake.housing.sro_buildings
            WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
            LIMIT 1
        """, [bbl])
        is_sro = len(sro_rows) > 0
    except Exception:
        sro_cols, sro_rows = [], []
        is_sro = False

    elapsed = int((time.time() - t0) * 1000)

    # Calculate enforcement score
    score = 0
    score += (hpd.get("class_c") or 0) * 3
    score += (hpd.get("class_b") or 0) * 1
    score += (hpd.get("open_v") or 0) * 2
    score += (dob.get("hazardous") or 0) * 5
    score += (dob.get("total") or 0)
    score += (fdny.get("total") or 0) * 2
    score += sum(r[5] for r in oath_rows) * 3  # defaults
    score += (complaints.get("open_complaints") or 0)
    score += (dob_comp.get("active") or 0) * 2
    score += len(facade_rows) * 2
    score += sum(1 for r in boiler_rows if r[1] and r[1] > 0) * 3
    score += 5 if is_sro else 0

    severity = "LOW" if score < 10 else "MODERATE" if score < 50 else "HIGH" if score < 200 else "CRITICAL"

    lines = [f"ENFORCEMENT WEB — BBL {bbl}"]
    lines.append(f"Severity: {severity} (score: {score})\n")

    # HPD
    if hpd.get("total"):
        lines.append(f"HPD VIOLATIONS: {hpd['total']} total "
                     f"({hpd.get('open_v', 0)} open, {hpd.get('class_c', 0)} Class C, "
                     f"{hpd.get('class_b', 0)} Class B, {hpd.get('class_a', 0)} Class A)")
        lines.append(f"  Range: {hpd.get('earliest', '?')} — {hpd.get('latest', '?')}")

    # HPD complaints
    if complaints.get("total_complaints"):
        lines.append(f"HPD COMPLAINTS: {complaints['total_complaints']} total "
                     f"({complaints.get('open_complaints', 0)} open)")

    # DOB ECB
    if dob.get("total"):
        penalties_str = f"${dob['total_penalties_due']:,.0f}" if dob.get("total_penalties_due") else "$0"
        paid_str = f"${dob['total_paid']:,.0f}" if dob.get("total_paid") else "$0"
        lines.append(f"DOB ECB VIOLATIONS: {dob['total']} total "
                     f"({dob.get('hazardous', 0)} hazardous)")
        lines.append(f"  Penalties due: {penalties_str} | Paid: {paid_str}")
        lines.append(f"  Range: {dob.get('earliest', '?')} — {dob.get('latest', '?')}")

    # FDNY
    if fdny.get("total"):
        lines.append(f"FDNY VIOLATIONS: {fdny['total']} total "
                     f"({fdny.get('open_v', 0)} open/failed)")
        lines.append(f"  Range: {fdny.get('earliest', '?')} — {fdny.get('latest', '?')}")

    # OATH hearings by agency
    if oath_rows:
        lines.append(f"\nOATH HEARINGS BY AGENCY:")
        for r in oath_rows:
            penalty_str = f"${r[2]:,.0f}" if r[2] else "$0"
            due_str = f"${r[4]:,.0f}" if r[4] else "$0"
            lines.append(f"  {r[0] or 'Unknown'}: {r[1]} hearings | "
                         f"Penalties: {penalty_str} | Due: {due_str} | "
                         f"Defaults: {r[5]} | Latest: {r[6] or '?'}")

    # Restaurant inspections
    if rest_rows:
        restaurants = {}
        for r in rest_rows:
            name = r[0] or "Unknown"
            if name not in restaurants:
                restaurants[name] = {"cuisine": r[1], "grade": r[2], "violations": []}
            if r[4]:
                restaurants[name]["violations"].append(r[5] or r[4])
        lines.append(f"\nRESTAURANT INSPECTIONS:")
        for name, info in restaurants.items():
            lines.append(f"  {name} ({info['cuisine']}) — Grade: {info['grade'] or 'Pending'}")
            for v in info["violations"][:3]:
                lines.append(f"    - {v[:80]}")

    # DOB Complaints (construction)
    if dob_comp.get("total"):
        lines.append(f"\nDOB COMPLAINTS: {dob_comp['total']} total "
                     f"({dob_comp.get('active', 0)} active, {dob_comp.get('categories', 0)} categories)")
        lines.append(f"  Range: {dob_comp.get('earliest', '?')} — {dob_comp.get('latest', '?')}")

    # Facade safety
    if facade_rows:
        lines.append(f"\nFACADE SAFETY (FISP): {len(facade_rows)} filings")
        for r in facade_rows:
            lines.append(f"  Cycle {r[2] or '?'} | {r[0] or '?'} | Filing: {r[1] or '?'} | Wall: {r[4] or '?'}")
            if r[5]:
                lines.append(f"    {r[5][:100]}")

    # Boiler safety
    if boiler_rows:
        total_boilers = sum(r[0] or 0 for r in boiler_rows)
        defect_count = sum(r[1] or 0 for r in boiler_rows)
        lines.append(f"\nBOILER SAFETY: {total_boilers} inspections ({defect_count} with defects)")
        for r in boiler_rows[:3]:
            lines.append(f"  Owner: {r[3] or '?'} | {r[0]} inspections, {r[1] or 0} defects | Latest: {r[2] or '?'}")

    # SRO status
    if is_sro:
        r = sro_rows[0]
        lines.append(f"\nSRO BUILDING: Yes — {r[4] or '?'} class, {r[5] or '?'} stories, "
                     f"Class A: {r[2] or 0}, Class B: {r[3] or 0}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "bbl": bbl,
            "severity": severity,
            "score": score,
            "hpd_violations": hpd,
            "dob_ecb_violations": dob,
            "fdny_violations": fdny,
            "hpd_complaints": complaints,
            "oath_hearings": [dict(zip(oath_cols, r)) for r in oath_rows],
            "restaurant_inspections": [dict(zip(rest_cols, r)) for r in rest_rows],
            "dob_complaints": dob_comp,
            "facade_safety": [dict(zip(facade_cols, r)) for r in facade_rows],
            "boiler_safety": [dict(zip(boiler_cols, r)) for r in boiler_rows],
            "is_sro": is_sro,
        },
        meta={"severity": severity, "score": score, "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"finance"})
def flipper_detector(ctx: Context, borough: Annotated[str, Field(description="Borough name or code 1-5. Empty for all boroughs")] = "", months: Annotated[int, Field(description="Max months between buy and sell. Default: 24", ge=1, le=120)] = 24, min_profit_pct: Annotated[int, Field(description="Min price increase percentage. Default: 20", ge=1, le=1000)] = 20) -> ToolResult:
    """Detect property flips — investors who buy and resell NYC properties for profit within a short window. Finds house flipping, investor speculation, and profit patterns. Cross-references with DOB permits (renovation) and tax lien sales (distressed acquisition). Use this for flip detection, investor speculation, or profit analysis questions. Parameters: borough (optional), months (max hold period, default 24), min_profit_pct (default 20). For a specific property's transaction history, use property_history(bbl)."""
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    boro_map = {"manhattan": "1", "bronx": "2", "brooklyn": "3", "queens": "4", "staten island": "5"}
    boro_code = borough.strip()
    if boro_code.lower() in boro_map:
        boro_code = boro_map[boro_code.lower()]
    borough_filter = f"AND l.borough = '{boro_code}'" if boro_code in ("1", "2", "3", "4", "5") else ""

    months = min(max(months, 3), 120)
    min_profit_pct = min(max(min_profit_pct, 5), 500)

    cols, rows = _execute(db, f"""
        WITH deed_sales AS (
            SELECT
                (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                TRY_CAST(m.document_amt AS DOUBLE) AS price,
                TRY_CAST(m.document_date AS DATE) AS sale_date,
                m.document_id,
                l.borough
            FROM lake.housing.acris_master m
            JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
            WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
              AND m.document_amt IS NOT NULL
              AND TRY_CAST(m.document_amt AS DOUBLE) > 50000
              AND TRY_CAST(m.document_date AS DATE) >= '2015-01-01'
              AND l.borough IS NOT NULL
              {borough_filter}
        ),
        ranked AS (
            SELECT *,
                   LAG(price) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_price,
                   LAG(sale_date) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_date,
                   LAG(document_id) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_doc_id
            FROM deed_sales
        ),
        flips AS (
            SELECT bbl, borough,
                   prev_date AS buy_date, sale_date AS sell_date,
                   prev_price AS buy_price, price AS sell_price,
                   price - prev_price AS profit,
                   ((price - prev_price) / prev_price * 100)::INT AS profit_pct,
                   DATEDIFF('month', prev_date, sale_date) AS months_held,
                   prev_doc_id AS buy_doc_id, document_id AS sell_doc_id
            FROM ranked
            WHERE prev_price IS NOT NULL
              AND prev_price > 50000
              AND price > prev_price
              AND ((price - prev_price) / prev_price * 100) >= {min_profit_pct}
              AND DATEDIFF('month', prev_date, sale_date) <= {months}
              AND DATEDIFF('month', prev_date, sale_date) > 0
        )
        SELECT f.bbl, f.buy_date, f.sell_date, f.buy_price, f.sell_price,
               f.profit, f.profit_pct, f.months_held,
               -- Buyer name (grantee on buy doc)
               (SELECT FIRST(p.name) FROM lake.housing.acris_parties p
                WHERE p.document_id = f.buy_doc_id AND p.party_type = '2'
                AND p.name IS NOT NULL LIMIT 1) AS buyer_name,
               -- Seller name on the sell doc (same entity who flipped)
               (SELECT FIRST(p.name) FROM lake.housing.acris_parties p
                WHERE p.document_id = f.sell_doc_id AND p.party_type = '1'
                AND p.name IS NOT NULL LIMIT 1) AS flipper_name,
               -- Tax lien flag
               EXISTS (
                   SELECT 1 FROM lake.housing.tax_lien_sales t
                   WHERE (t.borough || LPAD(t.block::VARCHAR, 5, '0') || LPAD(t.lot::VARCHAR, 4, '0')) = f.bbl
               ) AS had_tax_lien,
               -- DOB permit between buy and sell
               EXISTS (
                   SELECT 1 FROM lake.housing.dob_permit_issuance d
                   WHERE (d.borough || LPAD(d.block::VARCHAR, 5, '0') || LPAD(d.lot::VARCHAR, 4, '0')) = f.bbl
                     AND TRY_CAST(d.issuance_date AS DATE) BETWEEN f.buy_date AND f.sell_date
               ) AS had_renovation
        FROM flips f
        ORDER BY f.profit DESC
        LIMIT 50
    """)

    elapsed = int((time.time() - t0) * 1000)

    if not rows:
        borough_label = f" in {borough.strip()}" if borough.strip() else ""
        return ToolResult(
            content=f"No flips found{borough_label} with >={min_profit_pct}% profit within {months} months.",
            meta={"query_time_ms": elapsed},
        )

    borough_label = f" in {borough.strip()}" if borough.strip() else ""
    lines = [f"FLIPPER DETECTOR{borough_label} — {len(rows)} flips found"]
    lines.append(f"Criteria: >={min_profit_pct}% profit within {months} months (since 2015)\n")

    total_profit = sum(r[5] or 0 for r in rows)
    lines.append(f"Total profit across {len(rows)} flips: ${total_profit:,.0f}\n")

    lines.append(f"{'BBL':<12} {'Buy':<12} {'Sell':<12} {'Buy$':<14} {'Sell$':<14} "
                 f"{'Profit':<14} {'%':<6} {'Mo':<4} {'Reno':<5} {'Lien':<5} Flipper")
    lines.append("-" * 140)

    for r in rows:
        buy_str = f"${r[3]:,.0f}" if r[3] else "?"
        sell_str = f"${r[4]:,.0f}" if r[4] else "?"
        profit_str = f"${r[5]:,.0f}" if r[5] else "?"
        reno = "YES" if r[11] else ""
        lien = "YES" if r[10] else ""
        flipper = (r[9] or r[8] or "?")[:25]
        lines.append(f"{r[0]:<12} {str(r[1] or '?'):<12} {str(r[2] or '?'):<12} "
                     f"{buy_str:<14} {sell_str:<14} {profit_str:<14} "
                     f"{r[6] or 0:<6} {r[7] or 0:<4} {reno:<5} {lien:<5} {flipper}")

    # Find serial flippers (names appearing multiple times)
    flipper_names = {}
    for r in rows:
        name = (r[9] or r[8] or "").strip().upper()
        if name and len(name) > 3:
            flipper_names[name] = flipper_names.get(name, 0) + 1
    serial = {k: v for k, v in flipper_names.items() if v >= 2}
    if serial:
        lines.append(f"\nSERIAL FLIPPERS (2+ flips):")
        for name, count in sorted(serial.items(), key=lambda x: -x[1]):
            lines.append(f"  {name}: {count} flips")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "borough": borough.strip() or "all",
            "criteria": {"months": months, "min_profit_pct": min_profit_pct},
            "total_profit": total_profit,
            "flips": [
                {
                    "bbl": r[0], "buy_date": str(r[1]), "sell_date": str(r[2]),
                    "buy_price": r[3], "sell_price": r[4],
                    "profit": r[5], "profit_pct": r[6], "months_held": r[7],
                    "buyer_name": r[8], "flipper_name": r[9],
                    "had_tax_lien": r[10], "had_renovation": r[11],
                }
                for r in rows
            ],
            "serial_flippers": serial if serial else None,
        },
        meta={"flip_count": len(rows), "total_profit": total_profit, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# DuckPGQ graph tools — Transaction, Corporate, Influence, Contractor networks
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READONLY, tags={"graph", "finance"})
def transaction_network(name: NAME, ctx: Context) -> ToolResult:
    """Trace a person or entity through NYC property transactions using DuckPGQ graph traversal — all properties bought/sold, co-transactors on the same deeds, and up to 3 hops through the transaction network. Reveals hidden connections between buyers, sellers, and shell companies. Use this for property transaction network questions. Parameters: name (person or company). Example: 'BLACKSTONE'. For a single property's history, use property_history(bbl). For corporate network, use corporate_web(entity_name)."""
    _require_graph(ctx)
    if len(name.strip()) < 3:
        raise ToolError("Name must be at least 3 characters")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    search = name.strip().upper()

    extra_names = _vector_expand_names(ctx, search)

    # Find matching entities
    ent_cols, ent_rows = _execute(db, """
        SELECT entity_name, tx_count, property_count, as_seller, as_buyer, total_amount
        FROM main.graph_tx_entities
        WHERE entity_name LIKE ?
        ORDER BY tx_count DESC
        LIMIT 10
    """, [f"%{search}%"])

    if not ent_rows:
        return ToolResult(content=f"No transaction entities found matching '{name}'.")

    primary = ent_rows[0]
    primary_name = primary[0]

    # Get their properties
    prop_cols, prop_rows = _execute(db, """
        SELECT bbl, role, amount, doc_date, document_id
        FROM main.graph_tx_edges
        WHERE entity_name = ?
        ORDER BY doc_date DESC
    """, [primary_name])

    # Graph traversal: co-transactors via DuckPGQ
    try:
        graph_cols, graph_rows = _execute(db, f"""
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
        # Fallback to direct SQL if graph traversal fails
        graph_cols, graph_rows = _execute(db, """
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

    elapsed = int((time.time() - t0) * 1000)

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
            lines.append(f"  → {en}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "entity": dict(zip(ent_cols, primary)),
            "properties": [dict(zip(prop_cols, r)) for r in prop_rows],
            "connected_entities": [dict(zip(graph_cols, r)) for r in graph_rows] if graph_rows else [],
            "other_matches": [dict(zip(ent_cols, r)) for r in ent_rows[1:]],
        },
        meta={"property_count": len(prop_rows), "connected_count": len(graph_rows or []),
              "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"graph", "investigation"})
def corporate_web(entity_name: NAME, ctx: Context) -> ToolResult:
    """Expose the shell company and LLC corporate network behind an entity using DuckPGQ. Traverses NYS corporation filings to find connected LLCs, shared officers, shared incorporation addresses, and links to HPD landlord registrations. Use this for shell company investigation, corporate network mapping, or shared officer analysis. Parameters: entity_name (company or person). Example: 'KUSHNER'. For LLC piercing, use llc_piercer(entity_name). For property transaction networks, use transaction_network(name)."""
    _require_graph(ctx)
    if len(entity_name.strip()) < 3:
        raise ToolError("Name must be at least 3 characters")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    search = entity_name.strip().upper()

    extra_names = _vector_expand_names(ctx, search)

    # Search corps by name
    corp_cols, corp_rows = _execute(db, """
        SELECT dos_id, current_entity_name, entity_type, county,
               dos_process_name, chairman_name, initial_dos_filing_date
        FROM main.graph_corps
        WHERE UPPER(current_entity_name) LIKE ?
           OR UPPER(dos_process_name) LIKE ?
           OR UPPER(chairman_name) LIKE ?
        ORDER BY current_entity_name
        LIMIT 20
    """, [f"%{search}%"] * 3)

    # Search people
    people_cols, people_rows = _execute(db, """
        SELECT person_name, corp_count, primary_address
        FROM main.graph_corp_people
        WHERE person_name LIKE ?
        ORDER BY corp_count DESC
        LIMIT 10
    """, [f"%{search}%"])

    # If we found corps, traverse the graph for shared officers
    connected_corps = []
    if corp_rows:
        primary_dos = corp_rows[0][0]
        try:
            gc_cols, gc_rows = _execute(db, f"""
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
            # Fallback to SQL
            gc_cols, gc_rows = _execute(db, """
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

    # If we found people, get their corps
    person_corps = []
    if people_rows:
        primary_person = people_rows[0][0]
        pc_cols, pc_rows = _execute(db, """
            SELECT c.dos_id, c.current_entity_name, c.entity_type, c.chairman_name
            FROM main.graph_corp_officer_edges e
            JOIN main.graph_corps c ON e.dos_id = c.dos_id
            WHERE e.person_name = ?
            ORDER BY c.current_entity_name
            LIMIT 50
        """, [primary_person])
        person_corps = pc_rows

    # Cross-reference with HPD landlords
    hpd_matches = []
    corp_names = [r[1] for r in corp_rows] + [r[1] for r in connected_corps]
    if corp_names:
        placeholders = ", ".join(["?"] * min(len(corp_names), 50))
        hpd_cols, hpd_rows = _execute(db, f"""
            SELECT owner_name, owner_id
            FROM main.graph_owners
            WHERE UPPER(owner_name) IN ({placeholders})
            LIMIT 20
        """, [n.upper() for n in corp_names[:50]])
        hpd_matches = hpd_rows

    elapsed = int((time.time() - t0) * 1000)

    lines = [f"CORPORATE WEB — '{entity_name}'"]

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

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "search": entity_name,
            "direct_corps": [dict(zip(corp_cols, r)) for r in corp_rows],
            "connected_corps": [{"dos_id": r[0], "name": r[1], "type": r[2], "chairman": r[3]}
                                for r in connected_corps],
            "people": [dict(zip(people_cols, r)) for r in people_rows],
            "person_corps": [{"dos_id": r[0], "name": r[1], "type": r[2]} for r in person_corps],
            "hpd_matches": [{"owner_name": r[0], "owner_id": r[1]} for r in hpd_matches],
        },
        meta={"corp_count": len(corp_rows), "connected_count": len(connected_corps),
              "hpd_matches": len(hpd_matches), "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"finance", "investigation"})
def pay_to_play(entity_name: NAME, ctx: Context) -> ToolResult:
    """Trace corruption, donation, lobbying, and contract political influence networks using DuckPGQ. Starting from any donor, candidate, lobbyist, or vendor — follows the money through campaign donations, lobbying registrations, and city contracts. Exposes pay-to-play patterns. Use this for political corruption, donation, lobbying, or contract influence questions. Parameters: entity_name (person or company). Example: 'CATSIMATIDIS'. For broader entity search, use entity_xray(name)."""
    _require_graph(ctx)
    if len(entity_name.strip()) < 3:
        raise ToolError("Name must be at least 3 characters")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    search = entity_name.strip().upper()

    extra_names = _vector_expand_names(ctx, search)

    # Find matching political entities
    ent_cols, ent_rows = _execute(db, """
        SELECT entity_name, roles, total_amount, total_transactions
        FROM main.graph_pol_entities
        WHERE entity_name LIKE ?
        ORDER BY total_amount DESC NULLS LAST
        LIMIT 10
    """, [f"%{search}%"])

    if not ent_rows:
        return ToolResult(content=f"No political entities found matching '{entity_name}'.")

    primary = ent_rows[0]
    primary_name = primary[0]

    # Graph traversal: connected entities via DuckPGQ
    try:
        graph_cols, graph_rows = _execute(db, f"""
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

    # Donations: who they donate to / who donates to them
    don_to_cols, don_to_rows = _execute(db, """
        SELECT candidate_name, total_donated, donation_count, employer, latest_date
        FROM main.graph_pol_donations
        WHERE donor_name = ?
        ORDER BY total_donated DESC
        LIMIT 20
    """, [primary_name])

    don_from_cols, don_from_rows = _execute(db, """
        SELECT donor_name, total_donated, donation_count, employer, latest_date
        FROM main.graph_pol_donations
        WHERE candidate_name = ?
        ORDER BY total_donated DESC
        LIMIT 20
    """, [primary_name])

    # Contracts
    con_cols, con_rows = _execute(db, """
        SELECT agency_name, total_amount, contract_count, latest_date
        FROM main.graph_pol_contracts
        WHERE vendor_name = ?
        ORDER BY total_amount DESC
        LIMIT 10
    """, [primary_name])

    # Lobbying (as lobbyist or as client)
    lob_cols, lob_rows = _execute(db, """
        SELECT client_name, total_compensation, filing_count, industry, latest_year
        FROM main.graph_pol_lobbying
        WHERE lobbyist_name = ?
        ORDER BY total_compensation DESC NULLS LAST
        LIMIT 10
    """, [primary_name])

    client_cols, client_rows = _execute(db, """
        SELECT lobbyist_name, total_compensation, filing_count, industry, latest_year
        FROM main.graph_pol_lobbying
        WHERE client_name = ?
        ORDER BY total_compensation DESC NULLS LAST
        LIMIT 10
    """, [primary_name])

    # Pay-to-play detection: donors whose employers have city contracts
    p2p_cols, p2p_rows = _execute(db, """
        SELECT d.donor_name, d.candidate_name, d.total_donated, d.employer,
               c.agency_name, c.total_amount AS contract_amount
        FROM main.graph_pol_donations d
        JOIN main.graph_pol_contracts c ON UPPER(d.employer) = c.vendor_name
        WHERE d.candidate_name = ? OR d.donor_name = ?
        ORDER BY c.total_amount DESC
        LIMIT 20
    """, [primary_name, primary_name])

    # FEC federal donations
    fec_cols, fec_rows = _execute(db, """
        SELECT committee_id, total_donated, donation_count, employer, occupation, latest_cycle
        FROM main.graph_fec_contributions
        WHERE donor_name = ?
        ORDER BY total_donated DESC
        LIMIT 15
    """, [primary_name])

    # Litigation history (if this entity is a landlord)
    lit_cols, lit_rows = _execute(db, """
        SELECT respondent_name, bbl, casetype, casestatus, case_open_date, findingofharassment
        FROM main.graph_litigation_respondents
        WHERE respondent_name = ?
        ORDER BY case_open_date DESC
        LIMIT 15
    """, [primary_name])

    elapsed = int((time.time() - t0) * 1000)

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
            lines.append(f"  {r[0]} (works at {r[3]}) → donates ${r[2]:,.0f} → {r[1]}")
            lines.append(f"    {r[3]} has ${r[5]:,.0f} in contracts from {r[4]}")

    if graph_rows:
        lines.append(f"\nGRAPH-CONNECTED ENTITIES ({len(graph_rows)} within 2 hops):")
        for r in graph_rows[:15]:
            amt_str = f"${r[2]:,.0f}" if r[2] else "?"
            lines.append(f"  {r[0][:40]} | {r[1]} | {amt_str}")

    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={
            "entity": dict(zip(ent_cols, primary)),
            "donates_to": [dict(zip(don_to_cols, r)) for r in don_to_rows],
            "funded_by": [dict(zip(don_from_cols, r)) for r in don_from_rows],
            "contracts": [dict(zip(con_cols, r)) for r in con_rows],
            "lobbying_clients": [dict(zip(lob_cols, r)) for r in lob_rows],
            "hired_lobbyists": [dict(zip(client_cols, r)) for r in client_rows],
            "pay_to_play_signals": [dict(zip(p2p_cols, r)) for r in p2p_rows],
            "graph_connected": [dict(zip(graph_cols, r)) for r in graph_rows] if graph_rows else [],
        },
        meta={"p2p_signals": len(p2p_rows), "query_time_ms": elapsed},
    )


@mcp.tool(annotations=READONLY, tags={"graph"})
def contractor_network(name_or_license: Annotated[str, Field(description="Contractor name or license number. Example: 'NACHMAN PLUMBING' or '0042962'")], ctx: Context) -> ToolResult:
    """Expose a contractor's full NYC footprint using DuckPGQ — all buildings worked on, violation rates at those buildings, co-workers (other contractors on same buildings), and owners served. Reveals contractor rings and safety patterns across 4M+ DOB permits. Use this for contractor investigation, construction safety, or contractor network questions. Parameters: name_or_license (contractor name or license number). Example: 'NACHMAN PLUMBING' or '0042962'."""
    _require_graph(ctx)
    if len(name_or_license.strip()) < 3:
        raise ToolError("Input must be at least 3 characters")

    db = ctx.lifespan_context["db"]
    t0 = time.time()
    search = name_or_license.strip().upper()

    extra_names = set() if search.replace('-', '').isdigit() else _vector_expand_names(ctx, search)

    # Find contractor by license or name
    if search.isdigit():
        c_cols, c_rows = _execute(db, """
            SELECT license, business_name, permit_count, building_count, first_permit, last_permit
            FROM main.graph_contractors WHERE license = ?
        """, [search])
    else:
        c_cols, c_rows = _execute(db, """
            SELECT license, business_name, permit_count, building_count, first_permit, last_permit
            FROM main.graph_contractors WHERE business_name LIKE ?
            ORDER BY permit_count DESC LIMIT 10
        """, [f"%{search}%"])

    if not c_rows:
        return ToolResult(content=f"No contractor found matching '{name_or_license}'.")

    primary = c_rows[0]
    license_num = primary[0]

    # Buildings this contractor worked on
    bldg_cols, bldg_rows = _execute(db, """
        SELECT pe.bbl, pe.permit_count, pe.job_type, pe.first_date, pe.last_date, pe.owner_name,
               b.housenumber || ' ' || b.streetname AS address, b.zip, b.total_units
        FROM main.graph_permit_edges pe
        LEFT JOIN main.graph_buildings b ON pe.bbl = b.bbl
        WHERE pe.license = ?
        ORDER BY pe.permit_count DESC
        LIMIT 30
    """, [license_num])

    # Violation rates at those buildings
    building_bbls = [r[0] for r in bldg_rows if r[0]]
    violation_stats = {}
    if building_bbls:
        placeholders = ", ".join(["?"] * min(len(building_bbls), 30))
        v_cols, v_rows = _execute(db, f"""
            SELECT bbl,
                   COUNT(*) AS total_violations,
                   COUNT(*) FILTER (WHERE UPPER(class) = 'C') AS class_c,
                   COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_v
            FROM lake.housing.hpd_violations
            WHERE bbl IN ({placeholders})
            GROUP BY bbl
        """, building_bbls[:30])
        violation_stats = {r[0]: {"total": r[1], "class_c": r[2], "open": r[3]} for r in v_rows}

    # Connected contractors via DuckPGQ
    try:
        net_cols, net_rows = _execute(db, f"""
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
        net_cols, net_rows = _execute(db, """
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

    # Top owners served
    owner_cols, owner_rows = _execute(db, """
        SELECT owner_name, COUNT(*) AS buildings, SUM(permit_count) AS total_permits
        FROM main.graph_permit_edges
        WHERE license = ? AND owner_name IS NOT NULL AND LENGTH(owner_name) > 3
        GROUP BY owner_name
        ORDER BY total_permits DESC
        LIMIT 15
    """, [license_num])

    elapsed = int((time.time() - t0) * 1000)

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
            lines.append(f"  → {en}")

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


@mcp.tool(annotations=READONLY, tags={"graph", "investigation"})
def shell_detector(ctx: Context, borough: Annotated[str, Field(description="Borough/county filter. Empty for all")] = "", min_corps: Annotated[int, Field(description="Min corporations per cluster. Default: 5", ge=2, le=50)] = 5) -> ToolResult:
    """Detect shell company clusters connected through shared officers using DuckPGQ Weakly Connected Components. Reveals hidden LLC corporate networks — groups of companies linked through the same people, even across different corporate names. Use this for shell company detection, corporate opacity, or beneficial owner investigation. Parameters: borough (optional county filter), min_corps (default 5). For a specific entity's corporate web, use corporate_web(entity_name). For LLC piercing, use llc_piercer(entity_name)."""
    _require_graph(ctx)
    db = ctx.lifespan_context["db"]
    t0 = time.time()
    min_corps = min(max(min_corps, 2), 50)

    borough_clause = ""
    borough_params = []
    if borough.strip():
        borough_clause = "WHERE UPPER(corp.county) LIKE UPPER('%' || ? || '%')"
        borough_params = [borough.strip()]

    try:
        cols, rows = _execute(db, f"""
            WITH wcc AS (
                SELECT * FROM weakly_connected_component(nyc_corporate_web, Corp, SharedOfficer)
            ),
            clusters AS (
                SELECT w.componentid,
                       COUNT(*) AS cluster_size
                FROM wcc w
                GROUP BY w.componentid
                HAVING COUNT(*) >= {min_corps}
            )
            SELECT c.componentid, c.cluster_size,
                   corp.dos_id, corp.current_entity_name, corp.entity_type,
                   corp.chairman_name, corp.county
            FROM clusters c
            JOIN wcc w ON c.componentid = w.componentid
            JOIN main.graph_corps corp ON w.dos_id = corp.dos_id
            {borough_clause}
            ORDER BY c.cluster_size DESC, c.componentid, corp.current_entity_name
            LIMIT 500
        """, borough_params)
    except Exception as e:
        raise ToolError(f"Shell detection failed: {e}. The DuckPGQ graph may not be ready. Try corporate_web(name) instead.")

    elapsed = int((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(content="No shell company clusters found matching criteria.")

    clusters = {}
    for r in rows:
        cid = r[0]
        if cid not in clusters:
            clusters[cid] = {"size": r[1], "corps": []}
        clusters[cid]["corps"].append({
            "dos_id": r[2], "name": r[3], "type": r[4],
            "chairman": r[5], "county": r[6],
        })

    lines = [f"SHELL COMPANY DETECTOR — {len(clusters)} clusters (min {min_corps} corps)"]
    if borough.strip():
        lines[0] += f" in {borough.strip()}"
    lines.append("")

    for i, (cid, c) in enumerate(clusters.items(), 1):
        chairmen = list({corp["chairman"] for corp in c["corps"] if corp["chairman"]})
        types = list({corp["type"] for corp in c["corps"] if corp["type"]})
        lines.append(f"Cluster #{i}: {c['size']} corporations")
        if chairmen:
            lines.append(f"  People: {', '.join(chairmen[:5])}")
        if types:
            lines.append(f"  Types: {', '.join(types[:5])}")
        for corp in c["corps"][:8]:
            lines.append(f"    {corp['name']} ({corp['type'] or '?'}) | {corp['county'] or '?'}")
        if len(c["corps"]) > 8:
            lines.append(f"    ... and {len(c['corps']) - 8} more")
        lines.append("")

    lines.append(f"({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"clusters": list(clusters.values())},
        meta={"cluster_count": len(clusters), "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Cross-Reference Dashboard (Phase 08)
# ---------------------------------------------------------------------------

@mcp.tool(annotations=READONLY, tags={"investigation"})
def person_crossref(name: NAME, ctx: Context) -> ToolResult:
    """Find every appearance of a person across the NYC data lake using Splink probabilistic matching. Matches the same person across 44 source tables even when name formats differ (e.g., 'Barton Perlbinder' matches 'PERLBINDER, BARTON M'). Use this for thorough cross-referencing. For a quick entity scan, use entity_xray(name). Returns: all source tables, record counts, addresses, cities, zips, and cluster details."""
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    search = name.strip().upper()
    words = [w for w in search.split() if len(w) >= 2]
    if not words:
        return ToolResult(content=f"No usable name words in '{name}'. Provide a first and/or last name.")

    try:
        # Build candidate (last_name, first_name) pairs
        candidates = []
        if len(words) >= 2:
            candidates.append((words[-1], words[0]))
            candidates.append((words[0], words[-1]))
        else:
            candidates.append((words[0], ""))

        # Find cluster_ids
        cluster_ids = set()
        for last, first in candidates:
            if first:
                cols, rows = _execute(db, """
                    SELECT DISTINCT cluster_id
                    FROM lake.federal.resolved_entities
                    WHERE last_name = ? AND first_name = ?
                    LIMIT 10
                """, [last, first])
            else:
                cols, rows = _execute(db, """
                    SELECT DISTINCT cluster_id
                    FROM lake.federal.resolved_entities
                    WHERE last_name = ?
                    LIMIT 10
                """, [last])
            for r in rows:
                if r[0] is not None:
                    cluster_ids.add(r[0])

        if not cluster_ids:
            embed_fn = ctx.lifespan_context.get("embed_fn")
            if embed_fn:
                try:
                    from embedder import vec_to_sql
                    query_vec = embed_fn(search)
                    vec_literal = vec_to_sql(query_vec)
                    sim_sql = f"""
                        SELECT name, _distance AS dist
                        FROM lance_vector_search('{LANCE_DIR}/entity_name_embeddings.lance', 'embedding', {vec_literal}, k=3)
                        ORDER BY _distance ASC
                    """
                    with _db_lock:
                        sim_rows = db.execute(sim_sql).fetchall()
                    for matched_name, dist in sim_rows:
                        if dist < LANCE_TIGHT_DISTANCE:
                            parts = matched_name.split()
                            if len(parts) >= 2:
                                for last, first in [(parts[-1], parts[0]), (parts[0], parts[-1])]:
                                    cols, rows = _execute(db, """
                                        SELECT DISTINCT cluster_id
                                        FROM lake.federal.resolved_entities
                                        WHERE last_name = ? AND first_name = ?
                                        LIMIT 5
                                    """, [last, first])
                                    for r in rows:
                                        if r[0] is not None:
                                            cluster_ids.add(r[0])
                except Exception:
                    pass

        if not cluster_ids:
            return ToolResult(
                content=f"No records found for '{name}' in resolved_entities. "
                        "Try entity_xray for LIKE-based search.",
            )

        cluster_list = list(cluster_ids)
        placeholders = ", ".join(["?"] * len(cluster_list))

        # Get all records in the cluster(s)
        detail_cols, detail_rows = _execute(db, f"""
            SELECT source_table, last_name, first_name, address, city, zip, cluster_id
            FROM lake.federal.resolved_entities
            WHERE cluster_id IN ({placeholders})
            ORDER BY source_table
        """, cluster_list)

        # Aggregate by source_table
        agg_cols, agg_rows = _execute(db, f"""
            SELECT source_table, COUNT(*) as records,
                   ARRAY_AGG(DISTINCT city) FILTER (WHERE city IS NOT NULL) as cities,
                   ARRAY_AGG(DISTINCT zip) FILTER (WHERE zip IS NOT NULL) as zips
            FROM lake.federal.resolved_entities
            WHERE cluster_id IN ({placeholders})
            GROUP BY source_table
            ORDER BY records DESC
        """, cluster_list)

        # Collect name variants
        variant_cols, variant_rows = _execute(db, f"""
            SELECT DISTINCT last_name, first_name
            FROM lake.federal.resolved_entities
            WHERE cluster_id IN ({placeholders})
              AND last_name IS NOT NULL AND first_name IS NOT NULL
            LIMIT 50
        """, cluster_list)

        elapsed = int((time.time() - t0) * 1000)

        total_records = sum(r[1] for r in agg_rows)
        table_count = len(agg_rows)
        cluster_str = ", ".join(str(c) for c in cluster_list[:5])
        if len(cluster_list) > 5:
            cluster_str += f" (+{len(cluster_list) - 5} more)"

        lines = [f"# Person Cross-Reference: {search}"]
        lines.append("")
        lines.append(f"Cluster: {cluster_str} | Tables: {table_count} | Total records: {total_records}")
        lines.append("")
        lines.append("## Appearances by Source Table")
        lines.append("")
        lines.append("| Source Table | Records | Cities | ZIPs |")
        lines.append("|---|---|---|---|")
        for r in agg_rows:
            cities = ", ".join(r[2][:5]) if r[2] else ""
            zips = ", ".join(str(z) for z in r[3][:5]) if r[3] else ""
            lines.append(f"| {r[0]} | {r[1]} | {cities} | {zips} |")

        if variant_rows:
            lines.append("")
            lines.append("## All Name Variants in Cluster")
            for r in variant_rows:
                lines.append(f"  {r[0]}, {r[1]}")

        lines.append(f"\n({elapsed}ms)")

        return ToolResult(
            content="\n".join(lines),
            structured_content={
                "cluster_ids": cluster_list,
                "table_count": table_count,
                "total_records": total_records,
                "tables": [
                    {"source_table": r[0], "records": r[1],
                     "cities": r[2] if r[2] else [], "zips": r[3] if r[3] else []}
                    for r in agg_rows
                ],
                "name_variants": [{"last_name": r[0], "first_name": r[1]} for r in variant_rows],
            },
            meta={"table_count": table_count, "total_records": total_records,
                  "cluster_count": len(cluster_list), "query_time_ms": elapsed},
        )

    except Exception as e:
        elapsed = int((time.time() - t0) * 1000)
        return ToolResult(
            content=f"Cross-reference lookup failed (resolved_entities may not exist): {e}\n"
                    "Try entity_xray for non-Splink search.",
        )


@mcp.tool(annotations=READONLY, tags={"investigation"})
def top_crossrefs(
    ctx: Context,
    min_tables: Annotated[int, Field(description="Min data tables a person appears in. Default: 5", ge=1, le=44)] = 5,
    last_name_prefix: Annotated[str, Field(description="Optional last name prefix filter. Example: 'PERL'")] = "",
    limit: Annotated[int, Field(description="Max results. Default: 50", ge=1, le=200)] = 50,
) -> ToolResult:
    """Find the most cross-referenced people in the NYC data lake — those appearing in the most source tables. Reveals high-profile persons, repeat offenders, major property owners, or politically connected individuals via Splink entity resolution. Parameters: min_tables (default 5), last_name_prefix (optional filter), limit (default 50). For a specific person's cross-references, use person_crossref(name)."""
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    min_tables = min(max(min_tables, 2), 30)
    limit = min(max(limit, 1), 200)
    prefix = last_name_prefix.strip().upper()

    try:
        if prefix:
            cols, rows = _execute(db, """
                SELECT cluster_id,
                       MIN(last_name) as last_name,
                       MIN(first_name) as first_name,
                       COUNT(DISTINCT source_table) as table_count,
                       COUNT(*) as total_records,
                       ARRAY_AGG(DISTINCT source_table ORDER BY source_table) as tables
                FROM lake.federal.resolved_entities
                WHERE last_name IS NOT NULL
                  AND last_name LIKE ? || '%'
                GROUP BY cluster_id
                HAVING COUNT(DISTINCT source_table) >= ?
                ORDER BY table_count DESC, total_records DESC
                LIMIT ?
            """, [prefix, min_tables, limit])
        else:
            cols, rows = _execute(db, """
                SELECT cluster_id,
                       MIN(last_name) as last_name,
                       MIN(first_name) as first_name,
                       COUNT(DISTINCT source_table) as table_count,
                       COUNT(*) as total_records,
                       ARRAY_AGG(DISTINCT source_table ORDER BY source_table) as tables
                FROM lake.federal.resolved_entities
                WHERE last_name IS NOT NULL
                GROUP BY cluster_id
                HAVING COUNT(DISTINCT source_table) >= ?
                ORDER BY table_count DESC, total_records DESC
                LIMIT ?
            """, [min_tables, limit])

        elapsed = int((time.time() - t0) * 1000)

        if not rows:
            msg = f"No clusters found with {min_tables}+ tables"
            if prefix:
                msg += f" and last name starting with '{prefix}'"
            return ToolResult(content=msg + ".")

        lines = [f"# Top Cross-Referenced People ({min_tables}+ tables)"]
        if prefix:
            lines[0] += f" — last name '{prefix}*'"
        lines.append("")
        lines.append("| Last Name | First Name | Tables | Records | Source Tables |")
        lines.append("|---|---|---|---|---|")
        for r in rows:
            tables_str = ", ".join(r[5][:8]) if r[5] else ""
            if r[5] and len(r[5]) > 8:
                tables_str += f" (+{len(r[5]) - 8})"
            lines.append(f"| {r[1]} | {r[2]} | {r[3]} | {r[4]} | {tables_str} |")

        lines.append(f"\nTotal: {len(rows)} people shown")
        lines.append(f"({elapsed}ms)")

        return ToolResult(
            content="\n".join(lines),
            structured_content={
                "people": [
                    {"cluster_id": r[0], "last_name": r[1], "first_name": r[2],
                     "table_count": r[3], "total_records": r[4],
                     "tables": r[5] if r[5] else []}
                    for r in rows
                ],
            },
            meta={"result_count": len(rows), "min_tables": min_tables,
                  "query_time_ms": elapsed},
        )

    except Exception as e:
        elapsed = int((time.time() - t0) * 1000)
        return ToolResult(
            content=f"Top cross-refs lookup failed (resolved_entities may not exist): {e}",
        )


# ---------------------------------------------------------------------------
# Spatial + phonetic tools (Tasks 13, 14, 16)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READONLY)
def hotspot_map(
    latitude: Annotated[float, Field(description="Center latitude. Example: 40.7128")],
    longitude: Annotated[float, Field(description="Center longitude. Example: -74.006")],
    ctx: Context,
    category: Annotated[str, Field(description="Data category: 'crime', 'violations', 'complaints', '311', 'restaurants'")] = "crime",
    radius: Annotated[int, Field(description="H3 k-ring radius (1-5). 1=~200m, 3=~600m, 5=~1km")] = 3,
) -> str:
    """H3 hex heatmap around a point — see density of crime, violations, complaints."""
    TABLE_MAP = {
        "crime": "public_safety.nypd_complaints_ytd",
        "violations": "housing.hpd_violations",
        "complaints": "housing.hpd_complaints",
        "311": "social_services.n311_service_requests",
        "restaurants": "health.restaurant_inspections",
    }
    source = TABLE_MAP.get(category)
    if not source:
        raise ToolError(f"Unknown category '{category}'. Use: {', '.join(TABLE_MAP.keys())}")

    db = ctx.lifespan_context["db"]
    sql, params = h3_heatmap_sql(
        source_table="lake.foundation.h3_index",
        filter_table=source,
        lat=latitude, lng=longitude,
        radius_rings=min(radius, 5),
    )
    cols, raw_rows = _execute(db, sql, params)
    if not raw_rows:
        return f"No {category} data found near ({latitude}, {longitude}). Foundation H3 index may need materialization."

    rows = [dict(zip(cols, r)) for r in raw_rows]
    total = sum(r["count"] for r in rows)
    hottest = rows[0]
    return (
        f"## {category.title()} Hotspot Map ({len(rows)} hex cells)\n\n"
        f"**Center:** ({latitude}, {longitude}) | **Radius:** {radius} rings (~{radius * 200}m)\n"
        f"**Total events:** {total:,}\n"
        f"**Hottest cell:** {hottest['count']:,} events at ({hottest['cell_lat']:.4f}, {hottest['cell_lng']:.4f})\n\n"
        + "\n".join(
            f"| {r['h3_cell']} | {r['cell_lat']:.4f} | {r['cell_lng']:.4f} | {r['count']:,} |"
            for r in rows[:20]
        )
    )


@mcp.tool(annotations=READONLY)
def name_variants(
    name: NAME,
    ctx: Context,
) -> str:
    """Find all phonetic matches for a name across the entire data lake.
    Catches typos, maiden names, spelling variants, nicknames."""
    db = ctx.lifespan_context["db"]
    parts = name.strip().split()
    if len(parts) >= 2:
        sql, params = phonetic_search_sql(first_name=parts[0], last_name=parts[-1], limit=50)
    else:
        sql, params = phonetic_search_sql(first_name=None, last_name=name, limit=50)

    cols, raw_rows = _execute(db, sql, params)
    if not raw_rows:
        return f"No phonetic matches for '{name}'. The phonetic index may need materialization."

    rows = [dict(zip(cols, r)) for r in raw_rows]

    by_source = {}
    for r in rows:
        src = r.get("source_table", "unknown")
        by_source.setdefault(src, []).append(r)

    lines = [f"## Name Variants for '{name}' ({len(rows)} matches across {len(by_source)} tables)\n"]
    for src, matches in sorted(by_source.items(), key=lambda x: -len(x[1])):
        lines.append(f"\n### {src} ({len(matches)} matches)")
        for m in matches[:5]:
            score = m.get("combined_score", 0)
            lines.append(f"- {m.get('first_name', '')} {m.get('last_name', '')} (score: {score:.2f})")

    return "\n".join(lines)


@mcp.tool(annotations=READONLY)
def lake_health(
    ctx: Context,
    schema: Annotated[str | None, Field(description="Filter to specific schema. Example: 'housing'")] = None,
) -> str:
    """Data lake health dashboard — row counts, null rates, freshness per table."""
    db = ctx.lifespan_context["db"]
    if schema:
        where = "WHERE schema_name = ?"
        params = [schema]
    else:
        where = ""
        params = []
    cols, raw_rows = _execute(db, f"""
        SELECT schema_name, table_name, row_count, column_count, high_null_columns, profiled_at
        FROM lake.foundation.data_health
        {where}
        ORDER BY row_count DESC
    """, params)
    if not raw_rows:
        return "No health data. Run the foundation_rebuild job first."

    rows = [dict(zip(cols, r)) for r in raw_rows]
    total_rows = sum(r["row_count"] for r in rows)
    unhealthy = [r for r in rows if r["high_null_columns"] > 0]

    lines = [
        f"## Data Lake Health ({len(rows)} tables, {total_rows:,} total rows)\n",
        f"**Tables with high-null columns:** {len(unhealthy)}\n",
        "| Schema | Table | Rows | Cols | High-Null Cols |",
        "|--------|-------|------|------|----------------|",
    ]
    for r in rows[:30]:
        flag = " !" if r["high_null_columns"] > 0 else ""
        lines.append(
            f"| {r['schema_name']} | {r['table_name']} | {r['row_count']:,} | "
            f"{r['column_count']} | {r['high_null_columns']}{flag} |"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Prompts — user-invocable investigation templates
# ---------------------------------------------------------------------------


@mcp.prompt(
    name="investigate_building",
    description="Deep investigation of a NYC building — violations, ownership, complaints, transactions, and enforcement history",
)
def investigate_building(bbl: str) -> str:
    return f"""Investigate building BBL {bbl} thoroughly:

1. Start with building_profile({bbl}) for the overview
2. Check landlord_network({bbl}) to see the ownership network
3. Look at enforcement_web({bbl}) for multi-agency enforcement
4. Pull property_history({bbl}) for the full transaction chain
5. Run building_story({bbl}) for the narrative summary

After gathering all data, write a clear summary highlighting:
- Who owns it and through what corporate structure
- Its violation and complaint history (improving or worsening?)
- Any red flags (frequent sales, liens, AEP program)
- How it compares to similar buildings nearby"""


@mcp.prompt(
    name="compare_neighborhoods",
    description="Compare 2-3 NYC neighborhoods across safety, schools, housing, environment, and quality of life",
)
def compare_neighborhoods(zip_codes: str) -> str:
    zips = [z.strip() for z in zip_codes.split(",")]
    zip_list = ", ".join(zips)
    return f"""Compare these NYC neighborhoods: {zip_list}

1. Start with neighborhood_compare({zips}) for the side-by-side stats
2. For each ZIP, pull neighborhood_portrait(zip) for the full picture
3. Check safety_report for the closest precinct to each
4. Look at environmental_justice for environmental burden

Build a comparison that covers:
- Safety (crime rates, types of crime)
- Housing (rent, violations, landlord quality)
- Schools (if residential)
- Environment (air quality, flood risk, green space)
- Services (311 responsiveness, nearby facilities)

End with a clear recommendation based on the data."""


@mcp.prompt(
    name="follow_the_money",
    description="Trace a person or company's full NYC footprint — property, politics, corporations, violations, and financial connections",
)
def follow_the_money(name: str) -> str:
    return f"""Investigate "{name}" across all NYC public records:

1. Start with entity_xray('{name}') for the full cross-reference
2. Check corporate_web('{name}') for shell company networks
3. Run pay_to_play('{name}') for political donation chains
4. Look at transaction_network('{name}') for property deals
5. Try llc_piercer('{name}') if they operate through LLCs

Build a profile covering:
- All properties connected to this entity
- Corporate structure (LLCs, officers, registered agents)
- Political connections (donations, contracts, lobbying)
- Violation and enforcement history
- Any patterns that suggest conflicts of interest"""


# ---------------------------------------------------------------------------
# PostHog analytics middleware — track all tool calls + client identity
# ---------------------------------------------------------------------------

# Store client info per session (populated on initialize, read on tool calls)
_session_clients: dict[str, dict] = {}
_SESSION_MAX = 1000


class PostHogMiddleware(Middleware):
    """Capture every MCP tool call as a PostHog event with client identity."""

    async def on_initialize(self, context, call_next):
        result = await call_next(context)
        # Store client info for this session
        try:
            client_info = context.message.clientInfo
            session_id = context.fastmcp_context.session_id or "unknown"
            client_ip = "unknown"
            try:
                req = get_http_request()
                if req and req.client:
                    client_ip = req.client.host
            except Exception:
                pass
            _session_clients[session_id] = {
                "client_name": getattr(client_info, "name", "unknown"),
                "client_version": getattr(client_info, "version", "unknown"),
                "client_ip": client_ip,
            }
            # Prune old sessions if over limit
            if len(_session_clients) > _SESSION_MAX:
                excess = len(_session_clients) - _SESSION_MAX
                for key in list(_session_clients.keys())[:excess]:
                    del _session_clients[key]
        except Exception:
            pass
        return result

    async def on_call_tool(self, context, call_next):
        if not os.environ.get("POSTHOG_API_KEY"):
            return await call_next(context)

        t0 = time.time()
        tool_name = context.message.name
        arguments = context.message.arguments or {}
        error = None

        # Get client identity from session
        session_id = "unknown"
        try:
            session_id = context.fastmcp_context.session_id or "unknown"
        except Exception:
            pass
        client = _session_clients.get(session_id, {})

        # Get client IP (fallback if not captured on initialize)
        client_ip = client.get("client_ip", "unknown")
        if client_ip == "unknown":
            try:
                req = get_http_request()
                if req and req.client:
                    client_ip = req.client.host
            except Exception:
                pass

        try:
            result = await call_next(context)
            return result
        except Exception as exc:
            error = str(exc)[:200]
            raise
        finally:
            try:
                elapsed = round((time.time() - t0) * 1000)
                distinct_id = client.get("client_name", "unknown") + ":" + client_ip
                props = {
                    "tool": tool_name,
                    "server": "common-ground-mcp",
                    "duration_ms": elapsed,
                    "session_id": session_id,
                    "client_name": client.get("client_name", "unknown"),
                    "client_version": client.get("client_version", "unknown"),
                    "client_ip": client_ip,
                }
                for k, v in arguments.items():
                    props[f"arg_{k}"] = str(v)[:200]
                if error:
                    props["error"] = error
                posthog.capture(
                    distinct_id=distinct_id,
                    event="mcp_tool_called",
                    properties=props,
                )
            except Exception:
                pass  # Never let analytics break tool calls


mcp.add_middleware(ResponseLimitingMiddleware(max_size=50_000))
mcp.add_middleware(OutputFormatterMiddleware())
mcp.add_middleware(PercentileMiddleware())
mcp.add_middleware(PostHogMiddleware())


# ---------------------------------------------------------------------------
# OAuth stub — Claude Code probes /.well-known/oauth-* on every HTTP MCP
# connection even when "auth": "none" is set.  The plain-text 404 from
# Starlette causes a JSON-parse crash in the SDK.  Returning a JSON 404
# silences the error.  (anthropics/claude-code#34008)
# ---------------------------------------------------------------------------

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

_NOT_FOUND = JSONResponse({"error": "not_found"}, status_code=404)

async def _oauth_stub(request: Request) -> JSONResponse:
    return _NOT_FOUND

@mcp.custom_route("/health", methods=["GET"])
async def _health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# Public HTTP API — catalog endpoint for data health page
# ---------------------------------------------------------------------------

import json as _json
import datetime as _dt


def _catalog_connect():
    """Open a fresh read-only DuckDB connection attached to DuckLake."""
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute("PRAGMA disable_checkpoint_on_shutdown")

    from extensions import load_extensions
    load_extensions(conn)

    minio_user = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    minio_pass = os.environ.get("MINIO_ROOT_PASSWORD", "")
    conn.execute("SET s3_region = 'us-east-1'")
    conn.execute("SET s3_endpoint = 'minio:9000'")
    conn.execute(f"SET s3_access_key_id = '{minio_user}'")
    conn.execute(f"SET s3_secret_access_key = '{minio_pass}'")
    conn.execute("SET s3_use_ssl = false")
    conn.execute("SET s3_url_style = 'path'")

    pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "\\'")
    conn.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
        AS lake (METADATA_SCHEMA 'lake')
    """)
    return conn


@mcp.custom_route("/api/catalog", methods=["GET", "OPTIONS"])
async def catalog_json(request: Request) -> JSONResponse:
    """Return table catalog as JSON for the data health page."""

    # Handle CORS preflight
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Max-Age": "86400",
            },
        )

    db = None
    try:
        db = _catalog_connect()

        # Query table stats from DuckDB metadata
        cols, rows = _execute(db, """
            SELECT
                t.schema_name,
                t.table_name,
                t.estimated_size,
                (SELECT COUNT(*) FROM duckdb_columns() c
                 WHERE c.schema_name = t.schema_name AND c.table_name = t.table_name) AS column_count
            FROM duckdb_tables() t
            WHERE t.schema_name NOT LIKE '%staging%'
              AND t.schema_name NOT LIKE 'test%'
              AND t.schema_name NOT LIKE 'ducklake%'
              AND t.schema_name NOT LIKE 'information%'
              AND t.schema_name != 'lake'
              AND t.schema_name != 'foundation'
              AND t.table_name NOT LIKE '%__null'
              AND t.table_name NOT LIKE '%__footnotes'
            ORDER BY t.schema_name, t.estimated_size DESC
        """)

        # Try to get DuckLake file metadata separately (may fail if schema differs)
        ducklake_info = {}
        try:
            _, dl_rows = _execute(db, "SELECT table_name, file_count, file_size_bytes, table_uuid FROM ducklake_table_info('lake')")
            for dlr in dl_rows:
                ducklake_info[dlr[0]] = {"file_count": dlr[1], "file_size_bytes": dlr[2], "table_uuid": str(dlr[3]) if dlr[3] else None}
        except Exception:
            pass  # DuckLake metadata unavailable — continue without it

        # Get pipeline cursor state — last_run_at and row_count per dataset
        # Try with source columns first (added by freshness sensor), fall back to base columns
        pipeline_state = {}
        try:
            try:
                _, ps_rows = _execute(db, """
                    SELECT dataset_name, last_updated_at, row_count, last_run_at,
                           source_rows, sync_status, source_checked_at
                    FROM lake._pipeline_state
                """)
                for psr in ps_rows:
                    pipeline_state[psr[0]] = {
                        "cursor": str(psr[1]) if psr[1] else None,
                        "rows_written": psr[2],
                        "last_run_at": str(psr[3]) if psr[3] else None,
                        "source_rows": psr[4],
                        "sync_status": psr[5],
                        "source_checked_at": str(psr[6]) if psr[6] else None,
                    }
            except Exception:
                # Fallback: source columns don't exist yet (sensor hasn't run)
                _, ps_rows = _execute(db, """
                    SELECT dataset_name, last_updated_at, row_count, last_run_at
                    FROM lake._pipeline_state
                """)
                for psr in ps_rows:
                    pipeline_state[psr[0]] = {
                        "cursor": str(psr[1]) if psr[1] else None,
                        "rows_written": psr[2],
                        "last_run_at": str(psr[3]) if psr[3] else None,
                        "source_rows": None,
                        "sync_status": None,
                        "source_checked_at": None,
                    }
        except Exception:
            pass  # _pipeline_state may not exist yet

        tables = []
        schema_stats = {}
        total_rows = 0

        for row in rows:
            schema, table, est_size, col_count = row[:4]
            if table.startswith('_dlt_') or table.startswith('_pipeline'):
                continue

            dl = ducklake_info.get(table, {})
            file_count = dl.get("file_count", 0)
            file_size = dl.get("file_size_bytes", 0)
            table_uuid = dl.get("table_uuid")

            # Extract creation timestamp from UUIDv7 (first 48 bits = epoch ms)
            created_at = None
            if table_uuid:
                try:
                    hex_str = table_uuid.replace('-', '')
                    if len(hex_str) >= 13 and hex_str[12] == '7':  # UUIDv7 version check
                        epoch_ms = int(hex_str[:12], 16)
                        created_at = _dt.datetime.fromtimestamp(epoch_ms / 1000, tz=_dt.timezone.utc).isoformat()
                except (ValueError, OSError):
                    pass

            # Pipeline cursor state
            ps_key = f"{schema}.{table}"
            ps = pipeline_state.get(ps_key, {})

            tables.append({
                "schema": schema,
                "table": table,
                "rows": est_size or 0,
                "columns": col_count or 0,
                "files": file_count or 0,
                "size_bytes": file_size or 0,
                "created_at": created_at,
                "last_run_at": ps.get("last_run_at"),
                "cursor": ps.get("cursor"),
                "rows_written": ps.get("rows_written"),
                "source_rows": ps.get("source_rows"),
                "sync_status": ps.get("sync_status"),
                "source_checked_at": ps.get("source_checked_at"),
            })
            total_rows += (est_size or 0)
            if schema not in schema_stats:
                schema_stats[schema] = {"tables": 0, "rows": 0}
            schema_stats[schema]["tables"] += 1
            schema_stats[schema]["rows"] += (est_size or 0)

        result = {
            "as_of": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "summary": {
                "schemas": len(schema_stats),
                "tables": len(tables),
                "total_rows": total_rows,
            },
            "schemas": schema_stats,
            "tables": tables,
        }

        return JSONResponse(
            result,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=300",
            },
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        if db:
            db.close()


_well_known_routes = [
    Route("/.well-known/oauth-authorization-server", _oauth_stub, methods=["GET"]),
    Route("/.well-known/oauth-authorization-server/{path:path}", _oauth_stub, methods=["GET"]),
    Route("/.well-known/oauth-protected-resource", _oauth_stub, methods=["GET"]),
    Route("/.well-known/oauth-protected-resource/{path:path}", _oauth_stub, methods=["GET"]),
    Route("/.well-known/openid-configuration", _oauth_stub, methods=["GET"]),
    Route("/.well-known/openid-configuration/{path:path}", _oauth_stub, methods=["GET"]),
]

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=4213)
