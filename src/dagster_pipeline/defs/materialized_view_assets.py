"""Materialized view assets — pre-joined/pre-aggregated tables for MCP tool performance.

These assets create foundation.mv_* tables in DuckLake that replace expensive
S3 full-table scans with pre-computed results. Each auto-refreshes when its
upstream ingestion assets complete via AutomationCondition.eager().

Consumed by: MCP server tools (building_profile, landlord_watchdog, etc.)
Stored in: lake.foundation.mv_* (DuckLake Parquet on MinIO)
"""
import logging
import time

import dagster as dg
import duckdb

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)


@dg.asset(
    key=dg.AssetKey(["foundation", "mv_building_hub"]),
    group_name="foundation",
    description=(
        "Pre-joined building stats by BBL: address, owner, violations, complaints, "
        "DOB violations, latest sale. Replaces 5 S3 table scans in building_profile."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["housing", "hpd_violations"]),
        dg.AssetKey(["housing", "hpd_complaints"]),
        dg.AssetKey(["housing", "dob_ecb_violations"]),
        dg.AssetKey(["housing", "hpd_jurisdiction"]),
        dg.AssetKey(["city_government", "pluto"]),
    ],
)
def mv_building_hub(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_building_hub")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_building_hub AS
            WITH buildings AS (
                SELECT
                    bbl,
                    FIRST(address) AS address,
                    FIRST(ownername) AS ownername,
                    FIRST(zipcode) AS zipcode,
                    FIRST(yearbuilt) AS yearbuilt,
                    FIRST(unitsres) AS unitsres,
                    FIRST(unitstotal) AS unitstotal,
                    FIRST(bldgclass) AS bldgclass,
                    FIRST(zonedist1) AS zoning,
                    FIRST(TRY_CAST(assesstot AS DOUBLE)) AS assessed_total
                FROM lake.city_government.pluto
                WHERE bbl IS NOT NULL
                GROUP BY bbl
            ),
            viol AS (
                SELECT bbl,
                    COUNT(*) AS total_violations,
                    COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_violations,
                    COUNT(*) FILTER (WHERE UPPER(class) = 'C') AS class_c_violations,
                    MAX(TRY_CAST(novissueddate AS DATE)) AS latest_violation
                FROM lake.housing.hpd_violations
                WHERE bbl IS NOT NULL
                GROUP BY bbl
            ),
            comp AS (
                SELECT bbl,
                    COUNT(DISTINCT complaint_id) AS total_complaints,
                    COUNT(DISTINCT complaint_id) FILTER (WHERE complaint_status = 'OPEN') AS open_complaints,
                    MAX(TRY_CAST(received_date AS DATE)) AS latest_complaint
                FROM lake.housing.hpd_complaints
                WHERE bbl IS NOT NULL
                GROUP BY bbl
            ),
            dob AS (
                SELECT
                    (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl,
                    COUNT(*) AS dob_violations,
                    MAX(TRY_CAST(issue_date AS DATE)) AS latest_dob
                FROM lake.housing.dob_ecb_violations
                WHERE boro IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
                GROUP BY 1
            ),
            jurisdiction AS (
                SELECT
                    (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl,
                    FIRST(legalstories) AS stories,
                    MAX(COALESCE(TRY_CAST(legalclassa AS INTEGER), 0)
                      + COALESCE(TRY_CAST(legalclassb AS INTEGER), 0)) AS total_units,
                    FIRST(managementprogram) AS mgmt_program,
                    FIRST(registrationid) AS registration_id
                FROM lake.housing.hpd_jurisdiction
                WHERE boroid IS NOT NULL
                GROUP BY 1
            )
            SELECT
                COALESCE(b.bbl, j.bbl) AS bbl,
                b.address, b.ownername, b.zipcode, b.yearbuilt, b.unitsres,
                b.unitstotal, b.bldgclass, b.zoning, b.assessed_total,
                j.stories, j.total_units, j.mgmt_program, j.registration_id,
                COALESCE(v.total_violations, 0) AS total_violations,
                COALESCE(v.open_violations, 0) AS open_violations,
                COALESCE(v.class_c_violations, 0) AS class_c_violations,
                v.latest_violation,
                COALESCE(c.total_complaints, 0) AS total_complaints,
                COALESCE(c.open_complaints, 0) AS open_complaints,
                c.latest_complaint,
                COALESCE(d.dob_violations, 0) AS dob_violations,
                d.latest_dob
            FROM buildings b
            FULL OUTER JOIN jurisdiction j ON b.bbl = j.bbl
            LEFT JOIN viol v ON COALESCE(b.bbl, j.bbl) = v.bbl
            LEFT JOIN comp c ON COALESCE(b.bbl, j.bbl) = c.bbl
            LEFT JOIN dob d ON COALESCE(b.bbl, j.bbl) = d.bbl
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_building_hub"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_building_hub: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey(["foundation", "mv_acris_deeds"]),
    group_name="foundation",
    description=(
        "Pre-joined ACRIS deed transactions: master+legals+parties with BBL. "
        "Replaces 85M row 3-table S3 join in property_history, flipper_detector."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["housing", "acris_master"]),
        dg.AssetKey(["housing", "acris_legals"]),
        dg.AssetKey(["housing", "acris_parties"]),
    ],
)
def mv_acris_deeds(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_acris_deeds")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_acris_deeds AS
            SELECT
                (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                m.document_id,
                m.doc_type,
                TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                TRY_CAST(m.document_date AS DATE) AS doc_date,
                p.name AS party_name,
                CASE WHEN p.party_type = '1' THEN 'SELLER'
                     WHEN p.party_type = '2' THEN 'BUYER'
                     ELSE 'OTHER' END AS role,
                p.address_1,
                p.city,
                p.state
            FROM lake.housing.acris_master m
            JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
            JOIN lake.housing.acris_parties p ON m.document_id = p.document_id
            WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
              AND l.borough IS NOT NULL
              AND p.name IS NOT NULL AND LENGTH(TRIM(p.name)) > 1
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_acris_deeds"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_acris_deeds: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey(["foundation", "mv_zip_stats"]),
    group_name="foundation",
    description=(
        "Pre-aggregated neighborhood stats by ZIP: 311 complaints, HPD complaints, "
        "crime counts, restaurant grades, income. Replaces 37M+ row scans."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["social_services", "n311_service_requests"]),
        dg.AssetKey(["housing", "hpd_complaints"]),
        dg.AssetKey(["health", "restaurant_inspections"]),
        dg.AssetKey(["economics", "irs_soi_zip_income"]),
    ],
)
def mv_zip_stats(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_zip_stats")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_zip_stats AS
            WITH n311 AS (
                SELECT incident_zip AS zip,
                    COUNT(*) AS n311_total,
                    COUNT(*) FILTER (WHERE agency = 'NYPD') AS nypd_311_calls,
                    COUNT(*) FILTER (WHERE TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY) AS n311_1yr
                FROM lake.social_services.n311_service_requests
                WHERE incident_zip IS NOT NULL AND LENGTH(incident_zip) = 5 AND incident_zip LIKE '1%'
                GROUP BY incident_zip
            ),
            hpd AS (
                SELECT post_code AS zip,
                    COUNT(DISTINCT complaint_id) AS hpd_complaints_total,
                    COUNT(DISTINCT complaint_id) FILTER (
                        WHERE TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
                    ) AS hpd_complaints_1yr
                FROM lake.housing.hpd_complaints
                WHERE post_code IS NOT NULL AND LENGTH(post_code) = 5
                GROUP BY post_code
            ),
            rest AS (
                SELECT zipcode AS zip,
                    COUNT(DISTINCT camis) AS restaurant_count,
                    COUNT(DISTINCT camis) FILTER (WHERE grade = 'A') AS grade_a_count,
                    COUNT(DISTINCT camis) FILTER (WHERE grade IN ('A','B','C')) AS graded_count
                FROM lake.health.restaurant_inspections
                WHERE zipcode IS NOT NULL AND LENGTH(zipcode) = 5
                GROUP BY zipcode
            ),
            income AS (
                SELECT zipcode AS zip,
                    ROUND(1000.0 * SUM(TRY_CAST(agi_amount AS DOUBLE))
                        / NULLIF(SUM(TRY_CAST(num_returns AS DOUBLE)), 0), 0) AS avg_agi
                FROM lake.economics.irs_soi_zip_income
                WHERE TRY_CAST(tax_year AS INTEGER) = (
                    SELECT MAX(TRY_CAST(tax_year AS INTEGER)) FROM lake.economics.irs_soi_zip_income
                )
                GROUP BY zipcode
            )
            SELECT
                COALESCE(n.zip, h.zip, r.zip) AS zip,
                COALESCE(n.n311_total, 0) AS n311_total,
                COALESCE(n.nypd_311_calls, 0) AS nypd_311_calls,
                COALESCE(n.n311_1yr, 0) AS n311_1yr,
                COALESCE(h.hpd_complaints_total, 0) AS hpd_complaints_total,
                COALESCE(h.hpd_complaints_1yr, 0) AS hpd_complaints_1yr,
                COALESCE(r.restaurant_count, 0) AS restaurant_count,
                COALESCE(r.grade_a_count, 0) AS grade_a_count,
                COALESCE(r.graded_count, 0) AS graded_count,
                ROUND(100.0 * r.grade_a_count / NULLIF(r.graded_count, 0), 1) AS pct_grade_a,
                i.avg_agi
            FROM n311 n
            FULL OUTER JOIN hpd h ON n.zip = h.zip
            FULL OUTER JOIN rest r ON COALESCE(n.zip, h.zip) = r.zip
            LEFT JOIN income i ON COALESCE(n.zip, h.zip, r.zip) = i.zip
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_zip_stats"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_zip_stats: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey(["foundation", "mv_crime_precinct"]),
    group_name="foundation",
    description=(
        "Pre-aggregated crime stats by precinct: felonies, misdemeanors, arrests, "
        "top offense types. Replaces 9.5M+ row scan in safety_report."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["public_safety", "nypd_complaints_historic"]),
        dg.AssetKey(["public_safety", "nypd_complaints_ytd"]),
        dg.AssetKey(["public_safety", "nypd_arrests_historic"]),
    ],
)
def mv_crime_precinct(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_crime_precinct")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_crime_precinct AS
            WITH all_complaints AS (
                SELECT addr_pct_cd AS precinct, law_cat_cd, ofns_desc,
                       TRY_CAST(rpt_dt AS DATE) AS rpt_date
                FROM lake.public_safety.nypd_complaints_historic
                UNION ALL
                SELECT addr_pct_cd, law_cat_cd, ofns_desc,
                       TRY_CAST(rpt_dt AS DATE)
                FROM lake.public_safety.nypd_complaints_ytd
            ),
            stats AS (
                SELECT precinct,
                    COUNT(*) AS total_complaints,
                    COUNT(*) FILTER (WHERE rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS complaints_1yr,
                    COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS felonies_1yr,
                    COUNT(*) FILTER (WHERE law_cat_cd = 'MISDEMEANOR' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS misdemeanors_1yr,
                    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%assault%' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS assaults_1yr,
                    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%robbery%' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS robberies_1yr,
                    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%burglary%' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS burglaries_1yr
                FROM all_complaints
                WHERE precinct IS NOT NULL
                GROUP BY precinct
            ),
            arrests AS (
                SELECT
                    REGEXP_EXTRACT(arrest_precinct, '\\d+') AS precinct,
                    COUNT(*) AS arrests_total,
                    COUNT(*) FILTER (WHERE TRY_CAST(arrest_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY) AS arrests_1yr
                FROM lake.public_safety.nypd_arrests_historic
                WHERE arrest_precinct IS NOT NULL
                GROUP BY 1
            )
            SELECT s.precinct,
                s.total_complaints, s.complaints_1yr,
                s.felonies_1yr, s.misdemeanors_1yr,
                s.assaults_1yr, s.robberies_1yr, s.burglaries_1yr,
                COALESCE(a.arrests_total, 0) AS arrests_total,
                COALESCE(a.arrests_1yr, 0) AS arrests_1yr
            FROM stats s
            LEFT JOIN arrests a ON s.precinct = a.precinct
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_crime_precinct"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_crime_precinct: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey(["foundation", "mv_corp_network"]),
    group_name="foundation",
    description=(
        "Pre-joined NYS corps + entity addresses for NYC-relevant corps. "
        "Replaces 50M row join in llc_piercer and corporate_web."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["business", "nys_corporations"]),
        dg.AssetKey(["business", "nys_entity_addresses"]),
    ],
)
def mv_corp_network(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_corp_network")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_corp_network AS
            WITH nyc_corps AS (
                SELECT dos_id, current_entity_name, entity_type,
                       initial_dos_filing_date, county,
                       dos_process_name, dos_process_address_1, dos_process_city,
                       registered_agent_name, chairman_name
                FROM lake.business.nys_corporations
                WHERE current_entity_name IS NOT NULL
                  AND (county IN ('NEW YORK', 'KINGS', 'QUEENS', 'BRONX', 'RICHMOND')
                       OR dos_process_city IN ('NEW YORK', 'BROOKLYN', 'BRONX', 'QUEENS',
                                               'STATEN ISLAND', 'MANHATTAN', 'FLUSHING',
                                               'ASTORIA', 'JAMAICA', 'LONG ISLAND CITY'))
            ),
            people AS (
                SELECT
                    a.corpid_num AS dos_id,
                    UPPER(TRIM(a.name)) AS person_name,
                    a.addr_type AS role,
                    UPPER(TRIM(a.addr1)) AS address
                FROM lake.business.nys_entity_addresses a
                WHERE a.corpid_num IN (SELECT dos_id FROM nyc_corps)
                  AND a.name IS NOT NULL AND LENGTH(TRIM(a.name)) > 3
                  AND UPPER(a.name) NOT IN ('NONE', 'NA', 'N/A', 'SAME', 'THE LLC')
            )
            SELECT
                c.dos_id, c.current_entity_name, c.entity_type,
                c.initial_dos_filing_date, c.county,
                c.dos_process_name, c.dos_process_address_1, c.dos_process_city,
                c.registered_agent_name, c.chairman_name,
                p.person_name, p.role, p.address AS person_address
            FROM nyc_corps c
            LEFT JOIN people p ON c.dos_id = p.dos_id
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_corp_network"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_corp_network: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
