"""Common Ground — FastMCP server for NYC open data lake."""

import asyncio
import os
import threading
import time
from collections import defaultdict

import duckdb
import posthog
from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from fastmcp.server.lifespan import lifespan
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.server.middleware.rate_limiting import RateLimitingMiddleware
from fastmcp.server.middleware.response_limiting import ResponseLimitingMiddleware
from fastmcp.server.dependencies import get_http_request, get_http_headers

from middleware.response_middleware import OutputFormatterMiddleware
from middleware.citation_middleware import CitationMiddleware
from middleware.freshness_middleware import FreshnessMiddleware
from middleware.percentile_middleware import PercentileMiddleware

from percentiles import build_percentile_tables, build_lake_percentile_tables
from cursor_pool import CursorPool
from shared.db import execute, build_catalog, set_catalog_cache
from shared.graph import graph_cache_fresh, load_graph_from_cache, save_graph_to_cache
from shared.types import READONLY, EMBEDDINGS_DB

# ---------------------------------------------------------------------------
# Schema descriptions — used by lifespan for catalog embeddings & graph
# ---------------------------------------------------------------------------

SCHEMA_DESCRIPTIONS = {
    "business": "BLS employment stats, ACS census demographics, business licenses, M/WBE certs. 15 tables. Ask: which industries are growing? where are minority-owned businesses concentrated?",
    "city_government": "PLUTO lot data (every parcel in NYC), city payroll, OATH hearings, zoning, facilities. 25 tables, 3M+ rows. Ask: who owns this lot? what's the zoning? how much do city employees earn?",
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
    "pg_catalog", "information_schema", "ducklake", "public",
})


# ---------------------------------------------------------------------------
# Exploration builder — pre-computes highlights for suggest/semantic tools
# ---------------------------------------------------------------------------


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
            "Try: network(type='worst') or entity(name)",
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
            "Try: network(type='worst') or building(bbl)",
        ),
        (
            "Most complained-about restaurants",
            """SELECT camis, dba, zipcode, COUNT(*) AS violations,
                      COUNT(*) FILTER (WHERE violation_code LIKE '04%') AS critical
               FROM lake.health.restaurant_inspections
               GROUP BY camis, dba, zipcode
               ORDER BY critical DESC LIMIT 5""",
            "Restaurants with the most critical inspection violations.",
            "Try: health(zipcode) or semantic_search(query='restaurant violations')",
        ),
        (
            "Neighborhoods with most 311 complaints",
            """SELECT incident_zip, complaint_type, COUNT(*) AS complaints
               FROM lake.social_services.n311_service_requests
               WHERE incident_zip IS NOT NULL AND incident_zip != ''
               GROUP BY incident_zip, complaint_type
               ORDER BY complaints DESC LIMIT 5""",
            "What NYC residents complain about most, by ZIP.",
            "Try: neighborhood(zipcode) or neighborhood(zipcodes='zip1,zip2', view='compare')",
        ),
        (
            "Corporate shell networks",
            """SELECT COUNT(*) AS total_corps,
                      COUNT(DISTINCT dos_process_name) AS distinct_agents
               FROM lake.business.nys_corporations
               WHERE jurisdiction = 'NEW YORK'
                 AND dos_process_name IS NOT NULL""",
            "Active NYC corporations and how many share registered agents — a signal for shell company networks.",
            "Try: network(name, type='corporate') or entity(name)",
        ),
    ]

    for title, sql, description, follow_up in queries:
        try:
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
# Lifespan
# ---------------------------------------------------------------------------

_shared_pool = None
_shared_catalog = {}


def _define_property_graphs(conn) -> None:
    """Define DuckPGQ property graphs. Safe to call only after all graph_*
    tables exist (either loaded from cache or freshly built)."""
    try:
        conn.execute("LOAD duckpgq")
    except Exception:
        return  # extension not available

    _graphs = [
        ("nyc_ownership", """
            CREATE OR REPLACE PROPERTY GRAPH nyc_ownership
            VERTEX TABLES (
                main.graph_owners PROPERTIES (owner_name, registrationid) LABEL Owner,
                main.graph_buildings PROPERTIES (bbl, borough, address, stories, units, year_built) LABEL Building,
                main.graph_violations PROPERTIES (violation_id, bbl, class, status, description, issued_date) LABEL Violation
            )
            EDGE TABLES (
                main.graph_owns
                    SOURCE KEY (owner_id) REFERENCES main.graph_owners (owner_id)
                    DESTINATION KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    LABEL Owns,
                main.graph_has_violation
                    SOURCE KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    DESTINATION KEY (violation_id) REFERENCES main.graph_violations (violation_id)
                    LABEL HasViolation,
                main.graph_shared_owner
                    SOURCE KEY (src_bbl) REFERENCES main.graph_buildings (bbl)
                    DESTINATION KEY (dst_bbl) REFERENCES main.graph_buildings (bbl)
                    LABEL SharedOwner
            )
        """),
        ("nyc_transactions", """
            CREATE OR REPLACE PROPERTY GRAPH nyc_transactions
            VERTEX TABLES (
                main.graph_tx_entities PROPERTIES (entity_name, party_type) LABEL TxEntity
            )
            EDGE TABLES (
                main.graph_tx_shared
                    SOURCE KEY (src) REFERENCES main.graph_tx_entities (entity_name)
                    DESTINATION KEY (dst) REFERENCES main.graph_tx_entities (entity_name)
                    LABEL SharedTransaction
            )
        """),
        ("nyc_corporate_web", """
            CREATE OR REPLACE PROPERTY GRAPH nyc_corporate_web
            VERTEX TABLES (
                main.graph_corps PROPERTIES (dos_id, corp_name, jurisdiction, formed, registered_agent) LABEL Corporation,
                main.graph_corp_people PROPERTIES (dos_id, person_name, title) LABEL CorpPerson
            )
            EDGE TABLES (
                main.graph_corp_officer_edges
                    SOURCE KEY (corp_id) REFERENCES main.graph_corps (dos_id)
                    DESTINATION KEY (other_corp_id) REFERENCES main.graph_corps (dos_id)
                    LABEL SharedOfficer,
                main.graph_corp_shared_officer
                    SOURCE KEY (corp1) REFERENCES main.graph_corps (dos_id)
                    DESTINATION KEY (corp2) REFERENCES main.graph_corps (dos_id)
                    LABEL OfficerOverlap
            )
        """),
        ("nyc_influence", """
            CREATE OR REPLACE PROPERTY GRAPH nyc_influence
            VERTEX TABLES (
                main.graph_pol_entities PROPERTIES (entity_name, role) LABEL PoliticalEntity
            )
            EDGE TABLES (
                main.graph_pol_donations
                    SOURCE KEY (donor) REFERENCES main.graph_pol_entities (entity_name)
                    DESTINATION KEY (recipient) REFERENCES main.graph_pol_entities (entity_name)
                    LABEL Donated
            )
        """),
        ("nyc_contractor_network", """
            CREATE OR REPLACE PROPERTY GRAPH nyc_contractor_network
            VERTEX TABLES (
                main.graph_contractors PROPERTIES (name, license_number, license_type) LABEL Contractor
            )
            EDGE TABLES (
                main.graph_contractor_shared
                    SOURCE KEY (c1) REFERENCES main.graph_contractors (name)
                    DESTINATION KEY (c2) REFERENCES main.graph_contractors (name)
                    LABEL SharedPermit,
                main.graph_contractor_building_shared
                    SOURCE KEY (c1) REFERENCES main.graph_contractors (name)
                    DESTINATION KEY (c2) REFERENCES main.graph_contractors (name)
                    LABEL SharedBuilding
            )
        """),
        ("nyc_officer_network", """
            CREATE OR REPLACE PROPERTY GRAPH nyc_officer_network
            VERTEX TABLES (
                main.graph_officers
                    PROPERTIES (officer_name, shield_no, rank, command)
                    LABEL Officer
            )
            EDGE TABLES (
                main.graph_officer_shared_command
                    SOURCE KEY (officer1) REFERENCES main.graph_officers (officer_name)
                    DESTINATION KEY (officer2) REFERENCES main.graph_officers (officer_name)
                    LABEL SharedCommand
            )
        """),
        ("nyc_tradewaste_network", """
            CREATE OR REPLACE PROPERTY GRAPH nyc_tradewaste_network
            VERTEX TABLES (
                main.graph_bic_companies
                    PROPERTIES (business_name, license_number, address, vehicles)
                    LABEL TradeWasteCompany
            )
            EDGE TABLES (
                main.graph_bic_shared_bbl
                    SOURCE KEY (c1) REFERENCES main.graph_bic_companies (business_name)
                    DESTINATION KEY (c2) REFERENCES main.graph_bic_companies (business_name)
                    LABEL SharedLocation
            )
        """),
    ]

    for name, sql in _graphs:
        try:
            conn.execute(sql)
            print(f"Property graph {name} created", flush=True)
        except Exception as e:
            print(f"Warning: {name} graph definition failed: {e}", flush=True)

    # COIB network — skip if tables not yet built (avoids hang)
    coib_tables = ["graph_coib_donors", "graph_coib_policymakers", "graph_coib_donor_edges"]
    try:
        coib_ok = all(
            conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{t}'").fetchone()[0] > 0
            for t in coib_tables
        )
    except Exception:
        coib_ok = False
    if coib_ok:
        try:
            conn.execute("""
                CREATE OR REPLACE PROPERTY GRAPH nyc_coib_network
                VERTEX TABLES (
                    main.graph_coib_donors
                        PROPERTIES (donor_name, donation_types, total_donated, donation_count, city, state)
                        LABEL COIBDonor,
                    main.graph_coib_policymakers
                        PROPERTIES (policymaker_name, agency, title, latest_year, years_active)
                        LABEL Policymaker
                )
                EDGE TABLES (
                    main.graph_coib_donor_edges
                        SOURCE KEY (donor_name) REFERENCES main.graph_coib_donors (donor_name)
                        DESTINATION KEY (recipient) REFERENCES main.graph_coib_policymakers (policymaker_name)
                        LABEL DonatesTo
                )
            """)
            print("Property graph nyc_coib_network created", flush=True)
        except Exception as e:
            print(f"Warning: nyc_coib_network graph definition failed: {e}", flush=True)


@lifespan
async def app_lifespan(server):
    global _shared_pool, _shared_catalog

    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute("PRAGMA disable_checkpoint_on_shutdown")
    try:
        conn.execute('PRAGMA wal_autocheckpoint="0"')
    except duckdb.Error:
        pass  # Not supported in DuckDB 1.5+

    from extensions import load_extensions
    ext_status = load_extensions(conn)

    # Performance tuning
    conn.execute("SET memory_limit = '8GB'")
    conn.execute("SET threads = 8")
    conn.execute("SET temp_directory = '/tmp/duckdb_temp'")
    conn.execute("SET max_temp_directory_size = '50GB'")
    conn.execute("SET preserve_insertion_order = false")

    # NYC timezone + case-insensitive search
    conn.execute("SET TimeZone = 'America/New_York'")
    conn.execute("SET default_collation = 'NOCASE'")
    print("Performance tuning applied (local filesystem, no S3)", flush=True)

    pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "''")
    conn.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
        AS lake
    """)
    print("DuckLake catalog attached", flush=True)

    # Warm up: first query triggers compactor which may crash on orphaned snapshots.
    # If it does, close and reconnect — the second attach skips the compactor.
    try:
        conn.execute("SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' LIMIT 1")
        print("DuckLake warm-up OK", flush=True)
    except Exception as e:
        print(f"DuckLake warm-up failed (expected on corrupted snapshots): {e}", flush=True)
        conn.close()
        conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        from extensions import load_extensions
        ext_status = load_extensions(conn)
        # Reconfigure performance tuning on new connection
        conn.execute("SET memory_limit = '8GB'")
        conn.execute("SET threads = 8")
        conn.execute("SET temp_directory = '/tmp/duckdb_temp'")
        conn.execute("SET max_temp_directory_size = '50GB'")
        conn.execute("SET TimeZone = 'America/New_York'")
        conn.execute("SET default_collation = 'NOCASE'")
        conn.execute(f"""
            ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
            AS lake
        """)
        print("DuckLake reconnected after warm-up failure", flush=True)
    # Note: enable_compaction option removed in DuckLake 1.5.1 — compaction runs automatically

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

    # Skip CHECKPOINT on startup — DuckLake 1.5.1 runs compaction automatically
    print("DuckLake startup complete (checkpoint skipped)", flush=True)

    # DuckDB UI disabled — FORCE INSTALL ui segfaults on 1.5.1 after extension update
    # TODO: re-enable when ui extension is fixed
    print("DuckDB UI disabled (segfault workaround)", flush=True)

    # ---------------------------------------------------------------------------
    # JSON-parsed views — extract lat/lon from GeoJSON and legacy location cols
    # ---------------------------------------------------------------------------
    _JSON_VIEWS = [
        # GeoJSON Point columns → lat, lon
        ("business", "nys_contractor_registry", "georeference", "geojson_point"),
        ("business", "nys_liquor_authority", "georeference", "geojson_point"),
        ("environment", "lead_service_lines", "location", "geojson_point"),
        ("environment", "nys_solar", "georeference", "geojson_point"),
        ("environment", "waste_transfer", "point", "geojson_point"),
        ("health", "cdc_places", "geolocation", "geojson_point"),
        ("health", "restaurant_inspections", "location", "geojson_point"),
        ("housing", "designated_buildings", "the_geom", "geojson_point"),
        ("public_safety", "criminal_court_summons", "geocoded_column", "geojson_point"),
        ("public_safety", "nypd_arrests_historic", "lon_lat", "geojson_point"),
        ("public_safety", "nypd_arrests_ytd", "geocoded_column", "geojson_point"),
        ("public_safety", "nypd_complaints_ytd", "geocoded_column", "geojson_point"),
        ("recreation", "canine_waste", "point", "geojson_point"),
        ("recreation", "drinking_fountains", "the_geom", "geojson_point"),
        ("recreation", "fishing_sites", "the_geom", "geojson_point"),
        ("recreation", "play_areas", "point", "geojson_point"),
        ("recreation", "signs", "point", "geojson_point"),
        ("recreation", "spray_showers", "point", "geojson_point"),
        ("social_services", "dycd_program_sites", "geocoded_column", "geojson_point"),
        # literacy_programs removed from Socrata — table no longer exists

        ("social_services", "n311_service_requests", "location", "geojson_point"),
        ("social_services", "nys_child_care", "georeference", "geojson_point"),
        ("transportation", "mta_entrances", "entrance_georeference", "geojson_point"),
        ("transportation", "pedestrian_ramps", "the_geom", "geojson_point"),
        ("financial", "nys_child_support_warrants", "georeference", "geojson_point"),
        ("financial", "nys_notaries", "georeference", "geojson_point"),
        ("financial", "nys_tax_warrants", "georeference", "geojson_point"),

        # Legacy Socrata location objects → lat, lon
        ("health", "health_facilities", "facility_location", "legacy_latlon"),
        ("health", "rodent_inspections", "location", "legacy_latlon"),
        ("public_safety", "nypd_complaints_historic", "lat_lon", "legacy_latlon"),
        ("public_safety", "nypd_complaints_ytd", "lat_lon", "legacy_latlon"),

        # Complex geometries → centroid (first coordinate pair)
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

    catalog = build_catalog(conn)
    set_catalog_cache(catalog, conn)
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

    # --- Persistent embeddings (hnsw_acorn in DuckDB file on volume) ---
    def _init_emb_db(emb_conn, dims):
        """Create embedding tables if they don't exist."""
        emb_conn.execute("SET hnsw_enable_experimental_persistence = true")
        emb_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS catalog_embeddings (
                schema_name VARCHAR, table_name VARCHAR, description VARCHAR,
                embedding FLOAT[{dims}]
            )
        """)
        emb_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS description_embeddings (
                source VARCHAR, description VARCHAR,
                embedding FLOAT[{dims}]
            )
        """)
        emb_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS entity_names (
                name VARCHAR, sources VARCHAR,
                entity_id VARCHAR,
                embedding FLOAT[{dims}]
            )
        """)
        emb_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS building_vectors (
                bbl VARCHAR, borough VARCHAR,
                features FLOAT[6]
            )
        """)

    def _build_catalog_embeddings(emb_conn, embed_batch, dims, read_conn=None):
        """Embed schema + table descriptions — schemas always rebuilt, tables incremental."""
        # --- Schema-level (always rebuild, tiny) ---
        schema_rows = []
        for schema_name, description in SCHEMA_DESCRIPTIONS.items():
            schema_rows.append((schema_name, "__schema__", description))

        # --- Table-level (incremental) ---
        table_rows = []
        if read_conn:
            try:
                existing = set()
                try:
                    existing_rows = emb_conn.execute(
                        "SELECT schema_name, table_name FROM catalog_embeddings WHERE table_name != '__schema__'"
                    ).fetchall()
                    existing = {(r[0], r[1]) for r in existing_rows}
                except Exception:
                    pass

                lake_tables = read_conn.execute("""
                    SELECT schema_name, table_name, comment
                    FROM duckdb_tables()
                    WHERE database_name = 'lake'
                      AND schema_name NOT IN ('information_schema', 'pg_catalog', 'ducklake', 'public')
                    ORDER BY schema_name, table_name
                """).fetchall()

                for schema, table, comment in lake_tables:
                    if (schema, table) in existing:
                        continue
                    desc = comment if comment else f"{schema} — {table.replace('_', ' ')} table"
                    table_rows.append((schema, table, desc))
            except Exception as e:
                print(f"  Warning: table description scan failed: {e}", flush=True)

        # --- Embed schemas (always) ---
        if schema_rows:
            texts = [r[2] for r in schema_rows]
            vecs = embed_batch(texts)
            emb_conn.execute("DELETE FROM catalog_embeddings WHERE table_name = '__schema__'")
            for (s, t, d), vec in zip(schema_rows, vecs):
                emb_conn.execute(
                    f"INSERT INTO catalog_embeddings VALUES (?, ?, ?, ?::FLOAT[{dims}])",
                    [s, t, d, vec.tolist()],
                )

        # --- Embed new tables (incremental) ---
        if table_rows:
            BATCH = 100
            for i in range(0, len(table_rows), BATCH):
                batch = table_rows[i:i + BATCH]
                texts = [r[2] for r in batch]
                vecs = embed_batch(texts)
                for (s, t, d), vec in zip(batch, vecs):
                    emb_conn.execute(
                        f"INSERT INTO catalog_embeddings VALUES (?, ?, ?, ?::FLOAT[{dims}])",
                        [s, t, d, vec.tolist()],
                    )

        total = emb_conn.execute("SELECT COUNT(*) FROM catalog_embeddings").fetchone()[0]
        new_tables = len(table_rows)
        print(f"  Catalog embeddings: {total} rows ({len(schema_rows)} schemas, {new_tables} new tables)", flush=True)

    def _build_description_embeddings(read_conn, emb_conn, embed_batch, dims):
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
                all_descs = {r[0] for r in read_conn.execute(sql).fetchall() if r[0]}
                existing = set()
                try:
                    existing = {r[0] for r in emb_conn.execute(
                        "SELECT description FROM description_embeddings WHERE source = ?",
                        [source_name],
                    ).fetchall()}
                except Exception:
                    pass
                new_descs = sorted(all_descs - existing)
                if not new_descs:
                    print(f"  Descriptions [{source_name}]: {len(existing)} existing, 0 new", flush=True)
                    continue
                print(f"  Descriptions [{source_name}]: {len(existing)} existing, {len(new_descs)} new -- embedding...", flush=True)
                if len(new_descs) > 500:
                    new_descs = new_descs[:500]
                    print(f"    Capped to 500 descriptions to stay within memory budget", flush=True)
                vecs = embed_batch(new_descs)
                for desc, vec in zip(new_descs, vecs):
                    emb_conn.execute(
                        f"INSERT INTO description_embeddings VALUES (?, ?, ?::FLOAT[{dims}])",
                        [source_name, desc, vec.tolist()],
                    )
                total_new += len(new_descs)
            except Exception as e:
                print(f"  Warning: descriptions [{source_name}] failed: {e}", flush=True)
        print(f"  Description embeddings: {total_new} new rows", flush=True)

    def _create_hnsw_indexes(emb_conn):
        """Create HNSW indexes — disabled until hnsw_acorn segfault is resolved.
        Brute-force array_cosine_distance works fine for current data volumes."""
        for table in ["entity_names", "description_embeddings", "catalog_embeddings"]:
            try:
                count = emb_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if count > 0:
                    print(f"  {table}: {count:,} rows (brute-force search)", flush=True)
            except Exception:
                pass

    try:
        if graph_cache_fresh():
            print("Loading graph tables from Parquet cache...", flush=True)
            load_graph_from_cache(conn)
            owner_count = conn.execute("SELECT COUNT(*) FROM main.graph_owners").fetchone()[0]
            building_count = conn.execute("SELECT COUNT(*) FROM main.graph_buildings").fetchone()[0]
            violation_count = conn.execute("SELECT COUNT(*) FROM main.graph_violations").fetchone()[0]
            owns_count = conn.execute("SELECT COUNT(*) FROM main.graph_owns").fetchone()[0]
            shared_count = conn.execute("SELECT COUNT(*) FROM main.graph_shared_owner").fetchone()[0]
            print(f"Graph loaded from cache: {owner_count:,} owners, {building_count:,} buildings, "
                  f"{violation_count:,} violations", flush=True)
            graph_ready = True
            _define_property_graphs(conn)
        else:
            print("Building graph tables from lake (no cache or stale)...", flush=True)

            # --- BEGIN GRAPH TABLE BUILD ---
            # (This is the full graph build from the original lifespan)
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
                SELECT DISTINCT
                    boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') AS bbl,
                    boroid AS borough,
                    COALESCE(housenumber, '') || ' ' || COALESCE(streetname, '') AS address,
                    1 AS num_buildings,
                    COALESCE(TRY_CAST(lifecycle AS VARCHAR), '') AS lifecycle,
                    0 AS stories, 0 AS units, 0 AS year_built
                FROM lake.housing.hpd_jurisdiction
                WHERE boroid IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
            """)
            # Enrich with PLUTO data
            try:
                conn.execute("""
                    UPDATE main.graph_buildings b
                    SET stories = COALESCE(TRY_CAST(p.numfloors AS INT), 0),
                        units = COALESCE(TRY_CAST(p.unitsres AS INT), 0),
                        year_built = COALESCE(TRY_CAST(p.yearbuilt AS INT), 0)
                    FROM lake.city_government.pluto p
                    WHERE b.bbl = (p.borocode || LPAD(p.block::VARCHAR, 5, '0') || LPAD(p.lot::VARCHAR, 4, '0'))
                """)
            except Exception as e:
                print(f"Warning: PLUTO enrichment failed: {e}", flush=True)
            building_count = conn.execute("SELECT COUNT(*) FROM main.graph_buildings").fetchone()[0]

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_owns AS
                SELECT DISTINCT
                    j.registrationid AS owner_id,
                    j.boroid || LPAD(j.block::VARCHAR, 5, '0') || LPAD(j.lot::VARCHAR, 4, '0') AS bbl
                FROM lake.housing.hpd_jurisdiction j
                WHERE j.registrationid IS NOT NULL
                  AND j.registrationid != '0'
                  AND j.boroid IS NOT NULL AND j.block IS NOT NULL AND j.lot IS NOT NULL
            """)
            owns_count = conn.execute("SELECT COUNT(*) FROM main.graph_owns").fetchone()[0]

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_violations AS
                SELECT
                    violationid AS violation_id,
                    boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') AS bbl,
                    violationid,
                    COALESCE(class, '') AS class,
                    COALESCE(currentstatus, '') AS status,
                    COALESCE(novdescription, '') AS description,
                    TRY_CAST(novissueddate AS DATE) AS issued_date,
                    COALESCE(currentstatus, '') AS currentstatus
                FROM lake.housing.hpd_violations
                WHERE boroid IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
            """)
            violation_count = conn.execute("SELECT COUNT(*) FROM main.graph_violations").fetchone()[0]

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_has_violation AS
                SELECT DISTINCT bbl, violation_id FROM main.graph_violations
            """)

            conn.execute("""
                CREATE OR REPLACE TABLE main.graph_shared_owner AS
                SELECT DISTINCT
                    a.bbl AS src_bbl, b.bbl AS dst_bbl, a.owner_id
                FROM main.graph_owns a
                JOIN main.graph_owns b ON a.owner_id = b.owner_id AND a.bbl < b.bbl
            """)
            shared_count = conn.execute("SELECT COUNT(*) FROM main.graph_shared_owner").fetchone()[0]

            print(f"Core graph: {owner_count:,} owners, {building_count:,} buildings, "
                  f"{violation_count:,} violations, {owns_count:,} owns, {shared_count:,} shared", flush=True)

            # Building flags (AEP, rent stabilization, etc.)
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_building_flags AS
                    SELECT
                        boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') AS bbl,
                        BOOL_OR(CASE WHEN program = 'AEP' THEN TRUE ELSE FALSE END) AS aep,
                        BOOL_OR(CASE WHEN program = '7A' THEN TRUE ELSE FALSE END) AS seven_a,
                        BOOL_OR(CASE WHEN program = 'CONH' THEN TRUE ELSE FALSE END) AS conh
                    FROM lake.housing.hpd_building_programs
                    WHERE boroid IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
                    GROUP BY 1
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_building_flags (bbl VARCHAR, aep BOOLEAN, seven_a BOOLEAN, conh BOOLEAN)")
                print(f"Warning: building flags: {e}", flush=True)

            # ACRIS sales
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_acris_sales AS
                    SELECT
                        (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                        TRY_CAST(m.document_date AS DATE) AS sale_date,
                        TRY_CAST(m.document_amt AS DOUBLE) AS price,
                        m.doc_type,
                        m.document_id
                    FROM lake.housing.acris_master m
                    JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
                    WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
                      AND TRY_CAST(m.document_amt AS DOUBLE) > 0
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_acris_sales (bbl VARCHAR, sale_date DATE, price DOUBLE, doc_type VARCHAR, document_id VARCHAR)")
                print(f"Warning: ACRIS sales: {e}", flush=True)

            # Rent stabilization
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_rent_stabilization AS
                    SELECT
                        boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') AS bbl,
                        uc2007, uc2008, uc2009, uc2010, uc2011, uc2012, uc2013, uc2014,
                        uc2015, uc2016, uc2017, uc2018, uc2019, uc2020, uc2021, uc2022, uc2023
                    FROM lake.housing.rent_stabilization
                    WHERE boroid IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_rent_stabilization (bbl VARCHAR)")
                print(f"Warning: rent stabilization: {e}", flush=True)

            # Corp contacts
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_corp_contacts AS
                    SELECT
                        registrationid,
                        type,
                        COALESCE(corporationname, TRIM(COALESCE(firstname, '') || ' ' || COALESCE(lastname, ''))) AS contact_name
                    FROM lake.housing.hpd_registration_contacts
                    WHERE type IN ('CorporateOwner', 'IndividualOwner', 'Agent', 'Officer', 'SiteManager', 'HeadOfficer')
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_corp_contacts (registrationid VARCHAR, type VARCHAR, contact_name VARCHAR)")
                print(f"Warning: corp contacts: {e}", flush=True)

            # Business at building
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_business_at_building AS
                    SELECT DISTINCT
                        license_nbr,
                        business_name,
                        (CASE WHEN borough = 'Manhattan' THEN '1'
                              WHEN borough = 'Bronx' THEN '2'
                              WHEN borough = 'Brooklyn' THEN '3'
                              WHEN borough = 'Queens' THEN '4'
                              WHEN borough = 'Staten Island' THEN '5'
                              ELSE '0' END) || LPAD(COALESCE(TRY_CAST(block AS VARCHAR), '00000'), 5, '0') || LPAD(COALESCE(TRY_CAST(lot AS VARCHAR), '0000'), 4, '0') AS bbl
                    FROM lake.business.dca_licenses
                    WHERE block IS NOT NULL AND lot IS NOT NULL
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_business_at_building (license_nbr VARCHAR, business_name VARCHAR, bbl VARCHAR)")
                print(f"Warning: business at building: {e}", flush=True)

            # ACRIS chain (buyer/seller)
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_acris_chain AS
                    SELECT
                        (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                        p.name AS party_name,
                        p.party_type,
                        m.document_id,
                        TRY_CAST(m.document_date AS DATE) AS doc_date,
                        TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                        m.doc_type
                    FROM lake.housing.acris_parties p
                    JOIN lake.housing.acris_master m ON p.document_id = m.document_id
                    JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
                    WHERE p.party_type IN ('1', '2')
                      AND m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP', 'MTGE', 'AGMT', 'ASST')
                      AND p.name IS NOT NULL AND LENGTH(p.name) > 1
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_acris_chain (bbl VARCHAR, party_name VARCHAR, party_type VARCHAR, document_id VARCHAR, doc_date DATE, amount DOUBLE, doc_type VARCHAR)")
                print(f"Warning: ACRIS chain: {e}", flush=True)

            # DOB owners
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_dob_owners AS
                    SELECT DISTINCT
                        (CASE WHEN borough = 'MANHATTAN' THEN '1'
                              WHEN borough = 'BRONX' THEN '2'
                              WHEN borough = 'BROOKLYN' THEN '3'
                              WHEN borough = 'QUEENS' THEN '4'
                              WHEN borough = 'STATEN ISLAND' THEN '5'
                              ELSE '0' END) || LPAD(COALESCE(block, '00000'), 5, '0') || LPAD(COALESCE(lot, '0000'), 4, '0') AS bbl,
                        COALESCE(owner_s_first_name, '') || ' ' || COALESCE(owner_s_last_name, '') AS owner_name,
                        owner_s_business_name AS business_name
                    FROM lake.housing.dob_now_permits
                    WHERE (owner_s_last_name IS NOT NULL OR owner_s_business_name IS NOT NULL)
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_dob_owners (bbl VARCHAR, owner_name VARCHAR, business_name VARCHAR)")
                print(f"Warning: DOB owners: {e}", flush=True)

            # Eviction petitioners
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_eviction_petitioners AS
                    SELECT DISTINCT
                        eviction_address AS address,
                        court_index_number,
                        executed_date
                    FROM lake.housing.evictions
                    WHERE eviction_address IS NOT NULL
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_eviction_petitioners (address VARCHAR, court_index_number VARCHAR, executed_date VARCHAR)")
                print(f"Warning: eviction petitioners: {e}", flush=True)

            # Doing business
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_doing_business AS
                    SELECT DISTINCT
                        vendorname AS vendor_name,
                        transactiondescription AS description,
                        TRY_CAST(transactionamount AS DOUBLE) AS amount
                    FROM lake.city_government.doing_business
                    WHERE vendorname IS NOT NULL
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_doing_business (vendor_name VARCHAR, description VARCHAR, amount DOUBLE)")
                print(f"Warning: doing business: {e}", flush=True)

            # Campaign donors
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_campaign_donors AS
                    SELECT
                        COALESCE(name, '') AS donor_name,
                        COALESCE(recipname, '') AS recipient,
                        TRY_CAST(amnt AS DOUBLE) AS amount,
                        TRY_CAST(date AS DATE) AS donation_date,
                        COALESCE(officecd, '') AS office_code,
                        COALESCE(borough, '') AS donor_borough
                    FROM lake.city_government.campaign_contributions
                    WHERE name IS NOT NULL AND LENGTH(name) > 1
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_campaign_donors (donor_name VARCHAR, recipient VARCHAR, amount DOUBLE, donation_date DATE, office_code VARCHAR, donor_borough VARCHAR)")
                print(f"Warning: campaign donors: {e}", flush=True)

            # EPA facilities
            try:
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_epa_facilities AS
                    SELECT DISTINCT
                        fac_name AS facility_name,
                        fac_street AS street,
                        fac_city AS city,
                        fac_zip AS zip,
                        fac_county AS county,
                        COALESCE(fac_lat, 0) AS lat,
                        COALESCE(fac_long, 0) AS lon,
                        COALESCE(fac_qtrs_with_nc, 0) AS quarters_noncompliant,
                        COALESCE(fac_inspection_count, 0) AS inspection_count,
                        COALESCE(fac_formal_action_count, 0) AS formal_actions,
                        COALESCE(fac_penalty_count, 0) AS penalties
                    FROM lake.environment.epa_echo_facilities
                    WHERE fac_state = 'NY'
                """)
            except Exception as e:
                conn.execute("CREATE OR REPLACE TABLE main.graph_epa_facilities (facility_name VARCHAR, street VARCHAR, city VARCHAR, zip VARCHAR, county VARCHAR, lat DOUBLE, lon DOUBLE, quarters_noncompliant INT, inspection_count INT, formal_actions INT, penalties INT)")
                print(f"Warning: EPA facilities: {e}", flush=True)

            print("Graph table build: core tables complete", flush=True)

            # Mark graph ready with core tables — extended tables build in background
            graph_ready = True

            # --- Extended graph tables (background thread — heavy ACRIS joins) ---
            def _build_extended_graph():
                try:
                    # --- Transaction network ---
                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_tx_entities AS
                            SELECT DISTINCT name AS entity_name, party_type
                            FROM lake.housing.acris_parties
                            WHERE name IS NOT NULL AND LENGTH(name) > 1
                            AND party_type IN ('1', '2')
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_tx_edges AS
                            SELECT
                                p.name AS entity_name, p.party_type,
                                (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                                m.document_id, TRY_CAST(m.document_date AS DATE) AS doc_date,
                                TRY_CAST(m.document_amt AS DOUBLE) AS amount, m.doc_type
                            FROM lake.housing.acris_parties p
                            JOIN lake.housing.acris_master m ON p.document_id = m.document_id
                            JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
                            WHERE p.name IS NOT NULL AND LENGTH(p.name) > 1
                            AND m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP', 'MTGE', 'AGMT', 'ASST')
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_tx_shared AS
                            SELECT DISTINCT a.entity_name AS src, b.entity_name AS dst, a.document_id
                            FROM main.graph_tx_edges a
                            JOIN main.graph_tx_edges b ON a.document_id = b.document_id
                            AND a.entity_name < b.entity_name
                        """)
                    except Exception as e:
                        for t in ["graph_tx_entities", "graph_tx_edges", "graph_tx_shared"]:
                            try:
                                conn.execute(f"CREATE OR REPLACE TABLE main.{t} (dummy VARCHAR)")
                            except Exception:
                                pass
                        print(f"Warning: transaction network: {e}", flush=True)

                    # --- Corporate web ---
                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_corps AS
                            SELECT DISTINCT
                                dos_id, current_entity_name AS corp_name,
                                COALESCE(jurisdiction, '') AS jurisdiction,
                                COALESCE(entity_formation_date, '') AS formed,
                                COALESCE(dos_process_name, '') AS registered_agent
                            FROM lake.business.nys_corporations
                            WHERE current_entity_name IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_corp_people AS
                            SELECT DISTINCT
                                dos_id, COALESCE(name, '') AS person_name, COALESCE(title, '') AS title
                            FROM lake.business.nys_corporation_contacts
                            WHERE name IS NOT NULL AND LENGTH(name) > 1
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_corp_officer_edges AS
                            SELECT DISTINCT a.dos_id AS corp_id, a.person_name, b.dos_id AS other_corp_id
                            FROM main.graph_corp_people a
                            JOIN main.graph_corp_people b ON UPPER(a.person_name) = UPPER(b.person_name)
                            AND a.dos_id != b.dos_id
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_corp_shared_officer AS
                            SELECT DISTINCT a.dos_id AS corp1, b.dos_id AS corp2, a.person_name
                            FROM main.graph_corp_people a
                            JOIN main.graph_corp_people b ON UPPER(a.person_name) = UPPER(b.person_name)
                            AND a.dos_id < b.dos_id
                        """)
                    except Exception as e:
                        for t in ["graph_corps", "graph_corp_people", "graph_corp_officer_edges", "graph_corp_shared_officer"]:
                            try:
                                conn.execute(f"CREATE OR REPLACE TABLE main.{t} (dummy VARCHAR)")
                            except Exception:
                                pass
                        print(f"Warning: corporate web: {e}", flush=True)

                    # --- Influence network ---
                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_pol_entities AS
                            SELECT DISTINCT COALESCE(name, '') AS entity_name, 'donor' AS role
                            FROM lake.city_government.campaign_contributions WHERE name IS NOT NULL
                            UNION
                            SELECT DISTINCT COALESCE(recipname, '') AS entity_name, 'candidate' AS role
                            FROM lake.city_government.campaign_contributions WHERE recipname IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_pol_donations AS
                            SELECT COALESCE(name, '') AS donor, COALESCE(recipname, '') AS recipient,
                                   TRY_CAST(amnt AS DOUBLE) AS amount, TRY_CAST(date AS DATE) AS date
                            FROM lake.city_government.campaign_contributions
                            WHERE name IS NOT NULL AND recipname IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_pol_contracts AS
                            SELECT DISTINCT
                                vendorname AS vendor, agencyname AS agency,
                                TRY_CAST(currentamount AS DOUBLE) AS amount
                            FROM lake.city_government.contract_awards
                            WHERE vendorname IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_pol_lobbying AS
                            SELECT DISTINCT
                                COALESCE(lobbyist_name, '') AS lobbyist,
                                COALESCE(client_name, '') AS client,
                                COALESCE(government_body, '') AS target
                            FROM lake.city_government.nys_lobbyist_registration
                            WHERE lobbyist_name IS NOT NULL
                        """)
                    except Exception as e:
                        for t in ["graph_pol_entities", "graph_pol_donations", "graph_pol_contracts", "graph_pol_lobbying"]:
                            try:
                                conn.execute(f"CREATE OR REPLACE TABLE main.{t} (dummy VARCHAR)")
                            except Exception:
                                pass
                        print(f"Warning: influence network: {e}", flush=True)

                    # --- Contractor network ---
                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_contractors AS
                            SELECT DISTINCT
                                licensee_s_business_name AS name,
                                license_number, license_type
                            FROM lake.housing.dob_now_permits
                            WHERE licensee_s_business_name IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_permit_edges AS
                            SELECT
                                licensee_s_business_name AS contractor,
                                job_filing_number AS permit,
                                (CASE WHEN borough = 'MANHATTAN' THEN '1'
                                      WHEN borough = 'BRONX' THEN '2'
                                      WHEN borough = 'BROOKLYN' THEN '3'
                                      WHEN borough = 'QUEENS' THEN '4'
                                      WHEN borough = 'STATEN ISLAND' THEN '5'
                                      ELSE '0' END) || LPAD(COALESCE(block, '00000'), 5, '0') || LPAD(COALESCE(lot, '0000'), 4, '0') AS bbl
                            FROM lake.housing.dob_now_permits
                            WHERE licensee_s_business_name IS NOT NULL AND block IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_contractor_shared AS
                            SELECT DISTINCT a.contractor AS c1, b.contractor AS c2, a.permit
                            FROM main.graph_permit_edges a
                            JOIN main.graph_permit_edges b ON a.permit = b.permit AND a.contractor < b.contractor
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_contractor_building_shared AS
                            SELECT DISTINCT a.contractor AS c1, b.contractor AS c2, a.bbl
                            FROM main.graph_permit_edges a
                            JOIN main.graph_permit_edges b ON a.bbl = b.bbl AND a.contractor < b.contractor
                        """)
                    except Exception as e:
                        for t in ["graph_contractors", "graph_permit_edges", "graph_contractor_shared", "graph_contractor_building_shared"]:
                            try:
                                conn.execute(f"CREATE OR REPLACE TABLE main.{t} (dummy VARCHAR)")
                            except Exception:
                                pass
                        print(f"Warning: contractor network: {e}", flush=True)

                    # --- FEC + litigation ---
                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_fec_contributions AS
                            SELECT
                                COALESCE(contributor_name, '') AS donor_name,
                                COALESCE(committee_name, '') AS committee,
                                TRY_CAST(contribution_receipt_amount AS DOUBLE) AS amount,
                                TRY_CAST(contribution_receipt_date AS DATE) AS date,
                                COALESCE(contributor_city, '') AS city,
                                COALESCE(contributor_state, '') AS state,
                                COALESCE(contributor_employer, '') AS employer,
                                COALESCE(contributor_occupation, '') AS occupation
                            FROM lake.federal.fec_contributions
                            WHERE contributor_name IS NOT NULL
                        """)
                    except Exception as e:
                        conn.execute("CREATE OR REPLACE TABLE main.graph_fec_contributions (donor_name VARCHAR, committee VARCHAR, amount DOUBLE, date DATE, city VARCHAR, state VARCHAR, employer VARCHAR, occupation VARCHAR)")
                        print(f"Warning: FEC contributions: {e}", flush=True)

                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_litigation_respondents AS
                            SELECT DISTINCT
                                COALESCE(respondentname, '') AS respondent_name,
                                COALESCE(penaltyapplied, '') AS penalty,
                                TRY_CAST(penaltybalancedue AS DOUBLE) AS balance_due,
                                COALESCE(violationdate, '') AS violation_date
                            FROM lake.city_government.oath_hearings
                            WHERE respondentname IS NOT NULL AND LENGTH(respondentname) > 1
                        """)
                    except Exception as e:
                        conn.execute("CREATE OR REPLACE TABLE main.graph_litigation_respondents (respondent_name VARCHAR, penalty VARCHAR, balance_due DOUBLE, violation_date VARCHAR)")
                        print(f"Warning: litigation respondents: {e}", flush=True)

                    # --- Officer misconduct ---
                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_officers AS
                            SELECT DISTINCT
                                COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') AS officer_name,
                                COALESCE(shield_no, 0) AS shield_no,
                                COALESCE(rank_incident, '') AS rank,
                                COALESCE(command_at_incident, '') AS command
                            FROM lake.public_safety.ccrb_complaints
                            WHERE last_name IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_officer_complaints AS
                            SELECT
                                COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') AS officer_name,
                                COALESCE(allegation, '') AS allegation,
                                COALESCE(fado_type, '') AS category,
                                COALESCE(board_disposition, '') AS disposition
                            FROM lake.public_safety.ccrb_complaints
                            WHERE last_name IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_officer_shared_command AS
                            SELECT DISTINCT
                                a.officer_name AS officer1, b.officer_name AS officer2, a.command
                            FROM main.graph_officers a
                            JOIN main.graph_officers b ON a.command = b.command AND a.officer_name < b.officer_name
                            WHERE a.command != '' AND a.command IS NOT NULL
                        """)
                    except Exception as e:
                        for t in ["graph_officers", "graph_officer_complaints", "graph_officer_shared_command"]:
                            try:
                                conn.execute(f"CREATE OR REPLACE TABLE main.{t} (dummy VARCHAR)")
                            except Exception:
                                pass
                        print(f"Warning: officer misconduct: {e}", flush=True)

                    # --- COIB pay-to-play ---
                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_coib_donors AS
                            SELECT
                                donor_name,
                                STRING_AGG(DISTINCT donation_type, ', ') AS donation_types,
                                SUM(amount) AS total_donated,
                                COUNT(*) AS donation_count,
                                FIRST(city) AS city,
                                FIRST(state) AS state
                            FROM lake.city_government.coib_donations
                            WHERE donor_name IS NOT NULL
                            GROUP BY donor_name
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_coib_policymakers AS
                            SELECT
                                policymaker_name,
                                FIRST(agency) AS agency,
                                FIRST(title) AS title,
                                MAX(year) AS latest_year,
                                COUNT(DISTINCT year) AS years_active
                            FROM lake.city_government.coib_donations
                            WHERE policymaker_name IS NOT NULL
                            GROUP BY policymaker_name
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_coib_donor_edges AS
                            SELECT DISTINCT
                                donor_name, policymaker_name AS recipient, donation_type, amount
                            FROM lake.city_government.coib_donations
                            WHERE donor_name IS NOT NULL AND policymaker_name IS NOT NULL
                        """)
                    except Exception as e:
                        for t in ["graph_coib_donors", "graph_coib_policymakers", "graph_coib_donor_edges"]:
                            try:
                                conn.execute(f"CREATE OR REPLACE TABLE main.{t} (dummy VARCHAR)")
                            except Exception:
                                pass
                        print(f"Warning: COIB: {e}", flush=True)

                    # --- BIC trade waste ---
                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_bic_companies AS
                            SELECT DISTINCT
                                business_name, license_number,
                                COALESCE(business_address, '') AS address,
                                COALESCE(TRY_CAST(number_of_vehicles AS INT), 0) AS vehicles
                            FROM lake.business.bic_trade_waste
                            WHERE business_name IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_bic_violation_edges AS
                            SELECT
                                business_name, violation_type_code, violation_description,
                                TRY_CAST(violation_date AS DATE) AS date,
                                TRY_CAST(penalty_amount AS DOUBLE) AS penalty
                            FROM lake.business.bic_trade_waste
                            WHERE business_name IS NOT NULL AND violation_type_code IS NOT NULL
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_bic_shared_bbl AS
                            SELECT DISTINCT a.business_name AS c1, b.business_name AS c2
                            FROM main.graph_bic_companies a
                            JOIN main.graph_bic_companies b ON a.address = b.address AND a.business_name < b.business_name
                            WHERE a.address != ''
                        """)
                    except Exception as e:
                        for t in ["graph_bic_companies", "graph_bic_violation_edges", "graph_bic_shared_bbl"]:
                            try:
                                conn.execute(f"CREATE OR REPLACE TABLE main.{t} (dummy VARCHAR)")
                            except Exception:
                                pass
                        print(f"Warning: BIC trade waste: {e}", flush=True)

                    # --- DOB violations (respondent-named) ---
                    try:
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_dob_respondents AS
                            SELECT DISTINCT
                                respondent_name,
                                COUNT(*) AS violation_count,
                                SUM(TRY_CAST(penalty_balance_due AS DOUBLE)) AS total_penalties
                            FROM lake.housing.dob_ecb_violations
                            WHERE respondent_name IS NOT NULL AND LENGTH(respondent_name) > 2
                            GROUP BY respondent_name
                        """)
                        conn.execute("""
                            CREATE OR REPLACE TABLE main.graph_dob_respondent_bbl AS
                            SELECT DISTINCT
                                respondent_name,
                                (CASE WHEN boro = '1' THEN '1'
                                      WHEN boro = 'MANHATTAN' THEN '1'
                                      WHEN boro = '2' THEN '2'
                                      WHEN boro = 'BRONX' THEN '2'
                                      WHEN boro = '3' THEN '3'
                                      WHEN boro = 'BROOKLYN' THEN '3'
                                      WHEN boro = '4' THEN '4'
                                      WHEN boro = 'QUEENS' THEN '4'
                                      WHEN boro = '5' THEN '5'
                                      WHEN boro = 'STATEN ISLAND' THEN '5'
                                      ELSE boro END) || LPAD(COALESCE(block, '00000'), 5, '0') || LPAD(COALESCE(lot, '0000'), 4, '0') AS bbl
                            FROM lake.housing.dob_ecb_violations
                            WHERE respondent_name IS NOT NULL AND LENGTH(respondent_name) > 2
                              AND block IS NOT NULL AND lot IS NOT NULL
                        """)
                    except Exception as e:
                        for t in ["graph_dob_respondents", "graph_dob_respondent_bbl"]:
                            try:
                                conn.execute(f"CREATE OR REPLACE TABLE main.{t} (dummy VARCHAR)")
                            except Exception:
                                pass
                        print(f"Warning: DOB respondents: {e}", flush=True)

                    # Save graph to Parquet cache for fast restart
                    try:
                        save_graph_to_cache(conn)
                        print("Graph tables saved to Parquet cache", flush=True)
                    except Exception as e:
                        print(f"Warning: graph cache save failed: {e}", flush=True)

                    # DuckPGQ property graph definitions — run here in the background
                    # thread so we don't race with the main thread on the shared conn.
                    _define_property_graphs(conn)

                    print("Extended graph tables complete", flush=True)
                except Exception as e:
                    print(f"Warning: extended graph build failed: {e}", flush=True)

            extended_graph_thread = threading.Thread(target=_build_extended_graph, daemon=True)
            extended_graph_thread.start()
            print("Extended graph build started in background thread", flush=True)

            # --- END GRAPH TABLE BUILD ---

    except Exception as e:
        print(f"Warning: graph build/load failed: {e}", flush=True)

    # PostHog analytics — track MCP tool usage
    ph_key = os.environ.get("POSTHOG_API_KEY", "")
    if ph_key:
        posthog.api_key = ph_key
        posthog.host = os.environ.get("POSTHOG_HOST", "https://us.i.posthog.com")
        posthog.debug = False
        print("PostHog analytics enabled", flush=True)
    else:
        print("PostHog analytics disabled (no POSTHOG_API_KEY)", flush=True)

    # Explorations — defer to background (avoids contention with extended graph thread)
    explorations = []

    # Percentile tables — build in background to avoid contention with extended graph thread
    percentiles_ready = False

    def _build_percentiles_bg():
        nonlocal percentiles_ready
        # Wait for extended graph thread to finish first (avoids DuckDB write contention)
        if 'extended_graph_thread' in dir():
            extended_graph_thread.join(timeout=600)
        try:
            build_percentile_tables(conn)
            print("Percentile tables built (owners + buildings)", flush=True)
            percentiles_ready = True
        except Exception as e:
            print(f"Warning: Percentile table build failed: {e}", flush=True)
        try:
            build_lake_percentile_tables(conn)
            print("Lake percentile tables built (restaurants + ZIPs + precincts)", flush=True)
        except Exception as e:
            print(f"Warning: Lake percentile tables failed: {e}", flush=True)

    percentile_thread = threading.Thread(target=_build_percentiles_bg, daemon=True)
    percentile_thread.start()
    print("Percentile build started in background thread", flush=True)

    # --- Vector embeddings for semantic search (hnsw_acorn, persistent DuckDB) ---
    embed_fn = None
    embed_dims = 256
    emb_conn = None
    try:
        from embedder import create_embedder
        embed_fn, embed_batch_fn, embed_dims = create_embedder("/app/model")
        print(f"Embedding model loaded ({embed_dims} dims)", flush=True)
    except Exception as e:
        print(f"Warning: embedding model unavailable: {e}", flush=True)

    # Open persistent embeddings DB (shared by background writer + query readers)
    try:
        import duckdb as _duckdb
        pathlib.Path(EMBEDDINGS_DB).parent.mkdir(parents=True, exist_ok=True)
        emb_conn = _duckdb.connect(EMBEDDINGS_DB, config={"allow_unsigned_extensions": "true"})
        emb_conn.execute("INSTALL hnsw_acorn FROM community; LOAD hnsw_acorn;")
        emb_conn.execute("SET hnsw_enable_experimental_persistence = true")
        print("hnsw_acorn loaded in embeddings DB", flush=True)
        _init_emb_db(emb_conn, embed_dims)

        # Check dimension mismatch — wipe tables if dims changed
        for tbl in ["catalog_embeddings", "description_embeddings", "entity_names"]:
            try:
                row = emb_conn.execute(f"SELECT embedding FROM {tbl} LIMIT 1").fetchone()
                if row and len(row[0]) != embed_dims:
                    print(f"  Dimension mismatch on {tbl}: {len(row[0])}d -> {embed_dims}d, wiping...", flush=True)
                    emb_conn.execute(f"DROP INDEX IF EXISTS idx_{tbl}")
                    emb_conn.execute(f"DELETE FROM {tbl}")
            except Exception:
                pass

        print(f"Embeddings DB opened: {EMBEDDINGS_DB}", flush=True)
    except Exception as e:
        print(f"Warning: embeddings DB unavailable: {e}", flush=True)
        emb_conn = None

    def _build_entity_name_embeddings(read_conn, emb_conn, embed_batch, dims):
        """Embed entity names from name_index — two passes:
        Pass 1: names in 3+ lake tables (high-value cross-refs, ~1.1M, ~1.6h)
        Pass 2: names in exactly 2 tables (backfill, ~1.8M, ~2.5h)
        Incremental and resume-safe. Checkpoints every 50K names.
        Joins entity_master (when available) to attach entity_id."""

        # Fast early exit: if embeddings are already populated, skip the expensive scan.
        # The scan loads 2.9M names into memory and holds the main conn for minutes.
        existing_count = 0
        try:
            existing_count = emb_conn.execute("SELECT COUNT(*) FROM entity_names").fetchone()[0]
        except Exception:
            pass

        if existing_count > 100_000:
            print(f"  Entity names: {existing_count:,} already embedded — skipping scan", flush=True)
            print(f"  (To force rebuild, run: DELETE FROM entity_names)", flush=True)
            return

        existing = set()
        try:
            existing = {r[0] for r in emb_conn.execute("SELECT name FROM entity_names").fetchall()}
        except Exception:
            pass
        print(f"  Entity names: {len(existing):,} already embedded", flush=True)

        # Check if entity_master is available for entity_id join
        has_entity_master = False
        try:
            read_conn.execute("SELECT 1 FROM lake.foundation.entity_master LIMIT 1")
            has_entity_master = True
            print("  Entity master available — will attach entity_id", flush=True)
        except Exception:
            print("  Entity master not yet available — entity_id will be NULL", flush=True)

        # Drop HNSW index during bulk insert for speed
        try:
            emb_conn.execute("DROP INDEX IF EXISTS idx_entity_names")
        except Exception:
            pass

        CHUNK_SIZE = 200_000  # fetch from lake in chunks to avoid OOM
        grand_total = 0
        t0 = time.time()

        for min_tables, label in [(3, "3+ tables (high-value)"), (2, "2+ tables (backfill)")]:
            print(f"  Pass: {label}...", flush=True)

            # Stream names in chunks using OFFSET/LIMIT
            offset = 0
            pass_new = 0

            # Build query with optional entity_master join
            if has_entity_master:
                query_template = f"""
                    SELECT dn.name, dn.sources, em.entity_id
                    FROM (
                        SELECT
                            UPPER(TRIM(last_name)) || ', ' || UPPER(TRIM(first_name)) AS name,
                            UPPER(TRIM(last_name)) AS u_last,
                            UPPER(TRIM(first_name)) AS u_first,
                            STRING_AGG(DISTINCT source_table, ',' ORDER BY source_table) AS sources
                        FROM lake.federal.name_index
                        WHERE last_name IS NOT NULL AND LENGTH(last_name) >= 2
                          AND first_name IS NOT NULL AND LENGTH(first_name) >= 1
                        GROUP BY UPPER(TRIM(last_name)), UPPER(TRIM(first_name))
                        HAVING COUNT(DISTINCT source_table) >= {{min_tables}}
                        LIMIT {{chunk}} OFFSET {{offset}}
                    ) dn
                    LEFT JOIN lake.foundation.entity_master em
                        ON em.canonical_last = dn.u_last
                        AND em.canonical_first = dn.u_first
                """
            else:
                query_template = f"""
                    SELECT
                        UPPER(TRIM(last_name)) || ', ' || UPPER(TRIM(first_name)) AS name,
                        STRING_AGG(DISTINCT source_table, ',' ORDER BY source_table) AS sources,
                        NULL AS entity_id
                    FROM lake.federal.name_index
                    WHERE last_name IS NOT NULL AND LENGTH(last_name) >= 2
                      AND first_name IS NOT NULL AND LENGTH(first_name) >= 1
                    GROUP BY UPPER(TRIM(last_name)), UPPER(TRIM(first_name))
                    HAVING COUNT(DISTINCT source_table) >= {{min_tables}}
                    LIMIT {{chunk}} OFFSET {{offset}}
                """

            while True:
                try:
                    rows = read_conn.execute(
                        query_template.format(min_tables=min_tables, chunk=CHUNK_SIZE, offset=offset)
                    ).fetchall()
                except Exception as e:
                    print(f"  Entity names query failed at offset {offset}: {e}", flush=True)
                    break

                if not rows:
                    break

                new_entries = [(r[0], r[1], r[2]) for r in rows if r[0] and r[0] not in existing]
                print(f"    Chunk @{offset:,}: {len(rows):,} fetched, {len(new_entries):,} new", flush=True)

                for i in range(0, len(new_entries), 5000):
                    batch = new_entries[i:i + 5000]
                    names = [n for n, s, e in batch]
                    sources = [s for n, s, e in batch]
                    entity_ids = [str(e) if e else None for n, s, e in batch]
                    try:
                        vecs = embed_batch(names)
                        import pyarrow as pa
                        tbl = pa.table({
                            "name": pa.array(names, type=pa.utf8()),
                            "sources": pa.array(sources, type=pa.utf8()),
                            "entity_id": pa.array(entity_ids, type=pa.utf8()),
                            "embedding": pa.array([v.tolist() for v in vecs], type=pa.list_(pa.float32(), dims)),
                        })
                        emb_conn.execute("INSERT INTO entity_names SELECT * FROM tbl")
                        existing.update(names)
                        grand_total += len(batch)
                        pass_new += len(batch)
                    except Exception as e:
                        print(f"    Warning: embed batch failed: {e}", flush=True)
                        time.sleep(5)

                    if grand_total % 5000 < 100:
                        elapsed = time.time() - t0
                        rate = grand_total / elapsed if elapsed > 0 else 0
                        print(f"    Progress: {grand_total:,} total ({rate:.0f}/sec)", flush=True)

                    if grand_total % 50000 < 100:
                        emb_conn.execute("CHECKPOINT")

                offset += CHUNK_SIZE
                if len(rows) < CHUNK_SIZE:
                    break

            emb_conn.execute("CHECKPOINT")
            print(f"  {label}: {pass_new:,} embedded", flush=True)

        elapsed = time.time() - t0
        print(f"  Entity names complete: {grand_total:,} new in {elapsed/60:.1f}m", flush=True)

    if embed_fn is not None and emb_conn is not None:
        def _background_embed():
            try:
                read_cursor = conn.cursor()

                print("Background: building embeddings (hnsw_acorn)...", flush=True)
                _build_catalog_embeddings(emb_conn, embed_batch_fn, embed_dims, read_conn=conn)
                _build_description_embeddings(read_cursor, emb_conn, embed_batch_fn, embed_dims)
                _build_entity_name_embeddings(read_cursor, emb_conn, embed_batch_fn, embed_dims)
                _create_hnsw_indexes(emb_conn)
                emb_conn.execute("CHECKPOINT")
                print("Background: embedding pipeline complete", flush=True)
            except Exception as e:
                print(f"Background: embedding error: {e}", flush=True)

        embed_thread = threading.Thread(target=_background_embed, daemon=True)
        embed_thread.start()
        print("Embedding pipeline started in background", flush=True)

    pool = CursorPool(conn, size=24)
    _shared_pool = pool
    _shared_catalog = catalog
    try:
        # Build catalog_json for CitationMiddleware (flat list of {schema, table, rows})
        catalog_json = {"tables": [
            {"schema": schema, "table": table, "rows": info.get("row_count", 0)}
            for schema, tables in catalog.items()
            for table, info in tables.items()
        ]}

        # Load pipeline state for FreshnessMiddleware (stale data warnings)
        pipeline_state = {}
        try:
            ps_cur = conn.cursor()
            try:
                ps_result = ps_cur.execute(
                    "SELECT dataset_name, last_updated_at, row_count, last_run_at FROM lake._pipeline_state"
                )
                for psr in ps_result.fetchall():
                    pipeline_state[psr[0]] = {
                        "cursor": str(psr[1]) if psr[1] else None,
                        "rows_written": psr[2],
                        "last_run_at": str(psr[3]) if psr[3] else None,
                    }
            finally:
                ps_cur.close()
            print(f"Pipeline state loaded: {len(pipeline_state)} datasets", flush=True)
        except Exception:
            pass  # _pipeline_state may not exist yet

        # --- Name token routes (persistent local table for instant name→table routing) ---
        # Runs in a background thread — name_tokens has 246M rows, COPY takes minutes.
        # Server starts immediately; routing is degraded until background build finishes.
        if emb_conn:
            try:
                # Use a separate read-only connection to avoid blocking the background
                # embedding thread which holds the write lock on emb_conn.
                import duckdb as _duckdb2
                _ro_conn = _duckdb2.connect(EMBEDDINGS_DB, read_only=True, config={"allow_unsigned_extensions": "true"})
                try:
                    existing = _ro_conn.execute(
                        "SELECT COUNT(*) FROM name_token_routes"
                    ).fetchone()[0]
                finally:
                    _ro_conn.close()
            except Exception:
                existing = 0

            # Rebuild if fewer than 100K routes (incomplete previous build)
            needs_rebuild = existing < 100_000

            if needs_rebuild:
                # Capture pg_pass for the background thread's own connection
                _tok_pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "''")

                def _build_token_routes_bg(tok_pg_pass, bg_emb_conn):
                    """Build name_token_routes using its own DuckDB connection.
                    MUST NOT share the main conn — concurrent access causes pure virtual crash."""
                    import duckdb as _duckdb_bg, pathlib
                    try:
                        print(f"Background: building name_token_routes ({existing:,} existing)...", flush=True)
                        t_tok = time.time()
                        # Own connection — avoids thread-safety issues with the shared conn
                        bg_conn = _duckdb_bg.connect(config={"allow_unsigned_extensions": "true"})
                        bg_conn.execute("LOAD ducklake")
                        bg_conn.execute(f"""
                            ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={tok_pg_pass} host=postgres'
                            AS lake
                        """)
                        tmp_path = "/data/common-ground/lake/_tmp_routes.parquet"
                        bg_conn.execute(f"""
                            COPY (
                                SELECT DISTINCT token, source_table
                                FROM lake.foundation.name_tokens
                            ) TO '{tmp_path}' (FORMAT PARQUET)
                        """)
                        bg_conn.close()
                        print(f"  Routes exported to parquet in {time.time() - t_tok:.1f}s", flush=True)
                        bg_emb_conn.execute("DROP TABLE IF EXISTS name_token_routes")
                        bg_emb_conn.execute(f"""
                            CREATE TABLE name_token_routes AS
                            SELECT * FROM read_parquet('{tmp_path}')
                        """)
                        pathlib.Path(tmp_path).unlink(missing_ok=True)
                        tok_count = bg_emb_conn.execute("SELECT COUNT(*) FROM name_token_routes").fetchone()[0]
                        print(f"name_token_routes: {tok_count:,} routes in {time.time() - t_tok:.1f}s", flush=True)
                    except Exception as e:
                        print(f"Warning: name_token_routes build failed: {e}", flush=True)

                threading.Thread(
                    target=_build_token_routes_bg,
                    args=(_tok_pg_pass, emb_conn),
                    daemon=True,
                    name="token-routes-build",
                ).start()
                print("Background: name_token_routes build started (246M rows, will take a few minutes)", flush=True)
            elif existing > 0:
                print(f"name_token_routes: {existing:,} routes (up to date)", flush=True)
            else:
                print("name_token_routes: skipped (no DuckLake name_tokens table)", flush=True)

        try:
            yield {
                "db": conn, "pool": pool, "catalog": catalog,
                "catalog_json": catalog_json,
                "pipeline_state": pipeline_state,
                "graph_ready": graph_ready,
                "marriage_parquet": MARRIAGE_PARQUET if marriage_available else None,
                "posthog_enabled": bool(ph_key),
                "explorations": explorations,
                "embed_fn": embed_fn,
                "emb_conn": emb_conn,
                "percentiles_ready": percentiles_ready,
            }
        finally:
            print("Shutting down: cleaning up resources...", flush=True)
            try:
                pool.close()
            except Exception:
                pass
            if emb_conn:
                try:
                    emb_conn.execute("CHECKPOINT")
                    emb_conn.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass
            print("Shutdown complete", flush=True)
    finally:
        if ph_key:
            posthog.flush()
            posthog.shutdown()


# ---------------------------------------------------------------------------
# Instructions
# ---------------------------------------------------------------------------

INSTRUCTIONS = """Common Ground -- NYC open data lake. 294 tables, 60M+ rows, 14 schemas.

ROUTING -- pick the FIRST match:
* Street address or "where I live"       -> address_report()
* Address + specific question (violations, history) -> building()
* BBL (10-digit number)                  -> building()
* Person or company name                 -> entity()
* Cop by name                            -> entity(role="cop")
* Judge by name                          -> entity(role="judge")
* Birth/death/marriage records           -> entity(role="vitals")
* ZIP code or "this neighborhood"        -> neighborhood()
* Compare ZIPs                           -> neighborhood(view="compare")
* Restaurants in an area                 -> neighborhood(view="restaurants")
* Landlord portfolio or slumlord score   -> network(type="ownership")
* LLC piercing or shell companies        -> network(type="corporate")
* Campaign donations or lobbying         -> network(type="political")
* Worst landlords ranking                -> network(type="worst")
* School by name, DBN, or ZIP            -> school()
* "Find complaints about X" or concepts  -> semantic_search()
* Crime, crashes, shootings              -> safety()
* Health data, COVID, hospitals          -> health()
* Court cases, settlements, hearings     -> legal()
* City contracts, permits, jobs, budget  -> civic()
* Parking tickets, MTA, traffic          -> transit()
* Childcare, shelters, food pantries     -> services()
* "What can I explore?" or unsure        -> suggest()
* Custom SQL or "what tables have X"     -> query()
* Download or export data               -> query(format="xlsx")

WORKFLOWS -- chain tools for deep investigations:
* Landlord investigation: building() -> entity() -> network(type="ownership")
* Follow the money: entity() -> network(type="political") -> civic(view="contracts")
* School comparison: school("02M475,02M001")
* Health equity: health("10456") -> services("10456") -> neighborhood("10456")

PRESENTATION -- how to show Common Ground data to users:
* Every tool returns a complete, pre-formatted report. Present ALL sections to the user.
* Do NOT summarize, compress, or omit sections. Each section has independent data the user needs.
* Use interactive visualizations (charts, tables) when data contains numeric comparisons, rankings, or percentiles.
* Percentile bars and rankings should be shown visually, not described in prose.
* Preserve the tool's section headers and structure — they are designed for readability.
* When a report has a "Drill deeper" footer, show it — those are actionable tool calls the user can request.
* If the response includes a PRESENTATION directive, follow it exactly.

This is NYC-only data. Do not query for national/federal statistics."""


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "Common Ground",
    instructions=INSTRUCTIONS,
    lifespan=app_lifespan,
    # No transforms -- 14 tools fit in context (~3,500 tokens)
)


# ---------------------------------------------------------------------------
# Tools — 14 super tools registered from tools/ package
# ---------------------------------------------------------------------------

from tools import (
    address_report, building, entity, neighborhood, network, school,
    semantic_search, query, safety, health, legal,
    civic, transit, services, suggest,
)

mcp.tool(annotations=READONLY)(address_report)
mcp.tool(annotations=READONLY)(building)
mcp.tool(annotations=READONLY)(entity)
mcp.tool(annotations=READONLY)(neighborhood)
mcp.tool(annotations=READONLY)(network)
mcp.tool(annotations=READONLY)(school)
mcp.tool(annotations=READONLY)(semantic_search)
mcp.tool(annotations=READONLY)(query)
mcp.tool(annotations=READONLY)(safety)
mcp.tool(annotations=READONLY)(health)
mcp.tool(annotations=READONLY)(legal)
mcp.tool(annotations=READONLY)(civic)
mcp.tool(annotations=READONLY)(transit)
mcp.tool(annotations=READONLY)(services)
mcp.tool(annotations=READONLY)(suggest)


# ---------------------------------------------------------------------------
# Prompts — user-invocable investigation templates
# ---------------------------------------------------------------------------


@mcp.prompt(
    name="investigate_building",
    description="Deep investigation of a NYC building -- violations, ownership, complaints, transactions, and enforcement history",
)
def investigate_building(bbl: str) -> str:
    return f"""Investigate building BBL {bbl} thoroughly:

1. Start with building({bbl}) for the overview
2. Check network({bbl}, type='ownership') to see the ownership network
3. Pull entity(name) for cross-references on the owner
4. Run building({bbl}) again with full context

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

1. Start with neighborhood(zipcodes='{zip_codes}', view='compare') for side-by-side stats
2. For each ZIP, pull neighborhood(zipcode) for the full picture
3. Check safety() for the closest precinct to each
4. Look at health(zipcode) for environmental burden

Build a comparison that covers:
- Safety (crime rates, types of crime)
- Housing (rent, violations, landlord quality)
- Schools (if residential)
- Environment (air quality, flood risk, green space)
- Services (311 responsiveness, nearby facilities)

End with a clear recommendation based on the data."""


@mcp.prompt(
    name="follow_the_money",
    description="Trace a person or company's full NYC footprint -- property, politics, corporations, violations, and financial connections",
)
def follow_the_money(name: str) -> str:
    return f"""Investigate "{name}" across all NYC public records:

1. Start with entity('{name}') for the full cross-reference
2. Check network('{name}', type='corporate') for shell company networks
3. Run network('{name}', type='political') for political donation chains
4. Look at network('{name}', type='ownership') for property connections

Build a profile covering:
- All properties connected to this entity
- Corporate structure (LLCs, officers, registered agents)
- Political connections (donations, contracts, lobbying)
- Violation and enforcement history
- Any patterns that suggest conflicts of interest"""


# ---------------------------------------------------------------------------
# PostHog analytics middleware — track all tool calls + client identity
# ---------------------------------------------------------------------------

_session_clients: dict[str, dict] = {}
_SESSION_MAX = 1000


class PostHogMiddleware(Middleware):
    """Capture every MCP tool call with full client identity and Cloudflare headers."""

    def _extract_client_info(self) -> dict:
        """Extract real client IP, country, and user agent from Cloudflare headers."""
        info = {"ip": "unknown", "country": "unknown", "cf_ray": "", "user_agent": ""}
        try:
            req = get_http_request()
            if req:
                headers = dict(req.headers) if hasattr(req, "headers") else {}
                # CF-Connecting-IP is the real client IP (set by Cloudflare tunnel)
                info["ip"] = (
                    headers.get("cf-connecting-ip")
                    or headers.get("x-forwarded-for", "").split(",")[0].strip()
                    or (req.client.host if req.client else "unknown")
                )
                info["country"] = headers.get("cf-ipcountry", "unknown")
                info["cf_ray"] = headers.get("cf-ray", "")
                info["user_agent"] = headers.get("user-agent", "")
        except Exception:
            pass
        return info

    async def on_initialize(self, context, call_next):
        result = await call_next(context)
        try:
            client_info = context.message.clientInfo
            session_id = context.fastmcp_context.session_id or "unknown"
            cf = self._extract_client_info()
            _session_clients[session_id] = {
                "client_name": getattr(client_info, "name", "unknown"),
                "client_version": getattr(client_info, "version", "unknown"),
                "client_ip": cf["ip"],
                "client_country": cf["country"],
                "user_agent": cf["user_agent"],
            }
            if len(_session_clients) > _SESSION_MAX:
                excess = len(_session_clients) - _SESSION_MAX
                for key in list(_session_clients.keys())[:excess]:
                    del _session_clients[key]

            # Register session as a PostHog group (enables session-level analytics)
            if os.environ.get("POSTHOG_API_KEY"):
                posthog.group_identify("mcp_session", session_id, {
                    "client_name": getattr(client_info, "name", "unknown"),
                    "client_country": cf["country"],
                    "client_ip": cf["ip"],
                })
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

        session_id = "unknown"
        try:
            session_id = context.fastmcp_context.session_id or "unknown"
        except Exception:
            pass
        client = _session_clients.get(session_id, {})

        # Get real client info from Cloudflare headers (not Docker internal IP)
        cf = self._extract_client_info()
        client_ip = client.get("client_ip") or cf["ip"]
        client_country = client.get("client_country") or cf["country"]

        try:
            result = await call_next(context)
            return result
        except Exception as exc:
            error = str(exc)[:500]
            raise
        finally:
            try:
                elapsed = round((time.time() - t0) * 1000)
                distinct_id = f"mcp:{client_ip}"

                props = {
                    # Tool identity
                    "tool": tool_name,
                    "server": "common-ground-mcp",

                    # Performance
                    "duration_ms": elapsed,
                    "is_error": error is not None,

                    # Session / client
                    "session_id": session_id,
                    "client_name": client.get("client_name", "unknown"),
                    "client_version": client.get("client_version", "unknown"),

                    # Real client info from Cloudflare
                    "client_ip": client_ip,
                    "client_country": client_country,
                    "cf_ray": cf.get("cf_ray", ""),
                    "user_agent": cf.get("user_agent", ""),

                    # Don't create person profiles for anonymous API traffic
                    "$process_person_profile": False,
                }

                # Include actual arg values for short strings (useful for debugging)
                for k, v in arguments.items():
                    sv = str(v)
                    if len(sv) <= 100:
                        props[f"arg_{k}"] = sv
                    else:
                        props[f"arg_{k}"] = f"<{type(v).__name__}:{len(sv)}chars>"

                if error:
                    props["error"] = error

                posthog.capture(
                    distinct_id=distinct_id,
                    event="mcp_tool_called",
                    properties=props,
                    groups={"mcp_session": session_id},
                )

                # Also capture exceptions for PostHog Error Tracking
                if error:
                    posthog.capture(
                        distinct_id=distinct_id,
                        event="$exception",
                        properties={
                            "$exception_message": error,
                            "$exception_type": "ToolError",
                            "tool": tool_name,
                            "session_id": session_id,
                            "client_country": client_country,
                        },
                    )
            except Exception:
                pass  # Never let analytics break tool calls


# ---------------------------------------------------------------------------
# ConcurrencyMiddleware — limit concurrent tool calls per IP and globally
# ---------------------------------------------------------------------------

_concurrency_semaphores: dict[str, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(4)
)
_GLOBAL_SEMAPHORE = asyncio.Semaphore(20)  # leave 4 cursors for health/warmup


class ConcurrencyMiddleware(Middleware):
    """Limit concurrent tool calls per client IP and globally."""

    async def on_call_tool(self, context, call_next):
        client_ip = "unknown"
        try:
            req = get_http_request()
            if req:
                headers = dict(req.headers) if hasattr(req, "headers") else {}
                client_ip = (
                    headers.get("cf-connecting-ip")
                    or headers.get("x-forwarded-for", "").split(",")[0].strip()
                    or (req.client.host if req.client else "unknown")
                )
        except Exception:
            pass

        per_ip = _concurrency_semaphores[client_ip]

        if per_ip.locked():
            raise ToolError(
                "Too many concurrent requests from your IP (max 4). "
                "Wait for current queries to complete."
            )

        if _GLOBAL_SEMAPHORE.locked():
            raise ToolError(
                "Server at capacity (20 concurrent queries). "
                "Please retry in a few seconds."
            )

        async with _GLOBAL_SEMAPHORE:
            async with per_ip:
                return await call_next(context)


# ---------------------------------------------------------------------------
# Middleware registrations
# ---------------------------------------------------------------------------

mcp.add_middleware(RateLimitingMiddleware(
    max_requests_per_second=2.0,
    burst_capacity=10,
    global_limit=True,
))
mcp.add_middleware(ConcurrencyMiddleware())
mcp.add_middleware(ResponseLimitingMiddleware(max_size=50_000))
mcp.add_middleware(OutputFormatterMiddleware())
mcp.add_middleware(CitationMiddleware())
mcp.add_middleware(FreshnessMiddleware())
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


_ALLOWED_ORIGINS = frozenset({
    "https://common-ground.nyc",
    "https://www.common-ground.nyc",
    "http://localhost:3002",
    "http://localhost:3000",
})


def _cors_origin(request):
    origin = request.headers.get("origin", "")
    return origin if origin in _ALLOWED_ORIGINS else ""


@mcp.custom_route("/api/catalog", methods=["GET", "OPTIONS"])
async def catalog_json(request: Request) -> JSONResponse:
    """Return table catalog as JSON for the data health page."""

    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Max-Age": "86400",
            },
        )

    pool = _shared_pool
    if pool is None or not _shared_catalog:
        return JSONResponse({"error": "Server starting up"}, status_code=503)

    try:
        # Build table list from _shared_catalog (pure dict, no SQL view evaluation)
        _skip = {'staging', 'test', 'ducklake', 'information_schema', 'pg_catalog', 'lake', 'foundation'}
        rows = []
        for schema in sorted(_shared_catalog):
            if schema in _skip or 'staging' in schema or schema.startswith('test'):
                continue
            for table, info in sorted(_shared_catalog[schema].items(), key=lambda x: -(x[1].get("row_count", 0))):
                if table.startswith('_dlt_') or table.startswith('_pipeline') or table.endswith('__null') or table.endswith('__footnotes'):
                    continue
                rows.append((schema, table, info.get("row_count", 0), info.get("column_count", 0)))

        ducklake_info = {}
        try:
            _, dl_rows = execute(pool, "SELECT table_name, file_count, file_size_bytes, table_uuid FROM ducklake_table_info('lake')")
            for dlr in dl_rows:
                ducklake_info[dlr[0]] = {"file_count": dlr[1], "file_size_bytes": dlr[2], "table_uuid": str(dlr[3]) if dlr[3] else None}
        except Exception:
            pass

        pipeline_state = {}
        try:
            try:
                _, ps_rows = execute(pool, """
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
                _, ps_rows = execute(pool, """
                    SELECT dataset_name, last_updated_at, row_count, last_run_at
                    FROM lake._pipeline_state
                """)
                for psr in ps_rows:
                    pipeline_state[psr[0]] = {
                        "cursor": str(psr[1]) if psr[1] else None,
                        "rows_written": psr[2],
                        "last_run_at": str(psr[3]) if psr[3] else None,
                        "source_rows": None, "sync_status": None, "source_checked_at": None,
                    }
        except Exception:
            pass

        tables = []
        schema_stats = {}
        total_rows = 0

        for row in rows:
            schema, table, _zero, col_count = row[:4]
            cat_entry = _shared_catalog.get(schema, {}).get(table, {})
            est_size = cat_entry.get("row_count", 0)

            dl = ducklake_info.get(table, {})
            file_count = dl.get("file_count", 0)
            file_size = dl.get("file_size_bytes", 0)
            table_uuid = dl.get("table_uuid")

            created_at = None
            if table_uuid:
                try:
                    hex_str = table_uuid.replace('-', '')
                    if len(hex_str) >= 13 and hex_str[12] == '7':
                        epoch_ms = int(hex_str[:12], 16)
                        created_at = _dt.datetime.fromtimestamp(epoch_ms / 1000, tz=_dt.timezone.utc).isoformat()
                except (ValueError, OSError):
                    pass

            ps_key = f"{schema}.{table}"
            ps = pipeline_state.get(ps_key, {})

            tables.append({
                "schema": schema, "table": table,
                "rows": est_size or 0, "columns": col_count or 0,
                "files": file_count or 0, "size_bytes": file_size or 0,
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
            "summary": {"schemas": len(schema_stats), "tables": len(tables), "total_rows": total_rows},
            "schemas": schema_stats,
            "tables": tables,
        }

        return JSONResponse(
            result,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Cache-Control": "public, max-age=300",
            },
        )
    except Exception as e:
        print(f"[catalog] Error: {e}", flush=True)
        return JSONResponse({"error": "Internal server error"}, status_code=500)


# ---------------------------------------------------------------------------
# Public HTTP API — explore endpoints for data explorer UI
# ---------------------------------------------------------------------------

from api.explore import handle_table_meta as _handle_table_meta
from api.explore import handle_query as _handle_query


@mcp.custom_route("/api/table-meta", methods=["GET", "OPTIONS"])
async def table_meta_json(request: Request) -> JSONResponse:
    """Return column metadata for a table."""
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Max-Age": "86400",
            },
        )
    if _shared_pool is None or not _shared_catalog:
        return JSONResponse({"error": "Server starting up"}, status_code=503)
    return await _handle_table_meta(request, _shared_pool, _shared_catalog, _cors_origin(request))


@mcp.custom_route("/api/query", methods=["POST", "OPTIONS"])
async def query_json(request: Request) -> JSONResponse:
    """Execute a filtered query against a table."""
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Max-Age": "86400",
            },
        )
    if _shared_pool is None or not _shared_catalog:
        return JSONResponse({"error": "Server starting up"}, status_code=503)
    return await _handle_query(request, _shared_pool, _shared_catalog, _cors_origin(request))


# ---------------------------------------------------------------------------
# Public HTTP API — anomaly detection endpoint for website widget
# ---------------------------------------------------------------------------

from tools.anomalies import detect_anomalies as _detect_anomalies


@mcp.custom_route("/api/anomalies", methods=["GET", "OPTIONS"])
async def anomalies_json(request: Request) -> JSONResponse:
    """Return statistical anomalies across NYC datasets for the website widget."""
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Max-Age": "86400",
            },
        )
    if _shared_pool is None:
        return JSONResponse({"anomalies": [], "count": 0, "error": "Server starting up"}, status_code=503)
    try:
        z_threshold = float(request.query_params.get("z", "2.0"))
        max_results = int(request.query_params.get("limit", "15"))
        results = _detect_anomalies(_shared_pool, z_threshold=z_threshold, max_results=max_results)
        return JSONResponse(
            {"anomalies": results, "count": len(results)},
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Cache-Control": "public, max-age=900",
            },
        )
    except Exception as e:
        print(f"[anomalies] Error: {e}", flush=True)
        return JSONResponse({"anomalies": [], "count": 0, "error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# /explore dashboard REST endpoints — see routes/explore.py
# ---------------------------------------------------------------------------
from routes.explore import (
    neighborhood_endpoint as _explore_neighborhood,
    zips_search_endpoint as _explore_zips_search,
    worst_buildings_endpoint as _explore_worst_buildings,
)


@mcp.custom_route("/api/neighborhood/{zip_code}", methods=["GET", "OPTIONS"])
async def neighborhood_route(request: Request) -> JSONResponse:
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Max-Age": "86400",
            },
        )
    if _shared_pool is None or not _shared_catalog:
        return JSONResponse({"error": "Server starting up"}, status_code=503)
    response = await _explore_neighborhood(request)
    response.headers["Access-Control-Allow-Origin"] = _cors_origin(request)
    response.headers["Vary"] = "Origin"
    return response


@mcp.custom_route("/api/zips/search", methods=["GET", "OPTIONS"])
async def zips_search_route(request: Request) -> JSONResponse:
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Max-Age": "86400",
            },
        )
    if _shared_pool is None or not _shared_catalog:
        return JSONResponse({"error": "Server starting up"}, status_code=503)
    response = await _explore_zips_search(request)
    response.headers["Access-Control-Allow-Origin"] = _cors_origin(request)
    response.headers["Vary"] = "Origin"
    return response


@mcp.custom_route("/api/buildings/worst", methods=["GET", "OPTIONS"])
async def worst_buildings_route(request: Request) -> JSONResponse:
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Max-Age": "86400",
            },
        )
    if _shared_pool is None or not _shared_catalog:
        return JSONResponse({"error": "Server starting up"}, status_code=503)
    response = await _explore_worst_buildings(request)
    response.headers["Access-Control-Allow-Origin"] = _cors_origin(request)
    response.headers["Vary"] = "Origin"
    return response


# ---------------------------------------------------------------------------
# Mosaic data server endpoint — see routes/mosaic_route.py
# ---------------------------------------------------------------------------
from routes.mosaic_route import mosaic_query_endpoint as _explore_mosaic_query


@mcp.custom_route("/mosaic/query", methods=["POST", "OPTIONS"])
async def mosaic_query_route(request: Request) -> JSONResponse:
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Max-Age": "86400",
            },
        )
    if _shared_pool is None or not _shared_catalog:
        return JSONResponse({"error": "Server starting up"}, status_code=503)
    response = await _explore_mosaic_query(request)
    response.headers["Access-Control-Allow-Origin"] = _cors_origin(request)
    response.headers["Vary"] = "Origin"
    return response


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
