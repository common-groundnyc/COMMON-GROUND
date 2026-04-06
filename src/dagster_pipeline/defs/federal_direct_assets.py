"""Federal @asset functions — direct DuckDB ingestion (no dlt).

~49 assets across 20 source types. Each asset:
  - Full replace every run (all federal sources use replace)
  - Fetches via httpx → Arrow → DuckLake zero-copy write
  - API keys from env vars (same names as dlt convention)
"""
import bz2
import csv as csv_mod
import glob
import io
import logging
import os
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import dagster as dg
import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

from dagster_pipeline.resources.ducklake import DuckLakeResource
from dagster_pipeline.ingestion.writer import write_full_replace, update_cursor

logger = logging.getLogger(__name__)

SCHEMA = "federal"

# Shared HTTP client — gzip, generous timeout
_CLIENT = httpx.Client(timeout=httpx.Timeout(connect=30, read=300, write=60, pool=30),
                       headers={"Accept-Encoding": "gzip"},
                       follow_redirects=True)


def _write_asset(conn, table_name: str, arrow_table: pa.Table | None,
                 context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if arrow_table is None or arrow_table.num_rows == 0:
        context.log.warning("%s.%s: no data", SCHEMA, table_name)
        update_cursor(conn, f"{SCHEMA}.{table_name}", 0)
        return dg.MaterializeResult(metadata={"row_count": 0})
    rows = write_full_replace(conn, SCHEMA, table_name, arrow_table)
    update_cursor(conn, f"{SCHEMA}.{table_name}", rows)
    context.log.info("%s.%s: wrote %d rows", SCHEMA, table_name, rows)
    return dg.MaterializeResult(metadata={"row_count": rows})


def _get_json(url: str, params: dict | None = None,
              headers: dict | None = None) -> dict | list:
    resp = _CLIENT.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


def _post_json(url: str, payload: dict, headers: dict | None = None) -> dict:
    resp = _CLIENT.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    return resp.json()


# ═══════════════════════════════════════════════════════════════════
# BLS — 5 borough unemployment series (single-page REST, no auth)
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.bls import BLS_SERIES


def _make_bls_asset(series_id: str, table_name: str):
    @dg.asset(
        key=dg.AssetKey([SCHEMA, table_name]),
        group_name=SCHEMA,
        op_tags={"schema": SCHEMA},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        ducklake: DuckLakeResource,
    ) -> dg.MaterializeResult:
        data = _get_json(
            f"https://api.bls.gov/publicAPI/v1/timeseries/data/{series_id}"
        )
        records = data.get("Results", {}).get("series", [{}])[0].get("data", [])
        table = pa.Table.from_pylist(records) if records else None
        conn = ducklake.get_connection()
        try:
            return _write_asset(conn, table_name, table, context)
        finally:
            conn.close()
    return _asset


def _build_bls_assets() -> list:
    return [_make_bls_asset(sid, f"bls_unemployment_{borough}")
            for sid, borough in BLS_SERIES.items()]


# ═══════════════════════════════════════════════════════════════════
# Census ACS 5-Year — demographics by tract for NYC counties
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.census import (
    NYC_COUNTIES as CENSUS_NYC_COUNTIES, NYC_STATE, ACS_YEAR,
    VARIABLES as CENSUS_VARIABLES, ACS_TABLES, _parse_census_value,
)


def _fetch_census_county(county_fips: str, api_key: str,
                         variables: dict[str, str]) -> list[dict]:
    var_codes = ",".join(variables.keys())
    params = {
        "get": f"NAME,{var_codes}",
        "for": "tract:*",
        "in": f"state:{NYC_STATE} county:{county_fips}",
    }
    if api_key:
        params["key"] = api_key

    data = _get_json(f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5", params=params)
    if len(data) < 2:
        return []

    var_keys = list(variables.keys())
    records = []
    for row in data[1:]:
        record = {"name": row[0]}
        for i, var_code in enumerate(var_keys):
            record[variables[var_code]] = _parse_census_value(row[1 + i])
        record["state_fips"] = row[len(var_keys) + 1]
        record["county_fips"] = row[len(var_keys) + 2]
        record["tract_fips"] = row[len(var_keys) + 3]
        records.append(record)
    return records


def _fetch_census_table(api_key: str, variables: dict[str, str]) -> pa.Table | None:
    all_records = []
    for county_fips in CENSUS_NYC_COUNTIES:
        records = _fetch_census_county(county_fips, api_key, variables)
        all_records.extend(records)
    return pa.Table.from_pylist(all_records) if all_records else None


@dg.asset(
    key=dg.AssetKey([SCHEMA, "acs_demographics"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def census_acs_demographics(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    api_key = os.environ.get("SOURCES__CENSUS__API_KEY", "")
    conn = ducklake.get_connection()
    try:
        # Main demographics table
        table = _fetch_census_table(api_key, CENSUS_VARIABLES)
        result = _write_asset(conn, "acs_demographics", table, context)

        # All additional ACS tables
        for acs_name, config in ACS_TABLES.items():
            t = _fetch_census_table(api_key, config["variables"])
            _write_asset(conn, acs_name, t, context)
            context.log.info("Census %s: done", acs_name)

        return result
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# College Scorecard — paginated REST (page_number)
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.college_scorecard import SCORECARD_FIELDS


@dg.asset(
    key=dg.AssetKey([SCHEMA, "college_scorecard_schools"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def college_scorecard_schools(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    api_key = os.environ.get("SOURCES__COLLEGE_SCORECARD__API_KEY", "")
    all_records = []
    page = 0
    while page <= 100:
        data = _get_json(
            "https://api.data.gov/ed/collegescorecard/v1/schools",
            params={
                "fields": ",".join(SCORECARD_FIELDS),
                "per_page": 100,
                "page": page,
                "api_key": api_key,
            },
        )
        results = data.get("results", [])
        if not results:
            break
        all_records.extend(results)
        total = data.get("metadata", {}).get("total", 0)
        context.log.info("College Scorecard: page %d, %d/%d", page, len(all_records), total)
        if len(all_records) >= total:
            break
        page += 1

    table = pa.Table.from_pylist(all_records) if all_records else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "college_scorecard_schools", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# HUD ArcGIS — 5 feature services with offset pagination
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.hud import ARCGIS_BASE, ARCGIS_DATASETS, ARCGIS_PAGE_SIZE


def _fetch_arcgis(service: str, layer: int, where: str) -> pa.Table | None:
    all_records = []
    offset = 0
    while True:
        data = _get_json(
            f"{ARCGIS_BASE}/{service}/FeatureServer/{layer}/query",
            params={
                "where": where, "outFields": "*", "f": "json",
                "resultRecordCount": ARCGIS_PAGE_SIZE,
                "resultOffset": offset,
            },
        )
        features = data.get("features", [])
        if not features:
            break
        all_records.extend(f["attributes"] for f in features)
        offset += len(features)
        if len(features) < ARCGIS_PAGE_SIZE:
            break
    return pa.Table.from_pylist(all_records) if all_records else None


def _make_hud_asset(table_name: str, service: str, layer: int, where: str):
    @dg.asset(
        key=dg.AssetKey([SCHEMA, table_name]),
        group_name=SCHEMA,
        op_tags={"schema": SCHEMA},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        ducklake: DuckLakeResource,
    ) -> dg.MaterializeResult:
        table = _fetch_arcgis(service, layer, where)
        conn = ducklake.get_connection()
        try:
            return _write_asset(conn, table_name, table, context)
        finally:
            conn.close()
    return _asset


def _build_hud_assets() -> list:
    return [_make_hud_asset(tn, svc, ly, w) for tn, svc, ly, w in ARCGIS_DATASETS]


# ═══════════════════════════════════════════════════════════════════
# FEC — bulk ZIP download, streaming Arrow, NYC filter
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.fec import (
    FEC_BULK_URL, FEC_COLUMNS, NYC_ZIPS, CYCLES as FEC_CYCLES,
    DOWNLOAD_TIMEOUT as FEC_TIMEOUT, DOWNLOAD_CHUNK, _download_bulk, _filter_nyc_batch,
)


@dg.asset(
    key=dg.AssetKey([SCHEMA, "fec_contributions"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def fec_contributions(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    data_dir = os.environ.get("FEC_BULK_DIR", "/tmp/fec-bulk")
    os.makedirs(data_dir, exist_ok=True)

    read_opts = pacsv.ReadOptions(column_names=FEC_COLUMNS, block_size=32 * 1024 * 1024)
    parse_opts = pacsv.ParseOptions(delimiter="|", quote_char=False)
    convert_opts = pacsv.ConvertOptions(
        column_types={"transaction_amt": pa.float64(), "sub_id": pa.string()},
        strings_can_be_null=True,
    )

    all_tables = []
    for year in FEC_CYCLES:
        txt_path = _download_bulk(year, data_dir)
        context.log.info("FEC %d: streaming batches", year)
        reader = pacsv.open_csv(txt_path, read_options=read_opts,
                                parse_options=parse_opts, convert_options=convert_opts)
        for batch in reader:
            filtered = _filter_nyc_batch(batch, year)
            if filtered is not None:
                all_tables.append(filtered)
        if os.path.exists(txt_path):
            os.remove(txt_path)

    table = pa.concat_tables(all_tables, promote_options="permissive") if all_tables else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "fec_contributions", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# ProPublica — NY nonprofits (page_number pagination)
# ═══════════════════════════════════════════════════════════════════

@dg.asset(
    key=dg.AssetKey([SCHEMA, "propublica_nonprofits"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def propublica_nonprofits(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    all_records = []
    for page in range(400):
        try:
            data = _get_json(
                "https://projects.propublica.org/nonprofits/api/v2/search.json",
                params={"state[id]": "NY", "page": page},
            )
        except httpx.HTTPStatusError:
            break
        orgs = data.get("organizations", [])
        if not orgs:
            break
        all_records.extend(orgs)
        if page % 50 == 0:
            context.log.info("ProPublica: page %d, %d orgs", page, len(all_records))

    table = pa.Table.from_pylist(all_records) if all_records else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "propublica_nonprofits", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# LittleSis — power mapping (seed search + relationship expansion)
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.littlesis import (
    LS_BASE, NYC_SEED_SEARCHES, REL_CATEGORIES, _parse_entity,
)


def _collect_seed_entities_direct() -> list[tuple[str, dict]]:
    seen: set[str] = set()
    results: list[tuple[str, dict]] = []
    for query in NYC_SEED_SEARCHES:
        time.sleep(0.5)
        try:
            data = _get_json(f"{LS_BASE}/entities/search", params={"q": query})
        except Exception as e:
            logger.warning("LittleSis search failed for '%s': %s", query, e)
            continue
        for ent in data.get("data", []):
            eid = ent.get("id")
            if eid and eid not in seen:
                seen.add(eid)
                results.append((eid, ent))
    return results


def _fetch_relationships_direct(entity_id: str) -> list[dict]:
    time.sleep(0.3)
    try:
        data = _get_json(
            f"{LS_BASE}/entities/{entity_id}/relationships",
            params={"page": 1, "per_page": 50},
        )
    except Exception:
        return []
    rows = []
    for rel in data.get("data", []):
        attrs = rel.get("attributes", {})
        cat_id = attrs.get("category_id")
        rows.append({
            "id": rel.get("id"),
            "entity1_id": attrs.get("entity1_id"),
            "entity2_id": attrs.get("entity2_id"),
            "category_id": cat_id,
            "category": REL_CATEGORIES.get(cat_id, "unknown"),
            "description1": attrs.get("description1"),
            "description2": attrs.get("description2"),
            "amount": attrs.get("amount"),
            "currency": attrs.get("currency"),
            "start_date": attrs.get("start_date"),
            "end_date": attrs.get("end_date"),
            "is_current": attrs.get("is_current"),
            "updated_at": attrs.get("updated_at"),
        })
    return rows


@dg.asset(
    key=dg.AssetKey([SCHEMA, "littlesis_entities"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def littlesis_entities(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    seed = _collect_seed_entities_direct()
    records = [_parse_entity(ent) for _, ent in seed]
    table = pa.Table.from_pylist(records) if records else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "littlesis_entities", table, context)
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey([SCHEMA, "littlesis_relationships"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def littlesis_relationships(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    seed = _collect_seed_entities_direct()
    all_rels = []
    for eid, _ in seed:
        rels = _fetch_relationships_direct(eid)
        all_rels.extend(rels)
    context.log.info("LittleSis: %d relationships from %d entities", len(all_rels), len(seed))
    table = pa.Table.from_pylist(all_rels) if all_rels else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "littlesis_relationships", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# USAspending — federal contracts + grants to NYC (POST pagination)
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.usaspending import (
    USA_BASE, CONTRACT_CODES, GRANT_CODES, NYC_LOCATION, FIELDS as USA_FIELDS,
)


def _fetch_usaspending(award_codes: list[str], context) -> pa.Table | None:
    all_records = []
    page = 1
    while True:
        payload = {
            "filters": {
                "award_type_codes": award_codes,
                "place_of_performance_locations": [NYC_LOCATION],
                "time_period": [{"start_date": "2019-01-01", "end_date": "2026-12-31"}],
            },
            "fields": USA_FIELDS,
            "page": page,
            "limit": 100,
            "sort": "Award Amount",
            "order": "desc",
        }
        data = _post_json(f"{USA_BASE}/search/spending_by_award/", payload)
        results = data.get("results", [])
        if not results:
            break
        rows = [{
            "award_id": r.get("Award ID"),
            "recipient_name": r.get("Recipient Name"),
            "award_amount": r.get("Award Amount"),
            "total_outlays": r.get("Total Outlays"),
            "start_date": r.get("Start Date"),
            "end_date": r.get("End Date"),
            "awarding_agency": r.get("Awarding Agency"),
            "awarding_sub_agency": r.get("Awarding Sub Agency"),
            "award_type": r.get("Award Type"),
            "recipient_id": r.get("recipient_id"),
        } for r in results]
        all_records.extend(rows)
        num_pages = data.get("page_metadata", {}).get("num_pages", 1)
        if page % 50 == 0:
            context.log.info("USAspending: page %d/%d, %d rows", page, num_pages, len(all_records))
        if page >= num_pages:
            break
        page += 1
    return pa.Table.from_pylist(all_records) if all_records else None


@dg.asset(
    key=dg.AssetKey([SCHEMA, "usaspending_contracts"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def usaspending_contracts(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    table = _fetch_usaspending(CONTRACT_CODES, context)
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "usaspending_contracts", table, context)
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey([SCHEMA, "usaspending_grants"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def usaspending_grants(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    table = _fetch_usaspending(GRANT_CODES, context)
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "usaspending_grants", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# EPA ECHO — two-phase query per NYC borough
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.epa_echo import ECHO_BASE
from dagster_pipeline.sources.epa_echo import NYC_COUNTIES as EPA_NYC_COUNTIES


def _fetch_echo_county(county_fips: str) -> pa.Table | None:
    # Phase 1: get QueryID
    data = _get_json(
        f"{ECHO_BASE}/echo_rest_services.get_facilities",
        params={"output": "JSON", "p_st": "NY", "p_cnty": county_fips,
                "p_act": "Y", "responseset": 1000},
    )
    qid = data.get("Results", {}).get("QueryID")
    if not qid:
        return None

    # Phase 2: paginate facilities
    all_records = []
    page = 1
    while True:
        data = _get_json(
            f"{ECHO_BASE}/echo_rest_services.get_qid",
            params={"output": "JSON", "qid": qid, "pageno": page, "responseset": 1000},
        )
        facilities = data.get("Results", {}).get("Facilities", [])
        if not facilities:
            break
        all_records.extend(facilities)
        page += 1
    return pa.Table.from_pylist(all_records) if all_records else None


def _make_echo_asset(county_fips: str, table_name: str):
    @dg.asset(
        key=dg.AssetKey([SCHEMA, table_name]),
        group_name=SCHEMA,
        op_tags={"schema": SCHEMA},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        ducklake: DuckLakeResource,
    ) -> dg.MaterializeResult:
        table = _fetch_echo_county(county_fips)
        conn = ducklake.get_connection()
        try:
            return _write_asset(conn, table_name, table, context)
        finally:
            conn.close()
    return _asset


def _build_echo_assets() -> list:
    return [_make_echo_asset(fips, f"epa_echo_facilities_{borough}")
            for fips, borough in EPA_NYC_COUNTIES.items()]


# ═══════════════════════════════════════════════════════════════════
# NREL — alternative fuel stations (single page)
# ═══════════════════════════════════════════════════════════════════

@dg.asset(
    key=dg.AssetKey([SCHEMA, "nrel_alt_fuel_stations"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def nrel_alt_fuel_stations(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    api_key = os.environ.get("SOURCES__NREL__API_KEY", "")
    data = _get_json(
        "https://developer.nrel.gov/api/alt-fuel-stations/v1.json",
        params={"state": "NY", "limit": "all", "status": "E", "api_key": api_key},
    )
    records = data.get("fuel_stations", [])
    table = pa.Table.from_pylist(records) if records else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "nrel_alt_fuel_stations", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# FDA — food enforcement in NY (offset pagination)
# ═══════════════════════════════════════════════════════════════════

@dg.asset(
    key=dg.AssetKey([SCHEMA, "fda_food_enforcement"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def fda_food_enforcement(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    all_records = []
    skip = 0
    while skip < 25000:
        try:
            data = _get_json(
                "https://api.fda.gov/food/enforcement.json",
                params={"search": 'state:"NY"', "limit": 100, "skip": skip},
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                break
            raise
        results = data.get("results", [])
        if not results:
            break
        # Flatten nested dicts/lists to JSON strings (DuckLake can't store structs)
        import json as _json
        for row in results:
            for k, v in row.items():
                if isinstance(v, (dict, list)):
                    row[k] = _json.dumps(v)
        all_records.extend(results)
        total = data.get("meta", {}).get("results", {}).get("total", 0)
        if skip + 100 >= total:
            break
        skip += 100

    table = pa.Table.from_pylist(all_records) if all_records else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "fda_food_enforcement", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# Urban Institute Education — school directory + finance
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.urban_education import URBAN_BASE


def _fetch_urban_pages(path: str) -> list[dict]:
    rows = []
    current_url = f"{URBAN_BASE}/{path}"
    while current_url:
        try:
            data = _get_json(current_url)
        except Exception:
            break
        rows.extend(data.get("results", []))
        next_url = data.get("next")
        current_url = next_url if next_url and URBAN_BASE in next_url else None
    return rows


@dg.asset(
    key=dg.AssetKey([SCHEMA, "urban_school_directory"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def urban_school_directory(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    records = _fetch_urban_pages("schools/ccd/directory/2022/fips/36/")
    context.log.info("Urban Ed directory: %d NY schools", len(records))
    table = pa.Table.from_pylist(records) if records else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "urban_school_directory", table, context)
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey([SCHEMA, "urban_school_finance"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def urban_school_finance(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    records = _fetch_urban_pages("school-districts/ccd/finance/2021/fips/36/")
    context.log.info("Urban Ed finance: %d NY districts", len(records))
    table = pa.Table.from_pylist(records) if records else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "urban_school_finance", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# Marriage Index — local parquet (4.76M records)
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.marriage_index import PARQUET_PATH


@dg.asset(
    key=dg.AssetKey([SCHEMA, "nyc_marriage_index"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def nyc_marriage_index(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    import pyarrow.parquet as pq
    if not PARQUET_PATH.exists():
        context.log.error("Marriage index parquet not found at %s", PARQUET_PATH)
        return dg.MaterializeResult(metadata={"row_count": 0, "error": "file_not_found"})
    table = pq.read_table(PARQUET_PATH)
    context.log.info("Marriage index: %d records", table.num_rows)
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "nyc_marriage_index", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# NYPD Misconduct — CCRB complaints (ZIP) + officers (Airtable CSV)
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.nypd_misconduct import (
    NYCLU_ZIP_URL, NYCLU_AIRTABLE_CSV_URL,
)


def _fetch_ccrb_zip() -> pa.Table | None:
    resp = _CLIENT.get(NYCLU_ZIP_URL, timeout=120)
    resp.raise_for_status()
    with tempfile.SpooledTemporaryFile(max_size=50_000_000) as tmp:
        tmp.write(resp.content)
        tmp.seek(0)
        with zipfile.ZipFile(tmp) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                return None
            with zf.open(csv_names[0]) as f:
                table = pacsv.read_csv(
                    f, convert_options=pacsv.ConvertOptions(strings_can_be_null=True),
                )
    # Cast all to string
    cols = []
    for col in table.columns:
        cols.append(col.cast(pa.string()) if col.type != pa.string() else col)
    return pa.table({n: c for n, c in zip(table.column_names, cols)})


def _fetch_ccrb_airtable() -> pa.Table | None:
    resp = _CLIENT.get(NYCLU_AIRTABLE_CSV_URL, timeout=120)
    resp.raise_for_status()
    raw = resp.content
    if raw[:3] == b"\xef\xbb\xbf":
        raw = raw[3:]
    table = pacsv.read_csv(
        pa.BufferReader(raw),
        convert_options=pacsv.ConvertOptions(strings_can_be_null=True),
    )
    cols = []
    for col in table.columns:
        cols.append(col.cast(pa.string()) if col.type != pa.string() else col)
    return pa.table({n: c for n, c in zip(table.column_names, cols)})


@dg.asset(
    key=dg.AssetKey([SCHEMA, "nypd_ccrb_complaints"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def nypd_ccrb_complaints(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    table = _fetch_ccrb_zip()
    context.log.info("NYPD CCRB complaints: %d records", table.num_rows if table else 0)
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "nypd_ccrb_complaints", table, context)
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey([SCHEMA, "nypd_ccrb_officers_current"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def nypd_ccrb_officers_current(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    table = _fetch_ccrb_airtable()
    context.log.info("NYPD CCRB officers: %d records", table.num_rows if table else 0)
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "nypd_ccrb_officers_current", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# NYS Death Index — 30 CSV files from archive.org (10.5M records)
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.nys_death_index import ARCHIVE_BASE, CSV_FILES, COLUMN_RENAME


def _fetch_death_csv(filename: str) -> pa.Table:
    resp = _CLIENT.get(f"{ARCHIVE_BASE}/{filename}", timeout=300)
    resp.raise_for_status()
    raw = resp.content
    if raw[:3] == b"\xef\xbb\xbf":
        raw = raw[3:]
    # Force ALL columns to string — CSVs have inconsistent types across files
    table = pacsv.read_csv(
        pa.BufferReader(raw),
        convert_options=pacsv.ConvertOptions(
            auto_dict_encode=False,
            strings_can_be_null=True,
        ),
    )
    # Cast every column to string
    table = pa.table({
        col: table.column(col).cast(pa.string()) for col in table.column_names
    })
    new_names = [COLUMN_RENAME.get(n, n) for n in table.column_names]
    return table.rename_columns(new_names)


@dg.asset(
    key=dg.AssetKey([SCHEMA, "nys_death_index"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def nys_death_index(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    # Parallel download of 30 CSV files
    tables = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_fetch_death_csv, fn): fn for fn in CSV_FILES}
        for future in as_completed(futures):
            fn = futures[future]
            try:
                t = future.result()
                tables.append(t)
                context.log.info("Death index %s: %d rows", fn, t.num_rows)
            except Exception as e:
                context.log.warning("Death index %s failed: %s", fn, e)

    table = pa.concat_tables(tables, promote_options="permissive") if tables else None
    context.log.info("Death index total: %d rows", table.num_rows if table else 0)
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "nys_death_index", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# NYS BOE Campaign Finance — local CSV/ZIP files
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.nys_boe import NYS_BOE_COLUMNS

_NYS_BOE_DOWNLOADS = (
    "/root/Downloads" if os.path.isdir("/root/Downloads")
    else os.path.expanduser("~/Downloads")
)


@dg.asset(
    key=dg.AssetKey([SCHEMA, "nys_campaign_finance"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def nys_campaign_finance(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    patterns = [
        os.path.join(_NYS_BOE_DOWNLOADS, "ALL_REPORTS_*", "*.csv"),
        os.path.join(_NYS_BOE_DOWNLOADS, "ALL_REPORTS_*.csv"),
        os.path.join(_NYS_BOE_DOWNLOADS, "commcand", "*.csv"),
    ]
    csv_files = []
    for pattern in patterns:
        csv_files.extend(glob.glob(pattern))

    # Extract ZIPs if needed
    for pattern in [os.path.join(_NYS_BOE_DOWNLOADS, "ALL_REPORTS_*.zip"),
                    os.path.join(_NYS_BOE_DOWNLOADS, "commcand.zip")]:
        for zp in glob.glob(pattern):
            if os.path.getsize(zp) > 100:
                try:
                    with zipfile.ZipFile(zp) as zf:
                        for name in zf.namelist():
                            if name.endswith(".csv"):
                                extract_dir = zp.replace(".zip", "")
                                os.makedirs(extract_dir, exist_ok=True)
                                zf.extract(name, extract_dir)
                                csv_files.append(os.path.join(extract_dir, name))
                except zipfile.BadZipFile:
                    pass

    csv_files = list({f for f in csv_files if os.path.getsize(f) > 0})
    if not csv_files:
        context.log.error("No NYS BOE CSV files found in %s", _NYS_BOE_DOWNLOADS)
        return dg.MaterializeResult(metadata={"row_count": 0, "error": "no_files"})

    all_records = []
    for csv_path in csv_files:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv_mod.reader(f)
            for row_values in reader:
                row = {}
                for i, val in enumerate(row_values):
                    col = NYS_BOE_COLUMNS[i] if i < len(NYS_BOE_COLUMNS) else f"col_{i}"
                    row[col] = val.strip() if val else None
                row["source_file"] = os.path.basename(csv_path)
                all_records.append(row)

    table = pa.Table.from_pylist(all_records) if all_records else None
    context.log.info("NYS BOE: %d records from %d files", len(all_records), len(csv_files))
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "nys_campaign_finance", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# OCA Housing Court — XML ZIP files (rolling 5 years)
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.oca_housing_court import FILING_YEARS

_OCA_BASE = "https://ww2.nycourts.gov/sites/default/files/document/files"


def _find_oca_zip_url(year: int) -> str:
    patterns = [f"{_OCA_BASE}/LT_{year}.zip", f"{_OCA_BASE}/lt_{year}.zip",
                f"{_OCA_BASE}/LT{year}.zip"]
    for url in patterns:
        try:
            resp = _CLIENT.head(url, timeout=10)
            if resp.status_code == 200:
                return url
        except Exception:
            continue
    return patterns[0]


def _parse_oca_xml(zip_bytes: bytes) -> list[dict]:
    rows = []
    with tempfile.SpooledTemporaryFile(max_size=200_000_000) as tmp:
        tmp.write(zip_bytes)
        tmp.seek(0)
        with zipfile.ZipFile(tmp) as zf:
            for xml_name in [n for n in zf.namelist() if n.endswith(".xml")]:
                with zf.open(xml_name) as f:
                    tree = ET.parse(f)
                    root = tree.getroot()
                    ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
                    for case in root.iter(f"{ns}Case"):
                        row = {}
                        for elem in case.iter():
                            tag = elem.tag.replace(ns, "")
                            if elem.text and elem.text.strip():
                                if tag in row:
                                    i = 2
                                    while f"{tag}_{i}" in row:
                                        i += 1
                                    row[f"{tag}_{i}"] = elem.text.strip()
                                else:
                                    row[tag] = elem.text.strip()
                        if row:
                            rows.append(row)
    return rows


@dg.asset(
    key=dg.AssetKey([SCHEMA, "oca_housing_court_cases"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def oca_housing_court_cases(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    all_rows = []
    for year in FILING_YEARS:
        url = _find_oca_zip_url(year)
        context.log.info("OCA %d: downloading %s", year, url)
        try:
            resp = _CLIENT.get(url, timeout=300)
            resp.raise_for_status()
            rows = _parse_oca_xml(resp.content)
            all_rows.extend(rows)
            context.log.info("OCA %d: %d cases", year, len(rows))
        except httpx.HTTPStatusError as e:
            context.log.warning("OCA %d not available: %s", year, e)

    table = pa.Table.from_pylist(all_rows) if all_rows else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "oca_housing_court_cases", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# CourtListener — bulk CSV from S3 (bz2 compressed)
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.courtlistener import S3_BASE, API_BASE, BULK_DATASETS

CL_BATCH_SIZE = 50_000


def _download_cl_bulk(name: str, filename: str) -> pa.Table | None:
    url = f"{S3_BASE}/{filename}"
    resp = _CLIENT.get(url, timeout=600)
    resp.raise_for_status()
    raw = bz2.decompress(resp.content)

    header_end = raw.index(b"\n")
    header = raw[:header_end].decode("utf-8")
    col_names = [c.strip('"') for c in header.split(",")]
    col_types = {n: pa.string() for n in col_names}

    table = pacsv.read_csv(
        pa.BufferReader(raw),
        parse_options=pacsv.ParseOptions(
            invalid_row_handler=lambda row: "skip",
            newlines_in_values=True,
        ),
        convert_options=pacsv.ConvertOptions(
            column_types=col_types, strings_can_be_null=True,
        ),
    )
    # Replace empty strings with null
    for i, col_name in enumerate(table.column_names):
        col = table.column(i)
        mask = pc.equal(col, "")
        table = table.set_column(i, col_name, pc.if_else(mask, None, col))

    return table


def _make_cl_bulk_asset(name: str, filename: str):
    @dg.asset(
        key=dg.AssetKey([SCHEMA, name]),
        group_name=SCHEMA,
        op_tags={"schema": SCHEMA},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        ducklake: DuckLakeResource,
    ) -> dg.MaterializeResult:
        context.log.info("CourtListener bulk: downloading %s", filename)
        table = _download_cl_bulk(name, filename)
        context.log.info("%s: %d rows", name, table.num_rows if table else 0)
        conn = ducklake.get_connection()
        try:
            return _write_asset(conn, name, table, context)
        finally:
            conn.close()
    return _asset


def _build_courtlistener_bulk_assets() -> list:
    return [_make_cl_bulk_asset(n, f) for n, f in BULK_DATASETS.items()]


# CourtListener API — NYPD cases (SDNY + EDNY)

def _fetch_cl_api_search(query: str, court: str, token: str,
                         max_pages: int = 100) -> pa.Table | None:
    all_records = []
    url = f"{API_BASE}/search/"
    headers = {"Authorization": f"Token {token}"}
    params = {"q": query, "type": "r", "court": court}
    page = 0
    while url and page < max_pages:
        resp = _CLIENT.get(url, params=params if page == 0 else None,
                           headers=headers)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if results:
            all_records.extend(results)
        url = data.get("next")
        page += 1
    return pa.Table.from_pylist(all_records) if all_records else None


@dg.asset(
    key=dg.AssetKey([SCHEMA, "cl_nypd_cases_sdny"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def cl_nypd_cases_sdny(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    token = os.environ.get("SOURCES__COURTLISTENER_SOURCE__API_TOKEN", "")
    table = _fetch_cl_api_search('"city of new york" "police"', "nysd", token)
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "cl_nypd_cases_sdny", table, context)
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey([SCHEMA, "cl_nypd_cases_edny"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def cl_nypd_cases_edny(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    token = os.environ.get("SOURCES__COURTLISTENER_SOURCE__API_TOKEN", "")
    table = _fetch_cl_api_search('"city of new york" "police"', "nyed", token)
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "cl_nypd_cases_edny", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# Bulk CSV — FiveThirtyEight police settlements
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.bulk_csv import POLICE_SETTLEMENTS_URL


@dg.asset(
    key=dg.AssetKey([SCHEMA, "police_settlements_538"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def police_settlements_538(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    resp = _CLIENT.get(POLICE_SETTLEMENTS_URL)
    resp.raise_for_status()
    reader = csv_mod.DictReader(io.StringIO(resp.text))
    records = list(reader)
    table = pa.Table.from_pylist(records) if records else None
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "police_settlements_538", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# Census ZCTA — ACS 5-Year demographics by ZIP code
# ═══════════════════════════════════════════════════════════════════

from dagster_pipeline.sources.census_zcta import (
    VARIABLES as ZCTA_VARIABLES, NYC_ZIP_PREFIXES,
)

ZCTA_ACS_YEAR = 2023
ZCTA_ACS_BASE = f"https://api.census.gov/data/{ZCTA_ACS_YEAR}/acs/acs5"


@dg.asset(
    key=dg.AssetKey([SCHEMA, "acs_zcta_demographics"]),
    group_name=SCHEMA,
    op_tags={"schema": SCHEMA},
)
def acs_zcta_demographics(
    context: dg.AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> dg.MaterializeResult:
    var_codes = ",".join(ZCTA_VARIABLES.keys())
    url = f"{ZCTA_ACS_BASE}?get=NAME,{var_codes}&for=zip%20code%20tabulation%20area:*"
    data = _get_json(url)
    headers = data[0]

    records = []
    for row in data[1:]:
        record = dict(zip(headers, row))
        zcta = record.get("zip code tabulation area", "")
        if zcta[:3] in NYC_ZIP_PREFIXES:
            # Rename variable codes to friendly names
            clean = {"zcta": zcta}
            for var_code, friendly_name in ZCTA_VARIABLES.items():
                val = record.get(var_code)
                try:
                    clean[friendly_name] = float(val) if val and float(val) > -999999 else None
                except (ValueError, TypeError):
                    clean[friendly_name] = None
            records.append(clean)

    table = pa.Table.from_pylist(records) if records else None
    context.log.info("ZCTA: %d NYC ZIP codes", len(records))
    conn = ducklake.get_connection()
    try:
        return _write_asset(conn, "acs_zcta_demographics", table, context)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# Collect all federal assets
# ═══════════════════════════════════════════════════════════════════

all_federal_direct_assets = [
    *_build_bls_assets(),
    census_acs_demographics,
    college_scorecard_schools,
    *_build_hud_assets(),
    fec_contributions,
    propublica_nonprofits,
    littlesis_entities,
    littlesis_relationships,
    usaspending_contracts,
    usaspending_grants,
    *_build_echo_assets(),
    nrel_alt_fuel_stations,
    fda_food_enforcement,
    urban_school_directory,
    urban_school_finance,
    nyc_marriage_index,
    nypd_ccrb_complaints,
    nypd_ccrb_officers_current,
    nys_death_index,
    nys_campaign_finance,
    oca_housing_court_cases,
    *_build_courtlistener_bulk_assets(),
    cl_nypd_cases_sdny,
    cl_nypd_cases_edny,
    police_settlements_538,
    acs_zcta_demographics,
]
