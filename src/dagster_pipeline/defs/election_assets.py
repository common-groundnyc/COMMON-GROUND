"""Election results @asset functions — direct DuckDB ingestion.

Ingests NYC election results from:
  1. vote.nyc — ED-level CSVs for all NYC elections (2016-2024)
  2. MEDSL (Harvard Dataverse) — standardized precinct-level national/state data (2020, 2022)

Each asset: fetch CSV → Arrow → DuckLake zero-copy write (full replace).
"""
import io
import logging
import time

import dagster as dg
import httpx
import pyarrow as pa
import pyarrow.csv as pacsv

from dagster_pipeline.resources.ducklake import DuckLakeResource
from dagster_pipeline.ingestion.writer import write_full_replace, update_cursor

logger = logging.getLogger(__name__)

SCHEMA = "city_government"

_CLIENT = httpx.Client(
    timeout=httpx.Timeout(connect=30, read=300, write=60, pool=30),
    headers={"Accept-Encoding": "gzip", "User-Agent": "CommonGround/1.0"},
    follow_redirects=True,
)


def _write(conn, table_name: str, arrow_table: pa.Table | None,
           context: dg.AssetExecutionContext, schema: str = SCHEMA) -> dg.MaterializeResult:
    if arrow_table is None or arrow_table.num_rows == 0:
        context.log.warning("%s.%s: no data", schema, table_name)
        update_cursor(conn, f"{schema}.{table_name}", 0)
        return dg.MaterializeResult(metadata={"row_count": 0})
    rows = write_full_replace(conn, schema, table_name, arrow_table)
    update_cursor(conn, f"{schema}.{table_name}", rows)
    context.log.info("%s.%s: wrote %d rows", schema, table_name, rows)
    return dg.MaterializeResult(metadata={"row_count": rows})


# ═══════════════════════════════════════════════════════════════════
# vote.nyc — NYC Board of Elections ED-level results
# ═══════════════════════════════════════════════════════════════════

# Each tuple: (year, date+type URL segment, contest code, office description, table_name)
# NOTE: contest codes scraped from vote.nyc/page/election-results-summary-{year}
# The URL spaces are unencoded — vote.nyc serves both encoded and unencoded
VOTE_NYC_CONTESTS = [
    # 2025 General — Mayor, Public Advocate, Comptroller
    ("2025", "20251104General Election",
     "00000100000Citywide Mayor Citywide",
     "Mayor 2025", "election_2025_mayor"),
    ("2025", "20251104General Election",
     "00000200000Citywide Public Advocate Citywide",
     "Public Advocate 2025", "election_2025_public_advocate"),
    ("2025", "20251104General Election",
     "00000300000Citywide City Comptroller Citywide",
     "Comptroller 2025", "election_2025_comptroller"),

    # 2024 General
    ("2024", "20241105General Election",
     "00000100000Citywide President Vice President Citywide",
     "President 2024", "election_2024_president"),
    ("2024", "20241105General Election",
     "00000800000Citywide United States Senator Citywide",
     "US Senate 2024", "election_2024_us_senate"),

    # 2022 General
    ("2022", "20221108General Election",
     "00000200000Citywide Governor Lieutenant Governor Citywide",
     "Governor 2022", "election_2022_governor"),
    ("2022", "20221108General Election",
     "00000700000Citywide United States Senator Citywide",
     "US Senate 2022", "election_2022_us_senate"),

    # 2021 General
    ("2021", "20211102General Election",
     "00000200000Citywide Mayor Citywide",
     "Mayor 2021", "election_2021_mayor"),
    ("2021", "20211102General Election",
     "00000300000Citywide Public Advocate Citywide",
     "Public Advocate 2021", "election_2021_public_advocate"),
    ("2021", "20211102General Election",
     "00000400000Citywide City Comptroller Citywide",
     "Comptroller 2021", "election_2021_comptroller"),

    # 2020 General
    ("2020", "20201103General%20Election",
     "00000100000Citywide%20President%20Vice%20President%20Citywide",
     "President 2020", "election_2020_president"),

    # 2017 General
    ("2017", "20171107General%20Election",
     "00001100000Citywide%20Mayor%20Citywide",
     "Mayor 2017", "election_2017_mayor"),
    ("2017", "20171107General%20Election",
     "00001200000Citywide%20Public%20Advocate%20Citywide",
     "Public Advocate 2017", "election_2017_public_advocate"),
    ("2017", "20171107General%20Election",
     "00001300000Citywide%20City%20Comptroller%20Citywide",
     "Comptroller 2017", "election_2017_comptroller"),

    # 2016 General
    ("2016", "20161108General%20Election",
     "00000100000Citywide%20President%20Vice%20President%20Citywide",
     "President 2016", "election_2016_president"),
    ("2016", "20161108General%20Election",
     "00001000000Citywide%20United%20States%20Senator%20Citywide",
     "US Senate 2016", "election_2016_us_senate"),
]

VOTE_NYC_BASE = "https://vote.nyc/sites/default/files/pdf/election_results"


def _fetch_vote_nyc(year: str, election_segment: str, contest_code: str) -> pa.Table | None:
    """Download an ED-level CSV from vote.nyc and parse into Arrow.

    vote.nyc CSVs have a quirky format: each row has 22 columns where
    columns 0-10 are header labels and columns 11-21 are the actual values.
    We extract the data columns and apply the header names.
    """
    # Build URL — some entries use raw spaces, others use %20
    # Normalize to spaces then percent-encode the path segments
    from urllib.parse import quote
    seg_clean = election_segment.replace("%20", " ")
    code_clean = contest_code.replace("%20", " ")
    url = f"{VOTE_NYC_BASE}/{year}/{quote(seg_clean)}/{quote(code_clean)} EDLevel.csv"
    try:
        resp = _CLIENT.get(url)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("vote.nyc fetch failed for %s: %s", url, e)
        return None

    import csv as csv_mod

    text = resp.content.decode("utf-8-sig", errors="replace")
    reader = csv_mod.reader(io.StringIO(text))
    rows = list(reader)

    if len(rows) < 2:
        return None

    # Detect format: double-width (22 cols) vs normal (11 cols)
    first_row_len = len(rows[0]) if rows else 0
    headers = ["AD", "ED", "County", "EDAD Status", "Event",
               "Party/Independent Body", "Office/Position Title",
               "District Key", "VoteFor", "Unit Name", "Tally"]

    if first_row_len >= 22:
        # Double-width format (2020-2024): headers in cols 0-10, data in cols 11-21
        data_rows = []
        for row in rows:
            if len(row) >= 22:
                data_rows.append(row[11:22])
    else:
        # Normal CSV format (2016-2017): header row + data rows
        data_rows = rows[1:]  # skip header

    if not data_rows:
        return None

    # Build dict for Arrow
    columns = {}
    for i, header in enumerate(headers):
        col_name = header.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
        columns[col_name] = [row[i] if i < len(row) else None for row in data_rows]

    # Convert tally to int
    if "tally" in columns:
        columns["tally"] = [
            int(v) if v and v.strip().lstrip("-").isdigit() else 0
            for v in columns["tally"]
        ]

    # Convert votefor to int
    if "votefor" in columns:
        columns["votefor"] = [
            int(v) if v and v.strip().isdigit() else 1
            for v in columns["votefor"]
        ]

    table = pa.table(columns)

    # Filter out rows where AD is literally "AD" (header echo rows) or EDAD is "COMBINED"
    import pyarrow.compute as pc
    if "ad" in table.column_names:
        mask = pc.not_equal(table.column("ad"), "AD")
        table = table.filter(mask)

    return table


def _make_vote_nyc_asset(year: str, election_segment: str, contest_code: str,
                          office_desc: str, table_name: str):
    @dg.asset(
        key=dg.AssetKey([SCHEMA, table_name]),
        group_name="elections",
        op_tags={"schema": SCHEMA},
        description=f"NYC BOE ED-level results: {office_desc}",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        ducklake: DuckLakeResource,
    ) -> dg.MaterializeResult:
        context.log.info("Fetching %s from vote.nyc...", office_desc)
        table = _fetch_vote_nyc(year, election_segment, contest_code)
        conn = ducklake.get_connection()
        try:
            return _write(conn, table_name, table, context)
        finally:
            conn.close()

    _asset.__name__ = table_name
    _asset.__qualname__ = table_name
    return _asset


def _build_vote_nyc_assets() -> list:
    return [
        _make_vote_nyc_asset(year, seg, code, desc, name)
        for year, seg, code, desc, name in VOTE_NYC_CONTESTS
    ]


# ═══════════════════════════════════════════════════════════════════
# MEDSL — Harvard Dataverse precinct-level national/state results
# ═══════════════════════════════════════════════════════════════════

# File IDs from Harvard Dataverse API
MEDSL_FILES = [
    ("6100433", "2020", "medsl_ny_precinct_2020"),   # 2020 all races, NY only, 115 MB TSV
    ("10855145", "2022", "medsl_ny_precinct_2022"),   # 2022 all races, NY only, 110 MB TSV
]

MEDSL_BASE = "https://dataverse.harvard.edu/api/access/datafile"


def _fetch_medsl(file_id: str) -> pa.Table | None:
    """Download a MEDSL precinct file from Harvard Dataverse."""
    url = f"{MEDSL_BASE}/{file_id}"
    try:
        resp = _CLIENT.get(url)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("MEDSL fetch failed for file %s: %s", file_id, e)
        return None

    # MEDSL files are TSV
    table = pacsv.read_csv(
        io.BytesIO(resp.content),
        parse_options=pacsv.ParseOptions(delimiter="\t"),
    )

    if table.num_rows == 0:
        return None

    # Filter to NYC counties only (FIPS: 36005=Bronx, 36047=Kings/Brooklyn,
    # 36061=New York/Manhattan, 36081=Queens, 36085=Richmond/Staten Island)
    nyc_fips = ["36005", "36047", "36061", "36081", "36085"]

    # county_fips may be int or string depending on year
    fips_col = table.column("county_fips")
    if pa.types.is_integer(fips_col.type):
        nyc_fips_int = [int(f) for f in nyc_fips]
        import pyarrow.compute as pc
        mask = pc.is_in(fips_col, value_set=pa.array(nyc_fips_int))
    else:
        import pyarrow.compute as pc
        mask = pc.is_in(fips_col, value_set=pa.array(nyc_fips))

    table = table.filter(mask)

    # Normalize column names
    new_names = [c.lower().replace(" ", "_") for c in table.column_names]
    table = table.rename_columns(new_names)

    return table


def _make_medsl_asset(file_id: str, year: str, table_name: str):
    @dg.asset(
        key=dg.AssetKey(["federal", table_name]),
        group_name="elections",
        op_tags={"schema": "federal"},
        description=f"MEDSL precinct-level results: NY {year} (filtered to NYC)",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        ducklake: DuckLakeResource,
    ) -> dg.MaterializeResult:
        context.log.info("Fetching MEDSL %s from Harvard Dataverse (file %s)...", year, file_id)
        table = _fetch_medsl(file_id)
        conn = ducklake.get_connection()
        try:
            return _write(conn, table_name, table, context, schema="federal")
        finally:
            conn.close()

    _asset.__name__ = table_name
    _asset.__qualname__ = table_name
    return _asset


def _build_medsl_assets() -> list:
    return [_make_medsl_asset(fid, year, name) for fid, year, name in MEDSL_FILES]


# ═══════════════════════════════════════════════════════════════════
# Export all election assets
# ═══════════════════════════════════════════════════════════════════

election_assets = _build_vote_nyc_assets() + _build_medsl_assets()
