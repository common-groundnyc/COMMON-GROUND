# foundation.address_lookup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest NYC PAD (Property Address Directory) as `lake.foundation.address_lookup` and replace the fragile PLUTO LIKE-scan `_resolve_bbl()` with fast exact-match resolution.

**Architecture:** New Dagster asset downloads PAD zip from Socrata, parses the ADR CSV, loads into DuckLake. MCP server's `_resolve_bbl()` swaps from PLUTO queries to `foundation.address_lookup` queries. All PLUTO enrichment queries remain unchanged.

**Tech Stack:** Python, Dagster, DuckDB/DuckLake, requests (HTTP download), csv (stdlib), pytest

**Spec:** `docs/superpowers/specs/2026-04-02-address-lookup-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `src/dagster_pipeline/defs/address_lookup_asset.py` | Dagster asset: download PAD, parse ADR CSV, load to DuckLake |
| Create | `tests/test_address_lookup.py` | Unit tests for PAD parsing and address normalization |
| Modify | `src/dagster_pipeline/definitions.py:14,24,130` | Register `address_lookup` asset, import, add to jobs |
| Modify | `infra/duckdb-server/tools/building.py:29-159` | Replace `_resolve_bbl()`, simplify `_normalize_address()` |
| Modify | `infra/duckdb-server/tools/address_report.py:136` | Update import of `_resolve_bbl` (if needed) |

---

### Task 1: PAD parsing and loading — Dagster asset

**Files:**
- Create: `src/dagster_pipeline/defs/address_lookup_asset.py`
- Create: `tests/test_address_lookup.py`

- [ ] **Step 1: Write failing tests for PAD ADR row parsing**

Create `tests/test_address_lookup.py`:

```python
import pytest
from dagster_pipeline.defs.address_lookup_asset import parse_adr_row, parse_adr_csv


def test_parse_adr_row_basic():
    """Standard Brooklyn address row."""
    row = {
        "boro": "3",
        "block": "00123",
        "lot": "0045",
        "bin": "3012345",
        "lhns": "  200",
        "hhns": "  210",
        "stname": "ATLANTIC AVENUE             ",
        "b10sc": "3004001010",
        "zipcode": "11217",
        "addrtype": "",
    }
    result = parse_adr_row(row)
    assert result["bbl"] == "3001230045"
    assert result["bin"] == "3012345"
    assert result["house_number"] == 200
    assert result["house_number_high"] == 210
    assert result["street_std"] == "ATLANTIC AVENUE"
    assert result["street_code"] == "3004001010"
    assert result["boro_code"] == "3"
    assert result["zipcode"] == "11217"
    assert result["addr_type"] == ""
    assert result["address_std"] == "200 ATLANTIC AVENUE"


def test_parse_adr_row_vanity():
    """Vanity address (addr_type='V') should be included."""
    row = {
        "boro": "1",
        "block": "00077",
        "lot": "0001",
        "bin": "1001389",
        "lhns": "    1",
        "hhns": "    1",
        "stname": "WALL STREET                 ",
        "b10sc": "1008501010",
        "zipcode": "10005",
        "addrtype": "V",
    }
    result = parse_adr_row(row)
    assert result["bbl"] == "1000770001"
    assert result["addr_type"] == "V"
    assert result["address_std"] == "1 WALL STREET"


def test_parse_adr_row_pseudo_excluded():
    """Pseudo address (addr_type='Q') should return None."""
    row = {
        "boro": "1",
        "block": "00001",
        "lot": "0001",
        "bin": "1000000",
        "lhns": "     ",
        "hhns": "     ",
        "stname": "BROADWAY                    ",
        "b10sc": "1000401010",
        "zipcode": "10001",
        "addrtype": "Q",
    }
    result = parse_adr_row(row)
    assert result is None


def test_parse_adr_row_no_house_number():
    """Row with blank house number should return None."""
    row = {
        "boro": "2",
        "block": "00500",
        "lot": "0001",
        "bin": "2000000",
        "lhns": "     ",
        "hhns": "     ",
        "stname": "GRAND CONCOURSE             ",
        "b10sc": "2002001010",
        "zipcode": "10451",
        "addrtype": "",
    }
    result = parse_adr_row(row)
    assert result is None


def test_parse_adr_row_nap_excluded():
    """NAP address (addr_type='N') should return None."""
    row = {
        "boro": "4",
        "block": "00100",
        "lot": "0001",
        "bin": "4000001",
        "lhns": "  100",
        "hhns": "  100",
        "stname": "QUEENS BOULEVARD            ",
        "b10sc": "4005001010",
        "zipcode": "11101",
        "addrtype": "N",
    }
    result = parse_adr_row(row)
    assert result is None


def test_parse_adr_csv_filters_and_parses(tmp_path):
    """Integration test: parse a small CSV, verify filtering and output."""
    csv_content = (
        "boro,block,lot,bblscc,bin,lhnd,lhns,lcontpar,lsos,hhnd,hhns,hcontpar,"
        "scboro,sc5,sclgc,stname,addrtype,realb7sc,validlgcs,parity,b10sc,"
        "segid,zipcode\n"
        "1,00077,0001,0,1001389,1,    1,,O,1,    1,,1,00850,10,"
        "WALL STREET                 ,,       ,01  ,E,1008501010,0071780,10005\n"
        "1,00077,0001,0,1001389,68,   68,,O,68,   68,,1,00040,10,"
        "BROADWAY                    ,V,1008501,01  ,E,1000401010,0071780,10005\n"
        "3,00123,0045,0,3012345,,     ,,O,,     ,,3,00400,10,"
        "ATLANTIC AVENUE             ,Q,       ,01  ,E,3004001010,0012345,11217\n"
    )
    csv_file = tmp_path / "bobaadr.txt"
    csv_file.write_text(csv_content)

    rows = parse_adr_csv(str(csv_file))
    # Real (1 WALL ST) + Vanity (68 BROADWAY) = 2 rows, pseudo excluded
    assert len(rows) == 2
    assert rows[0]["address_std"] == "1 WALL STREET"
    assert rows[1]["address_std"] == "68 BROADWAY"
    assert rows[1]["addr_type"] == "V"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_address_lookup.py -v`
Expected: FAIL with `ImportError: cannot import name 'parse_adr_row'`

- [ ] **Step 3: Implement PAD parsing functions**

Create `src/dagster_pipeline/defs/address_lookup_asset.py`:

```python
"""Dagster asset producing lake.foundation.address_lookup from NYC PAD."""

import csv
import io
import logging
import os
import time
import zipfile
from pathlib import Path

import duckdb
import requests
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)

# PAD Socrata blob — download URL
PAD_SOCRATA_ID = "bc8t-ecyu"
PAD_DOWNLOAD_URL = (
    f"https://data.cityofnewyork.us/api/views/{PAD_SOCRATA_ID}/rows.csv"
    "?accessType=DOWNLOAD"
)
# Fallback: direct BYTES of the Big Apple
PAD_BYTES_URL = "https://data.cityofnewyork.us/api/views/bc8t-ecyu/files"

# ADR columns we need (positional in the CSV)
_INCLUDED_ADDR_TYPES = {"", "V"}  # real + vanity; exclude Q (pseudo), N (NAP)


def parse_adr_row(row: dict) -> dict | None:
    """Parse a single PAD ADR row into our lookup schema.

    Returns None for rows that should be excluded (pseudo, NAP, no house number).
    """
    addr_type = row.get("addrtype", "").strip()
    if addr_type not in _INCLUDED_ADDR_TYPES:
        return None

    lhns = row.get("lhns", "").strip()
    if not lhns or not lhns.isdigit():
        return None

    hhns = row.get("hhns", "").strip()
    house_number = int(lhns)
    house_number_high = int(hhns) if hhns and hhns.isdigit() else house_number

    boro = row["boro"].strip()
    block = row["block"].strip().zfill(5)
    lot = row["lot"].strip().zfill(4)
    bbl = f"{boro}{block}{lot}"

    street_std = row.get("stname", "").strip()
    if not street_std:
        return None

    address_std = f"{house_number} {street_std}"

    return {
        "bbl": bbl,
        "bin": row.get("bin", "").strip(),
        "house_number": house_number,
        "house_number_high": house_number_high,
        "street_std": street_std,
        "street_code": row.get("b10sc", "").strip(),
        "boro_code": boro,
        "zipcode": row.get("zipcode", "").strip(),
        "addr_type": addr_type,
        "address_std": address_std,
    }


def parse_adr_csv(csv_path: str) -> list[dict]:
    """Parse PAD ADR CSV file, returning filtered and cleaned rows."""
    rows = []
    with open(csv_path, "r", encoding="latin-1") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            parsed = parse_adr_row(raw)
            if parsed is not None:
                rows.append(parsed)
    return rows


def download_pad_zip(dest_dir: str) -> str:
    """Download PAD zip from Socrata and return path to extracted ADR CSV."""
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    zip_path = dest / "pad.zip"

    # Try Socrata metadata API to get the actual file download URL
    meta_url = f"https://data.cityofnewyork.us/api/views/{PAD_SOCRATA_ID}.json"
    resp = requests.get(meta_url, timeout=30)
    resp.raise_for_status()
    meta = resp.json()

    # Extract the blobId for the file attachment
    download_url = None
    if "blobId" in meta:
        download_url = (
            f"https://data.cityofnewyork.us/api/views/{PAD_SOCRATA_ID}"
            f"/files/{meta['blobId']}?download=true&filename=pad.zip"
        )
    if not download_url:
        # Fallback: try accessPoints
        access = meta.get("metadata", {}).get("accessPoints", {})
        download_url = access.get("blob", PAD_DOWNLOAD_URL)

    logger.info("Downloading PAD from %s", download_url)
    resp = requests.get(download_url, timeout=300, stream=True)
    resp.raise_for_status()

    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

    logger.info("Downloaded PAD zip: %.1f MB", zip_path.stat().st_size / 1e6)

    # Extract — find the ADR file (usually bobaadr.txt or similar)
    with zipfile.ZipFile(zip_path) as zf:
        adr_names = [n for n in zf.namelist() if "adr" in n.lower()]
        if not adr_names:
            # Fallback: look for any .txt that's not bbl
            adr_names = [n for n in zf.namelist()
                         if n.endswith(".txt") and "bbl" not in n.lower()]
        if not adr_names:
            raise RuntimeError(f"No ADR file found in PAD zip. Contents: {zf.namelist()}")

        adr_name = adr_names[0]
        zf.extract(adr_name, dest)
        logger.info("Extracted %s", adr_name)
        return str(dest / adr_name)


@asset(
    key=AssetKey(["foundation", "address_lookup"]),
    group_name="foundation",
    description=(
        "PAD-based address→BBL lookup table. DCP-standardized street names, "
        "~1M addresses, quarterly updates from NYC Property Address Directory."
    ),
    compute_kind="duckdb",
)
def address_lookup(context) -> MaterializeResult:
    """Download PAD, parse ADR CSV, load into lake.foundation.address_lookup."""
    import tempfile

    t0 = time.time()

    with tempfile.TemporaryDirectory() as tmp_dir:
        # 1. Download and extract PAD
        context.log.info("Downloading PAD from Socrata...")
        adr_path = download_pad_zip(tmp_dir)

        # 2. Parse ADR CSV
        context.log.info("Parsing ADR CSV...")
        rows = parse_adr_csv(adr_path)
        context.log.info("Parsed %d address rows", len(rows))

    # 3. Load into DuckLake
    conn = _connect_ducklake()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.address_lookup")

        conn.execute("""
            CREATE TABLE lake.foundation.address_lookup (
                bbl              VARCHAR(10),
                bin              VARCHAR(7),
                house_number     INT,
                house_number_high INT,
                street_std       VARCHAR,
                street_code      VARCHAR(10),
                boro_code        VARCHAR(1),
                zipcode          VARCHAR(5),
                addr_type        VARCHAR(1),
                address_std      VARCHAR
            )
        """)

        # Batch insert using DuckDB's executemany
        conn.executemany(
            """INSERT INTO lake.foundation.address_lookup VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )""",
            [
                (
                    r["bbl"], r["bin"], r["house_number"], r["house_number_high"],
                    r["street_std"], r["street_code"], r["boro_code"],
                    r["zipcode"], r["addr_type"], r["address_std"],
                )
                for r in rows
            ],
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.address_lookup"
        ).fetchone()[0]
        distinct_bbls = conn.execute(
            "SELECT COUNT(DISTINCT bbl) FROM lake.foundation.address_lookup"
        ).fetchone()[0]
        distinct_streets = conn.execute(
            "SELECT COUNT(DISTINCT street_std) FROM lake.foundation.address_lookup"
        ).fetchone()[0]

        elapsed = time.time() - t0
        context.log.info(
            "address_lookup: %s rows, %s BBLs, %s streets in %.1fs",
            f"{row_count:,}", f"{distinct_bbls:,}", f"{distinct_streets:,}", elapsed,
        )

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(row_count),
                "distinct_bbls": MetadataValue.int(distinct_bbls),
                "distinct_streets": MetadataValue.int(distinct_streets),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_address_lookup.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add src/dagster_pipeline/defs/address_lookup_asset.py tests/test_address_lookup.py
git commit -m "feat: add address_lookup Dagster asset — PAD ingestion and parsing"
```

---

### Task 2: Register asset in Dagster definitions

**Files:**
- Modify: `src/dagster_pipeline/definitions.py`

- [ ] **Step 1: Add import for address_lookup**

In `src/dagster_pipeline/definitions.py`, after line 14 (`from dagster_pipeline.defs.foundation_assets import ...`), add:

```python
from dagster_pipeline.defs.address_lookup_asset import address_lookup
```

- [ ] **Step 2: Add to all_assets list**

In `src/dagster_pipeline/definitions.py`, line 24, add `address_lookup` to the `all_assets` list. Change:

```python
all_assets = [*all_socrata_direct_assets, *all_federal_direct_assets, name_index, resolved_entities,
              h3_index, phonetic_index, row_fingerprints, data_health, entity_name_embeddings,
              entity_master,
              mv_building_hub, mv_acris_deeds, mv_zip_stats, mv_crime_precinct, mv_corp_network,
              spatial_views]
```

to:

```python
all_assets = [*all_socrata_direct_assets, *all_federal_direct_assets, name_index, resolved_entities,
              h3_index, phonetic_index, row_fingerprints, data_health, entity_name_embeddings,
              entity_master, address_lookup,
              mv_building_hub, mv_acris_deeds, mv_zip_stats, mv_crime_precinct, mv_corp_network,
              spatial_views]
```

- [ ] **Step 3: Verify Dagster can load the definitions**

Run: `cd ~/Desktop/dagster-pipeline && uv run python -c "from dagster_pipeline.definitions import defs; print(f'Loaded {len(defs.get_all_asset_specs())} assets')"`
Expected: prints a count (one more than before)

- [ ] **Step 4: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add src/dagster_pipeline/definitions.py
git commit -m "feat: register address_lookup asset in Dagster definitions"
```

---

### Task 3: Replace `_resolve_bbl()` in building.py

**Files:**
- Modify: `infra/duckdb-server/tools/building.py:29-159`
- Create: `infra/duckdb-server/tests/test_address_resolution.py`

- [ ] **Step 1: Write failing tests for new resolve_bbl**

Create `infra/duckdb-server/tests/test_address_resolution.py`:

```python
"""Tests for the new PAD-based address resolution functions."""

import pytest
from tools.building import _normalize_address_for_pad, _extract_borough, _extract_house_number


class TestNormalizeAddressForPad:
    def test_expands_abbreviations(self):
        assert "EAST" in _normalize_address_for_pad("200 E 10th St")

    def test_strips_ordinal_suffix(self):
        result = _normalize_address_for_pad("200 E 10th St")
        assert "10TH" not in result
        assert "10 " in result or result.endswith("10")

    def test_strips_borough(self):
        result = _normalize_address_for_pad("200 E 10th St, Manhattan")
        assert "MANHATTAN" not in result

    def test_strips_state_zip(self):
        result = _normalize_address_for_pad("200 E 10th St, NY 10003")
        assert "NY" not in result
        assert "10003" not in result

    def test_strips_apartment(self):
        result = _normalize_address_for_pad("200 E 10th St APT 4B")
        assert "APT" not in result
        assert "4B" not in result

    def test_expands_spelled_ordinals(self):
        result = _normalize_address_for_pad("200 Fifth Ave")
        assert "5 AVENUE" in result

    def test_preserves_numbers(self):
        result = _normalize_address_for_pad("350 5th Ave")
        assert result.startswith("350")


class TestExtractBorough:
    def test_manhattan(self):
        assert _extract_borough("200 E 10th St, Manhattan") == "1"

    def test_brooklyn_abbreviation(self):
        assert _extract_borough("100 Atlantic Ave, BK") == "3"

    def test_staten_island_nickname(self):
        assert _extract_borough("123 Victory Blvd, Shaolin") == "5"

    def test_no_borough(self):
        assert _extract_borough("200 E 10th St") is None

    def test_queens(self):
        assert _extract_borough("100 Queens Blvd, Queens") == "4"


class TestExtractHouseNumber:
    def test_simple(self):
        assert _extract_house_number("200 E 10th St") == 200

    def test_hyphenated_queens(self):
        assert _extract_house_number("45-17 21st St, Queens") == 45

    def test_no_number(self):
        assert _extract_house_number("Broadway, Manhattan") is None

    def test_leading_zeros(self):
        assert _extract_house_number("001 Main St") == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest infra/duckdb-server/tests/test_address_resolution.py -v`
Expected: FAIL with `ImportError: cannot import name '_normalize_address_for_pad'`

- [ ] **Step 3: Rewrite address normalization and resolution in building.py**

In `infra/duckdb-server/tools/building.py`, replace the entire block from `_ORDINAL_MAP` (line 29) through the end of `_resolve_bbl` (line 159) with:

```python
# ---------------------------------------------------------------------------
# Address normalization helpers (simplified for PAD matching)
# ---------------------------------------------------------------------------

_ORDINAL_MAP = {
    "FIRST": "1", "SECOND": "2", "THIRD": "3", "FOURTH": "4", "FIFTH": "5",
    "SIXTH": "6", "SEVENTH": "7", "EIGHTH": "8", "NINTH": "9", "TENTH": "10",
    "ELEVENTH": "11", "TWELFTH": "12",
}

_ABBREV_MAP = {
    "AVE": "AVENUE", "ST": "STREET", "BLVD": "BOULEVARD", "BL": "BOULEVARD",
    "DR": "DRIVE", "PL": "PLACE", "RD": "ROAD", "CT": "COURT", "LN": "LANE",
    "PKY": "PARKWAY", "PKWY": "PARKWAY", "TER": "TERRACE", "HWY": "HIGHWAY",
    "E": "EAST", "W": "WEST", "N": "NORTH", "S": "SOUTH",
}

_BOROUGH_MAP = {
    "MANHATTAN": "1", "MN": "1", "NEW YORK": "1", "NY": "1",
    "BRONX": "2", "BX": "2", "THE BRONX": "2",
    "BROOKLYN": "3", "BK": "3", "BKLYN": "3", "KINGS": "3",
    "QUEENS": "4", "QN": "4", "QNS": "4",
    "STATEN ISLAND": "5", "SI": "5", "RICHMOND": "5", "SHAOLIN": "5",
}


def _normalize_address_for_pad(raw: str) -> str:
    """Normalize user input to match PAD's DCP-standardized street names.

    PAD uses consistent formats like 'EAST 10 STREET' — no ordinal suffixes,
    no abbreviations. This function bridges user input to PAD format.
    """
    s = raw.strip().upper()
    s = re.sub(r',?\s*NY\s*\d*\s*$', '', s)
    for boro in sorted(_BOROUGH_MAP.keys(), key=len, reverse=True):
        s = re.sub(rf',\s*{re.escape(boro)}\s*$', '', s)
    s = s.strip().rstrip(',').strip()
    s = re.sub(r'\s*(APT|UNIT|FL|FLOOR|STE|SUITE|RM|ROOM|#)\s*\S+\s*$', '', s)
    # Strip ordinal suffixes: 10TH -> 10, 3RD -> 3
    s = re.sub(r'\b(\d+)(?:ST|ND|RD|TH)\b', r'\1', s)
    # Expand spelled-out ordinals: FIFTH -> 5
    for word, num in _ORDINAL_MAP.items():
        s = re.sub(rf'\b{word}\b', num, s)
    # Expand abbreviations: AVE -> AVENUE, E -> EAST
    for abbr, full in _ABBREV_MAP.items():
        s = re.sub(rf'\b{abbr}\b', full, s)
    return s


# Keep old name as alias for any imports in address_report.py
_normalize_address = _normalize_address_for_pad


def _extract_borough(raw: str) -> str | None:
    """Extract a borocode ('1'-'5') from an address string, or None."""
    s = raw.strip().upper()
    for boro in sorted(_BOROUGH_MAP.keys(), key=len, reverse=True):
        if re.search(rf'\b{re.escape(boro)}\b', s):
            return _BOROUGH_MAP[boro]
    return None


def _extract_house_number(raw: str) -> int | None:
    """Extract the house number from an address string.

    Handles hyphenated Queens-style addresses (45-17 → 45).
    Returns None if no house number found.
    """
    s = raw.strip().upper()
    m = re.match(r'^(\d+)', s)
    if m:
        return int(m.group(1))
    return None


# ---------------------------------------------------------------------------
# BBL resolution (PAD-based)
# ---------------------------------------------------------------------------


def _resolve_bbl(pool, identifier: str) -> str:
    """Resolve an address string to a 10-digit BBL via PAD lookup.

    Two-tier search against foundation.address_lookup:
    1. Exact house number + street prefix match (with borough filter if available)
    2. Nearest lot on same street (fallback)
    """
    street_norm = _normalize_address_for_pad(identifier)
    boro = _extract_borough(identifier)
    house_num = _extract_house_number(identifier)

    # Extract street part (everything after house number)
    street_part = re.sub(r'^\d+[\-\d]*\s*', '', street_norm).strip()

    if not street_part:
        raise ToolError(
            f"Could not parse street name from '{identifier}'. "
            "Try a format like '350 5th Ave, Manhattan'."
        )

    # Tier 1: exact house number + street prefix
    if house_num is not None:
        boro_clause = "AND boro_code = ?" if boro else ""

        _cols, _rows = execute(pool, f"""
            SELECT bbl FROM lake.foundation.address_lookup
            WHERE house_number <= ? AND house_number_high >= ?
              AND street_std LIKE ?
              AND addr_type IN ('', 'V')
              {boro_clause}
            LIMIT 1
        """, [house_num, house_num, street_part + "%"] + ([boro] if boro else []))
        if _rows:
            return str(_rows[0][0])

        # Try without range — exact house_number match
        _cols, _rows = execute(pool, f"""
            SELECT bbl FROM lake.foundation.address_lookup
            WHERE house_number = ?
              AND street_std LIKE ?
              AND addr_type IN ('', 'V')
              {boro_clause}
            LIMIT 1
        """, [house_num, street_part + "%"] + ([boro] if boro else []))
        if _rows:
            return str(_rows[0][0])

    # Tier 2: nearest lot on the same street
    if house_num is not None and street_part:
        boro_clause = "AND boro_code = ?" if boro else ""
        _cols, _rows = execute(pool, f"""
            SELECT bbl FROM lake.foundation.address_lookup
            WHERE street_std LIKE ?
              AND addr_type IN ('', 'V')
              {boro_clause}
            ORDER BY ABS(house_number - ?)
            LIMIT 1
        """, [street_part + "%"] + ([boro] if boro else []) + [house_num])
        if _rows:
            return str(_rows[0][0])

    raise ToolError(
        f"No building found for address '{identifier}'. "
        "Try a simpler address like '350 5th Ave' or include borough."
    )
```

- [ ] **Step 4: Run the new resolution tests**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest infra/duckdb-server/tests/test_address_resolution.py -v`
Expected: All tests PASS

- [ ] **Step 5: Run existing building.py tests to check for regressions**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest infra/duckdb-server/tests/ -v -k "not test_middleware and not test_percentile"`
Expected: All existing tests still PASS

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/building.py infra/duckdb-server/tests/test_address_resolution.py
git commit -m "feat: replace _resolve_bbl with PAD-based lookup against foundation.address_lookup"
```

---

### Task 4: Update address_report.py import

**Files:**
- Modify: `infra/duckdb-server/tools/address_report.py:136`

- [ ] **Step 1: Verify the import still works**

The `_resolve_context` function at line 136 of `address_report.py` imports `_resolve_bbl` from `tools.building`:

```python
from tools.building import _normalize_address, _resolve_bbl
```

Since we aliased `_normalize_address = _normalize_address_for_pad` in Task 3, this import should work unchanged. Verify:

Run: `cd ~/Desktop/dagster-pipeline && uv run python -c "import sys; sys.path.insert(0, 'infra/duckdb-server'); from tools.building import _normalize_address, _resolve_bbl; print('imports OK')"`
Expected: `imports OK`

- [ ] **Step 2: Commit (no-op if import works)**

If the import works, no file change needed. Skip this commit.

---

### Task 5: Materialize and smoke test

**Files:** None (operational verification)

- [ ] **Step 1: Materialize the address_lookup asset**

Run inside Docker (per project convention):

```bash
cd ~/Desktop/dagster-pipeline
docker compose run --rm dagster uv run dagster asset materialize \
  --select 'foundation/address_lookup' -m dagster_pipeline.definitions
```

Expected: asset materializes successfully, logs show ~1M rows, ~800K BBLs, ~30K streets.

- [ ] **Step 2: Verify table contents via DuckDB**

After materialization, query the table to verify:

```bash
cd ~/Desktop/dagster-pipeline
uv run python -c "
from src.dagster_pipeline.defs.name_index_asset import _connect_ducklake
conn = _connect_ducklake()
print(conn.execute('SELECT COUNT(*) FROM lake.foundation.address_lookup').fetchone())
print(conn.execute('SELECT * FROM lake.foundation.address_lookup LIMIT 3').fetchdf())
conn.close()
"
```

Expected: row count ~1M, sample rows with clean BBL/street data.

- [ ] **Step 3: Smoke test address resolution via MCP**

Restart the MCP server and test resolution:

```bash
ssh fattie@178.156.228.119 "cd /opt/duckdb-server && docker compose restart duckdb-server"
```

Then test via the MCP tool: query `building("200 E 10th St, Manhattan")` — should resolve to BBL `1004520054` and return in <100ms.

- [ ] **Step 4: Test edge cases that previously failed**

Test addresses that were problematic with the old PLUTO resolver:
- `"1 Wall Street, Manhattan"` — vanity address
- `"350 5th Ave, Manhattan"` — ordinal in address
- `"45-17 21st Street, Queens"` — hyphenated Queens address
- `"100 Victory Blvd, Staten Island"` — Staten Island

- [ ] **Step 5: Commit any fixes from smoke testing**

```bash
cd ~/Desktop/dagster-pipeline
git add -A
git commit -m "fix: address resolution adjustments from smoke testing"
```

(Only if fixes were needed — skip if smoke test passed clean.)

---

### Task 6: Update memory and clean up

**Files:**
- Update: `~/.claude/projects/-Users-fattie2020/memory/project_address_lookup_table.md`

- [ ] **Step 1: Update the project memory note**

Update the existing memory file to reflect that PAD is now the source instead of PLUTO normalization:

```markdown
---
name: Address lookup table — PAD-based
description: foundation.address_lookup uses NYC PAD (Property Address Directory) for address→BBL resolution, replacing PLUTO LIKE scans
type: project
---

`lake.foundation.address_lookup` — PAD-based address→BBL resolution table.

**Source:** NYC PAD (Property Address Directory), Socrata blob `bc8t-ecyu`
**Schema:** bbl, bin, house_number, house_number_high, street_std, street_code, boro_code, zipcode, addr_type, address_std
**Row count:** ~1M addresses (real + vanity)
**Update:** Quarterly (matches PAD release cadence)

**Why:** PLUTO's free-text address field was inconsistent. PAD uses DCP-standardized street names via street codes. Resolution went from 500ms–10s LIKE scans to <10ms exact matches.

**How it works:** Dagster asset downloads PAD zip, parses ADR CSV, loads to DuckLake. MCP server's `_resolve_bbl()` in `building.py` queries this table instead of PLUTO. All PLUTO enrichment queries remain unchanged.

**Decided:** 2026-04-02. Replaced the original plan (SQL-normalize PLUTO) after discovering PAD is the canonical NYC address directory.
```

- [ ] **Step 2: Commit memory update**

```bash
git add ~/.claude/projects/-Users-fattie2020/memory/project_address_lookup_table.md
git commit -m "docs: update memory — address_lookup now PAD-based"
```
