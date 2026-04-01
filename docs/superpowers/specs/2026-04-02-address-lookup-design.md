# foundation.address_lookup — PAD-based Address Resolution

**Date:** 2026-04-02
**Status:** Approved
**Author:** Claude + fattie2020

## Problem

Address→BBL resolution in the MCP server (`_resolve_bbl()` in `building.py`) uses LIKE scans against raw `lake.city_government.pluto`. PLUTO stores addresses as free-text strings with inconsistent formatting ("200 EAST 10TH STREET" vs "200 EAST 10 STREET" vs "200 E 10 ST"). The current 3-tier resolution does:

1. Python regex normalization (ordinal stripping, abbreviation expansion)
2. `WHERE UPPER(address) LIKE ? || '%'` — full table scan against ~870K PLUTO rows
3. Nearest-lot fallback with `ABS()` distance calculation

This is slow (500ms–10s), fragile (misses many valid addresses), and the normalization logic is duplicated at query time on every request.

## Solution

Ingest NYC's **PAD (Property Address Directory)** — the dataset behind GeoSearch and all NYC geocoding tools — as `lake.foundation.address_lookup`. PAD contains DCP-standardized street names via street codes, mapping every address in NYC to a BBL. Replace `_resolve_bbl()` with exact-match queries against this pre-standardized table.

## Data Source

- **Dataset:** PAD (Property Address Directory)
- **Publisher:** NYC Department of City Planning
- **Socrata ID:** `bc8t-ecyu` (blob download, not queryable API)
- **Format:** ZIP containing two CSVs: `bobaadr.txt` (ADR table — addresses) and `bobbbl.txt` (BBL table — condos)
- **We ingest:** ADR table only
- **Size:** ~46 MB compressed, ~1–2M address records
- **Update frequency:** Quarterly (versions: 26a, 26b, 26c, 26d)
- **Current version:** 26a (March 2026)

## Schema

```sql
CREATE TABLE lake.foundation.address_lookup (
    bbl          VARCHAR(10),   -- zero-padded: boro(1) + block(5) + lot(4)
    bin          VARCHAR(7),    -- building identification number
    house_number INT,           -- parsed from lhns (sort-format low house number)
    house_number_high INT,      -- parsed from hhns (sort-format high house number)
    street_std   VARCHAR,       -- stname from PAD (DCP-standardized, 32 chars)
    street_code  VARCHAR(10),   -- b10sc (full 10-digit street code, for joining)
    boro_code    VARCHAR(1),    -- 1–5
    zipcode      VARCHAR(5),
    addr_type    VARCHAR(1),    -- blank=real address, V=vanity, Q=pseudo, N=NAP
    address_std  VARCHAR        -- generated: house_number || ' ' || street_std
);
```

### Column mapping from PAD ADR

| PAD field | Our column | Transform |
|-----------|-----------|-----------|
| `boro` | `boro_code` | Direct (1 char) |
| `block` | part of `bbl` | `boro \|\| LPAD(block, 5, '0') \|\| LPAD(lot, 4, '0')` |
| `lot` | part of `bbl` | (see above) |
| `bin` | `bin` | Direct (7 chars) |
| `lhns` | `house_number` | `TRY_CAST(TRIM(lhns) AS INT)` — sort-format, no suffix |
| `hhns` | `house_number_high` | `TRY_CAST(TRIM(hhns) AS INT)` |
| `stname` | `street_std` | `TRIM(stname)` — already DCP-standardized |
| `b10sc` | `street_code` | Direct (10 chars) |
| `zipcode` | `zipcode` | Direct (5 chars) |
| `addrtype` | `addr_type` | Direct (1 char) |
| computed | `address_std` | `CAST(house_number AS VARCHAR) \|\| ' ' \|\| street_std` |

### Filtering

- **Include:** `addr_type IN ('', 'V')` — real addresses and vanity addresses (e.g., "1 Wall St" as alias for "68 Broadway")
- **Exclude:** `addr_type = 'Q'` (pseudo-addresses for vacant frontages) and `addr_type = 'N'` (NAP — non-addressable places)
- **Exclude:** rows where `lhns` is empty or non-numeric (street-only records with no house number)

### Expected row count

~1M rows after filtering (real + vanity addresses with valid house numbers).

## Resolution Algorithm

Replace `_resolve_bbl()` in `building.py` with:

```
Input: "200 E 10th St, Manhattan"

Step 1 — Python preprocessing (simplified):
  - Extract borough → boro_code '1'
  - Extract house number → 200
  - Normalize street: expand abbreviations (E→EAST), strip ordinal suffixes (10TH→10)
  - Result: house_number=200, street_search='EAST 10 STREET', boro='1'

Step 2 — SQL Tier 1 (exact match, handles ranges):
  SELECT bbl FROM lake.foundation.address_lookup
  WHERE boro_code = '1'
    AND house_number <= 200
    AND (house_number_high >= 200 OR house_number = 200)
    AND street_std LIKE 'EAST 10%'
    AND addr_type IN ('', 'V')
  LIMIT 1

Step 3 — SQL Tier 2 (nearest lot on same street, if Tier 1 misses):
  SELECT bbl FROM lake.foundation.address_lookup
  WHERE boro_code = '1'
    AND street_std LIKE '%10 STREET%'
    AND addr_type IN ('', 'V')
  ORDER BY ABS(house_number - 200)
  LIMIT 1
```

### Why this is better

| | Current (PLUTO) | New (PAD) |
|--|--|--|
| Street names | Free text, inconsistent | DCP-standardized |
| House number match | String LIKE prefix | INT equality (indexed) |
| Table scan | Full PLUTO (~870K rows) | Filtered by boro_code + house_number |
| Ordinal handling | Complex 2-pass regex | Minimal — PAD uses "10 STREET" consistently |
| Multiple addresses per lot | No (one per BBL) | Yes (aliases, vanity addresses) |
| Expected latency | 500ms–10s | <10ms |

## Dagster Asset

```python
@asset(
    key=AssetKey(["foundation", "address_lookup"]),
    group_name="foundation",
    description="PAD-based address→BBL lookup. DCP-standardized street names, quarterly updates.",
    compute_kind="duckdb",
)
def address_lookup(context) -> MaterializeResult:
    # 1. Download PAD zip from Socrata blob
    # 2. Extract ADR CSV from zip
    # 3. Parse, clean, filter
    # 4. CREATE OR REPLACE TABLE lake.foundation.address_lookup
    # 5. Return metadata (row count, distinct BBLs, distinct streets)
```

### Download strategy

PAD is a Socrata blob (`bc8t-ecyu`), not a queryable dataset. Download URL:
`https://data.cityofnewyork.us/api/views/bc8t-ecyu/files/<resource_id>?download=true&filename=pad.zip`

The resource ID for the current file can be discovered via the Socrata metadata API. Alternatively, use the direct BYTES of the Big Apple download from DCP.

### Schedule

- **Trigger:** Manual or quarterly cron (PAD releases 4x/year)
- **Not** tied to PLUTO's materialization — PAD and PLUTO update independently
- **Idempotent:** `CREATE OR REPLACE TABLE` — safe to re-run

### Dependencies

- None (PAD is an external download, not derived from other lake tables)
- Downstream: `mv_building_hub` should eventually depend on this for address standardization

## MCP Server Changes

### `building.py`

1. **Replace `_resolve_bbl()`** (~70 lines → ~20 lines): query `foundation.address_lookup` instead of `city_government.pluto`
2. **Simplify `_normalize_address()`**: keep borough extraction and abbreviation expansion, remove the ordinal two-pass logic (strip vs keep) — PAD is consistent
3. **Remove `_ORDINAL_MAP`**: no longer needed for resolution (PAD doesn't use spelled-out ordinals)
4. **Keep all PLUTO enrichment queries unchanged**: `STORY_PLUTO_SQL`, `BLOCK_PLUTO_SQL`, etc. still query PLUTO by BBL for building characteristics

### `address_report.py`

1. **`_resolve_context()`** (line ~84): swap the address→BBL step to use `foundation.address_lookup`, keep the PLUTO enrichment query that follows
2. **`_resolve_context_by_bbl()`**: unchanged (already resolves by BBL, not address)

### Other files — no changes

- `entity.py`: queries PLUTO by owner name (not address resolution)
- `neighborhood.py`: queries PLUTO by zipcode/CD (not address resolution)
- `safety.py`: derives precinct from PLUTO by BBL
- `spatial.py`: gets lat/lng from PLUTO by zipcode
- `_address_queries.py`: queries by BBL (already resolved)

## What's NOT in scope

- **AddressPoint ingestion** — PLUTO lot centroids remain the coordinate source. AddressPoint can be added later as a separate `foundation.address_points` asset for per-building geocoding.
- **PAD BBL table** — condo billing BBL mapping. Not needed for address resolution. Can add later if condo unit→billing BBL lookups are needed.
- **GeoSearch API fallback** — live API call for addresses not in PAD. Can be added as a Tier 3 in `_resolve_bbl()` later if needed (new construction between PAD releases).
- **Retiring PLUTO** — PLUTO stays as `city_government.pluto` for building enrichment (owner, zoning, yearbuilt, stories, assessment, etc.). PAD only takes over address→BBL resolution.

## Risks

1. **Blob download, not dlt source** — PAD is a Socrata file attachment, not a queryable dataset. The asset must download the zip, extract the CSV, and load it manually. This is a one-off custom asset (same pattern as `entity_master`).

2. **Street name normalization still needed** — PAD uses "EAST 10 STREET" but users type "E 10th St". We still need abbreviation expansion (`_ABBREV_MAP`) and ordinal suffix stripping. But this is simpler than today because PAD is consistent — no need for the two-pass strip-ordinals-or-not logic.

3. **PAD updates quarterly** — new construction between releases won't resolve. Same lag as PLUTO (annual). Acceptable for our use case.

4. **House number ranges** — PAD stores ranges (low/high). A building at "200-210 Broadway" has `lhns=200, hhns=210`. Resolution should check `house_number <= input <= house_number_high` for range addresses, not just exact match on `house_number`.

## Success Criteria

- `_resolve_bbl("200 E 10th St, Manhattan")` returns correct BBL in <10ms
- All addresses that currently resolve via PLUTO also resolve via PAD
- Addresses that currently fail (ordinal mismatches, abbreviation gaps) now succeed
- No regression in building profile, address report, or entity tools
- Table materializes in <60 seconds from PAD download
