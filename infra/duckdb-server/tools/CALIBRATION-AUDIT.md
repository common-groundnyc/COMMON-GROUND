# Address Report Calibration Audit — Structural Issues

Tested: 456 Greene Ave, Brooklyn (Bed-Stuy, ZIP 11216, Precinct 79)
Date: 2026-04-05

## The 4 Structural Problems

Every symptom in the address_report traces back to one of these root causes.
These same patterns exist (or will exist) in any tool that does geographic lookups.

---

### 1. NO SPATIAL LOOKUP LAYER

**Problem**: PLUTO gives us lat/lon for every address, but only the subway query
uses it. Everything else resolves geography through string matching — ZIP codes,
borough letters, precinct numbers — which are fragile, inconsistent across
source tables, and often wrong.

**Affected queries**:
- Precinct: regex on 311 `incident_address` text (completely broken)
- Schools: borough letter only (returns random Brooklyn schools for a Bed-Stuy address)
- Fishing: `ORDER BY RANDOM()` (no proximity at all)
- Nearest park/garden/health facility: ZIP-based (misses closer options in adjacent ZIPs)

**We already have the solution — it's just not used here.**

The codebase has a full spatial stack that `neighborhood()` already uses:

- **`spatial.py`** — H3 hex-based spatial queries (k-ring, aggregate, heatmap,
  neighborhood stats). Resolution 9 (~100m hexagons).
- **`lake.foundation.h3_index`** — Pre-computed H3 res9 cells for every
  geolocated row across the entire lake.
- **`lake.spatial.*`** — Spatial views with `ST_Point` geometry columns:
  `nypd_crimes`, `nypd_crimes_ytd`, `restaurant_inspections`, `subway_stops`,
  `n311_complaints`, `rat_inspections`, `street_trees`.
- **`neighborhood()` tool** — Already uses `ST_Distance`, `ST_DWithin`,
  `h3_neighborhood_stats_sql` for proper proximity lookups.

`address_report` ignores ALL of this and does string matching instead.

**Fix**: Reuse the existing spatial infrastructure in `_resolve_context()`:

For **precinct**: Use `addr_pct_cd` from `lake.spatial.nypd_crimes_ytd` with
`ST_DWithin` around the address coordinates (same pattern as neighborhood's
crime radius query at `neighborhood.py:229`).

For **nearest X** (schools, fishing, parks): Use `ST_Distance` ordering
(same pattern as neighborhood's restaurant/subway queries at
`neighborhood.py:257-274`).

For **aggregate stats** (safety, rats, 311): Use `h3_neighborhood_stats_sql`
from `spatial.py` (same as `neighborhood.py:824-831`).

**Where this pattern should be enforced**: `_resolve_context()` in
`address_report.py` lines 221-276. The lat/lon from PLUTO should feed into
the same spatial functions that `neighborhood()` already calls.

---

### 2. SCOPE MISMATCH — DATA RESOLUTION != PRESENTATION RESOLUTION

**Problem**: Some data only exists at city-wide or borough level, but the report
presents it as if it's specific to the address or ZIP. The user sees "Liam (446
born)" under their address and assumes it's local. It's not — it's NYC-wide.
Similarly, election results are borough-wide but appear under the address heading.

**Affected queries**:
- Baby names (`fun_baby_names`): city-wide, no ZIP/borough filter. Presented as local.
- Elections (`civic_election_*`): county/borough-level. Presented as address-specific.
- Air quality (`env_air`): community-district-level aggregate with no CD filter (!).
  Currently returns a global average across all CDs.
- Contracts (`civic_contracts`): city-wide aggregate. The WHERE clause is a tautology
  (vendor_name IN all vendor_names). Returns the same number for every address.

**Fix**: Two options per query:
1. **Scope it properly**: Add the right geographic filter (borough for baby names,
   district for elections, CD for air quality).
2. **Label it honestly**: If data doesn't exist at local resolution, prefix the
   section with the actual scope: "Borough-wide:", "City-wide:", "Community District:".

Create a `QueryMetadata` dataclass:
```python
@dataclass(frozen=True)
class QueryDef:
    name: str
    sql: str
    params: list | None
    scope: str  # "bbl", "block", "zip", "precinct", "cd", "borough", "citywide"
```
The report formatter can then label each section with its actual resolution.
This prevents future queries from being silently misrepresented.

---

### 3. FORMAT ASSUMPTIONS — NO KEY NORMALIZATION

**Problem**: Every source table stores geographic keys differently. BBL is
sometimes a string, sometimes a float-cast-to-int. ZIP codes are sometimes
5-digit strings, sometimes integers, sometimes have leading zeros stripped.
Precinct codes vary between int and varchar. The queries hard-code format
assumptions per table, and when they're wrong, the join silently returns 0 rows.

**Affected queries**:
- ACS demographics (`neighborhood_acs`): `zcta = ?` — ZCTA is a Census concept,
  not identical to USPS ZIP. 11216 might not have a matching ZCTA row if the
  table stores it differently (e.g., with a leading 'ZCTA5 ' prefix).
- Rat inspections (`health_rats`): `zip_code = ?` — if the table stores ZIP as
  integer (11216 vs "11216"), the string comparison silently fails -> 0 results
  -> "0% active rats" which is presented as fact.
- Liquor licenses (`neighborhood_liquor`): `ILIKE '%ON%PREMISES%'` assumes the
  class/type column contains "ON PREMISES" but the actual values may be coded
  differently (e.g., "OP" for on-premises, "L" for liquor store).

**Fix**:
1. **Audit every parameterized join** in `_address_queries.py`. For each `WHERE col = ?`,
   verify the actual data format by running:
   ```sql
   SELECT DISTINCT col FROM table LIMIT 20
   ```
2. **Normalize at ingestion or at query time**. For ZIP:
   ```sql
   WHERE LPAD(CAST(zip_code AS VARCHAR), 5, '0') = ?
   ```
3. **For categorical filters** (like liquor license type), don't guess —
   query the distinct values first and build the filter from actual data:
   ```sql
   SELECT DISTINCT class, type FROM nys_liquor_authority LIMIT 50
   ```

---

### 4. NO VALIDATION / SANITY CHECKS

**Problem**: When a query returns 0 rows or implausible values, the report
presents them as fact. "0 bars/restaurants" next to "661 liquor licenses" is
an obvious contradiction, but the system doesn't catch it. "0% rat activity"
in Bed-Stuy is implausible, but the system doesn't flag it.

**Affected queries**: All of them, potentially. Any query can silently return
wrong data due to format mismatches (issue #3) and the report will present it
without question.

**Fix**: Add post-query sanity checks in `assemble_report()`:
```python
SANITY_CHECKS = [
    # (condition, warning)
    (lambda r: r.get("neighborhood_liquor", {}).get("total", 0) > 100
               and r.get("neighborhood_liquor", {}).get("bars_restaurants", 0) == 0,
     "Liquor license count inconsistent with bars/restaurants count"),
    (lambda r: r.get("health_rats", {}).get("inspections", 0) > 1000
               and r.get("health_rats", {}).get("active_rats", 0) == 0,
     "Rat inspection count high but 0% active — possible query bug"),
]
```
Also: if a critical geographic lookup returns 0 rows (ACS, safety, schools),
append a warning to that section: "Data unavailable for this ZIP/precinct —
possible format mismatch."

---

## Symptom-to-Root-Cause Map

| Symptom | Root Cause | Fix Priority |
|---------|-----------|-------------|
| Wrong precinct (100 not 79) | #1 No spatial lookup | P0 — cascades to all safety |
| All safety stats wrong | #1 (cascading from precinct) | P0 |
| Schools: random Brooklyn list | #1 No spatial lookup (no district filter) | P0 |
| ACS missing for 11216 | #3 Format mismatch (ZCTA vs ZIP) | P1 |
| Fishing: Bronx for Brooklyn | #1 No spatial lookup | P1 |
| Baby name: city-wide as local | #2 Scope mismatch | P1 |
| Elections: borough as local | #2 Scope mismatch | P2 |
| Rat 0%: implausible | #3 Format mismatch + #4 No validation | P1 |
| Bars = 0 despite 661 licenses | #3 Filter assumption wrong | P1 |
| Air quality: global average | #2 Scope mismatch (no CD filter) | P2 |
| Contracts: city-wide tautology | #2 Scope mismatch | P2 |

---

## Implementation Order

### Phase 1: Fix the geography (solves 5 bugs at once)

1. Fix precinct resolution — use `addr_pct_cd` from NYPD data with lat/lon proximity
2. Add school district derivation from community district
3. Fix fishing/parks to use lat/lon nearest-neighbor
4. Move all geographic derivation into a single `_resolve_geography()` call

### Phase 2: Fix format mismatches (solves 3 bugs)

5. Audit ZCTA format in `acs_zcta_demographics`
6. Audit ZIP format in `rodent_inspections`
7. Audit class/type values in `nys_liquor_authority`

### Phase 3: Add scope labels + validation (prevents future bugs)

8. Add `scope` metadata to every query
9. Add sanity checks in report assembly
10. Label borough/city-wide sections honestly

---

## Applying to ALL Tools

These 4 structural issues aren't specific to address_report. Any tool that
maps an address to non-BBL data has the same risk:

- `building()` — uses BBL, mostly safe (issue #3 still applies)
- `neighborhood()` — ZIP-based, vulnerable to #3
- `safety()` — precinct-based, vulnerable to #1 if precinct is wrong
- `school()` — DBN-based, should be fine if DBN is correct
- `civic()` — district-based, vulnerable to #2
- `health()` — ZIP-based, vulnerable to #3

**The fix that helps everything**: A shared `resolve_geography(lat, lon)` in
`shared/geo.py` that every tool can call. One correct lookup, used everywhere.
