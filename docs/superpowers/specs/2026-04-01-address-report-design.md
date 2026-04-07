# address_report — 360 Address Dossier Design

**Date**: 2026-04-01
**Status**: Draft
**Scope**: Tool #15 — comprehensive address report with percentile rankings

---

## What It Is

The showcase tool. A user gives an address, gets back a complete 360-degree dossier of their location — building, block, neighborhood, safety, schools, health, environment, civic, services, and fun facts. Every metric percentile-ranked against the city. Formatted for screenshots.

This is most users' first query. It shows them what Common Ground can do and prompts further investigation.

## Architecture

- ~50 SQL queries executed in parallel via `parallel_queries()` (ThreadPoolExecutor + CursorPool)
- 8 concurrent queries (half the 16-cursor pool, leaves room for other MCP calls)
- Expected latency: ~500ms (vs 5s sequential)
- Response: ~1,200 tokens of formatted text with percentile bars
- `structured_content` carries the full data dict for programmatic access

## Query Flow

```
User: "305 Linden Blvd Brooklyn"
  |
  1. Resolve address → BBL via PLUTO (single query, must complete first)
  |
  2. From BBL, derive: ZIP, borough, precinct, community district, council district, census tract
  |
  3. Fire ~50 queries in parallel across all join keys:
     - BBL queries (building-level): ~20 queries
     - ZIP queries (neighborhood-level): ~10 queries
     - Precinct queries (safety): ~5 queries
     - Spatial queries (services/fun): ~10 queries
     - Percentile queries: ~5 queries
  |
  4. Assemble sections, compute percentile bars, format output
```

## Output Format

Unicode box-drawing dividers (pass through all Claude clients verbatim). No markdown headers (Claude re-renders those). Percentile bars: `████████░░ 87th — high` (10 chars + severity word).

Target: ~1,200 tokens total. Enough for a complete picture, compact enough for Claude to reason about.

## Sections

### 1. BUILDING (BBL-level, ~20 queries)
- Year built, stories, units, zoning, building class
- Owner name + "owns X other buildings" (from hpd_registration_contacts)
- Market value + assessed value (property_valuation)
- Last sale date + price (rolling_sales or acris_master)
- HPD violations: total, open, Class C — percentile
- HPD complaints: total, open — percentile
- DOB violations + ECB penalties — percentile
- FDNY violations — percentile
- Eviction filings — percentile
- Rent stabilized: yes/no + unit count (ll44_income_rent)
- Energy Star score (ll84_energy_2023) — percentile
- Facade status (dob_safety_facades): safe/unsafe
- Boiler status (dob_safety_boiler): compliant/non
- AEP status (aep_buildings): on worst buildings list?
- Tax lien sale history
- LLCs registered at address (nys_corporations)
- Housing court cases (oca_housing_court_cases)

### 2. BLOCK (block prefix, ~5 queries)
- Building count, avg age, avg floors
- This building vs block average violations
- Street trees: count + most common species
- Recent film shoots
- Crashes on block last year
- Construction activity (DOB permits)

### 3. NEIGHBORHOOD (ZIP, ~10 queries)
- Population, density
- Median income — percentile
- Poverty rate — percentile
- Rent burden (% paying 30%+ of income) — percentile
- Median rent — percentile
- Foreign-born %
- Top languages spoken
- Broadband adoption — percentile
- 311 top complaint type + volume — percentile
- Recycling rate — percentile

### 4. SAFETY (precinct, ~5 queries)
- Total crimes 12mo
- Violent crime rate — percentile
- Property crime rate — percentile
- Shootings 12mo — percentile
- Crashes 12mo — percentile
- Top offense types

### 5. SCHOOLS (nearest + district, ~3 queries)
- Nearest elementary: name, ELA %, Math % — percentiles
- Nearest middle school
- Nearest high school: graduation rate
- District average

### 6. HEALTH (ZIP/UHF, ~5 queries)
- Life expectancy or premature mortality — percentile
- Asthma ER rate — percentile
- Adult obesity rate — percentile
- COVID death rate — percentile
- Rat activity rate — percentile
- Nearest hospital

### 7. ENVIRONMENT (~5 queries)
- Flood zone
- Heat vulnerability — percentile
- Air quality PM2.5
- Street trees on block
- Energy grade (LL84)
- EV chargers nearby

### 8. CIVIC (~3 queries)
- Council member + district
- Community board
- Voter turnout last election — percentile
- City contracts to ZIP (5yr total)
- FEC donations from ZIP — percentile

### 9. SERVICES (~3 queries)
- Nearest subway + lines + distance
- Nearest hospital
- Food pantries within 0.5mi
- Parks within 0.5mi
- Library

### 10. FUN FACTS (~5 queries)
- Most popular dog breed in ZIP
- Most common street tree on block
- Top baby name in borough
- Film shoots nearby (12mo)
- 311 top complaint (the quirky one)
- Corporations registered at this address
- Nearest fishing spot
- Nearest pool/spray shower

## Percentile Bar Renderer

```python
def pctile_bar(value: float, label: str, raw: str, width: int = 10) -> str:
    n = round(value * 100)
    filled = round(value * width)
    bar = "█" * filled + "░" * (width - filled)
    severity = ["low","moderate","typical","high","very high","extreme"][min(n//20, 5)]
    return f" {label:<22s} {raw:>8s}   {bar} {n}th — {severity}"
```

## Parallel Query Helper

```python
def parallel_queries(pool, queries, max_workers=8):
    """Execute [(name, sql, params), ...] concurrently. Returns {name: (cols, rows)}."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = {}
    def _run(name, sql, params):
        try: return name, pool.execute(sql, params), None
        except Exception as e: return name, ([], []), str(e)
    with ThreadPoolExecutor(max_workers=min(max_workers, len(queries))) as ex:
        futures = {ex.submit(_run, n, s, p): n for n, s, p in queries}
        for f in as_completed(futures):
            name, data, err = f.result()
            if err: print(f"address_report: {name} failed: {err}", flush=True)
            results[name] = data
    return results
```

## Tool Signature

```python
def address_report(
    address: Annotated[str, Field(
        description="NYC street address, e.g. '305 Linden Blvd, Brooklyn' or '350 5th Ave, Manhattan'",
        examples=["305 Linden Blvd, Brooklyn", "350 5th Ave, Manhattan", "123 Main St, Bronx"],
    )],
    ctx: Context = None,
) -> ToolResult:
    """Complete 360-degree report for any NYC address. Returns building profile, violations with percentile rankings, neighborhood demographics, crime stats, school quality, health indicators, environmental data, civic representation, nearby services, and fun facts. Every metric ranked against the city. Use this as the first lookup for any address. For deeper investigation, use the drill-deeper suggestions at the end of the report."""
```

## Data Sources (by section)

Total unique tables queried: ~80 of 294

See full audit in research notes (data-audit agent output, 2026-04-01).
