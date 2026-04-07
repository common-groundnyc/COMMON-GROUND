# dlt Upgrade Plan — Tier 1 + 2 Implementation

## Current State
- 20+ dlt sources, most hand-rolled with httpx/tenacity
- 3 sources use `rest_api_source` properly (BLS, NREL, College Scorecard)
- CourtListener just migrated to `rest_api_source`
- Socrata uses custom `@dlt.source` with `dlt.defer()` (good pattern, keep)
- Global retry config added to config.toml
- No `response_actions` on any source
- Incremental loading only on Socrata (via `_updated_at` cursor)
- FEC uses bulk CSV download (custom, keep — not a REST API)

## Plan

### Phase 1: Global Config Hardening (30 min)
**Files:** `.dlt/config.toml`

1. Verify retry settings work in Docker step containers (test with cl_courts)
2. Add `response_actions` to `rest_api_source` defaults in CourtListener (done)
3. Add schema contract settings to prevent `ADD COLUMN NOT NULL` errors:
   ```toml
   [schema_contract]
   columns = "evolve"
   data_type = "evolve"
   ```
   Already present — verify it's respected by all sources.

### Phase 2: Migrate Federal Sources to `rest_api_source` (2-3 hours)
**Files:** `sources/propublica.py`, `sources/littlesis.py`, `sources/epa_echo.py`, `sources/fda.py`, `sources/hud.py`, `sources/usaspending.py`, `sources/nrel.py`

Each source currently hand-rolls httpx. Migrate to declarative `rest_api_source`:

| Source | Current | Lines | API Style | Migration Effort |
|--------|---------|-------|-----------|-----------------|
| **ProPublica** | httpx + tenacity | 129 | REST JSON, page-number pagination | Easy — `rest_api_source` with `page_number` paginator |
| **LittleSis** | httpx + tenacity | 183 | REST JSON, seed search + follow relationships | Medium — parent-child resource pattern |
| **EPA ECHO** | httpx + tenacity | 161 | REST JSON, offset pagination | Easy — `rest_api_source` with `offset` paginator |
| **FDA** | httpx + tenacity | 101 | REST JSON, skip/limit pagination | Easy — `rest_api_source` with `offset` paginator |
| **HUD** | httpx + tenacity | 117 | ArcGIS REST, offset pagination | Medium — ArcGIS has non-standard pagination |
| **USAspending** | httpx + tenacity | 145 | REST JSON, POST with pagination | Hard — POST-based API, may need custom resource |
| **NREL** | `rest_api_source` | 39 | Single page | Already done ✓ |
| **BLS** | `rest_api_source` | 40 | Single page | Already done ✓ |
| **College Scorecard** | `rest_api_source` | 79 | Page number | Already done ✓ |

**Priority order:** ProPublica → EPA ECHO → FDA → LittleSis → HUD → USAspending

**Each migration:**
1. Read current source, understand API
2. Rewrite as `rest_api_source` config dict
3. Test locally: `PYTHONPATH=src uv run python3 -c "from ... import; src = ...; print(list(src.resources.keys()))"`
4. Verify definitions load
5. Rebuild Docker, reload, materialize

### Phase 3: Add Incremental Loading to All Sources (1-2 hours)
**Files:** All source files + `datasets.py`

**What gets incremental:**

| Source | Cursor Field | Strategy |
|--------|-------------|----------|
| Socrata (208 datasets) | `_updated_at` | Already done ✓ |
| CourtListener (10 resources) | `date_modified` | Done for 6 resources ✓ |
| ProPublica | N/A (full replace OK — small dataset) | Skip |
| LittleSis | `updated_at` | Add `incremental` param |
| EPA ECHO | N/A (replace) | Skip |
| FDA | N/A (replace) | Skip |
| FEC | Election cycle year | Already partitioned by cycle |
| NYPD Misconduct | N/A (NYCLU static CSV) | Skip |
| NYS BOE | N/A (replace) | Skip |
| OCA Housing Court | `fileddate` | Add `incremental` param |

**For each source with incremental:**
1. Change `write_disposition` from `replace` to `merge`
2. Add `primary_key` if missing
3. Add `incremental` cursor configuration
4. Test: run twice, verify second run fetches 0 new rows

### Phase 4: Add `processing_steps` and `column_hints` (1 hour)
**Files:** Source configs

1. **Name normalization** — `add_map` to uppercase names before loading:
   ```python
   processing_steps = [
       {"map": lambda r: {**r, "name": r.get("name", "").upper() if r.get("name") else r.get("name")}},
   ]
   ```

2. **Column hints** — force types on known columns:
   ```python
   columns = {
       "zip": {"data_type": "text"},  # prevent zip codes being loaded as integers
       "bbl": {"data_type": "text"},
       "amount": {"data_type": "decimal", "precision": 12, "scale": 2},
   }
   ```

3. **Filter steps** — skip empty/null records at extract time:
   ```python
   processing_steps = [
       {"filter": lambda r: r.get("name") is not None and len(r.get("name", "")) > 0},
   ]
   ```

### Phase 5: Add `filesystem` Source for Bulk Data (1 hour)
**Files:** New `sources/bulk_csv.py`

Load static CSV/Parquet files directly into DuckLake:

| Dataset | Source | Format | URL |
|---------|--------|--------|-----|
| FiveThirtyEight police settlements | GitHub | CSV | `https://raw.githubusercontent.com/fivethirtyeight/police-settlements/main/new_york_ny/final/new_york_edited.csv` |
| CourtListener bulk data | S3 | CSV/Parquet | `s3://com-courtlistener-storage/bulk-data/` |
| NYCLU CCRB data | Static download | CSV | Already ingested via nypd_misconduct.py |

```python
from dlt.sources.filesystem import filesystem, read_csv

csv_source = filesystem(
    bucket_url="https://raw.githubusercontent.com/fivethirtyeight/police-settlements/main/new_york_ny/final/",
    file_glob="*.csv"
) | read_csv()
```

### Phase 6: Parent-Child Resources for CourtListener (when permissions granted)
**Files:** `sources/courtlistener.py`

When we get access to dockets/parties/attorneys endpoints:

```python
"resources": [
    {"name": "dockets", "endpoint": {"path": "/dockets/", "params": {"court": "nysd"}}},
    {
        "name": "docket_entries",
        "endpoint": {
            "path": "/docket-entries/",
            "params": {
                "docket__id": {"type": "resolve", "resource": "dockets", "field": "id"}
            },
        },
        "include_from_parent": ["case_name"],
    },
    {
        "name": "recap_documents",
        "endpoint": {
            "path": "/recap-documents/",
            "params": {
                "docket_entry__id": {"type": "resolve", "resource": "docket_entries", "field": "id"}
            },
        },
    },
]
```

This auto-fetches entries for each docket, then documents for each entry — full case file ingestion.

### Phase 7: dbt Post-Load Transforms (future)
**Files:** New `dbt/` directory

Replace manual cross-reference queries with dbt models:

```sql
-- models/officer_lawsuits.sql
SELECT
    o.last_name, o.first_name, o.shield_no,
    l.matter_name, l.total_city_payout_amt, l.docket_index
FROM {{ ref('nypd_misconduct_named') }} o
JOIN {{ ref('civil_litigation') }} l
    ON UPPER(l.defendants_respondents_firms) LIKE '%' || o.last_name || ', ' || o.first_name || '%'
```

```python
# in dagster pipeline
dbt = dlt.dbt.package(pipeline, "dbt/common_ground")
models = dbt.run_all()
```

## Estimated Total Effort

| Phase | Effort | Impact |
|-------|--------|--------|
| 1. Global config | 30 min | Fixes all retry/error issues |
| 2. Migrate sources | 2-3 hours | Eliminates hand-rolled code, consistent patterns |
| 3. Incremental loading | 1-2 hours | 10x faster daily syncs, less API load |
| 4. Processing steps | 1 hour | Cleaner data, fewer downstream bugs |
| 5. Filesystem source | 1 hour | Bulk data ingestion (settlements, bulk court data) |
| 6. Parent-child | When permitted | Full case file ingestion |
| 7. dbt transforms | Future | Automated cross-referencing |

**Total: ~6-8 hours for phases 1-5**

## Success Criteria
- [ ] All sources use `rest_api_source` or `@dlt.source` with proper dlt patterns
- [ ] No hand-rolled httpx/tenacity in any source
- [ ] Global retry config respected by all sources
- [ ] Incremental loading on all sources that support it
- [ ] Daily sync completes in < 30 min (vs current ~2 hours full replace)
- [ ] Zero crashes from 429/502/404 errors
- [ ] All materialized through Dagster, no ad-hoc scripts
