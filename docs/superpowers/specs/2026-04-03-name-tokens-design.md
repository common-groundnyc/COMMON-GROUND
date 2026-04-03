# foundation.name_tokens — Global Name Search Index

**Date:** 2026-04-03
**Status:** Approved

## Problem

Every MCP tool that searches by name does `LIKE '%NAME%'` against individual tables, causing full table scans. Entity_xray runs 22 of these sequentially (now parallel, but each still scans millions of rows). The ACRIS triple-join MV alone has 62M rows — `LIKE '%BLACKSTONE%'` takes 33 seconds even on the pre-joined MV.

## Solution

A single tokenized name index across the entire lake. Every name from every source is split into uppercase tokens and stored in a sorted table. Lookups use equality (`token = 'BLACKSTONE'`) instead of LIKE, enabling DuckDB zonemap row group skipping and bloom filter pruning. A shared `token_search()` helper replaces all `LIKE '%NAME%'` patterns in MCP tools.

## Architecture

```
name_index (47 tables, person names)     ─┐
acris_parties (company/person names)      ─┤─→ tokenize ─→ foundation.name_tokens (sorted by token)
pluto ownername                           ─┤      ↓
nys_corporations                          ─┘   token = 'BLACKSTONE' → source_table + source_id (instant)
```

### Token sources

| Source | Name field | ID field | Est. tokens |
|--------|-----------|----------|-------------|
| `federal.name_index` | `last_name \|\| ' ' \|\| first_name` | `unique_id` | ~15M |
| `housing.acris_parties` | `name` | `document_id` | ~60M |
| `city_government.pluto` | `ownername` | `bbl` | ~3M |
| `business.nys_corporations` | `current_entity_name` | `dos_id` | ~5M |

Total: ~80M token rows, ~20M unique tokens.

## Schema

```sql
CREATE TABLE lake.foundation.name_tokens (
    token        VARCHAR,    -- single uppercase word, table sorted by this column
    source_table VARCHAR,    -- e.g. 'housing.acris_parties', 'city_government.pluto'
    source_id    VARCHAR,    -- unique_id from name_index, or document_id/bbl for non-name sources
    full_name    VARCHAR     -- original full name for display and fuzzy scoring
)
-- ORDER BY token (critical for zonemap row group skipping)
```

### Why sorted by token

DuckDB stores min/max per column per row group (~120K rows). When sorted by `token`, all "BLACKSTONE" entries cluster in 1-2 row groups. An equality check `WHERE token = 'BLACKSTONE'` skips all other row groups — from scanning 80M rows to scanning ~120K.

### Filtering rules

- Tokens shorter than 2 characters are excluded (no single-letter noise)
- NULL/empty names excluded
- Tokens are UPPER() for case-insensitive matching
- Common stopwords excluded: "THE", "OF", "AND", "INC", "LLC", "CORP" (too many matches to be useful for search)

## Dagster Asset

```python
@asset(
    key=AssetKey(["foundation", "name_tokens"]),
    group_name="foundation",
    description="Global tokenized name index for fast name search across the entire lake.",
    compute_kind="duckdb",
    deps=[
        AssetKey(["federal", "name_index"]),
        AssetKey(["housing", "acris_parties"]),
        AssetKey(["city_government", "pluto"]),
        AssetKey(["business", "nys_corporations"]),
    ],
)
```

Build SQL:
```sql
CREATE TABLE lake.foundation.name_tokens AS
WITH all_names AS (
    -- Person names from name_index (47 source tables)
    SELECT
        UPPER(last_name || ' ' || first_name) AS full_name,
        source_table,
        unique_id AS source_id
    FROM lake.federal.name_index
    WHERE last_name IS NOT NULL

    UNION ALL

    -- ACRIS party names (companies + individuals)
    SELECT
        UPPER(name) AS full_name,
        'housing.acris_parties' AS source_table,
        document_id AS source_id
    FROM lake.housing.acris_parties
    WHERE name IS NOT NULL AND LENGTH(TRIM(name)) > 1

    UNION ALL

    -- PLUTO owner names
    SELECT
        UPPER(ownername) AS full_name,
        'city_government.pluto' AS source_table,
        LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') AS source_id
    FROM lake.city_government.pluto
    WHERE ownername IS NOT NULL AND LENGTH(TRIM(ownername)) > 1

    UNION ALL

    -- NYS corporation names
    SELECT
        UPPER(current_entity_name) AS full_name,
        'business.nys_corporations' AS source_table,
        dos_id AS source_id
    FROM lake.business.nys_corporations
    WHERE current_entity_name IS NOT NULL AND LENGTH(TRIM(current_entity_name)) > 1
),
tokenized AS (
    SELECT
        unnest(string_split(full_name, ' ')) AS token,
        source_table,
        source_id,
        full_name
    FROM all_names
)
SELECT token, source_table, source_id, full_name
FROM tokenized
WHERE LENGTH(token) >= 2
  AND token NOT IN ('THE', 'OF', 'AND', 'INC', 'LLC', 'CORP', 'LTD', 'CO', 'NY', 'NEW', 'YORK')
ORDER BY token
```

## Shared Helper

```python
# infra/duckdb-server/shared/token_search.py

def token_search(
    pool,
    name: str,
    source_filter: set[str] | None = None,
    limit: int = 500,
) -> dict[str, list[tuple[str, str]]]:
    """Search name_tokens by token equality, return {source_table: [(source_id, full_name)]}.

    For multi-word names, intersects results (all tokens must appear for same source_id).
    Optional source_filter limits to specific tables.
    """
```

### Multi-token logic

Single token ("BLACKSTONE"):
```sql
SELECT source_table, source_id, full_name
FROM lake.foundation.name_tokens
WHERE token = 'BLACKSTONE'
LIMIT 500
```

Multi-token ("JOHN SMITH"):
```sql
SELECT t1.source_table, t1.source_id, t1.full_name
FROM lake.foundation.name_tokens t1
JOIN lake.foundation.name_tokens t2
  ON t1.source_table = t2.source_table AND t1.source_id = t2.source_id
WHERE t1.token = 'JOHN' AND t2.token = 'SMITH'
LIMIT 500
```

Each token lookup hits ~1-2 row groups (sorted table), so even the self-join is fast.

### Source filter

Tools can narrow to relevant tables:
```python
# entity_xray only cares about these sources
results = token_search(pool, "BLACKSTONE", source_filter={
    "housing.acris_parties", "city_government.pluto", "business.nys_corporations",
    # ... all 22 entity_xray tables
})
```

## MCP Tool Changes

### entity.py — entity_xray

Replace the parallel batch of 22 `LIKE '%NAME%'` queries with:

```python
# 1. Token search (instant)
matches = token_search(pool, search_name)

# 2. For each source table with matches, fetch full records by ID
queries = []
if "housing.acris_parties" in matches:
    ids = [m[0] for m in matches["housing.acris_parties"]]
    queries.append(("acris", f"""
        SELECT ... FROM lake.foundation.mv_entity_acris
        WHERE party_name IN ({','.join('?' for _ in ids)})
        LIMIT 20
    """, ids))
# ... similar for other source tables
results = parallel_queries(pool, queries)
```

The key insight: token_search tells us WHICH tables have results, so we skip tables with no matches entirely. Then we fetch by ID (fast point lookup) instead of LIKE scan.

### network.py, legal.py, civic.py

Same pattern — replace `LIKE '%NAME%'` with `token_search()` → fetch by ID.

### Lance routing

Token search is faster and deterministic. Lance becomes a fallback for:
- Names not in any token (typos, partial names)
- Semantic similarity ("BLACKROCK" when searching "BLACKSTONE")

## What Doesn't Change

- `entity_master` — stays as-is, resolves entities across sources
- `phonetic_index` — stays as-is, used for Splink blocking
- `name_index` — stays as-is, provides person names from 47 tables
- `address_lookup` — PAD handles address resolution
- Free-text search (violation descriptions, 311 complaints) — different problem, stays as ILIKE
- Lance index — kept as fuzzy fallback

## Expected Performance

| Operation | Before | After |
|-----------|--------|-------|
| `LIKE '%BLACKSTONE%'` on 62M ACRIS rows | 33s | N/A (not used) |
| Token lookup `token = 'BLACKSTONE'` on 80M sorted rows | N/A | <100ms |
| Multi-token `'JOHN' AND 'SMITH'` | N/A | <200ms |
| entity_xray total | 54s | ~5-10s |
| network name search | 15s | ~3-5s |

## Risks

1. **Token table size** — ~80M rows, ~2-3GB on disk. Manageable for DuckLake on MinIO.
2. **Stopword filtering** — removing "LLC", "INC" etc. means searching for a company literally named "LLC" won't work. Acceptable tradeoff.
3. **Multi-word tokens** — names like "VAN DER BERG" tokenize as ["VAN", "DER", "BERG"]. "VAN" matches many unrelated names. The multi-token intersection handles this (all tokens must match same source_id).
4. **Rebuild time** — materializing 80M sorted rows will take 2-5 minutes. Run as a Dagster asset after name_index + ACRIS refresh.
5. **ACRIS document_id as source_id** — fetching full ACRIS records by document_id requires a join back to acris_master/legals. The MV `mv_entity_acris` already has the pre-joined data, so we query that instead.
