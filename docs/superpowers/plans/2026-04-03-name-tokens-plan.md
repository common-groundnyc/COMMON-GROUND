# foundation.name_tokens Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a global tokenized name index (`foundation.name_tokens`) and shared `token_search()` helper that replaces all `LIKE '%NAME%'` scans in MCP tools with instant equality lookups.

**Architecture:** Dagster asset tokenizes names from name_index + ACRIS + PLUTO + corps into a sorted DuckLake table (~80M tokens). MCP tools call `token_search(pool, name)` which returns `{source_table: [(source_id, full_name)]}`, then fetch full records by ID instead of scanning.

**Tech Stack:** Python, Dagster, DuckDB/DuckLake, pytest

**Spec:** `docs/superpowers/specs/2026-04-03-name-tokens-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `src/dagster_pipeline/defs/name_tokens_asset.py` | Dagster asset: build token table from 4 sources |
| Create | `tests/test_name_tokens.py` | Unit tests for tokenization logic |
| Modify | `src/dagster_pipeline/definitions.py` | Register asset + add to jobs/sensor |
| Create | `infra/duckdb-server/shared/token_search.py` | Shared `token_search()` helper for MCP tools |
| Modify | `infra/duckdb-server/tools/entity.py` | Use token_search in entity_xray |

---

### Task 1: Dagster asset — name_tokens

**Files:**
- Create: `src/dagster_pipeline/defs/name_tokens_asset.py`
- Create: `tests/test_name_tokens.py`

- [ ] **Step 1: Write tests for tokenization helper**

Create `tests/test_name_tokens.py`:

```python
import pytest
from dagster_pipeline.defs.name_tokens_asset import tokenize_name, STOPWORDS


class TestTokenizeName:
    def test_simple_name(self):
        assert tokenize_name("JOHN SMITH") == ["JOHN", "SMITH"]

    def test_strips_stopwords(self):
        tokens = tokenize_name("THE BLACKSTONE GROUP LLC")
        assert "THE" not in tokens
        assert "LLC" not in tokens
        assert "BLACKSTONE" in tokens
        assert "GROUP" in tokens

    def test_short_tokens_excluded(self):
        tokens = tokenize_name("A B SMITH")
        assert "A" not in tokens
        assert "B" not in tokens
        assert "SMITH" in tokens

    def test_empty_string(self):
        assert tokenize_name("") == []

    def test_none_returns_empty(self):
        assert tokenize_name(None) == []

    def test_whitespace_handling(self):
        assert tokenize_name("  JOHN   SMITH  ") == ["JOHN", "SMITH"]

    def test_uppercase(self):
        assert tokenize_name("john smith") == ["JOHN", "SMITH"]

    def test_hyphenated_name(self):
        tokens = tokenize_name("MARY SMITH-JONES")
        assert "SMITH-JONES" in tokens

    def test_stopwords_constant(self):
        for word in ["THE", "OF", "AND", "INC", "LLC", "CORP", "LTD", "CO", "NY", "NEW", "YORK"]:
            assert word in STOPWORDS
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_name_tokens.py -v`
Expected: FAIL with `ImportError: cannot import name 'tokenize_name'`

- [ ] **Step 3: Implement asset**

Create `src/dagster_pipeline/defs/name_tokens_asset.py`:

```python
"""Dagster asset producing lake.foundation.name_tokens — global tokenized name index."""

import logging
import time

from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)

STOPWORDS = frozenset({
    "THE", "OF", "AND", "INC", "LLC", "CORP", "LTD", "CO",
    "NY", "NEW", "YORK", "FOR", "AT", "IN", "TO", "AS",
})


def tokenize_name(name: str | None) -> list[str]:
    """Split a name into uppercase tokens, excluding stopwords and short tokens."""
    if not name:
        return []
    tokens = name.strip().upper().split()
    return [t for t in tokens if len(t) >= 2 and t not in STOPWORDS]


@asset(
    key=AssetKey(["foundation", "name_tokens"]),
    group_name="foundation",
    description=(
        "Global tokenized name index for instant name search across the lake. "
        "Replaces LIKE '%%NAME%%' scans with equality lookups on sorted tokens."
    ),
    compute_kind="duckdb",
    deps=[
        AssetKey(["federal", "name_index"]),
        AssetKey(["housing", "acris_parties"]),
        AssetKey(["city_government", "pluto"]),
        AssetKey(["business", "nys_corporations"]),
    ],
)
def name_tokens(context) -> MaterializeResult:
    """Build tokenized name index from 4 sources, sorted by token for fast lookups."""
    conn = _connect_ducklake()
    t0 = time.time()

    stopword_list = ", ".join(f"'{w}'" for w in STOPWORDS)

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.name_tokens_staging")
        conn.execute(f"""
            CREATE TABLE lake.foundation.name_tokens_staging AS
            WITH all_names AS (
                -- Person names from name_index (47 source tables)
                SELECT
                    UPPER(last_name || ' ' || first_name) AS full_name,
                    source_table,
                    unique_id AS source_id
                FROM lake.federal.name_index
                WHERE last_name IS NOT NULL

                UNION ALL

                -- ACRIS party names
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
                    CAST(dos_id AS VARCHAR) AS source_id
                FROM lake.business.nys_corporations
                WHERE current_entity_name IS NOT NULL
                  AND LENGTH(TRIM(current_entity_name)) > 1
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
              AND token NOT IN ({stopword_list})
            ORDER BY token
        """)

        # Atomic swap
        conn.execute("DROP TABLE IF EXISTS lake.foundation.name_tokens")
        conn.execute(
            "ALTER TABLE lake.foundation.name_tokens_staging "
            "RENAME TO name_tokens"
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.name_tokens"
        ).fetchone()[0]
        unique_tokens = conn.execute(
            "SELECT COUNT(DISTINCT token) FROM lake.foundation.name_tokens"
        ).fetchone()[0]
        source_count = conn.execute(
            "SELECT COUNT(DISTINCT source_table) FROM lake.foundation.name_tokens"
        ).fetchone()[0]

        elapsed = round(time.time() - t0, 1)
        context.log.info(
            "name_tokens: %s rows, %s unique tokens, %s sources in %ss",
            f"{row_count:,}", f"{unique_tokens:,}", source_count, elapsed,
        )

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(row_count),
                "unique_tokens": MetadataValue.int(unique_tokens),
                "source_tables": MetadataValue.int(source_count),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
```

- [ ] **Step 4: Run tests**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_name_tokens.py -v`
Expected: All 10 tests PASS

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add src/dagster_pipeline/defs/name_tokens_asset.py tests/test_name_tokens.py
git commit -m "feat: add name_tokens Dagster asset — global tokenized name index"
```

---

### Task 2: Register asset in Dagster definitions

**Files:**
- Modify: `src/dagster_pipeline/definitions.py`

- [ ] **Step 1: Add import**

After the existing materialized view imports, add:

```python
from dagster_pipeline.defs.name_tokens_asset import name_tokens
```

- [ ] **Step 2: Add to all_assets**

Add `name_tokens` to the `all_assets` list (after `address_lookup`).

- [ ] **Step 3: Add to materialized_views_job and mv_automation_sensor**

Add `dg.AssetKey(["foundation", "name_tokens"])` to both the `materialized_views_job` selection and the `mv_automation_sensor` target.

- [ ] **Step 4: Verify Dagster loads**

Run: `cd ~/Desktop/dagster-pipeline && uv run python -c "from dagster_pipeline.definitions import defs; print(f'Loaded {len(defs.get_all_asset_specs())} assets')"`

- [ ] **Step 5: Commit**

```bash
git add src/dagster_pipeline/definitions.py
git commit -m "feat: register name_tokens in Dagster definitions"
```

---

### Task 3: Materialize name_tokens

**Files:** None (operational)

- [ ] **Step 1: Materialize the asset**

```bash
cd ~/Desktop/dagster-pipeline
export CATALOG_URL="$(sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc 2>/dev/null | grep 'CATALOG=' | sed 's/^[^=]*=//' | sed 's/common-ground-postgres-1/178.156.228.119/')"
export MINIO_SECRET="$(sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc 2>/dev/null | grep 'SECRET_ACCESS_KEY=' | head -1 | sed 's/^[^=]*=//')"

DAGSTER_HOME=/tmp/dagster-home \
  DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG="$CATALOG_URL" \
  S3_ENDPOINT="178.156.228.119:9000" \
  DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_ACCESS_KEY_ID="minioadmin" \
  DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_SECRET_ACCESS_KEY="$MINIO_SECRET" \
  uv run dagster asset materialize \
    --select 'foundation/name_tokens' \
    -m dagster_pipeline.definitions
```

Expected: ~80M rows, 2-5 minutes.

- [ ] **Step 2: Verify via MCP query**

After server restart, test:
```sql
SELECT COUNT(*) FROM lake.foundation.name_tokens
SELECT COUNT(DISTINCT token) FROM lake.foundation.name_tokens
SELECT token, COUNT(*) AS cnt FROM lake.foundation.name_tokens WHERE token = 'BLACKSTONE' GROUP BY token
```

---

### Task 4: Shared token_search helper

**Files:**
- Create: `infra/duckdb-server/shared/token_search.py`

- [ ] **Step 1: Implement token_search**

```python
"""Token-based name search against foundation.name_tokens."""

import time

from shared.db import execute, safe_query


STOPWORDS = frozenset({
    "THE", "OF", "AND", "INC", "LLC", "CORP", "LTD", "CO",
    "NY", "NEW", "YORK", "FOR", "AT", "IN", "TO", "AS",
})


def tokenize_query(name: str) -> list[str]:
    """Split a search name into tokens, excluding stopwords and short tokens."""
    if not name:
        return []
    tokens = name.strip().upper().split()
    return [t for t in tokens if len(t) >= 2 and t not in STOPWORDS]


def token_search(
    pool,
    name: str,
    source_filter: set[str] | None = None,
    limit: int = 500,
) -> dict[str, list[tuple[str, str]]]:
    """Search name_tokens by token equality.

    Returns {source_table: [(source_id, full_name), ...]}.
    For multi-word names, requires all tokens to match on the same source_id.
    """
    tokens = tokenize_query(name)
    if not tokens:
        return {}

    t0 = time.time()

    if len(tokens) == 1:
        # Single token: direct equality lookup (fast — sorted table, zonemap skipping)
        source_clause = ""
        params: list = [tokens[0]]
        if source_filter:
            placeholders = ", ".join(["?"] * len(source_filter))
            source_clause = f"AND source_table IN ({placeholders})"
            params.extend(sorted(source_filter))

        _, rows = safe_query(pool, f"""
            SELECT source_table, source_id, full_name
            FROM lake.foundation.name_tokens
            WHERE token = ?
            {source_clause}
            LIMIT ?
        """, params + [limit])

    else:
        # Multi-token: self-join to require all tokens match same (source_table, source_id)
        # Build: t1.token = 'JOHN' AND t2.token = 'SMITH' ... with JOIN on source_id
        joins = []
        conditions = []
        params = []
        for i, token in enumerate(tokens):
            alias = f"t{i}"
            if i == 0:
                joins.append(f"lake.foundation.name_tokens {alias}")
            else:
                joins.append(
                    f"JOIN lake.foundation.name_tokens {alias} "
                    f"ON t0.source_table = {alias}.source_table "
                    f"AND t0.source_id = {alias}.source_id"
                )
            conditions.append(f"{alias}.token = ?")
            params.append(token)

        source_clause = ""
        if source_filter:
            placeholders = ", ".join(["?"] * len(source_filter))
            source_clause = f"AND t0.source_table IN ({placeholders})"
            params.extend(sorted(source_filter))

        sql = f"""
            SELECT t0.source_table, t0.source_id, t0.full_name
            FROM {' '.join(joins)}
            WHERE {' AND '.join(conditions)}
            {source_clause}
            LIMIT ?
        """
        params.append(limit)
        _, rows = safe_query(pool, sql, params)

    # Group by source_table
    result: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        source = row[0]
        result.setdefault(source, []).append((row[1], row[2]))

    elapsed = round((time.time() - t0) * 1000)
    if rows:
        print(f"[token_search] '{name}' → {len(rows)} matches across "
              f"{len(result)} tables ({elapsed}ms)", flush=True)

    return result
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/shared/token_search.py
git commit -m "feat: add shared token_search() helper for instant name lookups"
```

---

### Task 5: Wire entity_xray to use token_search

**Files:**
- Modify: `infra/duckdb-server/tools/entity.py`

This is the most impactful change — entity_xray currently runs 22 parallel `LIKE '%NAME%'` queries. With token_search, it first finds which tables have matches, then only queries those tables by ID.

- [ ] **Step 1: Add import**

At the top of `entity.py`, add:
```python
from shared.token_search import token_search
```

- [ ] **Step 2: Rewrite the routing logic in _entity_xray**

Replace the Lance routing + `_should_query()` pattern with token_search. The current flow is:

```python
lance_route = lance_route_entity(ctx, search)
routed_sources = lance_route.get("sources", set())
def _should_query(source_table):
    if not use_routing: return True
    return source_table in routed_sources
```

Replace with:

```python
    # Token-based routing — find which tables have this name (instant)
    token_matches = token_search(pool, search)
    token_sources = set(token_matches.keys())

    # Lance as fuzzy fallback
    lance_route = lance_route_entity(ctx, search)
    lance_sources = lance_route.get("sources", set())
    lance_matched = set(lance_route.get("matched_names", []))

    # Combine: token sources (exact) + lance sources (fuzzy)
    routed_sources = token_sources | lance_sources
    use_routing = len(routed_sources) > 0

    extra_names = vector_expand_names(ctx, search) | lance_matched

    def _should_query(source_table: str) -> bool:
        if source_table in _ALWAYS_QUERY:
            return True
        if not use_routing:
            return True
        return source_table in routed_sources
```

This keeps the same `_should_query` pattern but adds token-based routing. Tables not in the token index are skipped entirely.

- [ ] **Step 3: For ACRIS specifically, use token_matches to fetch by document_id**

The ACRIS query is the slowest (62M row MV). If token_search found ACRIS matches, we already have the document_ids — query by ID instead of LIKE:

Replace the ACRIS query in the parallel batch. Find the entry like:
```python
("acris", """SELECT ... FROM lake.foundation.mv_entity_acris WHERE party_name LIKE ? ...""", ...)
```

Replace with logic that checks `token_matches` first:

```python
    # ACRIS: use token results if available (instant), fallback to LIKE on MV
    acris_token_results = token_matches.get("housing.acris_parties", [])
    if acris_token_results and _should_query("acris_parties"):
        # Already have document_ids from token search — fetch by exact match
        acris_names = list(set(r[1] for r in acris_token_results))[:20]
        if acris_names:
            placeholders = ", ".join(["?"] * len(acris_names))
            queries.append(("acris", f"""
                SELECT party_name, party_type, doc_type, amount,
                       document_date, bbl, street_name, unit
                FROM lake.foundation.mv_entity_acris
                WHERE party_name IN ({placeholders})
                ORDER BY document_date DESC
                LIMIT 20
            """, acris_names))
    elif _should_query("acris_parties"):
        # Fallback: LIKE scan on MV (slower but handles names not in token index)
        queries.append(("acris", """
            SELECT party_name, party_type, doc_type, amount,
                   document_date, bbl, street_name, unit
            FROM lake.foundation.mv_entity_acris
            WHERE party_name LIKE ?
            ORDER BY document_date DESC
            LIMIT 20
        """, [f"%{search}%"]))
```

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/tools/entity.py
git commit -m "perf: entity_xray uses token_search for routing + ACRIS lookup by name"
```

---

### Task 6: Deploy and benchmark

**Files:** None (operational)

- [ ] **Step 1: Deploy to Hetzner**

```bash
cd ~/Desktop/dagster-pipeline
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  --exclude '__pycache__' --exclude '*.pyc' --exclude '.git' \
  --exclude 'model/' --exclude 'tests/' \
  infra/duckdb-server/ \
  fattie@178.156.228.119:/opt/common-ground/duckdb-server/
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose build duckdb-server && docker compose up -d duckdb-server"
```

- [ ] **Step 2: Wait for startup and test**

```bash
sleep 90
# Test entity_xray
curl -s --max-time 120 -X POST https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"entity","arguments":{"name":"BLACKSTONE"}}}'
```

- [ ] **Step 3: Check server timing logs**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "docker compose -f /opt/common-ground/docker-compose.yml logs duckdb-server 2>&1 | grep '\[token_search\]\|\[parallel\]' | tail -10"
```

Expected: `[token_search] 'BLACKSTONE' → N matches across M tables (XXms)` where XX < 200ms.

- [ ] **Step 4: Compare performance**

| Metric | Before | Expected After |
|--------|--------|---------------|
| entity_xray total | 54s | ~10-15s |
| ACRIS lookup | 33s | <1s (by name, not LIKE) |
| Token search | N/A | <200ms |
