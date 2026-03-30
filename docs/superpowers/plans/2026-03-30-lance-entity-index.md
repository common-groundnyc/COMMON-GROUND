# Lance Entity Index — Full Lake Name Embeddings

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dagster asset that embeds every distinct name from all 47 name-bearing tables into a single Lance dataset, then rewire `entity_xray` to use it for routing — cutting response time from 10-27s to ~2s.

**Architecture:** A new Dagster asset (`entity_name_embeddings`) depends on `name_index`. It reads distinct names from `lake.federal.name_index` (via `name_index_raw` to include single-source names), embeds them via Gemini, and writes to Lance on the Hetzner volume. The Dagster asset runs inside Docker on Hetzner (same as other assets) since Lance writes are local filesystem, not S3. The Lance table stores `(name, sources, embedding)`. The MCP server's `entity_xray` does a `lance_vector_search` first to find which sources match, then only queries those tables. Monthly schedule alongside entity resolution.

**Critical constraints:**
- The asset MUST run on Hetzner (Docker) since Lance writes to `/data/common-ground/lance/` which is a local volume
- `.fetchall()` on 14M rows will OOM — must use chunked/streaming approach via temp table + LIMIT/OFFSET
- `name_index` filters to 2+ source tables — use `name_index_raw` or query sources directly to include all names
- 9 of entity_xray's 23 sources search business/entity names (not person names) — these must ALWAYS be queried regardless of Lance routing
- Reuse `embedder.py` from `infra/duckdb-server/` — do NOT duplicate the Gemini client code
- Task 3 (remove startup embedding) MUST happen AFTER Task 4 (first run populates Lance)

**Tech Stack:** Dagster asset, Gemini `gemini-embedding-001` (768 dims, free tier), Lance format via DuckDB, existing `name_registry.py` + `name_index_asset.py`

---

## Context For The Implementer

### What Already Exists

1. **`src/dagster_pipeline/sources/name_registry.py`** — 47 `NameSource` entries mapping every person-name column in the lake. Each entry defines schema, table, extraction pattern (STRUCTURED, COMBINED_COMMA, etc.), and column names. The `get_extraction_sql()` function generates SELECT statements that normalize names into `(source_table, last_name, first_name, address, city, zip)`.

2. **`src/dagster_pipeline/defs/name_index_asset.py`** — Dagster asset that UNION ALLs all 47 sources into `lake.federal.name_index` (~52M rows). Already runs in the `entity_resolution` job. Includes `_connect_ducklake()` helper for DuckDB+DuckLake connection.

3. **`infra/duckdb-server/mcp_server.py` lines 1212-1248** — `_build_entity_name_embeddings()` function that embeds names from 4 graph tables to Lance. Runs on server startup in background thread. Uses `embed_batch()` from `embedder.py` (Gemini API, 790 texts/sec). Writes to `/data/common-ground/lance/entity_name_embeddings.lance`.

4. **`infra/duckdb-server/mcp_server.py` lines 82-106** — `_vector_expand_names()` function that searches Lance for similar names. Used by `entity_xray` but only for name expansion, not for source routing.

5. **`infra/duckdb-server/mcp_server.py` lines 10040-10972** — `entity_xray()` tool function. Runs 28 sequential `_execute(pool, ...)` calls with `UPPER(name) LIKE '%X%'` against every table. Takes 10-27s.

6. **`infra/duckdb-server/embedder.py`** — `create_embedder()` returns `(embed, embed_batch, dims)`. Gemini at 790 texts/sec with rate limiting, fallback to OpenRouter, fallback to local ONNX. `vec_to_sql()` helper converts numpy to DuckDB FLOAT[] literal.

### Key Numbers

- 47 name-bearing tables → 52M rows in `name_index`
- Distinct names (deduped by last+first): estimated ~14M
- Embedding rate: 790/sec via Gemini = ~5 hours for 14M names
- Lance storage: 14M × 768 dims × 4 bytes = ~42 GB
- Current entity_xray: 28 queries, 10-27s
- Target entity_xray: 1 Lance search + 3-8 targeted queries, ~2s

### The Approach: Embed Distinct Names, Not All 52M Rows

The `name_index` has 52M rows but massive duplication (same person in payroll every year, same donor repeated, etc.). We embed only **distinct (last_name, first_name) pairs** with their associated `source_table` list. This cuts embedding volume from 52M to ~14M and stores which tables each name appears in.

### Critical: Source Mapping

Each Lance row needs the **list of source tables** where that name appears. This is what `entity_xray` uses for routing:

```
name="PERLBINDER, BARTON" → sources=["acris_parties","oath_hearings","campaign_contributions","nys_corporations","dob_permit_issuance"]
```

`entity_xray` then ONLY queries those 5 tables instead of all 23.

---

## Task 1: Create the Dagster Asset

**Files:**
- Create: `src/dagster_pipeline/defs/entity_embeddings_asset.py`
- Modify: `src/dagster_pipeline/definitions.py` (register asset + add to monthly job)

### Step 1a: Write the asset file

- [ ] Create `src/dagster_pipeline/defs/entity_embeddings_asset.py`:

```python
"""Dagster asset for Lance entity name embeddings.

Reads distinct names from lake.federal.name_index, embeds via Gemini,
writes to /data/common-ground/lance/entity_name_embeddings.lance.

The Lance table stores:
  - name (VARCHAR): "LASTNAME, FIRSTNAME" normalized
  - sources (VARCHAR): comma-separated source_table list
  - embedding (FLOAT[768]): Gemini gemini-embedding-001 vector

entity_xray uses this for routing: search Lance first, then only
query the source tables that have matches.
"""
import logging
import os
import time

import duckdb
import numpy as np
import pyarrow as pa
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

logger = logging.getLogger(__name__)

LANCE_PATH = "/data/common-ground/lance/entity_name_embeddings.lance"
GEMINI_DIMS = 768
BATCH_SIZE = 100
MAX_NAMES = 15_000_000  # safety cap


def _connect_ducklake() -> duckdb.DuckDBPyConnection:
    from dagster_pipeline.resources.ducklake import DuckLakeResource
    resource = DuckLakeResource(
        catalog_url=os.environ.get("DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG", ""),
        s3_endpoint=os.environ.get("S3_ENDPOINT", "178.156.228.119:9000"),
        s3_access_key=os.environ.get(
            "DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_ACCESS_KEY_ID", ""),
        s3_secret_key=os.environ.get(
            "DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_SECRET_ACCESS_KEY", ""),
    )
    return resource.get_connection()


def _create_embedder():
    """Create Gemini embedder (same as mcp_server embedder.py)."""
    from google import genai
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY required for entity embeddings")
    client = genai.Client(api_key=api_key)
    import threading
    _rate_lock = threading.Lock()
    _req_times: list[float] = []
    MAX_RPS = 40

    def _rate_limit():
        with _rate_lock:
            now = time.time()
            _req_times[:] = [t for t in _req_times if now - t < 1.0]
            if len(_req_times) >= MAX_RPS:
                wait = 1.0 - (now - _req_times[0]) + 0.05
                if wait > 0:
                    time.sleep(wait)
            _req_times.append(time.time())

    def embed_batch(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, GEMINI_DIMS), dtype=np.float32)
        from concurrent.futures import ThreadPoolExecutor, as_completed
        chunks = [texts[i:i + BATCH_SIZE] for i in range(0, len(texts), BATCH_SIZE)]
        all_vecs = [None] * len(chunks)

        def _call(chunk):
            _rate_limit()
            for attempt in range(4):
                try:
                    result = client.models.embed_content(
                        model="gemini-embedding-001",
                        contents=chunk,
                        config={"output_dimensionality": GEMINI_DIMS},
                    )
                    return [e.values for e in result.embeddings]
                except Exception as e:
                    if attempt == 3:
                        raise
                    time.sleep(2 * (2 ** attempt))
            raise RuntimeError("Retry exhausted")

        n_workers = min(20, len(chunks))
        done = 0
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {pool.submit(_call, c): i for i, c in enumerate(chunks)}
            for f in as_completed(futures):
                idx = futures[f]
                try:
                    all_vecs[idx] = f.result()
                except Exception:
                    logger.warning("Chunk %d failed, zero-filling", idx)
                    all_vecs[idx] = [[0.0] * GEMINI_DIMS] * len(chunks[idx])
                done += 1
                if done % 200 == 0 or done == len(chunks):
                    elapsed = time.time() - t0
                    rate = (done * BATCH_SIZE) / elapsed if elapsed > 0 else 0
                    logger.info("  %d/%d chunks (%.0f names/sec)", done, len(chunks), rate)

        flat = []
        for batch in all_vecs:
            flat.extend(batch)
        return np.array(flat, dtype=np.float32)

    return embed_batch


@asset(
    key=AssetKey(["foundation", "entity_name_embeddings"]),
    group_name="foundation",
    deps=[AssetKey(["federal", "name_index"])],
    description=(
        "Lance vector index of every distinct person/entity name in the lake. "
        "Embeds ~14M names via Gemini for sub-second semantic search. "
        "Used by MCP entity_xray for source routing."
    ),
    compute_kind="lance",
)
def entity_name_embeddings(context) -> MaterializeResult:
    """Embed all distinct names from name_index into Lance."""
    conn = _connect_ducklake()
    embed_batch = _create_embedder()

    try:
        # Step 1: Extract distinct names with their source table lists
        context.log.info("Step 1: Extracting distinct names from name_index...")
        rows = conn.execute("""
            SELECT
                UPPER(TRIM(last_name)) || ', ' || UPPER(TRIM(first_name)) AS name,
                STRING_AGG(DISTINCT source_table, ',' ORDER BY source_table) AS sources,
                COUNT(DISTINCT source_table) AS source_count
            FROM lake.federal.name_index
            WHERE last_name IS NOT NULL AND LENGTH(last_name) >= 2
              AND first_name IS NOT NULL AND LENGTH(first_name) >= 1
            GROUP BY UPPER(TRIM(last_name)), UPPER(TRIM(first_name))
            ORDER BY source_count DESC
        """).fetchall()

        total_names = len(rows)
        context.log.info("Distinct names: %s", f"{total_names:,}")

        if total_names > MAX_NAMES:
            context.log.warning("Capping at %s names (was %s)", f"{MAX_NAMES:,}", f"{total_names:,}")
            rows = rows[:MAX_NAMES]
            total_names = MAX_NAMES

        names = [r[0] for r in rows]
        sources = [r[1] for r in rows]
        source_counts = [r[2] for r in rows]

        # Step 2: Check what's already embedded (incremental)
        existing_names = set()
        try:
            import pathlib
            if pathlib.Path(LANCE_PATH).exists():
                existing = conn.execute(
                    f"SELECT DISTINCT name FROM '{LANCE_PATH}'"
                ).fetchall()
                existing_names = {r[0] for r in existing}
                context.log.info("Existing embeddings: %s", f"{len(existing_names):,}")
        except Exception as e:
            context.log.warning("Could not read existing Lance: %s", e)

        # Find new names
        new_indices = [i for i, n in enumerate(names) if n not in existing_names]
        new_names = [names[i] for i in new_indices]
        new_sources = [sources[i] for i in new_indices]
        context.log.info("New names to embed: %s", f"{len(new_names):,}")

        if not new_names:
            context.log.info("No new names — Lance index is up to date")
            return MaterializeResult(
                metadata={
                    "total_distinct_names": MetadataValue.int(total_names),
                    "existing_embeddings": MetadataValue.int(len(existing_names)),
                    "new_embedded": MetadataValue.int(0),
                }
            )

        # Step 3: Embed in batches
        context.log.info("Step 3: Embedding %s names via Gemini...", f"{len(new_names):,}")
        t0 = time.time()
        vecs = embed_batch(new_names)
        embed_time = time.time() - t0
        context.log.info("Embedded in %.0fs (%.0f names/sec)", embed_time, len(new_names) / embed_time)

        # Step 4: Write to Lance (append or overwrite)
        context.log.info("Step 4: Writing to Lance...")
        tbl = pa.table({
            "name": new_names,
            "sources": new_sources,
            "embedding": [v.tolist() for v in vecs],
        })

        import pathlib
        mode = "append" if pathlib.Path(LANCE_PATH).exists() and existing_names else "overwrite"
        conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_emb AS SELECT * FROM tbl")
        conn.execute(f"COPY _tmp_emb TO '{LANCE_PATH}' (FORMAT lance, mode '{mode}')")
        conn.execute("DROP TABLE _tmp_emb")

        final_count = conn.execute(f"SELECT COUNT(*) FROM '{LANCE_PATH}'").fetchone()[0]
        context.log.info("Lance index: %s total rows", f"{final_count:,}")

        return MaterializeResult(
            metadata={
                "total_distinct_names": MetadataValue.int(total_names),
                "existing_embeddings": MetadataValue.int(len(existing_names)),
                "new_embedded": MetadataValue.int(len(new_names)),
                "final_lance_rows": MetadataValue.int(final_count),
                "embed_time_seconds": MetadataValue.float(embed_time),
                "embed_rate_per_sec": MetadataValue.float(len(new_names) / embed_time if embed_time > 0 else 0),
            }
        )
    finally:
        conn.close()
```

- [ ] **Step 1b: Register in definitions.py**

In `definitions.py`, add the import and wire it into the asset list and monthly job:

```python
# Add import at top
from dagster_pipeline.defs.entity_embeddings_asset import entity_name_embeddings

# Add to all_assets list
all_assets = [*all_socrata_direct_assets, *all_federal_direct_assets, name_index, resolved_entities,
              h3_index, phonetic_index, row_fingerprints, data_health, entity_name_embeddings]

# Add a new job for entity embeddings (runs after entity_resolution)
entity_embeddings_job = dg.define_asset_job(
    name="entity_embeddings",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["foundation", "entity_name_embeddings"]),
    ),
)

# Add monthly schedule for embeddings (1st of month, after entity resolution)
entity_embeddings_schedule = dg.ScheduleDefinition(
    job=entity_embeddings_job,
    cron_schedule="0 12 1 * *",  # Noon on 1st of month (after 3AM entity resolution)
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
```

Add `entity_embeddings_job` to the jobs list and `entity_embeddings_schedule` to the schedules list in the `dg.Definitions(...)` call.

- [ ] **Step 1c: Verify asset shows up in Dagster UI**

```bash
cd ~/Desktop/dagster-pipeline
uv run dagster asset list -m dagster_pipeline.definitions | grep entity_name_embeddings
```

Expected: `foundation/entity_name_embeddings`

- [ ] **Step 1d: Commit**

```bash
git add src/dagster_pipeline/defs/entity_embeddings_asset.py src/dagster_pipeline/definitions.py
git commit -m "feat: add entity_name_embeddings Dagster asset — monthly Lance index of all lake names"
```

---

## Task 2: Rewire entity_xray to Use Lance Routing

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (entity_xray function + new routing helper)

### Step 2a: Add the routing function

- [ ] Add `_lance_route_entity` helper near `_vector_expand_names` (around line 106):

```python
def _lance_route_entity(ctx, search_term: str, k: int = 30) -> dict[str, list[str]]:
    """Search Lance entity index to find which source tables contain matching names.

    Returns dict with:
      - 'sources': list of source_table names that have matches
      - 'matched_names': list of matched name strings
    Returns empty dict on failure (caller falls back to full scan).
    """
    embed_fn = ctx.lifespan_context.get("embed_fn")
    if not embed_fn:
        return {}
    try:
        from embedder import vec_to_sql
        query_vec = embed_fn(search_term)
        vec_literal = vec_to_sql(query_vec)
        sql = f"""
            SELECT name, sources, _distance AS dist
            FROM lance_vector_search('{LANCE_DIR}/entity_name_embeddings.lance', 'embedding', {vec_literal}, k={k})
            WHERE _distance < {LANCE_NAME_DISTANCE}
            ORDER BY _distance ASC
        """
        pool = ctx.lifespan_context["pool"]
        with pool.cursor() as cur:
            rows = cur.execute(sql).fetchall()

        if not rows:
            return {}

        all_sources = set()
        matched_names = []
        for name, sources_csv, dist in rows:
            matched_names.append(name)
            for src in sources_csv.split(","):
                s = src.strip()
                if s:
                    all_sources.add(s)

        return {"sources": sorted(all_sources), "matched_names": matched_names}
    except Exception:
        return {}
```

### Step 2b: Rewrite entity_xray to use routing

- [ ] Modify `entity_xray` function. Replace the 28 sequential queries with a routing check + conditional execution. The key change is at the top of the function, after `_vector_expand_names`:

```python
    # Route via Lance — find which tables actually have this name
    lance_route = _lance_route_entity(ctx, search)
    routed_sources = set(lance_route.get("sources", []))

    # If Lance routing found matches, only query those tables
    # If Lance returned nothing (index missing/error), fall back to full scan
    use_routing = len(routed_sources) > 0

    def _should_query(source_table: str) -> bool:
        """Return True if this source table should be queried."""
        if not use_routing:
            return True  # fallback: query everything
        return source_table in routed_sources
```

Then wrap each existing query block with the routing check. For example:

```python
    # 1. NYS corps
    corp_cols, corp_rows = [], []
    if _should_query("nys_corporations"):
        corp_cols, corp_rows = _execute(pool, """...""", [...])

    # 2. Business licenses
    biz_cols, biz_rows = [], []
    if _should_query("issued_licenses"):
        biz_cols, biz_rows = _execute(pool, """...""", [...])

    # ... etc for all 23 sources
```

**Important:** The source_table names in the Lance `sources` column match the `source_table` values from `name_registry.py` — they're the table names without schema prefix (e.g., `"oath_hearings"`, `"citywide_payroll"`).

**Source table mapping for entity_xray's 23 queries:**

| Query # | entity_xray variable | Lance routable? | source_table in Lance |
|---------|---------------------|-----------------|-----------------------|
| 1 | corp_rows | NO — business entity names | ALWAYS QUERY |
| 2 | biz_rows | NO — business names | ALWAYS QUERY |
| 3 | rest_rows | NO — DBA names | ALWAYS QUERY |
| 4 | camp_rows | YES | `campaign_contributions` |
| 5 | oath_rows | YES | `oath_hearings` |
| 6 | acris_rows | YES | `acris_parties` |
| 7 | pluto_rows | YES (owner name) | `pluto` |
| 8 | expend_rows | NO — vendor/payee names | ALWAYS QUERY |
| 9 | dob_rows | YES | `dob_permit_issuance` |
| 10 | dcwp_rows | NO — business names | ALWAYS QUERY |
| 11 | sbs_rows | YES | `sbs_certified` |
| 12 | payroll_rows | YES | `citywide_payroll` |
| 13 | dob_app_rows | NO — graph table | ALWAYS QUERY |
| 14 | doing_biz_rows | NO — graph table | ALWAYS QUERY |
| 15 | epa_rows | NO — graph table | ALWAYS QUERY |
| 16 | marriage_rows | NO — not in name_registry | ALWAYS QUERY |
| 16b | hist_marriage | YES | `marriage_certificates_1866_1937` |
| 17 | atty_rows | YES | `nys_attorney_registrations` |
| 18 | pp_rows | YES | `acris_pp_parties` |
| 19 | civil_rows | YES | `civil_service_active` |
| 20 | lobby_rows | NO — not in name_registry | ALWAYS QUERY |
| 21 | death_rows | YES | `nys_death_index` |
| 22 | broker_rows | YES | `nys_re_brokers` |
| 23 | notary_rows | YES | `nys_notaries` |

**9 ALWAYS-QUERY sources** (business/entity names or missing from registry):
`nys_corporations`, `issued_licenses`, `restaurant_inspections`, `campaign_expenditures`, `dcwp_charges`, `graph_dob_owners`, `graph_doing_business`, `graph_epa_facilities`, `marriage_licenses_1950_2017`, `nys_lobbyist_registration`

**14 ROUTABLE sources** (person names in name_registry → in Lance):
These are only queried when Lance finds matching names in their source_table.

The `_should_query()` function must have an `ALWAYS_QUERY` set for the 9 non-routable sources:

- [ ] **Step 2c: Add routing info to the response**

After the existing `lines = [f"ENTITY X-RAY — '{name}'\n"]`, add:

```python
    if use_routing:
        lines.append(f"Searched {len(routed_sources)} of 23 sources (Lance-routed, {len(lance_route.get('matched_names', []))} name matches)")
    else:
        lines.append(f"Full scan — 23 sources (Lance index unavailable)")
    lines.append("")
```

- [ ] **Step 2d: Test via MCP**

Call `entity_xray("Barton Perlbinder")` and verify:
- Response includes "Searched X of 23 sources (Lance-routed)"
- Response time is under 3s (vs 10-27s previously)
- Same data quality — all sources with actual hits are still queried

- [ ] **Step 2e: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: entity_xray uses Lance routing — skip empty sources, 5-10x faster"
```

---

## Task 3: Remove Startup Embedding From MCP Server

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (remove `_build_entity_name_embeddings` from warmup)

- [ ] **Step 3a: Remove the startup embedding code**

In the lifespan/warmup section of mcp_server.py, remove or comment out the call to `_build_entity_name_embeddings()`. The Dagster asset now owns this. Keep the Lance reading code (`_vector_expand_names`, `_lance_route_entity`) — only remove the writing/building code.

Keep `_build_catalog_embeddings` and `_build_description_embeddings` (those are small and fine on startup).

- [ ] **Step 3b: Verify server starts without building name embeddings**

```bash
# On Hetzner
docker compose restart duckdb-server
docker compose logs -f duckdb-server 2>&1 | head -50
```

Should NOT see "Entity name embeddings:" in startup logs.

- [ ] **Step 3c: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "refactor: move entity name embedding to Dagster asset, remove from MCP startup"
```

---

## Task 4: First Run — Populate the Lance Index

- [ ] **Step 4a: Ensure name_index is fresh**

```bash
cd ~/Desktop/dagster-pipeline
DAGSTER_HOME=.dagster-home uv run dagster asset materialize \
  --select 'federal/name_index' -m dagster_pipeline.definitions
```

- [ ] **Step 4b: Run the embedding asset**

```bash
DAGSTER_HOME=.dagster-home GEMINI_API_KEY=<key> uv run dagster asset materialize \
  --select 'foundation/entity_name_embeddings' -m dagster_pipeline.definitions
```

Expected: ~5 hours for 14M names at 790/sec. Monitor via Dagster UI logs.

- [ ] **Step 4c: Verify Lance file on server**

```bash
ssh fattie@178.156.228.119 "ls -lh /data/common-ground/lance/entity_name_embeddings.lance/"
```

Expected: ~42 GB directory with Lance segments.

- [ ] **Step 4d: Deploy MCP server and test**

```bash
cd ~/Desktop/dagster-pipeline && bash scripts/deploy.sh
```

Then test: call `entity_xray("Barton Perlbinder")` — should return in ~2s with "Searched N of 23 sources (Lance-routed)".

---

## Notes

### Incremental Updates

The asset is incremental — it reads existing names from Lance and only embeds new ones. Monthly runs after `name_index` refresh will only embed names that appeared in newly ingested data. First run is ~5 hours; subsequent runs should be minutes.

### Fallback Behavior

If the Lance index doesn't exist or the search fails, `entity_xray` falls back to the current full-scan behavior. No degradation — just slower.

### Future: FTS Hybrid

Once the Lance index is populated, `lance_hybrid_search` could replace `lance_vector_search` for even better matching (combines embedding similarity with keyword matching). This is a follow-up — get the basic routing working first.
