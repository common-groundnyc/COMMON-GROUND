# Lance Vector Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the DuckDB FLOAT[N] + hnsw_acorn + Parquet embedding storage with Lance — a vector-native format with built-in ANN search, zero DuckDB extension dependencies, and native incremental append.

**Architecture:** Embeddings are stored as Lance datasets on the Docker volume (`/data/common-ground/lance/`). On startup, the lance DuckDB extension ATTACHes the directory. MCP tools query via `lance_vector_search()` SQL function (returns `_distance` column). Incremental embedding uses `COPY ... TO ... (FORMAT lance, mode 'append')`. The OpenRouter adaptive-concurrency embedder stays unchanged.

**Tech Stack:** Lance DuckDB core extension (available on 1.5.0+), OpenRouter Gemini Embedding API, existing `embedder.py`

---

## What gets removed

All of these are replaced by Lance and should be deleted:

1. **Parquet cache infrastructure** in `mcp_server.py`: `EMB_DIR`, `_load_persisted_embeddings()`, `_save_embedding_table()`, `_create_hnsw_indexes()`
2. **DuckDB FLOAT[N] table creation** in all `_incremental_*` builders — no more `CREATE TABLE main.X (... embedding FLOAT[768])`
3. **Row-by-row INSERT** loops — replaced by `COPY ... TO lance (mode 'append')`
4. **hnsw_acorn** from `COMMUNITY_EXTS` list (no longer needed)
5. **main.* embedding tables** — tools query Lance directly via `lance_vector_search()`
6. **main.building_vectors** with FLOAT[6] — replaced by Lance dataset

## What stays unchanged

1. **`embedder.py`** — the adaptive-concurrency OpenRouter embedder is perfect as-is
2. **`vec_to_sql()`** — still needed to inline query vectors in SQL
3. **Tool signatures and docstrings** — `semantic_search`, `fuzzy_entity_search`, `similar_buildings` keep their interfaces
4. **Tool upgrades** — `data_catalog` semantic fallback, `entity_xray` vector pre-pass, `text_search` semantic augmentation

## File Structure

```
infra/duckdb-server/
├── embedder.py                 # UNCHANGED — adaptive OpenRouter + ONNX fallback
├── mcp_server.py               # MODIFY — replace embedding infra + tool queries
├── Dockerfile                  # MODIFY — add lancedb pip package (for Python writes)
└── tests/
    └── test_embedder.py        # UNCHANGED
```

On Docker volume (created at runtime):
```
/data/common-ground/lance/
├── catalog_embeddings.lance/   # 12 rows, schema descriptions
├── description_embeddings.lance/ # ~5K rows, complaint/violation categories
├── entity_name_embeddings.lance/ # ~2M rows, entity names
└── building_vectors.lance/     # ~348K rows, numeric feature vectors
```

## Lance API Reference (for implementers)

```sql
-- Load extension
INSTALL lance;
LOAD lance;

-- Vector search (returns _distance column, lower = closer)
SELECT * FROM lance_vector_search(
    '/data/common-ground/lance/entity_name_embeddings.lance',
    'embedding',           -- vector column name
    [0.1, 0.2, ...]::FLOAT[],  -- query vector (variable-length OK)
    k = 20,                -- top K results
    prefilter = true       -- apply WHERE before top-k
)
WHERE source = 'owner'
ORDER BY _distance ASC;

-- Write new data (overwrite)
COPY (SELECT 'owner' AS source, 'John Smith' AS name, [0.1, ...]::FLOAT[768] AS embedding)
TO '/path/to/dataset.lance' (FORMAT lance, mode 'overwrite');

-- Append incrementally
COPY (SELECT ...) TO '/path/to/dataset.lance' (FORMAT lance, mode 'append');

-- Read lance dataset as table
SELECT COUNT(*) FROM '/path/to/dataset.lance';

-- Create ANN index for faster search (optional, helps at >100K rows)
CREATE INDEX vec_idx ON '/path/to/dataset.lance' (embedding)
USING IVF_FLAT WITH (num_partitions=32, metric_type='cosine');
```

---

## Task 1: Add Lance extension + remove hnsw_acorn + fix vec_to_sql

**Files:**
- Modify: `infra/duckdb-server/extensions.py` (CORE_EXTS and COMMUNITY_EXTS)
- Modify: `infra/duckdb-server/Dockerfile` (add lance to pre-install step)
- Modify: `infra/duckdb-server/embedder.py` (vec_to_sql outputs FLOAT[] not FLOAT[N])

- [ ] **Step 1: Add `lance` to CORE_EXTS, remove `hnsw_acorn` from COMMUNITY_EXTS**

In `extensions.py` (NOT mcp_server.py):
```python
# Before:
CORE_EXTS = ["ducklake", "postgres", "spatial", "fts", "httpfs", "json"]
# hnsw_acorn is in COMMUNITY_EXTS list

# After:
CORE_EXTS = ["ducklake", "postgres", "spatial", "fts", "httpfs", "json", "lance"]
# Remove hnsw_acorn from COMMUNITY_EXTS
```

- [ ] **Step 2: Add lance to Dockerfile pre-install step**

In the Dockerfile RUN python -c block that pre-installs community extensions, add `conn.execute('INSTALL lance')` (core extension, no `FROM community` needed). Do NOT add `lancedb` pip package — all Lance operations use DuckDB SQL, not Python lancedb.

- [ ] **Step 3: Fix vec_to_sql to output FLOAT[] (variable-length)**

In `embedder.py`, change `vec_to_sql()` to output `FLOAT[]` instead of `FLOAT[N]`:
```python
def vec_to_sql(vec: np.ndarray) -> str:
    """Convert numpy vector to DuckDB FLOAT[] SQL literal for Lance vector search."""
    inner = ",".join(str(float(v)) for v in vec)
    return f"[{inner}]::FLOAT[]"
```

This is required because `lance_vector_search()` expects `FLOAT[]` (variable-length). Fixed-size `FLOAT[768]` may not be compatible.

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py infra/duckdb-server/Dockerfile
git commit -m "feat: add lance core extension, remove hnsw_acorn"
```

---

## Task 2: Replace embedding infrastructure with Lance

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (lines ~1012-1230, the entire embedding infrastructure block)

This is the big task. Replace all of: `EMB_DIR`, `_load_persisted_embeddings`, `_save_embedding_table`, `_create_hnsw_indexes`, `_incremental_catalog_embeddings`, `_incremental_description_embeddings`, `_incremental_entity_name_embeddings`, `_build_building_vectors`.

- [ ] **Step 1: Replace the embedding infrastructure block**

Delete everything from `# --- Persistent embeddings on volume` through `_build_building_vectors` (inclusive). Replace with:

```python
    # --- Lance vector storage (persistent, incremental, built-in ANN search) ---
    # NOTE: LANCE_DIR is defined as a module-level constant near the top of the file
    # (outside app_lifespan) so both builders and tool functions can access it.
    # Add this near line 59 (next to _db_lock): LANCE_DIR = "/data/common-ground/lance"

    def _lance_path(name):
        return f"{LANCE_DIR}/{name}.lance"

    def _lance_exists(name):
        import pathlib
        return pathlib.Path(_lance_path(name)).exists()

    def _lance_count(conn, name):
        try:
            return conn.execute(f"SELECT COUNT(*) FROM '{_lance_path(name)}'").fetchone()[0]
        except Exception:
            return 0

    def _build_catalog_embeddings(conn, embed_batch, dims):
        """Always rebuild — tiny (12 rows)."""
        rows = []
        for schema_name, description in SCHEMA_DESCRIPTIONS.items():
            rows.append((schema_name, "__schema__", description))
        if not rows:
            return
        texts = [r[2] for r in rows]
        vecs = embed_batch(texts)
        # Build as DuckDB table, then COPY to Lance
        conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_cat (schema_name VARCHAR, table_name VARCHAR, description VARCHAR, embedding FLOAT[])")
        for (s, t, d), vec in zip(rows, vecs):
            conn.execute("INSERT INTO _tmp_cat VALUES (?, ?, ?, ?::FLOAT[])", [s, t, d, vec.tolist()])
        import pathlib
        pathlib.Path(LANCE_DIR).mkdir(parents=True, exist_ok=True)
        conn.execute(f"COPY _tmp_cat TO '{_lance_path('catalog_embeddings')}' (FORMAT lance, mode 'overwrite')")
        conn.execute("DROP TABLE _tmp_cat")
        print(f"  Catalog embeddings: {len(rows)} rows (Lance)", flush=True)

    def _build_description_embeddings(conn, embed_batch, dims):
        """Incremental — only embed new descriptions."""
        sources = [
            ("311", "SELECT DISTINCT (complaint_type || ': ' || COALESCE(descriptor, '')) AS description FROM lake.social_services.n311_service_requests WHERE complaint_type IS NOT NULL LIMIT 3000"),
            ("restaurant", "SELECT DISTINCT violation_description AS description FROM lake.health.restaurant_inspections WHERE violation_description IS NOT NULL LIMIT 500"),
            ("hpd", "SELECT DISTINCT novdescription AS description FROM lake.housing.hpd_violations WHERE novdescription IS NOT NULL LIMIT 1000"),
            ("oath", "SELECT DISTINCT charge_1_code_description AS description FROM lake.city_government.oath_hearings WHERE charge_1_code_description IS NOT NULL LIMIT 500"),
        ]
        import pathlib
        pathlib.Path(LANCE_DIR).mkdir(parents=True, exist_ok=True)
        total_new = 0
        for source_name, sql in sources:
            try:
                all_descs = {r[0] for r in conn.execute(sql).fetchall() if r[0]}
                existing = set()
                if _lance_exists("description_embeddings"):
                    try:
                        existing = {r[0] for r in conn.execute(
                            f"SELECT description FROM '{_lance_path('description_embeddings')}' WHERE source = ?", [source_name]
                        ).fetchall()}
                    except Exception:
                        pass
                new_descs = sorted(all_descs - existing)
                if not new_descs:
                    print(f"  Descriptions [{source_name}]: {len(existing)} existing, 0 new", flush=True)
                    continue
                print(f"  Descriptions [{source_name}]: {len(existing)} existing, {len(new_descs)} new — embedding...", flush=True)
                vecs = embed_batch(new_descs)
                conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_desc (source VARCHAR, description VARCHAR, embedding FLOAT[])")
                for desc, vec in zip(new_descs, vecs):
                    conn.execute("INSERT INTO _tmp_desc VALUES (?, ?, ?::FLOAT[])", [source_name, desc, vec.tolist()])
                mode = "append" if _lance_exists("description_embeddings") else "overwrite"
                conn.execute(f"COPY _tmp_desc TO '{_lance_path('description_embeddings')}' (FORMAT lance, mode '{mode}')")
                conn.execute("DROP TABLE _tmp_desc")
                total_new += len(new_descs)
            except Exception as e:
                print(f"  Warning: descriptions [{source_name}] failed: {e}", flush=True)
        print(f"  Description embeddings: {total_new} new rows", flush=True)

    def _build_entity_name_embeddings(conn, embed_batch, dims):
        """Incremental — only embed new names. Bulk write via Arrow → Lance."""
        name_sources = [
            ("owner", "SELECT DISTINCT owner_name AS name FROM main.graph_owners WHERE owner_name IS NOT NULL AND LENGTH(owner_name) > 2"),
            ("corp_officer", "SELECT DISTINCT person_name AS name FROM main.graph_corp_people WHERE person_name IS NOT NULL AND LENGTH(person_name) > 2"),
            ("donor", "SELECT DISTINCT donor_name AS name FROM main.graph_campaign_donors WHERE donor_name IS NOT NULL AND LENGTH(donor_name) > 2"),
            ("tx_party", "SELECT DISTINCT entity_name AS name FROM main.graph_tx_entities WHERE entity_name IS NOT NULL AND LENGTH(entity_name) > 2"),
        ]
        import pathlib
        pathlib.Path(LANCE_DIR).mkdir(parents=True, exist_ok=True)
        total_new = 0
        for source_name, sql in name_sources:
            try:
                all_names = {r[0] for r in conn.execute(sql).fetchall() if r[0]}
                existing = set()
                if _lance_exists("entity_name_embeddings"):
                    try:
                        existing = {r[0] for r in conn.execute(
                            f"SELECT name FROM '{_lance_path('entity_name_embeddings')}' WHERE source = ?", [source_name]
                        ).fetchall()}
                    except Exception:
                        pass
                new_names = sorted(all_names - existing)
                if not new_names:
                    print(f"  Names [{source_name}]: {len(existing):,} existing, 0 new", flush=True)
                    continue
                print(f"  Names [{source_name}]: {len(existing):,} existing, {len(new_names):,} new — embedding...", flush=True)
                vecs = embed_batch(new_names)
                # Bulk write via Arrow → temp table → COPY to Lance
                import pyarrow as pa
                tbl = pa.table({
                    "source": [source_name] * len(new_names),
                    "name": new_names,
                    "embedding": [v.tolist() for v in vecs],
                })
                conn.execute("CREATE OR REPLACE TEMP TABLE _tmp_names AS SELECT * FROM tbl")
                # Cast embedding list to FLOAT[] for Lance
                mode = "append" if _lance_exists("entity_name_embeddings") else "overwrite"
                conn.execute(f"""
                    COPY (SELECT source, name, embedding::FLOAT[] AS embedding FROM _tmp_names)
                    TO '{_lance_path('entity_name_embeddings')}' (FORMAT lance, mode '{mode}')
                """)
                conn.execute("DROP TABLE _tmp_names")
                total_new += len(new_names)
                print(f"  Names [{source_name}]: {len(new_names):,} embedded → Lance", flush=True)
            except Exception as e:
                print(f"  Warning: names [{source_name}] failed: {e}", flush=True)
        print(f"  Entity name embeddings: {total_new:,} new rows total", flush=True)

    def _build_building_vectors(conn):
        """Numeric feature vectors — always rebuilt from graph tables (fast, no API)."""
        import pathlib
        pathlib.Path(LANCE_DIR).mkdir(parents=True, exist_ok=True)
        try:
            conn.execute(f"""
                COPY (
                    WITH base AS (
                        SELECT b.bbl, b.boroid AS borough,
                            COALESCE(TRY_CAST(b.stories AS DOUBLE), 0) AS stories,
                            COALESCE(TRY_CAST(b.units AS DOUBLE), 0) AS units,
                            COALESCE(TRY_CAST(b.year_built AS DOUBLE), 0) AS year_built,
                            COUNT(v.violation_id) AS viol_count,
                            COUNT(CASE WHEN v.currentstatus = 'Open' THEN 1 END) AS open_viol_count
                        FROM main.graph_buildings b
                        LEFT JOIN main.graph_violations v ON b.bbl = v.bbl
                        GROUP BY b.bbl, b.boroid, b.stories, b.units, b.year_built
                    ),
                    normed AS (
                        SELECT bbl, borough,
                            stories / NULLIF(MAX(stories) OVER (), 0) AS n1,
                            units / NULLIF(MAX(units) OVER (), 0) AS n2,
                            year_built / NULLIF(MAX(year_built) OVER (), 0) AS n3,
                            viol_count / NULLIF(MAX(viol_count) OVER (), 0) AS n4,
                            open_viol_count / NULLIF(MAX(open_viol_count) OVER (), 0) AS n5,
                            CASE WHEN units > 0 THEN viol_count::DOUBLE / units ELSE 0 END /
                                NULLIF(MAX(CASE WHEN units > 0 THEN viol_count::DOUBLE / units ELSE 0 END) OVER (), 0) AS n6
                        FROM base
                    )
                    SELECT bbl, borough,
                        [COALESCE(n1,0), COALESCE(n2,0), COALESCE(n3,0),
                         COALESCE(n4,0), COALESCE(n5,0), COALESCE(n6,0)]::FLOAT[] AS features
                    FROM normed
                ) TO '{_lance_path('building_vectors')}' (FORMAT lance, mode 'overwrite')
            """)
            count = _lance_count(conn, "building_vectors")
            print(f"  Building vectors: {count:,} rows (Lance)", flush=True)
        except Exception as e:
            print(f"  Warning: building vectors failed: {e}", flush=True)

    def _create_lance_indexes(conn):
        """Create ANN indexes on large Lance datasets."""
        try:
            if _lance_count(conn, "entity_name_embeddings") > 10000:
                conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS name_emb_idx
                    ON '{_lance_path('entity_name_embeddings')}' (embedding)
                    USING IVF_FLAT WITH (num_partitions=32, metric_type='cosine')
                """)
                print("  ANN index created on entity_name_embeddings", flush=True)
        except Exception as e:
            print(f"  Warning: Lance ANN index failed: {e}", flush=True)
```

- [ ] **Step 2: Update the startup block**

Replace the startup block (around line 2670-2705) with:

```python
    # --- Vector embeddings for semantic search (Lance backend) ---
    embed_fn = None
    embed_dims = 768
    try:
        from embedder import create_embedder
        embed_fn, embed_batch_fn, embed_dims = create_embedder("/app/model")
        print(f"Embedding model loaded ({embed_dims} dims)", flush=True)
    except Exception as e:
        print(f"Warning: embedding model unavailable: {e}", flush=True)

    if embed_fn is not None:
        try:
            print("Building/updating Lance embeddings...", flush=True)
            _build_catalog_embeddings(conn, embed_batch_fn, embed_dims)
            _build_description_embeddings(conn, embed_batch_fn, embed_dims)
            if graph_ready:
                _build_entity_name_embeddings(conn, embed_batch_fn, embed_dims)
                _build_building_vectors(conn)
            _create_lance_indexes(conn)
            print("Lance embedding pipeline complete", flush=True)
        except Exception as e:
            print(f"Warning: Lance pipeline partially complete: {e}", flush=True)
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: replace DuckDB FLOAT[N] embedding tables with Lance vector storage

- Embeddings stored as .lance datasets on Docker volume
- Incremental append via COPY TO (FORMAT lance, mode 'append')
- No more Parquet cache, no hnsw_acorn, no FLOAT[N] columns
- ANN index auto-created on entity names (>10K rows)"
```

---

## Task 3: Update semantic_search tool to use Lance

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (semantic_search function)

- [ ] **Step 1: Replace the SQL query in semantic_search**

Find the `semantic_search` function. Replace the SQL that queries `main.description_embeddings` with `lance_vector_search`:

```python
    # Old: queried main.description_embeddings with array_cosine_distance
    # New: use lance_vector_search
    from embedder import vec_to_sql
    query_vec = embed_fn(query.strip())
    vec_literal = vec_to_sql(query_vec)

    lance_path = f"{LANCE_DIR}/description_embeddings.lance"
    source_filter = ""
    if source != "all":
        source_filter = f"WHERE source = '{source}'"  # safe: validated against allowlist

    # NOTE: prefilter=true pushes the outer WHERE into the vector search.
    # If this doesn't work (returns fewer than k results), remove prefilter
    # and increase k to compensate for post-filtering.
    sql = f"""
        SELECT source, description, _distance
        FROM lance_vector_search(
            '{lance_path}', 'embedding', {vec_literal},
            k = {limit}, prefilter = true
        )
        {source_filter}
        ORDER BY _distance ASC
    """
```

Change the output formatting: `_distance` replaces `array_cosine_distance`. Lance returns L2 distance by default (not cosine). For similarity display, use `similarity = max(0, 1.0 / (1.0 + dist))` which works for both L2 and cosine distances (maps 0→1.0, large→0). Apply this same formula in ALL tools (Tasks 3-6).

- [ ] **Step 2: Add a guard if Lance dataset doesn't exist**

Before the SQL, check:
```python
    if not os.path.exists(f"{LANCE_DIR}/description_embeddings.lance"):
        raise ToolError("Description embeddings not yet built. Server is still starting up.")
```

- [ ] **Step 3: Commit**

```bash
git commit -m "feat: semantic_search uses lance_vector_search"
```

---

## Task 4: Update fuzzy_entity_search tool to use Lance

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (fuzzy_entity_search function)

- [ ] **Step 1: Replace the SQL query**

Same pattern as Task 3 but against `entity_name_embeddings.lance`:

```python
    lance_path = f"{LANCE_DIR}/entity_name_embeddings.lance"
    source_filter = ""
    if source != "all":
        source_filter = f"WHERE source = '{source}'"

    sql = f"""
        SELECT source, name, _distance
        FROM lance_vector_search(
            '{lance_path}', 'embedding', {vec_literal},
            k = {limit}, prefilter = true
        )
        {source_filter}
        ORDER BY _distance ASC
    """
```

- [ ] **Step 2: Add existence guard**

- [ ] **Step 3: Commit**

```bash
git commit -m "feat: fuzzy_entity_search uses lance_vector_search"
```

---

## Task 5: Update similar_buildings tool to use Lance

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (similar_buildings function)

- [ ] **Step 1: Replace the SQL query**

Get the target building's features from Lance, then search:

```python
    lance_path = f"{LANCE_DIR}/building_vectors.lance"

    # Get target features
    with _db_lock:
        target = db.execute(
            f"SELECT features FROM '{lance_path}' WHERE bbl = ?", [bbl]
        ).fetchone()
    if not target:
        raise ToolError(f"No feature vector for BBL {bbl}.")

    vec_literal = "[" + ",".join(f"{v:.6g}" for v in target[0]) + "]::FLOAT[]"

    sql = f"""
        SELECT bv.bbl, bv.borough, bv._distance,
               b.housenumber, b.streetname, b.zip, b.stories, b.units, b.year_built
        FROM lance_vector_search('{lance_path}', 'features', {vec_literal}, k = {limit + 1}) bv
        LEFT JOIN main.graph_buildings b ON bv.bbl = b.bbl
        WHERE bv.bbl != '{bbl}'
        ORDER BY bv._distance ASC
        LIMIT {limit}
    """
```

- [ ] **Step 2: Commit**

```bash
git commit -m "feat: similar_buildings uses lance_vector_search"
```

---

## Task 6: Update data_catalog, entity_xray, text_search upgrades

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (3 functions)

- [ ] **Step 1: Update data_catalog semantic fallback**

Replace `main.catalog_embeddings` query with:
```python
    sem_sql = f"""
        SELECT schema_name, table_name, description, _distance
        FROM lance_vector_search(
            '{LANCE_DIR}/catalog_embeddings.lance', 'embedding', {vec_literal},
            k = 5
        )
        WHERE table_name != '__schema__'
        ORDER BY _distance ASC
    """
```
Filter: use `1.0 / (1.0 + _distance) > 0.3` instead of `_distance < 0.7` (L2 distance scale differs from cosine).

- [ ] **Step 2: Update entity_xray vector pre-pass**

Replace `main.entity_name_embeddings` query with:
```python
    sim_sql = f"""
        SELECT name, _distance
        FROM lance_vector_search(
            '{LANCE_DIR}/entity_name_embeddings.lance', 'embedding', {vec_literal},
            k = 5
        )
        ORDER BY _distance ASC
    """
```
Filter: `_distance < 0.4` stays the same.

- [ ] **Step 3: Update text_search semantic enhancement**

Replace `main.description_embeddings` query with:
```python
    sem_sql = f"""
        SELECT source, description, _distance
        FROM lance_vector_search(
            '{LANCE_DIR}/description_embeddings.lance', 'embedding', {vec_literal},
            k = 5
        )
        {source_filter}
        ORDER BY _distance ASC
    """
```

- [ ] **Step 4: Add `LANCE_DIR` reference to all tools**

Each tool function needs access to `LANCE_DIR`. Since it's defined inside `app_lifespan`, either:
- Add it to the lifespan context yield: `"lance_dir": LANCE_DIR`
- Or define it as a module-level constant (simpler): `LANCE_DIR = "/data/common-ground/lance"` at the top of the file

Choose module-level constant — simpler, no context threading needed.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: update data_catalog, entity_xray, text_search to use Lance"
```

---

## Task 7: Clean up old embedding Parquet files on server

**Files:**
- No code changes — operational cleanup

- [ ] **Step 1: Remove old Parquet embedding cache from server**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "sudo rm -rf /data/common-ground/embeddings/"
```

- [ ] **Step 2: Deploy and verify**

```bash
# Sync files
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
    infra/duckdb-server/ fattie@178.156.228.119:/opt/common-ground/duckdb-server/

# Rebuild
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose up -d --build duckdb-server"
```

- [ ] **Step 3: Watch startup logs**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "docker logs -f common-ground-duckdb-server-1 2>&1 | grep -iE 'lance|embed|Names|Desc'"
```

Expected output:
```
Embedding model loaded (768 dims)
Building/updating Lance embeddings...
  Catalog embeddings: 12 rows (Lance)
  Descriptions [311]: 0 existing, 1511 new — embedding...
  ...
  Names [owner]: 0 existing, 130,697 new — embedding...
  ...
Lance embedding pipeline complete
```

- [ ] **Step 4: Smoke test semantic_search**

```bash
# Via MCP
curl -s http://localhost:4213/mcp -X POST \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"semantic_search","arguments":{"query":"pest problems in restaurants"}}}'
```

- [ ] **Step 5: Commit tag**

```bash
git tag v2.1-lance-vectors
```
