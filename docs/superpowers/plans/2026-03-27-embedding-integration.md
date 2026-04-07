# Embedding Integration — Full Platform Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add vector similarity search to all name-based MCP tools, upgrade resource_finder with semantic category matching, migrate embedding storage to Lance, and integrate embedding-based blocking into Splink entity resolution.

**Architecture:** Three independent phases that each produce working software: (A) MCP tool upgrades — vector pre-pass on 10 name tools + semantic resource_finder, (B) Lance storage migration — replace Parquet + DuckDB FLOAT[] with Lance format, (C) Pipeline enhancements — BlockingPy for Splink, SemHash for dedup, BERTopic for complaint clustering.

**Tech Stack:** Google Gemini embedding-001 (768 dims), Lance DuckDB extension, BlockingPy, SemHash, BERTopic, Dagster assets

---

## Phase A: MCP Tool Vector Pre-Pass (10 tools + resource_finder)

All 10 name-based tools use the same pattern: `search = name.strip().upper()` → `UPPER(col) LIKE '%{search}%'`. We add the same vector pre-pass block to each, then append a "Similar Names" section to the output.

### The Pattern (reference: entity_xray lines 9528-9550)

This exact block gets inserted into each tool after the search term is set and before the first SQL query:

```python
    # Vector pre-pass: find similar names to expand search
    embed_fn = ctx.lifespan_context.get("embed_fn")
    extra_names = set()
    if embed_fn:
        try:
            from embedder import vec_to_sql
            query_vec = embed_fn(search)
            vec_literal = vec_to_sql(query_vec)
            sim_sql = f"""
                SELECT name, array_cosine_distance(embedding, {vec_literal}) AS dist
                FROM main.entity_name_embeddings
                ORDER BY array_cosine_distance(embedding, {vec_literal})
                LIMIT 5
            """
            with _db_lock:
                sim_rows = db.execute(sim_sql).fetchall()
            for matched_name, dist in sim_rows:
                if dist < 0.4:
                    extra_names.add(matched_name.upper())
            extra_names.discard(search)
        except Exception:
            pass
```

And this block gets appended before the elapsed time / return:

```python
    if extra_names:
        lines.append(f"\nSIMILAR NAMES (vector match):")
        for en in sorted(extra_names):
            lines.append(f"  → {en}")
        lines.append("Use entity_xray(name) for full details on any match.")
```

### Task 1: llc_piercer vector pre-pass

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (llc_piercer at line ~9370)

- [ ] **Step 1: Insert vector pre-pass block**

After line `search_term = entity_name.strip().upper()` (line ~9377), insert the vector pre-pass block. Use `search_term` as the variable name (not `search`).

- [ ] **Step 2: Insert "Similar Names" output block**

Before the `elapsed = round(...)` line at the end of the function, insert the similar names output block.

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add vector pre-pass to llc_piercer"
```

### Task 2: transaction_network vector pre-pass

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (transaction_network at line ~11090)

- [ ] **Step 1: Insert vector pre-pass block**

After the search term setup. The tool uses `search = name.strip().upper()`.

- [ ] **Step 2: Insert "Similar Names" output block**

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(mcp): add vector pre-pass to transaction_network"
```

### Task 3: corporate_web vector pre-pass

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (corporate_web at line ~11204)

- [ ] **Step 1: Insert vector pre-pass block**

Uses `search = entity_name.strip().upper()`.

- [ ] **Step 2: Insert "Similar Names" output block**

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(mcp): add vector pre-pass to corporate_web"
```

### Task 4: pay_to_play vector pre-pass

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (pay_to_play at line ~11346)

- [ ] **Step 1: Insert vector pre-pass block**

Uses `search = entity_name.strip().upper()`.

- [ ] **Step 2: Insert "Similar Names" output block**

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(mcp): add vector pre-pass to pay_to_play"
```

### Task 5: contractor_network vector pre-pass

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (contractor_network at line ~11543)

- [ ] **Step 1: Insert vector pre-pass block**

Uses `search = name_or_license.strip().upper()`. Skip vector search if the input looks like a license number (all digits).

```python
    # Vector pre-pass: skip for license numbers (all digits)
    embed_fn = ctx.lifespan_context.get("embed_fn")
    extra_names = set()
    if embed_fn and not search.isdigit():
        # ... same block ...
```

- [ ] **Step 2: Insert "Similar Names" output block**

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(mcp): add vector pre-pass to contractor_network"
```

### Task 6: due_diligence vector pre-pass

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (due_diligence at line ~6527)

- [ ] **Step 1: Read the function to find the search term setup**

due_diligence takes a single `name: NAME` param (NOT separate first/last). It splits into first/last internally. Insert the standard vector pre-pass using `name.strip().upper()` as the search term, same as entity_xray.

- [ ] **Step 2: Insert "Similar Names" output block**

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(mcp): add vector pre-pass to due_diligence"
```

### Task 7: money_trail + cop_sheet + marriage_search vector pre-pass

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (3 tools)

These three tools follow the same pattern. Apply the vector pre-pass to each:

- **money_trail** (line ~7012): takes `name: NAME`, uses `name.strip().upper()` (no `search` variable — use `name` directly)
- **cop_sheet** (line ~6728): takes `name: NAME` (single param, splits internally), use `name.strip().upper()`
- **marriage_search** (line ~10382): takes `surname` + `first_name`, embed `f"{search_first} {search_last}".strip()` (these are the uppercased vars set on lines ~10519-10520)

- [ ] **Step 1: Add vector pre-pass to money_trail**
- [ ] **Step 2: Add vector pre-pass to cop_sheet**
- [ ] **Step 3: Add vector pre-pass to marriage_search**
- [ ] **Step 4: Add "Similar Names" output to all three**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(mcp): add vector pre-pass to money_trail, cop_sheet, marriage_search"
```

### Task 8: person_crossref vector fallback

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (person_crossref at line ~11781)

person_crossref does exact Splink cluster lookup. Add a vector fallback when the exact lookup returns empty:

- [ ] **Step 1: Add vector fallback after Splink lookup**

After the section where cluster_ids are found (and if `cluster_ids` is empty), add:

```python
        # Vector fallback: if exact Splink lookup found nothing, try embedding similarity
        if not cluster_ids:
            embed_fn = ctx.lifespan_context.get("embed_fn")
            if embed_fn:
                try:
                    from embedder import vec_to_sql
                    query_vec = embed_fn(search)
                    vec_literal = vec_to_sql(query_vec)
                    sim_sql = f"""
                        SELECT name, array_cosine_distance(embedding, {vec_literal}) AS dist
                        FROM main.entity_name_embeddings
                        ORDER BY array_cosine_distance(embedding, {vec_literal})
                        LIMIT 3
                    """
                    with _db_lock:
                        sim_rows = db.execute(sim_sql).fetchall()
                    # Try the closest match in Splink — try both name orderings
                    for matched_name, dist in sim_rows:
                        if dist < 0.3:  # stricter threshold for cross-ref
                            parts = matched_name.split()
                            if len(parts) >= 2:
                                # Try both orderings (FIRST LAST and LAST FIRST)
                                candidates = [(parts[-1], parts[0]), (parts[0], parts[-1])]
                                for last, first in candidates:
                                    cols, rows = _execute(db, """
                                        SELECT DISTINCT cluster_id
                                        FROM lake.federal.resolved_entities
                                        WHERE last_name = ? AND first_name = ?
                                        LIMIT 5
                                    """, [last, first])
                                    for r in rows:
                                        if r[0] is not None:
                                            cluster_ids.add(r[0])
                except Exception:
                    pass
```

- [ ] **Step 2: Commit**

```bash
git commit -m "feat(mcp): add vector fallback to person_crossref when Splink lookup fails"
```

### Task 9: resource_finder semantic categories

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (resource_finder at line ~5543)

Currently uses hardcoded keyword matching: `any(k in need_lower for k in ("health", "clinic", ...))`. Replace with semantic matching using pre-embedded category descriptions.

- [ ] **Step 1: Define category embeddings at module level**

Near the resource_finder function, add:

```python
# Resource categories with semantic descriptions for embedding matching
RESOURCE_CATEGORIES = {
    "food": "food assistance, groceries, meals, SNAP benefits, food pantry, soup kitchen, hunger",
    "health": "health clinic, doctor, hospital, mental health, therapy, counseling, medical care",
    "childcare": "childcare, daycare, babysitting, preschool, early childhood, after school care",
    "youth": "youth programs, teen activities, summer camp, mentoring, after school, tutoring",
    "benefits": "government benefits, welfare, public assistance, SSI, disability, unemployment",
    "legal": "legal aid, lawyer, tenant rights, immigration help, court, eviction defense",
    "shelter": "shelter, housing assistance, emergency housing, homeless services, transitional housing",
    "wifi": "free wifi, internet access, computer lab, digital access, library, technology center",
    "senior": "senior services, elder care, meals on wheels, senior center, aging, retirement",
}
```

- [ ] **Step 2: Replace keyword matching with semantic matching**

In resource_finder, after `need_lower = need.strip().lower()`, add:

```python
    # Semantic category matching: compare user's need against pre-embedded categories
    # NOTE: Category embeddings MUST be pre-computed at startup (in app_lifespan) and stored
    # in lifespan context as "resource_category_vecs": dict[str, np.ndarray].
    # Do NOT call embed_fn in a loop here — that's 9 API calls per request.
    embed_fn = ctx.lifespan_context.get("embed_fn")
    cat_vecs = ctx.lifespan_context.get("resource_category_vecs", {})
    matched_categories = set()
    if need_lower == "all":
        matched_categories = set(RESOURCE_CATEGORIES.keys())
    elif embed_fn and cat_vecs and need_lower not in RESOURCE_CATEGORIES:
        try:
            import numpy as np
            query_vec = embed_fn(need_lower)
            q_norm = query_vec / (np.linalg.norm(query_vec) + 1e-9)
            for cat, cat_vec in cat_vecs.items():
                c_norm = cat_vec / (np.linalg.norm(cat_vec) + 1e-9)
                sim = float(np.dot(q_norm, c_norm))
                if sim > 0.5:
                    matched_categories.add(cat)
        except Exception:
            pass
    if need_lower in RESOURCE_CATEGORIES:
        matched_categories.add(need_lower)

    show_all = "all" in matched_categories or not matched_categories
```

Then replace all the `if show_all or any(k in need_lower for k in (...)):` blocks with `if show_all or "food" in matched_categories:`, etc.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(mcp): semantic category matching in resource_finder"
```

---

## Phase B: Lance Storage Migration

### Task 10: Add Lance extension, update vec_to_sql

**Files:**
- Modify: `infra/duckdb-server/extensions.py` — add `"lance"` to CORE_EXTS, remove `"hnsw_acorn"` from COMMUNITY_EXTS
- Modify: `infra/duckdb-server/embedder.py` — `vec_to_sql` already outputs `FLOAT[]` (done in earlier commit)

- [ ] **Step 1: Update extensions.py**

```python
CORE_EXTS = ["ducklake", "postgres", "spatial", "fts", "httpfs", "json", "lance"]
# Remove hnsw_acorn from COMMUNITY_EXTS
```

- [ ] **Step 2: Add lance to Dockerfile pre-install**

In the Dockerfile RUN block that pre-installs extensions, add `conn.execute('INSTALL lance')`.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat: add lance core extension, remove hnsw_acorn"
```

### Task 11: Replace embedding infrastructure with Lance

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` — replace `EMB_DIR`, `_load_persisted_embeddings`, `_save_embedding_table`, all `_incremental_*` builders, `_create_hnsw_indexes`

Replace all Parquet cache + DuckDB FLOAT[] infrastructure with Lance:
- `LANCE_DIR = "/data/common-ground/lance"` at module level
- Builders use `COPY ... TO '...lance' (FORMAT lance, mode 'append'|'overwrite')`
- Reading: `SELECT * FROM 'path.lance'`
- Existence check: `pathlib.Path(path).exists()`
- No more `_create_hnsw_indexes` — Lance has native ANN via `lance_vector_search()`

See `docs/superpowers/plans/2026-03-27-lance-vector-backend.md` Task 2 for full replacement code.

- [ ] **Step 1: Replace embedding infrastructure block**
- [ ] **Step 2: Update startup block**
- [ ] **Step 3: Commit**

```bash
git commit -m "feat: replace DuckDB FLOAT[] embedding tables with Lance"
```

### Task 12: Update all tools to use lance_vector_search

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

Replace every `array_cosine_distance(embedding, {vec_literal})` / `FROM main.*_embeddings` with `lance_vector_search()`:

```python
# Old pattern (in semantic_search, fuzzy_entity_search, data_catalog, entity_xray, text_search):
SELECT ... FROM main.description_embeddings
ORDER BY array_cosine_distance(embedding, {vec_literal})

# New pattern:
SELECT ... FROM lance_vector_search(
    '{LANCE_DIR}/description_embeddings.lance', 'embedding', {vec_literal},
    k = {limit}
)
ORDER BY _distance ASC
```

Also update `similar_buildings` to use `lance_vector_search` on `building_vectors.lance`.

Similarity display: Lance `_distance` semantics depend on metric. For cosine index: `_distance` is cosine distance [0, 2], use `1 - _distance/2` for similarity. For L2 (default): use `1.0 / (1.0 + _distance)`. Check the index metric type and use the appropriate formula. If no index exists (brute force), Lance uses L2 by default.

- [ ] **Step 1: Update semantic_search**
- [ ] **Step 2: Update fuzzy_entity_search**
- [ ] **Step 3: Update similar_buildings**
- [ ] **Step 4: Update data_catalog semantic fallback**
- [ ] **Step 5: Update entity_xray vector pre-pass (and all 10 tools from Phase A)**
- [ ] **Step 6: Update text_search semantic enhancement**
- [ ] **Step 7: Commit**

```bash
git commit -m "feat: all tools use lance_vector_search instead of DuckDB array_cosine_distance"
```

### Task 13: Deploy + clean up old Parquet

- [ ] **Step 1: Remove old Parquet embedding cache**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "sudo rm -rf /data/common-ground/embeddings/"
```

- [ ] **Step 2: Deploy**
- [ ] **Step 3: Smoke test all vector tools**

---

## Phase C: Pipeline Enhancements (Dagster Assets)

### Task 14: BlockingPy integration with Splink

**Files:**
- Modify: `infra/duckdb-server/Dockerfile` — add `blockingpy` pip package
- Create: `src/dagster_pipeline/sources/embedding_blocking.py`
- Modify: `scripts/batch_resolve_entities.py` or equivalent Splink script

BlockingPy generates candidate pairs using embeddings before Splink comparison:

```python
from blockingpy import Blocker

blocker = Blocker()
# x = left dataset name column, y = right dataset name column
result = blocker.block(
    x=left_names,
    y=right_names,
    ann="faiss",  # or "voyager", "annoy"
    text="model2vec",  # uses potion-base-8m
    n_neighbors=10,
)
# result.candidate_pairs → feed to Splink
```

- [ ] **Step 1: Add blockingpy to Dockerfile**
- [ ] **Step 2: Create embedding_blocking.py with Blocker wrapper**
- [ ] **Step 3: Integrate into Splink pipeline as pre-blocking step**
- [ ] **Step 4: Test with sample data**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat: embedding-based blocking for Splink via BlockingPy"
```

### Task 15: SemHash corporation dedup

**Files:**
- Create: `src/dagster_pipeline/sources/semantic_dedup.py`
- Modify: `infra/duckdb-server/Dockerfile` — add `semhash` pip package

```python
from semhash import SemHash

# Deduplicate corporation names
hasher = SemHash.from_records(corp_names, columns=["current_entity_name"])
duplicates = hasher.self_find_duplicates(threshold=0.9)
```

- [ ] **Step 1: Add semhash to Dockerfile**
- [ ] **Step 2: Create semantic_dedup.py Dagster asset**
- [ ] **Step 3: Run on nys_corporations (16M names)**
- [ ] **Step 4: Store dedup clusters in DuckLake**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat: semantic deduplication of corporation names via SemHash"
```

### Task 16: BERTopic complaint clustering

**Files:**
- Create: `src/dagster_pipeline/sources/complaint_clustering.py`
- Modify: `infra/duckdb-server/Dockerfile` — add `bertopic` pip package

```python
from bertopic import BERTopic

# NOTE: BERTopic runs as a batch Dagster asset, NOT in the MCP server.
# Using MiniLM here is fine — its 384-dim embeddings stay in the clustering pipeline.
# Only topic LABELS get stored in DuckLake, not raw vectors.
topic_model = BERTopic(embedding_model="all-MiniLM-L6-v2")
topics, probs = topic_model.fit_transform(complaint_texts)
```

- [ ] **Step 1: Add bertopic to Dockerfile**
- [ ] **Step 2: Create complaint_clustering.py Dagster asset**
- [ ] **Step 3: Run on 311 complaint descriptions (~3K unique)**
- [ ] **Step 4: Run on HPD violation descriptions (~1K unique)**
- [ ] **Step 5: Store topic assignments in DuckLake**
- [ ] **Step 6: Add `complaint_topics` MCP tool to query clusters**
- [ ] **Step 7: Commit**

```bash
git commit -m "feat: auto-categorize complaints via BERTopic semantic clustering"
```
