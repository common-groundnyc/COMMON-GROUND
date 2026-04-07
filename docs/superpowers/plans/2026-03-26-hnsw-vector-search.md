# Vector Similarity Search (hnsw_acorn) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add vector similarity search to the MCP server using the hnsw_acorn DuckDB extension, enabling semantic data discovery, semantic text search, and fuzzy entity name matching across 1B+ rows of NYC open data.

**Architecture:** Pre-compute embeddings at server startup using ONNX Runtime with all-MiniLM-L6-v2 (384-dim vectors). Store embeddings in `main.*` tables alongside existing graph tables, with HNSW indexes for sub-millisecond KNN queries. Embedding cache persisted as Parquet files (same pattern as graph cache). ACORN-1 filtered search enables efficient per-source-type queries.

**Tech Stack:** DuckDB hnsw_acorn extension, ONNX Runtime (CPU), all-MiniLM-L6-v2 (INT8 quantized, 22MB), tokenizers (Rust), numpy

---

## Critical Research Findings

These findings from the research phase MUST be respected during implementation:

1. **HNSW indexes only work on `main.*` tables** — not on DuckLake attached tables. All embedding tables go in `main` schema (same as graph tables).
2. **Parameterized queries DO NOT use the HNSW index** — vectors must be inlined as literals in the SQL string (not as `?` params). Use `f"...{vec_literal}::FLOAT[384]..."` pattern.
3. **Persistence is experimental** — we treat embedding tables like graph tables: Parquet cache on disk, rebuild from lake if cache is stale. No reliance on DuckDB's native HNSW persistence.
4. **FLOAT[384] is the column type** — cast from Python list with `::FLOAT[384]`. Lists must have exactly 384 elements.
5. **Bulk load first, then create index** — faster than inserting into an indexed table.
6. **Docker base image is `python:3.12-slim`** — current pip deps are `duckdb==1.5.0`, `fastmcp[http]==3.1.1`, `posthog`. Adding `onnxruntime`, `tokenizers`, `numpy`, `huggingface_hub` is straightforward.
7. **hnsw_acorn is already in COMMUNITY_EXTS** (line 638 of mcp_server.py) — it will be installed/loaded at startup automatically.
8. **All DB operations MUST use `_db_lock`** — the server is multi-threaded. Every `db.execute()` call must be inside `with _db_lock:`.
9. **Bulk INSERT pattern**: For large embedding tables (300K+ rows), write embeddings to a temp Parquet file first, then `CREATE TABLE AS SELECT * FROM read_parquet(...)`. Row-by-row INSERT is too slow.
10. **Source filter params must be validated against allowlists** — never interpolate user input into SQL. Validate against known source values, then interpolate the validated string.

## File Structure

```
infra/duckdb-server/
├── Dockerfile                  # MODIFY: add onnxruntime, tokenizers, numpy, huggingface_hub
├── mcp_server.py               # MODIFY: embedding startup, new tools, tool upgrades
├── embedder.py                 # CREATE: ONNX embedding module (load model, embed, embed_batch)
├── download_model.py           # CREATE: one-time model download script (run at Docker build)
├── model/                      # CREATE: directory for ONNX model + tokenizer files
│   ├── onnx/model_int8.onnx    # Downloaded at build time
│   ├── tokenizer.json          # Downloaded at build time
│   ├── tokenizer_config.json   # Downloaded at build time
│   └── special_tokens_map.json # Downloaded at build time
└── tests/
    ├── test_embedder.py        # CREATE: unit tests for embedding module
    └── test_vector_tools.py    # CREATE: integration tests for vector search tools
```

## Embedding Inventory

| Table | Source | Rows to embed | What gets embedded | HNSW Filter Column |
|-------|--------|---------------|--------------------|--------------------|
| `main.catalog_embeddings` | SCHEMA_DESCRIPTIONS + table comments | ~300 | schema + table description text | `schema_name` |
| `main.description_embeddings` | 311 descriptors, restaurant violations, HPD descriptions, OATH charges | ~5,000 unique | category/description text | `source` |
| `main.entity_name_embeddings` | graph_owners, graph_corp_people, graph_campaign_donors, graph_tx_entities | ~300,000 | concatenated entity names | `source` |
| `main.building_vectors` | graph_buildings + violations + complaints | ~348,000 | numeric feature vector (no model) | `borough` |

Total HNSW index RAM (with RaBitQ): ~40MB. Total embedding compute at startup: ~3-5 min first run, <5 sec cached.

---

## Task 1: Embedding Module (`embedder.py`)

**Files:**
- Create: `infra/duckdb-server/embedder.py`
- Create: `infra/duckdb-server/download_model.py`
- Create: `infra/duckdb-server/tests/test_embedder.py`

- [ ] **Step 1: Write the failing test for single embedding**

```python
# tests/test_embedder.py
import numpy as np

def test_embed_returns_384_dim_vector():
    from embedder import create_embedder
    embed, _ = create_embedder("./model")
    vec = embed("hello world")
    assert isinstance(vec, np.ndarray)
    assert vec.shape == (384,)
    assert vec.dtype == np.float32

def test_embed_is_unit_normalized():
    from embedder import create_embedder
    embed, _ = create_embedder("./model")
    vec = embed("test sentence")
    norm = np.linalg.norm(vec)
    assert abs(norm - 1.0) < 1e-5

def test_embed_batch_returns_correct_shape():
    from embedder import create_embedder
    _, embed_batch = create_embedder("./model")
    vecs = embed_batch(["hello", "world", "test"])
    assert vecs.shape == (3, 384)
    assert vecs.dtype == np.float32

def test_similar_texts_have_high_cosine_similarity():
    from embedder import create_embedder
    embed, _ = create_embedder("./model")
    v1 = embed("rodent infestation in kitchen")
    v2 = embed("mice found in food preparation area")
    v3 = embed("property tax assessment appeal")
    sim_related = np.dot(v1, v2)
    sim_unrelated = np.dot(v1, v3)
    assert sim_related > sim_unrelated
    assert sim_related > 0.5

def test_embed_empty_string():
    from embedder import create_embedder
    embed, _ = create_embedder("./model")
    vec = embed("")
    assert vec.shape == (384,)

def test_embed_batch_empty_list():
    from embedder import create_embedder
    _, embed_batch = create_embedder("./model")
    vecs = embed_batch([])
    assert vecs.shape == (0, 384)

def test_vec_to_sql_literal():
    from embedder import vec_to_sql
    import numpy as np
    vec = np.array([0.1, 0.2, 0.3], dtype=np.float32)
    sql = vec_to_sql(vec)
    assert sql.startswith("[") and sql.endswith("::FLOAT[3]")
    assert "0.1" in sql and "0.2" in sql and "0.3" in sql
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd infra/duckdb-server && python -m pytest tests/test_embedder.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'embedder'`

- [ ] **Step 3: Write the model download script**

```python
# download_model.py
"""Download all-MiniLM-L6-v2 ONNX model + tokenizer. Run once at Docker build time."""
from huggingface_hub import hf_hub_download
from pathlib import Path

MODEL_DIR = Path(__file__).parent / "model"
REPO = "sentence-transformers/all-MiniLM-L6-v2"

def download():
    MODEL_DIR.mkdir(exist_ok=True)
    (MODEL_DIR / "onnx").mkdir(exist_ok=True)

    # Download ONNX model
    hf_hub_download(REPO, "onnx/model.onnx", local_dir=str(MODEL_DIR))

    # Download tokenizer files
    for f in ["tokenizer.json", "tokenizer_config.json", "special_tokens_map.json", "vocab.txt"]:
        hf_hub_download(REPO, f, local_dir=str(MODEL_DIR))

    # Quantize to INT8 (80MB → 22MB, <1% quality loss)
    from onnxruntime.quantization import quantize_dynamic, QuantType
    quantize_dynamic(
        str(MODEL_DIR / "onnx" / "model.onnx"),
        str(MODEL_DIR / "onnx" / "model_int8.onnx"),
        weight_type=QuantType.QUInt8,
    )
    print(f"Model downloaded and quantized to {MODEL_DIR / 'onnx' / 'model_int8.onnx'}")

if __name__ == "__main__":
    download()
```

- [ ] **Step 4: Write the embedder module**

```python
# embedder.py
"""Lightweight text embeddings via ONNX Runtime + tokenizers. No PyTorch."""
import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer
from pathlib import Path


def create_embedder(model_dir: str = "/app/model"):
    """Return (embed, embed_batch) functions. Load model once, call many times."""
    model_dir = Path(model_dir)

    # Prefer INT8 quantized model (22MB), fall back to FP32 (80MB)
    model_path = model_dir / "onnx" / "model_int8.onnx"
    if not model_path.exists():
        model_path = model_dir / "onnx" / "model.onnx"

    opts = ort.SessionOptions()
    opts.inter_op_num_threads = 2
    opts.intra_op_num_threads = 2
    opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    session = ort.InferenceSession(
        str(model_path), sess_options=opts, providers=["CPUExecutionProvider"]
    )

    tokenizer = Tokenizer.from_file(str(model_dir / "tokenizer.json"))
    tokenizer.enable_truncation(max_length=128)  # short texts, save compute
    tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")

    def _pool_and_normalize(token_embeddings, attention_mask):
        mask_expanded = attention_mask[:, :, np.newaxis].astype(np.float32)
        summed = np.sum(token_embeddings * mask_expanded, axis=1)
        counts = np.clip(mask_expanded.sum(axis=1), a_min=1e-9, a_max=None)
        pooled = summed / counts
        norms = np.linalg.norm(pooled, axis=1, keepdims=True)
        return pooled / np.clip(norms, a_min=1e-9, a_max=None)

    def embed(text: str) -> np.ndarray:
        """Return a 384-dim unit-normalized float32 vector."""
        encoded = tokenizer.encode(text)
        input_ids = np.array([encoded.ids], dtype=np.int64)
        attention_mask = np.array([encoded.attention_mask], dtype=np.int64)
        token_type_ids = np.array([encoded.type_ids], dtype=np.int64)
        outputs = session.run(None, {
            "input_ids": input_ids,
            "attention_mask": attention_mask,
            "token_type_ids": token_type_ids,
        })
        return _pool_and_normalize(outputs[0], attention_mask)[0]

    def embed_batch(texts: list[str], batch_size: int = 64) -> np.ndarray:
        """Return (N, 384) unit-normalized float32 array."""
        if not texts:
            return np.empty((0, 384), dtype=np.float32)
        all_vecs = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            encoded = tokenizer.encode_batch(batch)
            input_ids = np.array([e.ids for e in encoded], dtype=np.int64)
            attention_mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)
            token_type_ids = np.array([e.type_ids for e in encoded], dtype=np.int64)
            outputs = session.run(None, {
                "input_ids": input_ids,
                "attention_mask": attention_mask,
                "token_type_ids": token_type_ids,
            })
            all_vecs.append(_pool_and_normalize(outputs[0], attention_mask))
        return np.vstack(all_vecs).astype(np.float32)

    return embed, embed_batch


def vec_to_sql(vec: np.ndarray) -> str:
    """Convert numpy vector to DuckDB FLOAT[N] literal for HNSW index use.

    CRITICAL: hnsw_acorn only uses the index when the vector is a SQL literal,
    NOT a parameterized value (?). Always inline via this function.
    """
    dim = vec.shape[0]
    csv = ",".join(f"{v:.6g}" for v in vec)
    return f"[{csv}]::FLOAT[{dim}]"
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd infra/duckdb-server && pip install onnxruntime tokenizers numpy huggingface_hub && python download_model.py && python -m pytest tests/test_embedder.py -v`
Expected: All 6 tests PASS

- [ ] **Step 6: Commit**

```bash
git add infra/duckdb-server/embedder.py infra/duckdb-server/download_model.py infra/duckdb-server/tests/test_embedder.py
git commit -m "feat(mcp): add ONNX embedding module for vector similarity search"
```

---

## Task 2: Docker Image Update

**Files:**
- Modify: `infra/duckdb-server/Dockerfile`

- [ ] **Step 1: Update Dockerfile to install ONNX deps and download model**

```dockerfile
FROM python:3.12-slim

# anofox_forecast needs OpenSSL 1.1 (not available in Bookworm) — pull from Bullseye
RUN echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list.d/bullseye.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends -t bullseye libssl1.1 && \
    apt-get install -y --no-install-recommends nginx && \
    rm /etc/apt/sources.list.d/bullseye.list && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    duckdb==1.5.0 \
    "fastmcp[http]==3.1.1" \
    posthog \
    onnxruntime \
    tokenizers \
    numpy \
    pyarrow \
    huggingface_hub

WORKDIR /app
COPY *.py .
COPY start.sh .
RUN chmod +x start.sh

# Download and quantize embedding model at build time (~22MB INT8)
RUN python download_model.py

EXPOSE 4213 9999

ENV FASTMCP_STATELESS_HTTP=true

CMD ["./start.sh"]
```

- [ ] **Step 2: Build the image locally to verify**

Run: `cd infra/duckdb-server && docker build -t duckdb-server:vector-test .`
Expected: Build succeeds, model download + quantization completes

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/Dockerfile
git commit -m "feat(docker): add onnxruntime and embedding model to MCP server image"
```

---

## Task 3: Embedding Cache Infrastructure

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (lines ~955-1020 area — after graph cache, before PostHog)

This task adds the embedding cache pattern (mirrors graph cache) and the startup embedding pipeline.

- [ ] **Step 1: Write the embedding cache constants and helpers**

Add after the graph cache section (after `_save_graph_to_cache`), before the `try: if _graph_cache_fresh()` block. These go at module level inside `app_lifespan`:

```python
    # --- Embedding cache (same pattern as graph cache) ---
    EMBEDDING_CACHE_DIR = "/data/common-ground/embedding-cache"
    EMBEDDING_TABLES = [
        "catalog_embeddings",
        "description_embeddings",
        "entity_name_embeddings",
        "building_vectors",
    ]

    def _embedding_cache_fresh():
        """Check if embedding Parquet cache exists and is < 24 hours old."""
        import pathlib
        cache = pathlib.Path(EMBEDDING_CACHE_DIR)
        marker = cache / "_built_at.txt"
        if not marker.exists():
            return False
        try:
            built_ts = float(marker.read_text().strip())
            age_hours = (time.time() - built_ts) / 3600
            return age_hours < 24 and all(
                (cache / f"{t}.parquet").exists() for t in EMBEDDING_TABLES
            )
        except (ValueError, OSError):
            return False

    def _load_embeddings_from_cache(conn):
        """Load embedding tables from Parquet cache."""
        for t in EMBEDDING_TABLES:
            path = f"{EMBEDDING_CACHE_DIR}/{t}.parquet"
            conn.execute(
                f"CREATE OR REPLACE TABLE main.{t} AS SELECT * FROM read_parquet('{path}')"
            )

    def _save_embeddings_to_cache(conn):
        """Export embedding tables to Parquet for fast restart."""
        import pathlib
        cache = pathlib.Path(EMBEDDING_CACHE_DIR)
        cache.mkdir(parents=True, exist_ok=True)
        for t in EMBEDDING_TABLES:
            conn.execute(
                f"COPY main.{t} TO '{EMBEDDING_CACHE_DIR}/{t}.parquet' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        (cache / "_built_at.txt").write_text(str(time.time()))

    def _create_hnsw_indexes(conn):
        """Create HNSW indexes on embedding tables. Run after loading/building."""
        try:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS catalog_emb_idx
                ON main.catalog_embeddings USING HNSW (embedding)
                WITH (metric = 'cosine')
            """)
        except Exception as e:
            print(f"Warning: catalog HNSW index failed: {e}", flush=True)
        try:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS desc_emb_idx
                ON main.description_embeddings USING HNSW (embedding)
                WITH (metric = 'cosine')
            """)
        except Exception as e:
            print(f"Warning: description HNSW index failed: {e}", flush=True)
        try:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS name_emb_idx
                ON main.entity_name_embeddings USING HNSW (embedding)
                WITH (metric = 'cosine')
            """)
        except Exception as e:
            print(f"Warning: entity name HNSW index failed: {e}", flush=True)
        try:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS building_vec_idx
                ON main.building_vectors USING HNSW (features)
                WITH (metric = 'l2sq')
            """)
        except Exception as e:
            print(f"Warning: building vector HNSW index failed: {e}", flush=True)
```

- [ ] **Step 2: Write the embedding builder functions**

These compute embeddings from graph tables + lake data. Add right after the cache helpers:

```python
    def _build_catalog_embeddings(conn, embed_batch):
        """Embed table descriptions for semantic data discovery."""
        texts, metas = [], []
        try:
            catalog = conn.execute(
                "SELECT schema_name, table_name, comment FROM ducklake_table_info() "
                "WHERE schema_name NOT LIKE '%staging%'"
            ).fetchall()
        except Exception:
            catalog = []  # ducklake_table_info may not exist in all DuckLake versions
        for schema_name in SCHEMA_DESCRIPTIONS:
            desc = SCHEMA_DESCRIPTIONS[schema_name]
            texts.append(f"{schema_name}: {desc}")
            metas.append((schema_name, "__schema__", desc))
        for row in catalog:
            s, t, comment = row[0], row[1], row[2] or ""
            if comment:
                texts.append(f"{s}.{t}: {comment}")
                metas.append((s, t, comment))

        if not texts:
            return

        vecs = embed_batch(texts)
        conn.execute("CREATE OR REPLACE TABLE main.catalog_embeddings ("
                     "schema_name VARCHAR, table_name VARCHAR, "
                     "description VARCHAR, embedding FLOAT[384])")
        for i, (s, t, d) in enumerate(metas):
            vec_sql = "[" + ",".join(f"{v:.6g}" for v in vecs[i]) + "]::FLOAT[384]"
            conn.execute(
                f"INSERT INTO main.catalog_embeddings VALUES (?, ?, ?, {vec_sql})",
                [s, t, d]
            )
        count = conn.execute("SELECT COUNT(*) FROM main.catalog_embeddings").fetchone()[0]
        print(f"  Catalog embeddings: {count} entries", flush=True)

    def _build_description_embeddings(conn, embed_batch):
        """Embed unique violation/complaint description categories."""
        texts, metas = [], []
        sources = {
            "311": ("lake.social_services.n311_service_requests",
                    "SELECT DISTINCT complaint_type || ': ' || COALESCE(descriptor, '') "
                    "FROM lake.social_services.n311_service_requests "
                    "WHERE complaint_type IS NOT NULL LIMIT 3000"),
            "restaurant": ("lake.health.restaurant_inspections",
                          "SELECT DISTINCT violation_description "
                          "FROM lake.health.restaurant_inspections "
                          "WHERE violation_description IS NOT NULL LIMIT 500"),
            "hpd": ("lake.housing.hpd_violations",
                   "SELECT DISTINCT novdescription "
                   "FROM lake.housing.hpd_violations "
                   "WHERE novdescription IS NOT NULL LIMIT 1000"),
            "oath": ("lake.city_government.oath_hearings",
                    "SELECT DISTINCT charge_1_code_description "
                    "FROM lake.city_government.oath_hearings "
                    "WHERE charge_1_code_description IS NOT NULL LIMIT 500"),
        }
        for source, (_, sql) in sources.items():
            try:
                rows = conn.execute(sql).fetchall()
                for r in rows:
                    if r[0] and len(r[0].strip()) > 3:
                        texts.append(r[0].strip())
                        metas.append((source, r[0].strip()))
            except Exception as e:
                print(f"  Warning: {source} descriptions unavailable: {e}", flush=True)

        if not texts:
            return

        vecs = embed_batch(texts)
        conn.execute("CREATE OR REPLACE TABLE main.description_embeddings ("
                     "source VARCHAR, description VARCHAR, embedding FLOAT[384])")
        for i, (src, desc) in enumerate(metas):
            vec_sql = "[" + ",".join(f"{v:.6g}" for v in vecs[i]) + "]::FLOAT[384]"
            conn.execute(
                f"INSERT INTO main.description_embeddings VALUES (?, ?, {vec_sql})",
                [src, desc]
            )
        count = conn.execute("SELECT COUNT(*) FROM main.description_embeddings").fetchone()[0]
        print(f"  Description embeddings: {count} entries", flush=True)

    def _build_entity_name_embeddings(conn, embed_batch):
        """Embed entity names from graph tables for fuzzy matching."""
        texts, metas = [], []
        name_sources = [
            ("owner", "SELECT DISTINCT owner_name FROM main.graph_owners "
                      "WHERE owner_name IS NOT NULL AND LENGTH(owner_name) > 2"),
            ("corp_officer", "SELECT DISTINCT person_name FROM main.graph_corp_people "
                            "WHERE person_name IS NOT NULL AND LENGTH(person_name) > 2"),
            ("donor", "SELECT DISTINCT donor_name FROM main.graph_campaign_donors "
                     "WHERE donor_name IS NOT NULL AND LENGTH(donor_name) > 2"),
            ("tx_party", "SELECT DISTINCT entity_name FROM main.graph_tx_entities "
                        "WHERE entity_name IS NOT NULL AND LENGTH(entity_name) > 2"),
        ]
        for source, sql in name_sources:
            try:
                rows = conn.execute(sql).fetchall()
                for r in rows:
                    texts.append(r[0])
                    metas.append((source, r[0]))
            except Exception as e:
                print(f"  Warning: {source} names unavailable: {e}", flush=True)

        if not texts:
            return

        print(f"  Embedding {len(texts):,} entity names...", flush=True)
        vecs = embed_batch(texts)

        # Bulk load via temp Parquet — row-by-row INSERT is too slow for 300K+ rows
        import pyarrow as pa, pyarrow.parquet as pq
        import os
        table = pa.table({
            "source": [m[0] for m in metas],
            "name": [m[1] for m in metas],
            "embedding": [v.tolist() for v in vecs],
        })
        tmp_path = "/tmp/_entity_embeddings.parquet"
        pq.write_table(table, tmp_path)
        conn.execute(f"""
            CREATE OR REPLACE TABLE main.entity_name_embeddings AS
            SELECT source, name, embedding::FLOAT[384] AS embedding
            FROM read_parquet('{tmp_path}')
        """)
        os.unlink(tmp_path)
        count = conn.execute("SELECT COUNT(*) FROM main.entity_name_embeddings").fetchone()[0]
        print(f"  Entity name embeddings: {count:,} entries", flush=True)

    def _build_building_vectors(conn):
        """Build numeric feature vectors for building similarity (no embedding model needed)."""
        try:
            conn.execute("""
                CREATE OR REPLACE TABLE main.building_vectors AS
                WITH stats AS (
                    SELECT
                        b.bbl,
                        SUBSTRING(b.bbl, 1, 1) AS borough,
                        COALESCE(TRY_CAST(b.stories AS FLOAT), 0) AS stories,
                        COALESCE(TRY_CAST(b.units AS FLOAT), 0) AS units,
                        COALESCE(TRY_CAST(b.year_built AS FLOAT), 1900) AS year_built,
                        COUNT(DISTINCT v.violationid) AS violations,
                        COUNT(DISTINCT v.violationid) FILTER (WHERE v.status = 'Open') AS open_violations,
                    FROM main.graph_buildings b
                    LEFT JOIN main.graph_violations v ON b.bbl = v.bbl
                    GROUP BY b.bbl, b.stories, b.units, b.year_built
                ),
                normalized AS (
                    SELECT
                        bbl, borough,
                        -- Normalize each feature to 0-1 range
                        stories / NULLIF(MAX(stories) OVER (), 0) AS n_stories,
                        units / NULLIF(MAX(units) OVER (), 0) AS n_units,
                        (year_built - 1800) / 226.0 AS n_age,
                        violations / NULLIF(MAX(violations) OVER (), 0) AS n_violations,
                        open_violations / NULLIF(MAX(open_violations) OVER (), 0) AS n_open_violations,
                        CASE WHEN units > 0 THEN violations / units ELSE 0 END /
                            NULLIF(MAX(CASE WHEN units > 0 THEN violations / units ELSE 0 END) OVER (), 0)
                            AS n_violations_per_unit,
                    FROM stats
                )
                SELECT
                    bbl, borough,
                    [COALESCE(n_stories, 0), COALESCE(n_units, 0), COALESCE(n_age, 0),
                     COALESCE(n_violations, 0), COALESCE(n_open_violations, 0),
                     COALESCE(n_violations_per_unit, 0)]::FLOAT[6] AS features
                FROM normalized
            """)
            count = conn.execute("SELECT COUNT(*) FROM main.building_vectors").fetchone()[0]
            print(f"  Building vectors: {count:,} entries", flush=True)
        except Exception as e:
            print(f"  Warning: building vectors failed: {e}", flush=True)
            conn.execute("CREATE TABLE main.building_vectors (bbl VARCHAR, borough VARCHAR, features FLOAT[6])")
```

- [ ] **Step 3: Wire embedding pipeline into app_lifespan**

Add after the explorations block (line ~2431) and before the `yield`:

```python
    # --- Vector embeddings for semantic search ---
    embed_fn = None
    try:
        from embedder import create_embedder
        embed_fn, embed_batch_fn = create_embedder("/app/model")
        print("Embedding model loaded", flush=True)

        if _embedding_cache_fresh():
            print("Loading embeddings from Parquet cache...", flush=True)
            _load_embeddings_from_cache(conn)
        else:
            print("Building embeddings (no cache or stale)...", flush=True)
            _build_catalog_embeddings(conn, embed_batch_fn)
            _build_description_embeddings(conn, embed_batch_fn)
            if graph_ready:
                _build_entity_name_embeddings(conn, embed_batch_fn)
                _build_building_vectors(conn)
            _save_embeddings_to_cache(conn)
            print("Embeddings cached to Parquet", flush=True)

        _create_hnsw_indexes(conn)
        print("HNSW indexes created", flush=True)
    except Exception as e:
        print(f"Warning: vector search unavailable: {e}", flush=True)
        embed_fn = None
```

- [ ] **Step 4: Add embed_fn to lifespan yield**

```python
    try:
        yield {
            "db": conn, "catalog": catalog,
            "graph_ready": graph_ready,
            "marriage_parquet": MARRIAGE_PARQUET if marriage_available else None,
            "posthog_enabled": bool(ph_key),
            "explorations": explorations,
            "embed_fn": embed_fn,  # NEW
        }
```

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add embedding cache infrastructure and startup pipeline"
```

---

## Task 4: New Tool — `semantic_search`

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (add new tool after `text_search`)
- Create: `infra/duckdb-server/tests/test_vector_tools.py`

- [ ] **Step 1: Write the semantic_search tool**

Add after the existing `text_search` function:

```python
@mcp.tool(annotations=READONLY, tags={"discovery"})
def semantic_search(
    query: Annotated[str, Field(description="Natural language search query. Examples: 'pest problems in restaurants', 'landlord neglect complaints', 'noise from construction'")],
    ctx: Context,
    source: Annotated[str, Field(description="Filter by data source: '311', 'restaurant', 'hpd', 'oath', or 'all'")] = "all",
    limit: Annotated[int, Field(description="Max results (1-50). Default: 15", ge=1, le=50)] = 15,
) -> ToolResult:
    """Semantic similarity search across NYC complaint and violation descriptions. Unlike text_search (keyword matching), this finds conceptually similar records even when different words are used. 'pest problems' finds 'mice', 'roaches', 'flies'. 'building neglect' finds 'peeling paint', 'broken elevator', 'no heat'. Uses vector embeddings + HNSW index. Sources: 311 complaints, restaurant violations, HPD violations, OATH hearings. For keyword search, use text_search(). For table discovery, use data_catalog()."""
    embed_fn = ctx.lifespan_context.get("embed_fn")
    if embed_fn is None:
        raise ToolError("Semantic search unavailable — embedding model not loaded.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    VALID_DESC_SOURCES = frozenset({"311", "restaurant", "hpd", "oath"})
    if source != "all" and source not in VALID_DESC_SOURCES:
        raise ToolError(f"Invalid source. Must be one of: {', '.join(sorted(VALID_DESC_SOURCES))}, or 'all'.")

    from embedder import vec_to_sql
    query_vec = embed_fn(query.strip())
    vec_literal = vec_to_sql(query_vec)

    source_filter = ""
    if source != "all":
        source_filter = f"WHERE source = '{source}'"  # safe: validated against allowlist

    sql = f"""
        SELECT source, description,
               array_cosine_distance(embedding, {vec_literal}) AS distance
        FROM main.description_embeddings
        {source_filter}
        ORDER BY array_cosine_distance(embedding, {vec_literal})
        LIMIT {limit}
    """

    with _db_lock:
        rows = db.execute(sql).fetchall()

    if not rows:
        return ToolResult(content=f"No semantic matches for '{query}' in source '{source}'.")

    lines = [f"SEMANTIC SEARCH — '{query}'", f"Found {len(rows)} matching description categories", "=" * 50]
    for i, (src, desc, dist) in enumerate(rows, 1):
        similarity = max(0, 1 - dist)  # cosine distance → similarity
        lines.append(f"  {i}. [{src}] (similarity: {similarity:.1%}) {desc}")

    lines.append(f"\nThese are description CATEGORIES. To see actual records, use sql_query() to query the source table with these descriptions.")
    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"query": query, "results": [{"source": r[0], "description": r[1], "similarity": round(1 - r[2], 4)} for r in rows]},
        meta={"query": query, "source_filter": source, "query_time_ms": elapsed},
    )
```

- [ ] **Step 2: Add to ALWAYS_VISIBLE list**

In the `ALWAYS_VISIBLE` array, add `"semantic_search"` after `"suggest_explorations"`.

- [ ] **Step 3: Update INSTRUCTIONS routing**

Add to the routing block:
```
* SEMANTIC SEARCH by concept → semantic_search (finds similar descriptions even with different words)
```

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add semantic_search tool — vector similarity across complaint/violation descriptions"
```

---

## Task 5: New Tool — `fuzzy_entity_search`

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Write the fuzzy_entity_search tool**

Add after `entity_xray`:

```python
@mcp.tool(annotations=READONLY, tags={"entity"})
def fuzzy_entity_search(
    name: NAME,
    ctx: Context,
    source: Annotated[str, Field(description="Filter by source: 'owner', 'corp_officer', 'donor', 'tx_party', or 'all'")] = "all",
    limit: Annotated[int, Field(description="Max results (1-50). Default: 20", ge=1, le=50)] = 20,
) -> ToolResult:
    """Find entities with similar names using vector similarity. Catches variations that exact search misses: 'Bob Smith' finds 'Robert Smith', 'R. Smith', 'Smith, Robert J.' Use this when entity_xray returns too few results or when you suspect name variations exist. Sources: property owners, corporate officers, campaign donors, ACRIS transaction parties. Follow up with entity_xray(name) for the full X-ray of a matched entity."""
    embed_fn = ctx.lifespan_context.get("embed_fn")
    if embed_fn is None:
        raise ToolError("Fuzzy entity search unavailable — embedding model not loaded.")

    VALID_ENTITY_SOURCES = frozenset({"owner", "corp_officer", "donor", "tx_party"})
    if source != "all" and source not in VALID_ENTITY_SOURCES:
        raise ToolError(f"Invalid source. Must be one of: {', '.join(sorted(VALID_ENTITY_SOURCES))}, or 'all'.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    from embedder import vec_to_sql
    query_vec = embed_fn(name.strip())
    vec_literal = vec_to_sql(query_vec)

    source_filter = ""
    if source != "all":
        source_filter = f"WHERE source = '{source}'"  # safe: validated against allowlist

    sql = f"""
        SELECT source, name,
               array_cosine_distance(embedding, {vec_literal}) AS distance
        FROM main.entity_name_embeddings
        {source_filter}
        ORDER BY array_cosine_distance(embedding, {vec_literal})
        LIMIT {limit}
    """

    with _db_lock:
        rows = db.execute(sql).fetchall()

    if not rows:
        return ToolResult(content=f"No similar entities found for '{name}'.")

    lines = [f"FUZZY ENTITY SEARCH — '{name}'", f"Found {len(rows)} similar names", "=" * 50]
    for i, (src, matched_name, dist) in enumerate(rows, 1):
        similarity = max(0, 1 - dist)
        lines.append(f"  {i}. [{src}] {matched_name} (similarity: {similarity:.1%})")

    lines.append(f"\nUse entity_xray(name) to get the full X-ray for any matched entity.")
    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"query": name, "results": [{"source": r[0], "name": r[1], "similarity": round(1 - r[2], 4)} for r in rows]},
        meta={"query": name, "source_filter": source, "query_time_ms": elapsed},
    )
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add fuzzy_entity_search tool — vector similarity on entity names"
```

---

## Task 6: New Tool — `similar_buildings`

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Write the similar_buildings tool**

Add after `building_story`:

```python
@mcp.tool(annotations=READONLY, tags={"housing"})
def similar_buildings(
    bbl: BBL,
    ctx: Context,
    limit: Annotated[int, Field(description="Number of similar buildings (1-30). Default: 10", ge=1, le=30)] = 10,
) -> ToolResult:
    """Find buildings most similar to the given BBL based on physical characteristics and violation patterns — stories, units, age, violation rate, open violations. Useful for comparison, risk assessment, and pattern detection. Returns similar buildings with their BBLs for follow-up with building_profile() or landlord_watchdog()."""
    if not re.match(r"^\d{10}$", bbl):
        raise ToolError("BBL must be 10 digits: borough(1) + block(5) + lot(4)")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Get the target building's feature vector
    with _db_lock:
        target = db.execute(
            "SELECT features FROM main.building_vectors WHERE bbl = ?", [bbl]
        ).fetchone()
    if not target:
        raise ToolError(f"No feature vector for BBL {bbl}. Building may not be in the dataset.")

    import numpy as np
    vec_literal = "[" + ",".join(f"{v:.6g}" for v in target[0]) + "]::FLOAT[6]"

    # Inline BBL exclusion (not parameterized) to avoid disabling HNSW index
    # BBL is already validated as 10 digits above, safe to interpolate
    sql = f"""
        SELECT bv.bbl, bv.borough,
               array_distance(bv.features, {vec_literal}) AS distance,
               b.housenumber, b.streetname, b.zip, b.stories, b.units, b.year_built
        FROM main.building_vectors bv
        LEFT JOIN main.graph_buildings b ON bv.bbl = b.bbl
        WHERE bv.bbl != '{bbl}'
        ORDER BY array_distance(bv.features, {vec_literal})
        LIMIT {limit}
    """

    with _db_lock:
        rows = db.execute(sql).fetchall()

    if not rows:
        return ToolResult(content=f"No similar buildings found for {bbl}.")

    lines = [f"SIMILAR BUILDINGS — {bbl}", f"Top {len(rows)} most similar by building profile + violations", "=" * 55]
    for i, r in enumerate(rows, 1):
        addr = f"{r[3] or ''} {r[4] or ''}".strip() or "Unknown"
        lines.append(f"  {i}. {r[0]} — {addr}, {r[5] or '?'}")
        lines.append(f"     {r[6] or '?'} stories, {r[7] or '?'} units, built {r[8] or '?'}")

    lines.append(f"\nUse building_profile(bbl) or landlord_watchdog(bbl) for any building above.")
    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        meta={"target_bbl": bbl, "query_time_ms": elapsed},
    )
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add similar_buildings tool — KNN on building feature vectors"
```

---

## Task 7: Upgrade `data_catalog` with Semantic Fallback

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (the `data_catalog` function, around line 2820)

- [ ] **Step 1: Add semantic fallback when rapidfuzz returns low-quality results**

After the existing `if not rows:` check (line ~2832), before the `return make_result(...)`, add a semantic search pass:

```python
    # Semantic fallback: if rapidfuzz found < 3 tables or best score is low,
    # also check catalog embeddings for conceptual matches
    embed_fn = ctx.lifespan_context.get("embed_fn")
    semantic_matches = []
    if embed_fn and (not rows or len(rows) < 3):
        try:
            from embedder import vec_to_sql
            query_vec = embed_fn(kw)
            vec_literal = vec_to_sql(query_vec)
            sem_sql = f"""
                SELECT schema_name, table_name, description,
                       array_cosine_distance(embedding, {vec_literal}) AS distance
                FROM main.catalog_embeddings
                ORDER BY array_cosine_distance(embedding, {vec_literal})
                LIMIT 5
            """
            with _db_lock:
                sem_rows = db.execute(sem_sql).fetchall()
            # Only include if similarity > 0.3 (distance < 0.7)
            semantic_matches = [r for r in sem_rows if r[3] < 0.7]
        except Exception:
            pass
```

Then if semantic_matches exist and no rapidfuzz rows, build the response from semantic results. If both exist, append semantic suggestions at the bottom.

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add semantic fallback to data_catalog for conceptual discovery"
```

---

## Task 8: Upgrade `entity_xray` with Vector Pre-Pass

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (the `entity_xray` function, around line 8875)

- [ ] **Step 1: Add name variant expansion before LIKE queries**

At the top of entity_xray, after `words = [w for w in search.split()...]`, add:

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
                if dist < 0.4:  # high similarity threshold
                    extra_names.add(matched_name.upper())
        except Exception:
            pass
```

Then for each table query, add OR clauses for the extra names if they exist. The simplest approach: for each extra name, add `OR UPPER(name) LIKE '%{extra_name}%'`. Use proper parameterization.

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add vector name expansion to entity_xray for fuzzy matching"
```

---

## Task 9: Upgrade `text_search` with Semantic Primary

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (the `text_search` function)

- [ ] **Step 1: Add semantic results alongside ILIKE results**

At the top of text_search, after the ILIKE queries, add semantic search results if available:

```python
    # Semantic enhancement: find conceptually similar descriptions
    embed_fn = ctx.lifespan_context.get("embed_fn")
    if embed_fn and all_results:
        try:
            from embedder import vec_to_sql
            query_vec = embed_fn(query.strip())
            vec_literal = vec_to_sql(query_vec)
            # Map corpus to source filter
            source_filter = ""
            if corpus == "financial":
                source_filter = "WHERE source = 'cfpb'"
            elif corpus == "restaurants":
                source_filter = "WHERE source = 'restaurant'"
            sem_sql = f"""
                SELECT source, description,
                       array_cosine_distance(embedding, {vec_literal}) AS distance
                FROM main.description_embeddings
                {source_filter}
                ORDER BY array_cosine_distance(embedding, {vec_literal})
                LIMIT 5
            """
            with _db_lock:
                sem_rows = db.execute(sem_sql).fetchall()
            # Append as "Related categories" section
        except Exception:
            pass
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): enhance text_search with semantic similarity suggestions"
```

---

## Task 10: Deploy and Smoke Test

**Files:**
- No new files — deployment and testing

- [ ] **Step 1: Build the Docker image**

```bash
cd ~/Desktop/dagster-pipeline
docker compose -f infra/docker-compose.yml build duckdb-server
```

- [ ] **Step 2: Deploy to Hetzner**

```bash
cd ~/Desktop/dagster-pipeline
./deploy.sh  # rsync + docker compose up
```

- [ ] **Step 3: Smoke test — semantic_search**

Via MCP: `semantic_search("pest problems in restaurants")`
Expected: Returns restaurant violation descriptions about mice, roaches, flies with similarity scores

- [ ] **Step 4: Smoke test — fuzzy_entity_search**

Via MCP: `fuzzy_entity_search("Barton Perlbinder")`
Expected: Returns name variants like "PERLBINDER, BARTON M", "BARTON PERLBINDER"

- [ ] **Step 5: Smoke test — similar_buildings**

Via MCP: `similar_buildings("1000670001")`
Expected: Returns 10 similar buildings by feature vector

- [ ] **Step 6: Smoke test — data_catalog semantic**

Via MCP: `data_catalog("housing discrimination")`
Expected: Returns HMDA, eviction, fair housing tables via semantic fallback

- [ ] **Step 7: Smoke test — entity_xray with vector expansion**

Via MCP: `entity_xray("Bob Perlbinder")`
Expected: Finds results for "Barton Perlbinder" via vector name expansion

- [ ] **Step 8: Commit deploy tag**

```bash
git tag v2.1-vector-search
```
