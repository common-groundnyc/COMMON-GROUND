"""DuckDB extension loader with graceful fallback."""

CORE_EXTS = ["ducklake", "postgres", "spatial", "fts", "httpfs", "json", "lance"]

COMMUNITY_EXTS = [
    # Graph (phase 08 tools)
    "duckpgq",
    # Entity resolution
    "rapidfuzz",
    "splink_udfs",
    # Spatial
    "h3",
    "lindel",
    # Data quality & profiling
    "anofox_tabular",
    "anofox_forecast",
    "datasketches",
    "dqtest",
    # Performance
    "hashfuncs",
    # Vector search (lance replaces hnsw_acorn)
    # Available on 1.5.0
    "lsh",
    "finetype",
    "curl_httpfs",
]


def load_extensions(conn) -> dict[str, bool]:
    """Load all extensions, returning {name: loaded} status map."""
    status = {}

    for ext in CORE_EXTS:
        try:
            conn.execute(f"INSTALL {ext}")
            conn.execute(f"LOAD {ext}")
            status[ext] = True
        except Exception:
            try:
                conn.execute(f"LOAD {ext}")
                status[ext] = True
            except Exception as e:
                print(f"Warning: core extension {ext} unavailable: {e}", flush=True)
                status[ext] = False

    for ext in COMMUNITY_EXTS:
        try:
            conn.execute(f"INSTALL {ext} FROM community")
            conn.execute(f"LOAD {ext}")
            status[ext] = True
        except Exception as e:
            print(f"Warning: community extension {ext} unavailable: {e}", flush=True)
            status[ext] = False

    loaded = [k for k, v in status.items() if v]
    missing = [k for k, v in status.items() if not v]
    print(f"Extensions loaded: {len(loaded)}/{len(CORE_EXTS) + len(COMMUNITY_EXTS)} "
          f"({', '.join(missing) if missing else 'all OK'})", flush=True)

    return status
