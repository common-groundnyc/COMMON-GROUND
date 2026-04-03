"""Token-based name routing against local name_token_routes table.

Uses the persistent local DuckDB (emb_conn) for instant lookups,
not DuckLake on S3. The table is synced from lake.foundation.name_tokens
at server startup (incremental — only rebuilds when data changes).
"""

import time


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
    ctx,
    name: str,
    source_filter: set[str] | None = None,
) -> set[str]:
    """Find which source tables contain a name, using local token index.

    Returns a set of source_table strings, e.g. {'housing.acris_parties', 'city_government.pluto'}.
    Queries the local emb_conn DuckDB (not DuckLake S3) for instant results.

    For multi-word names, returns tables that have ALL tokens (intersection).
    """
    emb_conn = ctx.lifespan_context.get("emb_conn")
    if not emb_conn:
        return set()

    tokens = tokenize_query(name)
    if not tokens:
        return set()

    t0 = time.time()

    try:
        if len(tokens) == 1:
            rows = emb_conn.execute(
                "SELECT DISTINCT source_table FROM name_token_routes WHERE token = ?",
                [tokens[0]],
            ).fetchall()
            sources = {r[0] for r in rows}

        else:
            # Multi-token: intersect — only tables that have ALL tokens
            sets = []
            for token in tokens:
                rows = emb_conn.execute(
                    "SELECT DISTINCT source_table FROM name_token_routes WHERE token = ?",
                    [token],
                ).fetchall()
                sets.append({r[0] for r in rows})
            sources = sets[0].intersection(*sets[1:]) if sets else set()

        if source_filter:
            sources = sources & source_filter

        elapsed = round((time.time() - t0) * 1000)
        if sources:
            print(f"[token_search] '{name}' → {len(sources)} tables ({elapsed}ms)",
                  flush=True)

        return sources

    except Exception as e:
        print(f"[token_search] error: {e}", flush=True)
        return set()
