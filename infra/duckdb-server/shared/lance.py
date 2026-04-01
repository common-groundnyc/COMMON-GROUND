"""Vector search helpers — name expansion and entity routing via hnsw_acorn."""

from shared.types import VS_NAME_DISTANCE


def vector_expand_names(ctx: object, search_term: str, threshold: float = VS_NAME_DISTANCE, k: int = 5) -> set:
    """Find similar entity names via HNSW vector search. Returns empty set on failure."""
    embed_fn = ctx.lifespan_context.get("embed_fn")
    emb_conn = ctx.lifespan_context.get("emb_conn")
    if not embed_fn or not emb_conn:
        return set()
    try:
        query_vec = embed_fn(search_term)
        rows = emb_conn.execute(
            "SELECT name, array_cosine_distance(embedding, ?::FLOAT[]) AS dist "
            "FROM entity_names ORDER BY dist LIMIT ?",
            [query_vec.tolist(), k],
        ).fetchall()
        result = set()
        for name, dist in rows:
            if dist < threshold:
                result.add(name.upper())
        result.discard(search_term.upper())
        return result
    except Exception:
        return set()


def lance_route_entity(ctx: object, search_term: str, k: int = 30) -> dict:
    """Search entity index to find which source tables contain matching names.

    Returns dict with:
      - 'sources': set of source_table names that have matches
      - 'matched_names': list of matched name strings
    Returns empty dict on failure (caller falls back to full scan).
    """
    embed_fn = ctx.lifespan_context.get("embed_fn")
    emb_conn = ctx.lifespan_context.get("emb_conn")
    if not embed_fn or not emb_conn:
        return {}
    try:
        query_vec = embed_fn(search_term)
        rows = emb_conn.execute(
            "SELECT name, sources, array_cosine_distance(embedding, ?::FLOAT[]) AS dist "
            "FROM entity_names WHERE dist < ? ORDER BY dist LIMIT ?",
            [query_vec.tolist(), VS_NAME_DISTANCE, k],
        ).fetchall()

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

        return {"sources": all_sources, "matched_names": matched_names}
    except Exception:
        return {}
