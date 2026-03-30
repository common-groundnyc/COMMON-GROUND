"""Lance vector search helpers — name expansion and entity routing."""

from shared.types import LANCE_DIR, LANCE_NAME_DISTANCE


def vector_expand_names(ctx: object, search_term: str, threshold: float = LANCE_NAME_DISTANCE, k: int = 5) -> set:
    """Find similar entity names via Lance vector search. Returns empty set on failure."""
    embed_fn = ctx.lifespan_context.get("embed_fn")
    if not embed_fn:
        return set()
    try:
        from embedder import vec_to_sql
        query_vec = embed_fn(search_term)
        vec_literal = vec_to_sql(query_vec)
        sql = f"""
            SELECT name, _distance AS dist
            FROM lance_vector_search('{LANCE_DIR}/entity_name_embeddings.lance', 'embedding', {vec_literal}, k={k})
            ORDER BY _distance ASC
        """
        pool = ctx.lifespan_context["pool"]
        with pool.cursor() as cur:
            rows = cur.execute(sql).fetchall()
        result = set()
        for name, dist in rows:
            if dist < threshold:
                result.add(name.upper())
        result.discard(search_term.upper())
        return result
    except Exception:
        return set()


def lance_route_entity(ctx: object, search_term: str, k: int = 30) -> dict:
    """Search Lance entity index to find which source tables contain matching names.

    Returns dict with:
      - 'sources': set of source_table names that have matches
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

        return {"sources": all_sources, "matched_names": matched_names}
    except Exception:
        return {}
