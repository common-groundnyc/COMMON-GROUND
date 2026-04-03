"""Token-based name search against foundation.name_tokens."""

import time

from shared.db import safe_query


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

    result: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        source = row[0]
        result.setdefault(source, []).append((row[1], row[2]))

    elapsed = round((time.time() - t0) * 1000)
    if rows:
        print(f"[token_search] '{name}' -> {len(rows)} matches across "
              f"{len(result)} tables ({elapsed}ms)", flush=True)

    return result
