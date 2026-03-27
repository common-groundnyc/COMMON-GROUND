"""Semantic deduplication of corporation names using SemHash.

Finds near-duplicate corporation records that string matching misses:
"ABC PROPERTIES LLC" ↔ "A.B.C. PROPERTIES L.L.C."

Runs as a Dagster asset over nys_corporations data.
"""


def find_duplicate_corps(names: list[str], threshold: float = 0.9) -> list[tuple[int, int, float]]:
    """Find semantically duplicate corporation names.

    Args:
        names: List of corporation names
        threshold: Cosine similarity threshold (0.9 = very similar)

    Returns:
        List of (idx1, idx2, similarity) tuples
    """
    from semhash import SemHash

    hasher = SemHash.from_records(
        [{"name": n} for n in names],
        columns=["name"],
    )
    duplicates = hasher.self_find_duplicates(threshold=threshold)
    return [(d.idx, d.duplicate_idx, d.score) for d in duplicates]
