"""Embedding-based blocking for Splink entity resolution.

BlockingPy generates candidate pairs using vector similarity instead of
phonetic blocking (Soundex/Metaphone). Catches name variations that
rule-based blocking misses: "KUSHNER COMPANIES LLC" ↔ "KUSHNER COS".

Usage: import and call generate_blocking_pairs() before Splink comparison.
"""
from blockingpy import Blocker


def generate_blocking_pairs(left_names: list[str], right_names: list[str], n_neighbors: int = 10) -> list[tuple[int, int]]:
    """Generate candidate pairs using embedding similarity.

    Args:
        left_names: Names from first dataset
        right_names: Names from second dataset
        n_neighbors: Number of nearest neighbors per record

    Returns:
        List of (left_idx, right_idx) candidate pairs
    """
    blocker = Blocker()
    result = blocker.block(
        x=left_names,
        y=right_names,
        ann="faiss",
        text="model2vec",  # uses potion-base-8m embeddings
        n_neighbors=n_neighbors,
    )
    return list(zip(result.x_indices, result.y_indices))
