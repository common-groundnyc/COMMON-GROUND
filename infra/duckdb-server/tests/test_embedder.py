import numpy as np
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from embedder import create_embedder, vec_to_sql

MODEL_DIR = Path(__file__).parent.parent / "model"


@pytest.fixture(scope="module")
def embedder():
    embed, embed_batch = create_embedder(MODEL_DIR)
    return embed, embed_batch


def test_embed_returns_384_dim_vector(embedder):
    embed, _ = embedder
    vec = embed("hello world")
    assert vec.shape == (384,)
    assert vec.dtype == np.float32


def test_embed_is_unit_normalized(embedder):
    embed, _ = embedder
    vec = embed("test sentence for normalization")
    norm = np.linalg.norm(vec)
    assert abs(norm - 1.0) < 1e-5


def test_embed_batch_returns_correct_shape(embedder):
    _, embed_batch = embedder
    texts = ["first sentence", "second sentence", "third sentence"]
    result = embed_batch(texts)
    assert result.shape == (3, 384)
    assert result.dtype == np.float32


def test_similar_texts_have_high_cosine_similarity(embedder):
    embed, _ = embedder
    v1 = embed("rodent infestation in kitchen")
    v2 = embed("mice found in food prep area")
    v3 = embed("property tax assessment appeal")

    sim_related = float(np.dot(v1, v2))
    sim_unrelated = float(np.dot(v1, v3))
    assert sim_related > sim_unrelated, (
        f"Expected related texts to be more similar: {sim_related:.4f} vs {sim_unrelated:.4f}"
    )


def test_embed_empty_string(embedder):
    embed, _ = embedder
    vec = embed("")
    assert vec.shape == (384,)


def test_embed_batch_empty_list(embedder):
    _, embed_batch = embedder
    result = embed_batch([])
    assert result.shape == (0, 384)
    assert result.dtype == np.float32


def test_vec_to_sql_literal():
    vec = np.array([0.1, 0.2, 0.3], dtype=np.float32)
    sql = vec_to_sql(vec)
    assert sql.startswith("[")
    assert sql.endswith("]::FLOAT[3]")
    assert "0.1" in sql
    assert "0.2" in sql
    assert "0.3" in sql
