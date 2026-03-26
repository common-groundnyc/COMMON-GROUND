"""Text embeddings via OpenRouter API (primary) or ONNX Runtime (fallback)."""
import os
import numpy as np
from pathlib import Path

_DEFAULT_MODEL_DIR = Path(__file__).parent / "model"

# OpenRouter config
_OR_URL = "https://openrouter.ai/api/v1/embeddings"
_OR_MODEL = "google/gemini-embedding-001"  # 768 dims, $0.15/1M tokens
_OR_BATCH_SIZE = 2048  # OpenAI-compatible API max


def create_embedder(model_dir: Path = _DEFAULT_MODEL_DIR, api_key: str | None = None):
    """Return (embed, embed_batch, dims) using OpenRouter API or ONNX fallback.

    Priority: OpenRouter API (fast, ~$0.50 for 300K names) → local ONNX (slow, free).
    """
    api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")

    if api_key:
        return _create_api_embedder(api_key)
    else:
        print("No OPENROUTER_API_KEY — using local ONNX model (slow)", flush=True)
        return _create_onnx_embedder(model_dir)


def _create_api_embedder(api_key: str):
    """OpenRouter embeddings API — ~2000 texts/sec, 768 dims."""
    import urllib.request
    import json

    dims = 768

    def _call_api(texts: list[str]) -> np.ndarray:
        payload = json.dumps({
            "model": _OR_MODEL,
            "input": texts,
        }).encode()
        req = urllib.request.Request(
            _OR_URL,
            data=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
        # Sort by index (API may return out of order)
        embeddings = sorted(data["data"], key=lambda x: x["index"])
        return np.array([e["embedding"] for e in embeddings], dtype=np.float32)

    def embed(text: str) -> np.ndarray:
        return _call_api([text])[0]

    def embed_batch(texts: list[str], batch_size: int = _OR_BATCH_SIZE) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        results = []
        for i in range(0, len(texts), batch_size):
            chunk = texts[i:i + batch_size]
            results.append(_call_api(chunk))
            if i > 0 and i % 10000 == 0:
                print(f"    Embedded {i:,}/{len(texts):,}...", flush=True)
        return np.vstack(results).astype(np.float32)

    return embed, embed_batch, dims


def _create_onnx_embedder(model_dir: Path):
    """Local ONNX Runtime — ~17 rows/sec, 384 dims. Free but slow."""
    import onnxruntime as ort
    from tokenizers import Tokenizer

    model_dir = Path(model_dir)
    int8_path = model_dir / "onnx" / "model_int8.onnx"
    fp32_path = model_dir / "onnx" / "model.onnx"
    model_path = int8_path if int8_path.exists() else fp32_path

    sess_options = ort.SessionOptions()
    sess_options.intra_op_num_threads = 2
    sess_options.inter_op_num_threads = 2
    session = ort.InferenceSession(str(model_path), sess_options=sess_options)

    tokenizer = Tokenizer.from_file(str(model_dir / "tokenizer.json"))
    tokenizer.enable_truncation(max_length=128)
    tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")

    dims = 384

    def _mean_pool(token_embeddings: np.ndarray, attention_mask: np.ndarray) -> np.ndarray:
        mask = attention_mask[..., np.newaxis].astype(np.float32)
        summed = (token_embeddings * mask).sum(axis=1)
        counts = mask.sum(axis=1).clip(min=1e-9)
        return summed / counts

    def _normalize(vecs: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(vecs, axis=1, keepdims=True).clip(min=1e-9)
        return vecs / norms

    def _run_batch(texts: list) -> np.ndarray:
        encoded = tokenizer.encode_batch(texts)
        input_ids = np.array([e.ids for e in encoded], dtype=np.int64)
        attention_mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)
        token_type_ids = np.zeros_like(input_ids)
        outputs = session.run(None, {
            "input_ids": input_ids,
            "attention_mask": attention_mask,
            "token_type_ids": token_type_ids,
        })
        return _normalize(_mean_pool(outputs[0], attention_mask)).astype(np.float32)

    def embed(text: str) -> np.ndarray:
        return _run_batch([text])[0]

    def embed_batch(texts: list, batch_size: int = 64) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        results = []
        for i in range(0, len(texts), batch_size):
            chunk = texts[i:i + batch_size]
            results.append(_run_batch(chunk))
        return np.vstack(results).astype(np.float32)

    return embed, embed_batch, dims


def vec_to_sql(vec: np.ndarray) -> str:
    """Convert numpy vector to DuckDB FLOAT[N] SQL literal for HNSW index use."""
    n = len(vec)
    inner = ",".join(str(float(v)) for v in vec)
    return f"[{inner}]::FLOAT[{n}]"
