"""Text embeddings via OpenRouter API (fast, parallel) or ONNX Runtime (fallback)."""
import os
import time
import numpy as np
from pathlib import Path

_DEFAULT_MODEL_DIR = Path(__file__).parent / "model"

# OpenRouter config
_OR_URL = "https://openrouter.ai/api/v1/embeddings"
_OR_MODEL = "google/gemini-embedding-001"  # 768 dims (reduced via Matryoshka)
_OR_BATCH_SIZE = 100  # OpenRouter relay limit (~150 fails, 100 safe)
_OR_CONCURRENCY = 30  # parallel threads (no rate limit on key)


def create_embedder(model_dir: Path = _DEFAULT_MODEL_DIR, api_key: str | None = None):
    """Return (embed, embed_batch, dims) using OpenRouter API or ONNX fallback."""
    api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")

    if api_key:
        return _create_api_embedder(api_key)
    else:
        print("No OPENROUTER_API_KEY — using local ONNX model (slow)", flush=True)
        return _create_onnx_embedder(model_dir)


def _create_api_embedder(api_key: str):
    """OpenRouter Gemini embeddings — parallel threads, 768 dims."""
    import urllib.request
    import json

    dims = 768

    def _call_api(texts: list[str]) -> list[list[float]]:
        """Single API call for ≤250 texts. Retries up to 3 times."""
        for attempt in range(3):
            try:
                payload = json.dumps({"model": _OR_MODEL, "input": texts}).encode()
                req = urllib.request.Request(
                    _OR_URL, data=payload,
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read())
                if "data" not in data:
                    raise ValueError(f"API error: {data.get('error', data)}")
                embeddings = sorted(data["data"], key=lambda x: x["index"])
                # Truncate to dims (Matryoshka) — Gemini returns 3072 natively
                return [e["embedding"][:dims] for e in embeddings]
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)

    def embed(text: str) -> np.ndarray:
        vecs = _call_api([text])
        return np.array(vecs[0], dtype=np.float32)

    def embed_batch(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        chunks = [texts[i:i + _OR_BATCH_SIZE] for i in range(0, len(texts), _OR_BATCH_SIZE)]
        if len(chunks) == 1:
            vecs = _call_api(texts)
            return np.array(vecs, dtype=np.float32)
        # Parallel via thread pool
        print(f"    Embedding {len(texts):,} texts ({len(chunks)} API calls, {_OR_CONCURRENCY} threads)...", flush=True)
        from concurrent.futures import ThreadPoolExecutor, as_completed
        all_embeddings = [None] * len(chunks)
        done_count = 0
        with ThreadPoolExecutor(max_workers=_OR_CONCURRENCY) as pool:
            futures = {pool.submit(_call_api, chunk): i for i, chunk in enumerate(chunks)}
            for future in as_completed(futures):
                idx = futures[future]
                all_embeddings[idx] = future.result()
                done_count += 1
                if done_count % 50 == 0:
                    print(f"    {done_count * _OR_BATCH_SIZE:,}/{len(texts):,} embedded...", flush=True)
        flat = []
        for batch in all_embeddings:
            flat.extend(batch)
        return np.array(flat, dtype=np.float32)

    return embed, embed_batch, dims


def _create_onnx_embedder(model_dir: Path):
    """Local ONNX Runtime fallback — ~17 rows/sec, 384 dims."""
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

    def _mean_pool(token_embeddings, attention_mask):
        mask = attention_mask[..., np.newaxis].astype(np.float32)
        summed = (token_embeddings * mask).sum(axis=1)
        counts = mask.sum(axis=1).clip(min=1e-9)
        return summed / counts

    def _normalize(vecs):
        norms = np.linalg.norm(vecs, axis=1, keepdims=True).clip(min=1e-9)
        return vecs / norms

    def _run_batch(texts):
        encoded = tokenizer.encode_batch(texts)
        input_ids = np.array([e.ids for e in encoded], dtype=np.int64)
        attention_mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)
        token_type_ids = np.zeros_like(input_ids)
        outputs = session.run(None, {
            "input_ids": input_ids, "attention_mask": attention_mask, "token_type_ids": token_type_ids,
        })
        return _normalize(_mean_pool(outputs[0], attention_mask)).astype(np.float32)

    def embed(text: str) -> np.ndarray:
        return _run_batch([text])[0]

    def embed_batch(texts: list, batch_size: int = 64) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        results = []
        for i in range(0, len(texts), batch_size):
            results.append(_run_batch(texts[i:i + batch_size]))
        return np.vstack(results).astype(np.float32)

    return embed, embed_batch, dims


def vec_to_sql(vec: np.ndarray) -> str:
    """Convert numpy vector to DuckDB FLOAT[N] SQL literal for HNSW index use."""
    n = len(vec)
    inner = ",".join(str(float(v)) for v in vec)
    return f"[{inner}]::FLOAT[{n}]"
