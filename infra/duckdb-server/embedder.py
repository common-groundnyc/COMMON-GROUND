"""Text embeddings via OpenRouter API (fast, parallel) or ONNX Runtime (fallback)."""
import os
import asyncio
import numpy as np
from pathlib import Path

_DEFAULT_MODEL_DIR = Path(__file__).parent / "model"

# OpenRouter config
_OR_URL = "https://openrouter.ai/api/v1/embeddings"
_OR_MODEL = "google/gemini-embedding-001"  # 768 dims, $0.15/1M tokens
_OR_BATCH_SIZE = 250  # Gemini max per request
_OR_CONCURRENCY = 15  # parallel requests → ~3,750 texts in flight


def create_embedder(model_dir: Path = _DEFAULT_MODEL_DIR, api_key: str | None = None):
    """Return (embed, embed_batch, dims) using OpenRouter API or ONNX fallback."""
    api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")

    if api_key:
        return _create_api_embedder(api_key)
    else:
        print("No OPENROUTER_API_KEY — using local ONNX model (slow)", flush=True)
        return _create_onnx_embedder(model_dir)


def _create_api_embedder(api_key: str):
    """OpenRouter embeddings via parallel async requests — ~5,000-10,000 texts/sec."""
    import urllib.request
    import json

    dims = 768

    def _call_api_sync(texts: list[str]) -> list[list[float]]:
        """Single synchronous API call for ≤250 texts."""
        payload = json.dumps({"model": _OR_MODEL, "input": texts, "dimensions": dims}).encode()
        req = urllib.request.Request(
            _OR_URL, data=payload,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
        embeddings = sorted(data["data"], key=lambda x: x["index"])
        return [e["embedding"] for e in embeddings]

    async def _call_api_async(session, texts: list[str], semaphore) -> list[list[float]]:
        """Single async API call with concurrency control."""
        import aiohttp
        async with semaphore:
            payload = json.dumps({"model": _OR_MODEL, "input": texts, "dimensions": dims})
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            for attempt in range(3):
                try:
                    async with session.post(_OR_URL, data=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                        data = await resp.json()
                        if "data" not in data:
                            raise ValueError(f"API error: {data.get('error', data)}")
                        embeddings = sorted(data["data"], key=lambda x: x["index"])
                        return [e["embedding"] for e in embeddings]
                except Exception as e:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(2 ** attempt)

    async def _embed_batch_async(texts: list[str]) -> np.ndarray:
        """Embed all texts with parallel async requests."""
        import aiohttp
        semaphore = asyncio.Semaphore(_OR_CONCURRENCY)
        chunks = [texts[i:i + _OR_BATCH_SIZE] for i in range(0, len(texts), _OR_BATCH_SIZE)]

        all_embeddings = []
        async with aiohttp.ClientSession() as session:
            # Process in waves to show progress
            wave_size = _OR_CONCURRENCY * 4  # ~60 chunks per wave = ~15,000 texts
            for wave_start in range(0, len(chunks), wave_size):
                wave = chunks[wave_start:wave_start + wave_size]
                results = await asyncio.gather(*[
                    _call_api_async(session, chunk, semaphore) for chunk in wave
                ])
                for result in results:
                    all_embeddings.extend(result)
                done = min(wave_start + wave_size, len(chunks)) * _OR_BATCH_SIZE
                if done < len(texts):
                    print(f"    Embedded {done:,}/{len(texts):,}...", flush=True)

        return np.array(all_embeddings, dtype=np.float32)

    def embed(text: str) -> np.ndarray:
        vecs = _call_api_sync([text])
        return np.array(vecs[0], dtype=np.float32)

    def embed_batch(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        if len(texts) <= _OR_BATCH_SIZE:
            # Small batch — just do it synchronously
            vecs = _call_api_sync(texts)
            return np.array(vecs, dtype=np.float32)
        # Large batch — async parallel
        print(f"    Embedding {len(texts):,} texts ({len(texts) // _OR_BATCH_SIZE + 1} API calls, {_OR_CONCURRENCY} parallel)...", flush=True)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're inside an async context (FastMCP) — run in thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(1) as pool:
                    return pool.submit(lambda: asyncio.run(_embed_batch_async(texts))).result()
            else:
                return loop.run_until_complete(_embed_batch_async(texts))
        except RuntimeError:
            return asyncio.run(_embed_batch_async(texts))

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
