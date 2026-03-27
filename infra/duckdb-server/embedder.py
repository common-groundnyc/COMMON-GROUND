"""Text embeddings via Google Gemini API (fast, adaptive) or ONNX Runtime (fallback)."""
import os
import time
import threading
import numpy as np
from pathlib import Path

_DEFAULT_MODEL_DIR = Path(__file__).parent / "model"

# Google Gemini direct config
_GEMINI_MODEL = "gemini-embedding-001"
_GEMINI_BATCH_SIZE = 100  # max per request
_GEMINI_DIMS = 768        # reduced via output_dimensionality (native is 3072)

# Adaptive concurrency
_MIN_WORKERS = 5
_MAX_WORKERS = 60
_RAMP_UP_EVERY = 10
_BACKOFF_FACTOR = 0.5
_COOLDOWN_SECS = 2


class _AdaptiveThrottle:
    """Ramps up on success, backs off on failure."""
    def __init__(self):
        self.workers = _MIN_WORKERS
        self._successes = 0
        self._lock = threading.Lock()

    def success(self):
        with self._lock:
            self._successes += 1
            if self._successes >= _RAMP_UP_EVERY and self.workers < _MAX_WORKERS:
                self.workers = min(self.workers + 2, _MAX_WORKERS)
                self._successes = 0

    def failure(self):
        with self._lock:
            old = self.workers
            self.workers = max(int(self.workers * _BACKOFF_FACTOR), _MIN_WORKERS)
            self._successes = 0
            return self.workers < old


def create_embedder(model_dir: Path = _DEFAULT_MODEL_DIR, api_key: str | None = None):
    """Return (embed, embed_batch, dims). Uses Google Gemini direct or ONNX fallback."""
    api_key = api_key or os.environ.get("GEMINI_API_KEY", "")

    if api_key:
        return _create_gemini_embedder(api_key)
    else:
        # Try OpenRouter as second option
        or_key = os.environ.get("OPENROUTER_API_KEY", "")
        if or_key:
            print("No GEMINI_API_KEY — falling back to OpenRouter", flush=True)
            return _create_openrouter_embedder(or_key)
        print("No GEMINI_API_KEY or OPENROUTER_API_KEY — using local ONNX (slow)", flush=True)
        return _create_onnx_embedder(model_dir)


def _create_gemini_embedder(api_key: str):
    """Google Gemini direct — 3,000 RPM paid tier, 768 dims."""
    from google import genai

    client = genai.Client(api_key=api_key)
    dims = _GEMINI_DIMS
    throttle = _AdaptiveThrottle()

    def _call_api(texts: list[str]) -> list[list[float]]:
        for attempt in range(3):
            try:
                result = client.models.embed_content(
                    model=_GEMINI_MODEL,
                    contents=texts,
                    config={"output_dimensionality": dims},
                )
                throttle.success()
                return [e.values for e in result.embeddings]
            except Exception as e:
                backed_off = throttle.failure()
                if attempt == 2:
                    raise
                # Parse retry delay from error if available
                err_str = str(e)
                wait = 3 * (2 ** attempt)
                if "retryDelay" in err_str:
                    try:
                        import re
                        match = re.search(r'"retryDelay":\s*"(\d+)s"', err_str)
                        if match:
                            wait = int(match.group(1)) + 1
                    except Exception:
                        pass
                if backed_off:
                    wait = max(wait, _COOLDOWN_SECS)
                time.sleep(wait)

    def embed(text: str) -> np.ndarray:
        return np.array(_call_api([text])[0], dtype=np.float32)

    def embed_batch(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        chunks = [texts[i:i + _GEMINI_BATCH_SIZE] for i in range(0, len(texts), _GEMINI_BATCH_SIZE)]
        if len(chunks) == 1:
            return np.array(_call_api(texts), dtype=np.float32)

        from concurrent.futures import ThreadPoolExecutor, as_completed

        print(f"    {len(texts):,} texts, {len(chunks)} calls, adaptive {_MIN_WORKERS}→{_MAX_WORKERS} threads...", flush=True)

        all_embeddings = [None] * len(chunks)
        done = 0
        failed = 0
        t0 = time.time()

        i = 0
        while i < len(chunks):
            wave_size = throttle.workers
            wave_chunks = list(enumerate(chunks[i:i + wave_size], start=i))

            with ThreadPoolExecutor(max_workers=wave_size) as pool:
                futures = {pool.submit(_call_api, chunk): idx for idx, chunk in wave_chunks}
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        all_embeddings[idx] = future.result()
                        done += 1
                    except Exception:
                        all_embeddings[idx] = [[0.0] * dims] * len(chunks[idx])
                        done += 1
                        failed += 1

            i += wave_size
            elapsed = time.time() - t0
            rate = (done * _GEMINI_BATCH_SIZE) / elapsed if elapsed > 0 else 0
            if done % 20 == 0 or i >= len(chunks):
                print(f"    {done * _GEMINI_BATCH_SIZE:,}/{len(texts):,} ({rate:,.0f}/sec, {throttle.workers}w, {failed}f)", flush=True)

        elapsed = time.time() - t0
        rate = len(texts) / elapsed if elapsed > 0 else 0
        print(f"    Done: {len(texts):,} in {elapsed:.0f}s ({rate:,.0f}/sec, peak {throttle.workers}w, {failed}f)", flush=True)

        flat = []
        for batch in all_embeddings:
            flat.extend(batch)
        return np.array(flat, dtype=np.float32)

    return embed, embed_batch, dims


def _create_openrouter_embedder(api_key: str):
    """OpenRouter fallback — slower, 100 batch, 768 dims."""
    import urllib.request
    import json

    dims = 768
    throttle = _AdaptiveThrottle()
    url = "https://openrouter.ai/api/v1/embeddings"
    model = "google/gemini-embedding-001"

    def _call_api(texts: list[str]) -> list[list[float]]:
        for attempt in range(3):
            try:
                payload = json.dumps({"model": model, "input": texts}).encode()
                req = urllib.request.Request(url, data=payload, headers={
                    "Authorization": f"Bearer {api_key}", "Content-Type": "application/json",
                })
                with urllib.request.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read())
                if "data" not in data:
                    raise ValueError(f"API error: {data.get('error', data)}")
                embeddings = sorted(data["data"], key=lambda x: x["index"])
                throttle.success()
                return [e["embedding"][:dims] for e in embeddings]
            except Exception:
                throttle.failure()
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)

    def embed(text: str) -> np.ndarray:
        return np.array(_call_api([text])[0], dtype=np.float32)

    def embed_batch(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        chunks = [texts[i:i + 100] for i in range(0, len(texts), 100)]
        if len(chunks) == 1:
            return np.array(_call_api(texts), dtype=np.float32)

        from concurrent.futures import ThreadPoolExecutor, as_completed
        print(f"    {len(texts):,} texts via OpenRouter ({len(chunks)} calls)...", flush=True)
        all_embeddings = [None] * len(chunks)
        done = 0
        with ThreadPoolExecutor(max_workers=throttle.workers) as pool:
            futures = {pool.submit(_call_api, c): i for i, c in enumerate(chunks)}
            for f in as_completed(futures):
                idx = futures[f]
                try:
                    all_embeddings[idx] = f.result()
                except Exception:
                    all_embeddings[idx] = [[0.0] * dims] * len(chunks[idx])
                done += 1
        flat = []
        for batch in all_embeddings:
            flat.extend(batch)
        return np.array(flat, dtype=np.float32)

    return embed, embed_batch, dims


def _create_onnx_embedder(model_dir: Path):
    """Local ONNX Runtime — ~17 rows/sec, 384 dims."""
    import onnxruntime as ort
    from tokenizers import Tokenizer

    model_dir = Path(model_dir)
    int8_path = model_dir / "onnx" / "model_int8.onnx"
    fp32_path = model_dir / "onnx" / "model.onnx"
    model_path = int8_path if int8_path.exists() else fp32_path

    opts = ort.SessionOptions()
    opts.intra_op_num_threads = 2
    opts.inter_op_num_threads = 2
    session = ort.InferenceSession(str(model_path), sess_options=opts)
    tokenizer = Tokenizer.from_file(str(model_dir / "tokenizer.json"))
    tokenizer.enable_truncation(max_length=128)
    tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")
    dims = 384

    def _pool_norm(tok_emb, attn_mask):
        m = attn_mask[..., np.newaxis].astype(np.float32)
        pooled = (tok_emb * m).sum(1) / m.sum(1).clip(min=1e-9)
        return pooled / np.linalg.norm(pooled, axis=1, keepdims=True).clip(min=1e-9)

    def _run(texts):
        enc = tokenizer.encode_batch(texts)
        ids = np.array([e.ids for e in enc], dtype=np.int64)
        mask = np.array([e.attention_mask for e in enc], dtype=np.int64)
        out = session.run(None, {"input_ids": ids, "attention_mask": mask, "token_type_ids": np.zeros_like(ids)})
        return _pool_norm(out[0], mask).astype(np.float32)

    def embed(text): return _run([text])[0]
    def embed_batch(texts, bs=64):
        if not texts: return np.empty((0, dims), dtype=np.float32)
        return np.vstack([_run(texts[i:i+bs]) for i in range(0, len(texts), bs)])

    return embed, embed_batch, dims


def vec_to_sql(vec: np.ndarray) -> str:
    """Convert numpy vector to DuckDB FLOAT[] SQL literal."""
    inner = ",".join(str(float(v)) for v in vec)
    return f"[{inner}]::FLOAT[]"
