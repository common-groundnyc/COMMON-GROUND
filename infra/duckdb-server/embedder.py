"""Text embeddings via Google Gemini direct API (rate-limited, ~790/sec)."""
import os
import re
import time
import json
import threading
import numpy as np
from pathlib import Path

_DEFAULT_MODEL_DIR = Path(__file__).parent / "model"

_GEMINI_MODEL = "gemini-embedding-001"
_GEMINI_DIMS = 768
_GEMINI_BATCH_SIZE = 100  # 100 texts/call — proven stable, no 400s
_MAX_RPS = 100            # ~6000 RPM — well above free tier, let 429 retries throttle


def create_embedder(model_dir: Path = _DEFAULT_MODEL_DIR, api_key: str | None = None):
    """Return (embed, embed_batch, dims)."""
    api_key = api_key or os.environ.get("GEMINI_API_KEY", "")
    if api_key:
        return _create_gemini_embedder(api_key)

    or_key = os.environ.get("OPENROUTER_API_KEY", "")
    if or_key:
        print("No GEMINI_API_KEY — using OpenRouter (slower)", flush=True)
        return _create_openrouter_embedder(or_key)

    print("No API keys — using local ONNX (very slow)", flush=True)
    return _create_onnx_embedder(model_dir)


def _create_gemini_embedder(api_key: str):
    """Gemini direct API — rate-limited to 40 req/sec, ~790 texts/sec at 100/batch."""
    from google import genai

    client = genai.Client(api_key=api_key)
    dims = _GEMINI_DIMS

    # Token bucket rate limiter
    _rate_lock = threading.Lock()
    _req_times: list[float] = []

    def _rate_limit():
        wait = 0
        with _rate_lock:
            now = time.time()
            _req_times[:] = [t for t in _req_times if now - t < 1.0]
            if len(_req_times) >= _MAX_RPS:
                wait = 1.0 - (now - _req_times[0]) + 0.05
            _req_times.append(time.time())
        if wait > 0:
            time.sleep(wait)

    def _call_api(texts: list[str]) -> list[list[float]]:
        _rate_limit()
        for attempt in range(4):
            try:
                result = client.models.embed_content(
                    model=_GEMINI_MODEL,
                    contents=texts,
                    config={"output_dimensionality": dims},
                )
                return [e.values for e in result.embeddings]
            except Exception as e:
                if attempt == 3:
                    raise
                err = str(e)
                wait = 2 * (2 ** attempt)
                # Honor Retry-After from 429
                m = re.search(r'[Rr]etry.*?(\d+)', err)
                if m:
                    wait = max(wait, int(m.group(1)) + 1)
                time.sleep(wait)
        raise RuntimeError("All retry attempts exhausted")

    def embed(text: str) -> np.ndarray:
        return np.array(_call_api([text])[0], dtype=np.float32)

    def embed_batch(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        chunks = [texts[i:i + _GEMINI_BATCH_SIZE] for i in range(0, len(texts), _GEMINI_BATCH_SIZE)]
        if len(chunks) == 1:
            return np.array(_call_api(texts), dtype=np.float32)

        from concurrent.futures import ThreadPoolExecutor, as_completed

        n_workers = min(20, len(chunks))
        print(f"    {len(texts):,} texts, {len(chunks)} calls, {n_workers} threads, {_MAX_RPS} req/sec cap...", flush=True)

        all_embeddings = [None] * len(chunks)
        done = 0
        failed = 0
        t0 = time.time()

        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {pool.submit(_call_api, c): i for i, c in enumerate(chunks)}
            for f in as_completed(futures):
                idx = futures[f]
                try:
                    all_embeddings[idx] = f.result()
                except Exception as e:
                    print(f"    Warning: chunk {idx} failed, retrying in sub-batches: {e}", flush=True)
                    sub_results = []
                    chunk = chunks[idx]
                    for sub_start in range(0, len(chunk), 10):
                        sub = chunk[sub_start:sub_start + 10]
                        try:
                            sub_results.extend(_call_api(sub))
                        except Exception:
                            print(f"    Sub-batch {sub_start}-{sub_start+len(sub)} failed, zero-filling", flush=True)
                            sub_results.extend([[0.0] * dims] * len(sub))
                    all_embeddings[idx] = sub_results
                    failed += 1
                done += 1
                if done % 100 == 0 or done == len(chunks):
                    elapsed = time.time() - t0
                    rate = (done * _GEMINI_BATCH_SIZE) / elapsed if elapsed > 0 else 0
                    print(f"    {done * _GEMINI_BATCH_SIZE:,}/{len(texts):,} ({rate:,.0f}/sec, {failed}f)", flush=True)

        elapsed = time.time() - t0
        rate = len(texts) / elapsed if elapsed > 0 else 0
        print(f"    Done: {len(texts):,} in {elapsed:.0f}s ({rate:,.0f}/sec, {failed}f)", flush=True)

        flat = []
        for batch in all_embeddings:
            flat.extend(batch)
        return np.array(flat, dtype=np.float32)

    return embed, embed_batch, dims


def _create_openrouter_embedder(api_key: str):
    """OpenRouter fallback."""
    import urllib.request
    dims = 768

    def _call(texts):
        for attempt in range(3):
            try:
                payload = json.dumps({"model": "google/gemini-embedding-001", "input": texts}).encode()
                req = urllib.request.Request("https://openrouter.ai/api/v1/embeddings", data=payload, headers={
                    "Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read())
                if "data" not in data:
                    raise ValueError(str(data)[:200])
                return [e["embedding"][:dims] for e in sorted(data["data"], key=lambda x: x["index"])]
            except Exception:
                if attempt == 2: raise
                time.sleep(2 ** attempt)

    def embed(text): return np.array(_call([text])[0], dtype=np.float32)
    def embed_batch(texts):
        if not texts: return np.empty((0, dims), dtype=np.float32)
        from concurrent.futures import ThreadPoolExecutor
        chunks = [texts[i:i+100] for i in range(0, len(texts), 100)]
        results = [None] * len(chunks)
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_call, c): i for i, c in enumerate(chunks)}
            for f in futs:
                try: results[futs[f]] = f.result()
                except Exception: results[futs[f]] = [[0.0]*dims]*len(chunks[futs[f]])
        return np.array([v for batch in results for v in batch], dtype=np.float32)

    return embed, embed_batch, dims


def _create_onnx_embedder(model_dir: Path):
    """Local ONNX — 384 dims, ~17 rows/sec."""
    import onnxruntime as ort
    from tokenizers import Tokenizer
    model_dir = Path(model_dir)
    p = model_dir / "onnx" / "model_int8.onnx"
    if not p.exists(): p = model_dir / "onnx" / "model.onnx"
    opts = ort.SessionOptions(); opts.intra_op_num_threads = 2; opts.inter_op_num_threads = 2
    session = ort.InferenceSession(str(p), sess_options=opts)
    tok = Tokenizer.from_file(str(model_dir / "tokenizer.json"))
    tok.enable_truncation(max_length=128); tok.enable_padding(pad_id=0, pad_token="[PAD]")
    dims = 384
    def _run(texts):
        enc = tok.encode_batch(texts)
        ids = np.array([e.ids for e in enc], dtype=np.int64)
        mask = np.array([e.attention_mask for e in enc], dtype=np.int64)
        out = session.run(None, {"input_ids": ids, "attention_mask": mask, "token_type_ids": np.zeros_like(ids)})
        m = mask[..., np.newaxis].astype(np.float32)
        pooled = (out[0] * m).sum(1) / m.sum(1).clip(min=1e-9)
        return (pooled / np.linalg.norm(pooled, axis=1, keepdims=True).clip(min=1e-9)).astype(np.float32)
    def embed(text): return _run([text])[0]
    def embed_batch(texts, bs=64):
        if not texts: return np.empty((0, dims), dtype=np.float32)
        return np.vstack([_run(texts[i:i+bs]) for i in range(0, len(texts), bs)])
    return embed, embed_batch, dims


def vec_to_sql(vec: np.ndarray) -> str:
    """Convert numpy vector to DuckDB FLOAT[] SQL literal."""
    return f"[{','.join(str(float(v)) for v in vec)}]::FLOAT[]"
