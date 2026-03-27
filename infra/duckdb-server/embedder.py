"""Text embeddings via Google Gemini Batch API (fast) or direct API (fallback)."""
import os
import time
import json
import threading
import numpy as np
from pathlib import Path

_DEFAULT_MODEL_DIR = Path(__file__).parent / "model"

_GEMINI_MODEL = "gemini-embedding-001"
_GEMINI_DIMS = 768
_GEMINI_BATCH_SIZE = 100  # max texts per embed_content call
_DIRECT_RPS = 40          # stay under 3,000 RPM


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
    """Gemini direct: Batch API for bulk, direct API for single queries."""
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)
    dims = _GEMINI_DIMS

    # --- Rate-limited direct API (for single queries + small batches) ---
    _rate_lock = threading.Lock()
    _req_times: list[float] = []

    def _rate_limit():
        with _rate_lock:
            now = time.time()
            _req_times[:] = [t for t in _req_times if now - t < 1.0]
            if len(_req_times) >= _DIRECT_RPS:
                wait = 1.0 - (now - _req_times[0]) + 0.05
                if wait > 0:
                    time.sleep(wait)
            _req_times.append(time.time())

    def _direct_call(texts: list[str]) -> list[list[float]]:
        _rate_limit()
        for attempt in range(3):
            try:
                result = client.models.embed_content(
                    model=_GEMINI_MODEL,
                    contents=texts,
                    config={"output_dimensionality": dims},
                )
                return [e.values for e in result.embeddings]
            except Exception as e:
                if attempt == 2:
                    raise
                err = str(e)
                wait = 3 * (2 ** attempt)
                if "retryDelay" in err:
                    import re
                    m = re.search(r'retryDelay.*?(\d+)', err)
                    if m:
                        wait = int(m.group(1)) + 1
                time.sleep(wait)

    # --- Batch API (for bulk embedding — no RPM limits, 50% cheaper) ---
    def _batch_embed(texts: list[str]) -> np.ndarray:
        """Use Gemini Batch API for large jobs. Fire-and-forget, poll for results."""
        print(f"    Submitting {len(texts):,} texts to Gemini Batch API...", flush=True)

        # Build JSONL inline requests
        inline_requests = []
        for i in range(0, len(texts), _GEMINI_BATCH_SIZE):
            chunk = texts[i:i + _GEMINI_BATCH_SIZE]
            inline_requests.append({
                "contents": [{"parts": [{"text": t}]} for t in chunk],
            })

        try:
            batch_job = client.batches.create_embeddings(
                model=_GEMINI_MODEL,
                src={"inlined_requests": inline_requests},
                config={
                    "display_name": f"mcp-embeddings-{int(time.time())}",
                    "output_dimensionality": dims,
                },
            )
        except Exception as e:
            print(f"    Batch API failed, falling back to direct: {e}", flush=True)
            return _direct_batch(texts)

        job_name = batch_job.name
        print(f"    Batch job: {job_name}", flush=True)

        # Poll for completion
        completed = {"JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED",
                      "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"}
        t0 = time.time()
        while True:
            job = client.batches.get(name=job_name)
            state = job.state.name if hasattr(job.state, 'name') else str(job.state)
            elapsed = time.time() - t0
            if state in completed:
                print(f"    Batch {state} in {elapsed:.0f}s", flush=True)
                break
            if elapsed > 3600:  # 1 hour timeout
                print(f"    Batch timeout after {elapsed:.0f}s, falling back to direct", flush=True)
                return _direct_batch(texts)
            print(f"    Batch {state} ({elapsed:.0f}s)...", flush=True)
            time.sleep(15)

        if state != "JOB_STATE_SUCCEEDED":
            print(f"    Batch failed: {state}, falling back to direct", flush=True)
            return _direct_batch(texts)

        # Extract embeddings from results
        all_embeddings = []
        if hasattr(job.dest, 'inlined_embed_content_responses') and job.dest.inlined_embed_content_responses:
            for resp in job.dest.inlined_embed_content_responses:
                if hasattr(resp, 'embeddings'):
                    for emb in resp.embeddings:
                        all_embeddings.append(emb.values[:dims])
                elif hasattr(resp, 'embedding'):
                    all_embeddings.append(resp.embedding.values[:dims])
        elif hasattr(job.dest, 'file_name') and job.dest.file_name:
            content = client.files.download(file=job.dest.file_name)
            for line in content.decode('utf-8').strip().split('\n'):
                entry = json.loads(line)
                if 'response' in entry and 'embeddings' in entry['response']:
                    for emb in entry['response']['embeddings']:
                        all_embeddings.append(emb['values'][:dims])

        if len(all_embeddings) != len(texts):
            print(f"    Batch returned {len(all_embeddings)} embeddings for {len(texts)} texts, padding...", flush=True)
            while len(all_embeddings) < len(texts):
                all_embeddings.append([0.0] * dims)

        return np.array(all_embeddings[:len(texts)], dtype=np.float32)

    def _direct_batch(texts: list[str]) -> np.ndarray:
        """Rate-limited direct API with threading for medium batches."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        chunks = [texts[i:i + _GEMINI_BATCH_SIZE] for i in range(0, len(texts), _GEMINI_BATCH_SIZE)]
        print(f"    Direct: {len(texts):,} texts, {len(chunks)} calls, {_DIRECT_RPS} req/sec...", flush=True)

        all_embeddings = [None] * len(chunks)
        done = 0
        failed = 0
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=min(10, len(chunks))) as pool:
            futures = {pool.submit(_direct_call, c): i for i, c in enumerate(chunks)}
            for f in as_completed(futures):
                idx = futures[f]
                try:
                    all_embeddings[idx] = f.result()
                    done += 1
                except Exception:
                    all_embeddings[idx] = [[0.0] * dims] * len(chunks[idx])
                    done += 1
                    failed += 1
                if done % 50 == 0:
                    rate = (done * _GEMINI_BATCH_SIZE) / (time.time() - t0)
                    print(f"    {done * _GEMINI_BATCH_SIZE:,}/{len(texts):,} ({rate:,.0f}/sec, {failed}f)", flush=True)

        elapsed = time.time() - t0
        print(f"    Direct done: {len(texts):,} in {elapsed:.0f}s ({len(texts)/elapsed:,.0f}/sec, {failed}f)", flush=True)
        flat = []
        for batch in all_embeddings:
            flat.extend(batch)
        return np.array(flat, dtype=np.float32)

    # --- Public interface ---
    def embed(text: str) -> np.ndarray:
        return np.array(_direct_call([text])[0], dtype=np.float32)

    def embed_batch(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        if len(texts) <= _GEMINI_BATCH_SIZE:
            return np.array(_direct_call(texts), dtype=np.float32)
        # Use Batch API for large jobs (>100 texts)
        return _batch_embed(texts)

    return embed, embed_batch, dims


def _create_openrouter_embedder(api_key: str):
    """OpenRouter fallback."""
    import urllib.request
    dims = 768
    url = "https://openrouter.ai/api/v1/embeddings"

    def _call(texts):
        for attempt in range(3):
            try:
                payload = json.dumps({"model": "google/gemini-embedding-001", "input": texts}).encode()
                req = urllib.request.Request(url, data=payload, headers={
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
                except: results[futs[f]] = [[0.0]*dims]*len(chunks[futs[f]])
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
        norms = np.linalg.norm(pooled, axis=1, keepdims=True).clip(min=1e-9)
        return (pooled / norms).astype(np.float32)
    def embed(text): return _run([text])[0]
    def embed_batch(texts, bs=64):
        if not texts: return np.empty((0, dims), dtype=np.float32)
        return np.vstack([_run(texts[i:i+bs]) for i in range(0, len(texts), bs)])
    return embed, embed_batch, dims


def vec_to_sql(vec: np.ndarray) -> str:
    """Convert numpy vector to DuckDB FLOAT[] SQL literal."""
    return f"[{','.join(str(float(v)) for v in vec)}]::FLOAT[]"
