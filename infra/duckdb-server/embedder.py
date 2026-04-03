"""Text embeddings via Google Gemini direct API (rate-limited, ~200 names/sec)."""
import os
import re
import time
import json
import threading
import numpy as np
from pathlib import Path

_DEFAULT_MODEL_DIR = Path(__file__).parent / "model"

_GEMINI_MODEL = "gemini-embedding-2-preview"
_GEMINI_DIMS = 256
_GEMINI_BATCH_SIZE = 250  # Vertex AI max: 250. AI Studio max: 100. Auto-adjusted below.
_MAX_RPM_PER_KEY = 12     # Tier 1: ~15 RPM per key for batchEmbedContents. 12 RPM = safe zero-429 rate

def _load_gemini_keys() -> list[str]:
    """Load Gemini API keys from environment. GEMINI_API_KEYS (comma-separated) takes
    precedence; falls back to single GEMINI_API_KEY."""
    multi = os.environ.get("GEMINI_API_KEYS", "")
    if multi:
        return [k.strip() for k in multi.split(",") if k.strip()]
    single = os.environ.get("GEMINI_API_KEY", "")
    if single:
        return [single]
    return []


def create_embedder(model_dir: Path = _DEFAULT_MODEL_DIR, api_key: str | None = None):
    """Return (embed, embed_batch, dims)."""
    # Load keys from env (multi-key pool for rate-limit rotation)
    keys = _load_gemini_keys()
    if not keys and api_key:
        keys = [api_key]
    if keys:
        return _create_gemini_embedder(keys)

    or_key = os.environ.get("OPENROUTER_API_KEY", "")
    if or_key:
        print("No GEMINI_API_KEY — using OpenRouter (slower)", flush=True)
        return _create_openrouter_embedder(or_key)

    print("No API keys — using local ONNX (very slow)", flush=True)
    return _create_onnx_embedder(model_dir)


def _create_gemini_embedder(api_keys: list[str]):
    """Gemini Vertex AI embedder — uses TPM quota (5M tokens/min) instead of RPM.
    Falls back to multi-key AI Studio if Vertex unavailable."""
    from google import genai

    dims = _GEMINI_DIMS
    vertex_client = None
    ai_studio_clients = []

    # Try Vertex AI first (massively higher throughput — TPM not RPM)
    _vertex_url = None
    _vertex_token = [None, 0.0]  # [token, expiry]
    _vertex_lock = threading.Lock()

    try:
        import google.auth
        import google.auth.transport.requests
        _gcp_creds, _gcp_project = google.auth.default()
        _gcp_project = os.environ.get("GCP_PROJECT", _gcp_project or "")
        _vertex_model = "text-embedding-005"
        _vertex_url = f"https://us-central1-aiplatform.googleapis.com/v1/projects/{_gcp_project}/locations/us-central1/publishers/google/models/{_vertex_model}:predict"

        # Verify
        _gcp_creds.refresh(google.auth.transport.requests.Request())
        import urllib.request as _ur
        _test_payload = json.dumps({"instances": [{"content": "test"}], "parameters": {"outputDimensionality": dims}}).encode()
        _test_req = _ur.Request(_vertex_url, data=_test_payload, headers={"Content-Type": "application/json", "Authorization": f"Bearer {_gcp_creds.token}"})
        _test_resp = _ur.urlopen(_test_req, timeout=15)
        _test_data = json.loads(_test_resp.read())
        if _test_data.get("predictions"):
            print(f"Gemini embedder: Vertex AI ({_vertex_model}, 256d, ~5K+ names/sec)", flush=True)
        else:
            raise ValueError("No predictions returned")
    except Exception as e:
        _vertex_url = None
        print(f"Vertex AI unavailable ({e}), falling back to AI Studio multi-key", flush=True)

    if _vertex_url:
        import urllib.request as _ur

        def _get_token():
            now = time.time()
            if _vertex_token[0] and _vertex_token[1] > now + 60:
                return _vertex_token[0]
            with _vertex_lock:
                if _vertex_token[0] and _vertex_token[1] > time.time() + 60:
                    return _vertex_token[0]
                _gcp_creds.refresh(google.auth.transport.requests.Request())
                _vertex_token[0] = _gcp_creds.token
                _vertex_token[1] = time.time() + 3500
                return _vertex_token[0]

        def _call_api(texts: list[str]) -> list[list[float]]:
            payload = json.dumps({
                "instances": [{"content": t} for t in texts],
                "parameters": {"outputDimensionality": dims},
            }).encode()
            for attempt in range(4):
                try:
                    tok = _get_token()
                    req = _ur.Request(_vertex_url, data=payload, headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {tok}",
                    })
                    resp = _ur.urlopen(req, timeout=30)
                    data = json.loads(resp.read())
                    return [p["embeddings"]["values"] for p in data["predictions"]]
                except Exception as e:
                    if attempt == 3:
                        raise
                    wait = 2 * (2 ** attempt)
                    time.sleep(wait)
            raise RuntimeError("All retry attempts exhausted")

        vertex_client = True  # flag for batch_size selection
    else:
        # AI Studio multi-key fallback
        n_keys = len(api_keys)
        ai_studio_clients = [genai.Client(api_key=k) for k in api_keys]

        _MIN_INTERVAL = 60.0 / _MAX_RPM_PER_KEY
        _last_call = [0.0] * n_keys
        _locks = [threading.Lock() for _ in range(n_keys)]
        _call_counter = [0]
        _counter_lock = threading.Lock()

        def _pick_key() -> int:
            with _counter_lock:
                idx = _call_counter[0] % n_keys
                _call_counter[0] += 1
            return idx

        def _rate_limit_studio(key_idx: int):
            with _locks[key_idx]:
                now = time.time()
                earliest = _last_call[key_idx] + _MIN_INTERVAL
                if earliest > now:
                    time.sleep(earliest - now)
                _last_call[key_idx] = time.time()

        def _call_api(texts: list[str]) -> list[list[float]]:
            key_idx = _pick_key()
            _rate_limit_studio(key_idx)
            for attempt in range(4):
                try:
                    result = ai_studio_clients[key_idx].models.embed_content(
                        model=_GEMINI_MODEL, contents=texts,
                        config={"output_dimensionality": dims},
                    )
                    return [e.values for e in result.embeddings]
                except Exception as e:
                    if attempt == 3:
                        raise
                    wait = 2 * (2 ** attempt)
                    m = re.search(r'[Rr]etry.*?(\d+)', str(e))
                    if m:
                        wait = max(wait, int(m.group(1)) + 1)
                    time.sleep(wait)
            raise RuntimeError("All retry attempts exhausted")

        print(f"Gemini embedder: {n_keys} AI Studio keys, {_MAX_RPM_PER_KEY} RPM/key = ~{n_keys * _MAX_RPM_PER_KEY * 100 // 60} names/sec", flush=True)

    # Set batch size based on backend
    batch_size = 250 if vertex_client else 100
    max_workers = 30 if vertex_client else len(api_keys) * 2

    def embed(text: str) -> np.ndarray:
        return np.array(_call_api([text])[0], dtype=np.float32)

    def embed_batch(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, dims), dtype=np.float32)
        chunks = [texts[i:i + batch_size] for i in range(0, len(texts), batch_size)]
        if len(chunks) == 1:
            return np.array(_call_api(texts), dtype=np.float32)

        from concurrent.futures import ThreadPoolExecutor, as_completed

        n_workers = min(max_workers, len(chunks))
        print(f"    {len(texts):,} texts, {len(chunks)} calls, {n_workers} threads...", flush=True)

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
                    print(f"    Warning: chunk {idx} failed: {e}", flush=True)
                    all_embeddings[idx] = [[0.0] * dims] * len(chunks[idx])
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
