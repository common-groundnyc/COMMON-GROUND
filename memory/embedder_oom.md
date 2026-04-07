# Embedder OOM during startup

**Symptom:** `duckdb-server` container enters a crash loop at startup. Logs show the MiniLM ONNX model trying to use MPS (Apple Metal) or CUDA and failing.

**Root cause:** ONNX Runtime picks up whatever provider is available. On Hetzner (CPU-only) and on macOS dev machines, this can trigger SIGBUS or OOM on model load.

**Fix:** Force CPU execution via env var in `docker-compose.yml` (and `.env`):

```
HINDSIGHT_API_EMBEDDINGS_LOCAL_FORCE_CPU=1
```

This is a real requirement — **not** a placebo. Removing it triggers the crash loop.

**Related:** hindsight memory daemon uses the same pattern. See global memory `project_mcp_server_march26.md` for the original incident.
