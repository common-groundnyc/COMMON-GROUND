"""Lightweight text embeddings via ONNX Runtime + tokenizers. No PyTorch."""
from pathlib import Path
import numpy as np

_DEFAULT_MODEL_DIR = Path(__file__).parent / "model"


def create_embedder(model_dir: Path = _DEFAULT_MODEL_DIR):
    """Return (embed, embed_batch) functions backed by ONNX Runtime."""
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
        token_embeddings = outputs[0]
        pooled = _mean_pool(token_embeddings, attention_mask)
        return _normalize(pooled).astype(np.float32)

    def embed(text: str) -> np.ndarray:
        return _run_batch([text])[0]

    def embed_batch(texts: list, batch_size: int = 64) -> np.ndarray:
        if len(texts) == 0:
            return np.empty((0, 384), dtype=np.float32)
        results = []
        for i in range(0, len(texts), batch_size):
            chunk = texts[i:i + batch_size]
            results.append(_run_batch(chunk))
        return np.vstack(results).astype(np.float32)

    return embed, embed_batch


def vec_to_sql(vec: np.ndarray) -> str:
    """Convert numpy vector to DuckDB FLOAT[N] SQL literal for HNSW index use."""
    n = len(vec)
    inner = ",".join(str(float(v)) for v in vec)
    return f"[{inner}]::FLOAT[{n}]"
