"""Download all-MiniLM-L6-v2 ONNX model + tokenizer. Run once at Docker build time."""
from huggingface_hub import hf_hub_download
from pathlib import Path

MODEL_DIR = Path(__file__).parent / "model"
REPO = "sentence-transformers/all-MiniLM-L6-v2"

def download():
    MODEL_DIR.mkdir(exist_ok=True)
    (MODEL_DIR / "onnx").mkdir(exist_ok=True)
    hf_hub_download(REPO, "onnx/model.onnx", local_dir=str(MODEL_DIR))
    for f in ["tokenizer.json", "tokenizer_config.json", "special_tokens_map.json", "vocab.txt"]:
        hf_hub_download(REPO, f, local_dir=str(MODEL_DIR))
    from onnxruntime.quantization import quantize_dynamic, QuantType
    quantize_dynamic(
        str(MODEL_DIR / "onnx" / "model.onnx"),
        str(MODEL_DIR / "onnx" / "model_int8.onnx"),
        weight_type=QuantType.QUInt8,
    )
    print(f"Model downloaded and quantized to {MODEL_DIR / 'onnx' / 'model_int8.onnx'}")

if __name__ == "__main__":
    download()
