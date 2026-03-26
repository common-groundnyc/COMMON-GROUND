FROM python:3.12-slim

# anofox_forecast needs OpenSSL 1.1 (not available in Bookworm) — pull from Bullseye
RUN echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list.d/bullseye.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends -t bullseye libssl1.1 && \
    apt-get install -y --no-install-recommends nginx && \
    rm /etc/apt/sources.list.d/bullseye.list && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    duckdb==1.5.0 \
    "fastmcp[http]==3.1.1" \
    posthog \
    onnxruntime \
    onnx \
    tokenizers \
    numpy \
    pyarrow \
    huggingface_hub

WORKDIR /app
COPY *.py .
COPY start.sh .
RUN chmod +x start.sh

# Download and quantize embedding model at build time (~22MB INT8)
RUN python download_model.py

EXPOSE 4213 9999

ENV FASTMCP_STATELESS_HTTP=true

CMD ["./start.sh"]
