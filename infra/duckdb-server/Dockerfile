FROM python:3.12-slim

# anofox_forecast needs OpenSSL 1.1 (not available in Bookworm) — pull from Bullseye
RUN echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list.d/bullseye.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends -t bullseye libssl1.1 && \
    apt-get install -y --no-install-recommends nginx && \
    rm /etc/apt/sources.list.d/bullseye.list && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    duckdb==1.5.1 \
    "fastmcp[http,code-mode]==3.1.1" \
    posthog==7.9.12 \
    openpyxl==3.1.5 \
    onnxruntime==1.24.4 \
    onnx==1.20.1 \
    tokenizers==0.22.2 \
    numpy==2.4.3 \
    pyarrow==23.0.1 \
    huggingface_hub==1.8.0 \
    aiohttp==3.13.3 \
    google-genai==1.68.0 \
    google-auth==2.49.1

WORKDIR /app
COPY *.py ./
COPY middleware/ middleware/
COPY shared/ shared/
COPY tools/ tools/
COPY start.sh ./
RUN chmod +x start.sh

# Download and quantize embedding model at build time (~22MB INT8)
RUN python download_model.py
# model/ dir now exists from download_model.py

EXPOSE 4213 9999

ENV FASTMCP_STATELESS_HTTP=true

# Pre-install community extensions at build time (avoids download on every startup)
RUN python -c "\
import duckdb; conn = duckdb.connect(); \
[conn.execute(f'INSTALL {ext} FROM community') for ext in \
['h3','splink_udfs','rapidfuzz','anofox_tabular','datasketches','hashfuncs', \
'lindel','dqtest','anofox_forecast','lsh','curl_httpfs']]; \
print('Extensions pre-installed'); conn.close()"

CMD ["./start.sh"]
