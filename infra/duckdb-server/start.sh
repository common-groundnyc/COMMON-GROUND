#!/bin/bash
set -e

# Generate self-signed SSL cert (DuckDB UI requires secure origin for Auth0)
if [ ! -f /tmp/ssl/cert.pem ]; then
    mkdir -p /tmp/ssl
    openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
        -keyout /tmp/ssl/key.pem -out /tmp/ssl/cert.pem \
        -subj "/CN=duckdb-ui" 2>/dev/null
fi

# nginx reverse-proxies HTTPS 0.0.0.0:9999 → [::1]:8080
# Host header rewrite tricks DuckDB UI into thinking it's localhost
cat > /tmp/nginx.conf << 'NGINX'
events { worker_connections 64; }
http {
    server {
        listen 9999 ssl;
        ssl_certificate /tmp/ssl/cert.pem;
        ssl_certificate_key /tmp/ssl/key.pem;

        location / {
            proxy_pass http://[::1]:8080;
            proxy_http_version 1.1;
            proxy_set_header Host localhost:8080;
            proxy_set_header Origin http://localhost:8080;
            proxy_set_header Referer http://localhost:8080/;
            proxy_set_header X-Forwarded-Proto "";
            proxy_set_header X-Forwarded-For "";
            proxy_set_header X-Forwarded-Port "";
            proxy_buffering off;
        }
    }
}
NGINX

# Wait for DuckDB UI to start, then launch nginx
(sleep 3 && nginx -c /tmp/nginx.conf && echo "nginx SSL proxy running on :9999") &

exec python mcp_server.py
