#!/usr/bin/env bash
set -euo pipefail

SERVER="fattie@178.156.228.119"
SSH_KEY="$HOME/.ssh/id_ed25519_hetzner"
REMOTE_DIR="/opt/common-ground"

echo "=== Stopping old containers ==="
ssh -i "$SSH_KEY" "$SERVER" "cd $REMOTE_DIR && docker compose down 2>/dev/null || true"

echo "=== Creating remote directory ==="
ssh -i "$SSH_KEY" "$SERVER" "mkdir -p $REMOTE_DIR"

echo "=== Syncing project files ==="
rsync -avz --delete \
    -e "ssh -i $SSH_KEY" \
    --exclude '.env' \
    --exclude '.venv' \
    --exclude '.claude' \
    --exclude '.dlt' \
    --exclude '.meltano' \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --exclude '.git' \
    --exclude 'output/' \
    "$(dirname "$0")/" \
    "$SERVER:$REMOTE_DIR/"

echo "=== Syncing .env (no overwrite if exists) ==="
rsync -avz \
    -e "ssh -i $SSH_KEY" \
    --ignore-existing \
    "$(dirname "$0")/.env" \
    "$SERVER:$REMOTE_DIR/.env"

echo "=== Building and starting containers ==="
ssh -i "$SSH_KEY" "$SERVER" "cd $REMOTE_DIR && docker compose up -d --build"

echo "=== Checking container status ==="
ssh -i "$SSH_KEY" "$SERVER" "cd $REMOTE_DIR && docker compose ps"

echo ""
echo "Done. Access points:"
echo "  Dagster UI:  https://dagster.common-ground.nyc"
echo "  MCP:         https://mcp.common-ground.nyc/mcp"
echo "  DuckDB UI:   https://duckdb.common-ground.nyc"
