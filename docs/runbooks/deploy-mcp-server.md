# Deploy MCP Server (duckdb-server)

The server source lives in `infra/duckdb-server/`. On the Hetzner host, `/opt/common-ground/duckdb-server` is a symlink to `/opt/dagster-pipeline/infra/duckdb-server/`, so `git pull` is the deploy mechanism.

## Procedure

```bash
# 1. Commit and push locally
git add infra/duckdb-server/...
git commit -m "feat: ..."
git push origin main

# 2. Pull on the server
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
cd /opt/dagster-pipeline && git pull

# 3. Rebuild the container (the compose file is in /opt/common-ground/)
cd /opt/common-ground
sudo docker compose build duckdb-server
sudo docker compose up -d duckdb-server
```

## Verify

```bash
curl https://mcp.common-ground.nyc/health
curl https://mcp.common-ground.nyc/api/catalog | jq '.schemas | length'
```

Expected: `"ok"` and `14`.
