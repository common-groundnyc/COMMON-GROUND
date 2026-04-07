# Symlink deploy flow

The Hetzner host has **two compose projects** that share the same duckdb-server source via a symlink:

```
/opt/dagster-pipeline/              # git-tracked repo
├── docker-compose.yml              # dagster-code, dagster-daemon, dagster-webserver
└── infra/duckdb-server/            # source of truth for MCP server

/opt/common-ground/                 # legacy directory
├── docker-compose.yml              # postgres, duckdb-server, duck-ui, cloudflared, xyops
└── duckdb-server → /opt/dagster-pipeline/infra/duckdb-server   # SYMLINK
```

## Implications

- **Deploy the MCP server with `git pull`**, then rebuild from `/opt/common-ground/`:
  ```bash
  cd /opt/dagster-pipeline && git pull
  cd /opt/common-ground && sudo docker compose build duckdb-server && sudo docker compose up -d duckdb-server
  ```
- The two compose projects **bridge via the `common-ground-shared` external Docker network**.
- Editing files under `/opt/common-ground/duckdb-server/` edits the git tree directly. Don't.

**See also:** `docs/adr/0009-git-deploy-over-rsync.md`, `docs/runbooks/deploy-mcp-server.md`.
