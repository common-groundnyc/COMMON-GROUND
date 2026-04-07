# 0009 — Git-based deploy replaces rsync

**Status:** Accepted (2026-04-07)

## Context

The original deploy was `infra/deploy.sh` — a bash script that rsynced the local tree to the Hetzner host. Problems:

- Diverging server state (edits on server not reflected in repo)
- No audit trail for what was deployed
- Had to re-rsync to redeploy even unchanged files
- The server's `/opt/common-ground/duckdb-server/` directory had diverged from `/opt/dagster-pipeline/infra/duckdb-server/`

## Decision

- Server checks out the repo at `/opt/dagster-pipeline/`
- `/opt/common-ground/duckdb-server` is a **symlink** to `/opt/dagster-pipeline/infra/duckdb-server/`
- Deploy = `git push` locally → `ssh` → `git pull` on server → `docker compose build duckdb-server && docker compose up -d duckdb-server`
- `infra/deploy.sh` is kept as legacy but is no longer primary

## Consequences

- **+** One source of truth (the git tree)
- **+** Full audit trail via git history
- **+** Redeploy is cheap (only fetches new commits)
- **+** The symlink makes `/opt/common-ground/docker-compose.yml` see the latest code automatically
- **−** Requires server to have git + GitHub auth configured
- **−** Two compose projects now bridge via `common-ground-shared` external network
