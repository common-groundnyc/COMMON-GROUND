# Deploy Pipeline & Launch Beta

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the hardened Dagster pipeline to Hetzner, connect it through the Cloudflare Tunnel, deploy the website with updated MCP URLs, and verify the full stack end-to-end.

**Architecture:** rsync the pipeline repo to `/opt/dagster-pipeline/` on Hetzner, create the shared `tunnel_net` Docker network, build and start the 3 Dagster containers, then verify the tunnel serves the Dagster UI. Deploy the website to Cloudflare Workers. Run a test materialization to confirm the pipeline→DuckLake→MinIO chain works.

**Tech Stack:** Docker Compose, rsync, Cloudflare Workers (wrangler), Dagster, dlt, DuckLake

**Servers:**
- Hetzner prod: `fattie@178.156.228.119` (SSH key: `~/.ssh/id_ed25519_hetzner`)
- Pipeline repo: `~/Desktop/dagster-pipeline/`
- Website repo: `~/Desktop/common-ground-website/`

---

## What This Deploys

The pipeline has never run on the server before — it has only run locally in Docker on the Mac Studio. This deploy:
1. Copies the pipeline code to the server
2. Builds the Docker image on the server
3. Starts 3 containers (code server, webserver, daemon)
4. Connects the webserver to the Cloudflare Tunnel via shared `tunnel_net` network
5. The Dagster UI becomes accessible at `https://dagster.common-ground.nyc`

---

## File Structure

| File | Action | Where | Responsibility |
|------|--------|-------|----------------|
| Pipeline repo (entire) | rsync | Server `/opt/dagster-pipeline/` | Pipeline code + compose |
| `.env.secrets` | Decrypt + copy | Server `/opt/dagster-pipeline/.env.secrets` | Runtime credentials |
| `tunnel_net` Docker network | Create | Server | Shared between infra + pipeline composes |
| Website repo | Deploy | Cloudflare Workers | Updated MCP URLs live |

---

## Task 1: Create Shared Docker Network on Server

**Priority:** First — both composes reference `tunnel_net` as external.

- [ ] **Step 1: Create the tunnel_net network**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "sudo docker network create tunnel_net"
```

Expected: network ID hash printed.

- [ ] **Step 2: Connect cloudflared to the network**

The infra compose's `cloudflared` service needs to join `tunnel_net` so it can reach `dagster-webserver` in the pipeline compose.

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "sudo docker network connect tunnel_net common-ground-cloudflared-1"
```

Expected: no output (success).

- [ ] **Step 3: Verify**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "sudo docker network inspect tunnel_net --format '{{range .Containers}}{{.Name}} {{end}}'"
```

Expected: `common-ground-cloudflared-1` listed.

---

## Task 2: Deploy Pipeline to Server

**Priority:** Second — the main deploy.

- [ ] **Step 1: Create pipeline directory on server**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "sudo mkdir -p /opt/dagster-pipeline && sudo chown fattie:fattie /opt/dagster-pipeline"
```

- [ ] **Step 2: Decrypt secrets for deployment**

```bash
cd ~/Desktop/dagster-pipeline
sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc > .env.secrets
```

- [ ] **Step 3: rsync pipeline to server**

```bash
cd ~/Desktop/dagster-pipeline
rsync -avz --delete \
  -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  --exclude '.venv' \
  --exclude '.claude' \
  --exclude '.git' \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  --exclude '.dagster-home' \
  --exclude 'data/' \
  --exclude 'reports/' \
  --exclude 'infra/' \
  --exclude 'docs/' \
  --exclude 'test_*.duckdb' \
  --exclude '.tmp_dagster_home_*' \
  ./ \
  fattie@178.156.228.119:/opt/dagster-pipeline/
```

This copies: `docker-compose.yml`, `Dockerfile`, `workspace.yaml`, `dagster.yaml`, `pyproject.toml`, `uv.lock`, `src/`, `patches/`, `models/`, `.dlt/config.toml`, `.env.secrets`, `.sops.yaml`.

- [ ] **Step 4: Create dagster-home directory on server**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "mkdir -p /opt/dagster-pipeline/dagster-home"
```

The Dagster home directory needs to exist for the volume mount. The `dagster.yaml` is copied by the Dockerfile into `/dagster-home/` inside the container, but the host mount also needs the directory.

- [ ] **Step 5: Copy dagster.yaml to the host dagster-home**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cp /opt/dagster-pipeline/dagster.yaml /opt/dagster-pipeline/dagster-home/dagster.yaml"
```

The webserver and daemon mount `./dagster-home:/dagster-home` — they need `dagster.yaml` there.

- [ ] **Step 6: Build the Docker image**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/dagster-pipeline && docker compose build"
```

Expected: image builds successfully. This takes 2-5 minutes (installs Python deps, applies patches).

- [ ] **Step 7: Start the pipeline**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/dagster-pipeline && docker compose up -d"
```

Expected: 3 containers start (dagster-code, dagster-webserver, dagster-daemon).

- [ ] **Step 8: Verify containers are running**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/dagster-pipeline && docker compose ps"
```

Expected: all 3 containers `Up`, webserver healthy.

- [ ] **Step 9: Check dagster-webserver logs**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "docker logs dagster-webserver --tail 20"
```

Expected: `Serving dagster-webserver on http://0.0.0.0:3000` (or similar).

---

## Task 3: Verify Dagster Through Tunnel

**Priority:** After deploy — confirm the tunnel routes to the webserver.

- [ ] **Step 1: Check dagster-webserver joined tunnel_net**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "sudo docker network inspect tunnel_net --format '{{range .Containers}}{{.Name}} {{end}}'"
```

Expected: both `common-ground-cloudflared-1` and `dagster-webserver` listed.

- [ ] **Step 2: Test through tunnel**

```bash
curl -s -o /dev/null -w "%{http_code}" --connect-timeout 10 https://dagster.common-ground.nyc
```

Expected: `200` (Dagster UI HTML).

If you get `502`:
- Check cloudflared can resolve `dagster-webserver`: the container might be named `dagster-webserver` or `dagster-pipeline-dagster-webserver-1` depending on compose project name.
- Check: `ssh fattie@178.156.228.119 "docker ps --format '{{.Names}}' | grep webserver"`
- If the name is `dagster-pipeline-dagster-webserver-1`, update the tunnel hostname config in the Cloudflare dashboard to use that name.

- [ ] **Step 3: Open Dagster UI in browser**

Visit: https://dagster.common-ground.nyc

Expected: Dagster UI loads. If Cloudflare Access is configured (Task 3 from tunnels plan), you'll see an email OTP prompt first.

---

## Task 4: Deploy Website

**Priority:** Can run in parallel with Tasks 2-3.

- [ ] **Step 1: Build website**

```bash
cd ~/Desktop/common-ground-website
npm run build
```

Expected: build succeeds, all pages generated.

- [ ] **Step 2: Deploy to Cloudflare Workers**

```bash
npm run deploy
```

Expected: deployment succeeds, URL printed.

- [ ] **Step 3: Verify MCP URL on live site**

```bash
curl -s https://common-ground.nyc/connect | grep -o 'mcp\.common-ground\.nyc'
```

Expected: `mcp.common-ground.nyc` (not `mcp.commonground.nyc`).

- [ ] **Step 4: Enable HSTS in Cloudflare dashboard**

In Cloudflare dashboard for common-ground.nyc:
1. SSL/TLS → Edge Certificates → Always Use HTTPS: **ON**
2. SSL/TLS → Edge Certificates → HSTS: Enable with max-age 6 months, includeSubDomains

---

## Task 5: Test Pipeline End-to-End

**Priority:** Last — verifies the full chain works.

- [ ] **Step 1: Open Dagster UI**

Go to: https://dagster.common-ground.nyc

Navigate to Assets.

- [ ] **Step 2: Materialize a small asset**

Pick a small dataset — e.g., `recreation/park_events` or `health/restaurant_inspections`.

Click the asset → Materialize.

- [ ] **Step 3: Watch the run**

Go to Runs tab. Watch the run progress:
1. Run should start and show "In Progress"
2. The docker_executor should spawn a step container
3. dlt should extract data from Socrata API
4. dlt should load data into DuckLake (Postgres catalog + MinIO parquet)
5. Run should complete with "Success"

If the run fails, check:
- **Step container logs**: click the failed step → View Logs
- **Daemon logs**: `ssh fattie@178.156.228.119 "docker logs dagster-daemon --tail 50"`
- **Common failures**:
  - `DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG` not found → `.env.secrets` not in container, check `env_file` mount
  - Connection refused to Postgres → wrong password (check rotation was applied) or Postgres SSL issue
  - MinIO SSL error → check `CLIENT_KWARGS={"verify": false}` is in `.env.secrets`
  - Network `dagster-pipeline_backend` not found → the compose project name might differ on the server

- [ ] **Step 4: Verify data landed in DuckLake**

Use the MCP server to check:

```bash
curl -s https://mcp.common-ground.nyc/mcp -X POST \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","method":"tools/call","id":2,"params":{"name":"sql_query","arguments":{"sql":"SELECT schema_name, table_name, estimated_size FROM information_schema.tables WHERE schema_name NOT IN ('"'"'information_schema'"'"','"'"'pg_catalog'"'"') ORDER BY estimated_size DESC LIMIT 5"}}}'
```

Or just use the DuckDB MCP tool in Claude Code to run a query.

- [ ] **Step 5: Celebrate**

You're live. `https://common-ground.nyc` serves the website, `https://mcp.common-ground.nyc/mcp` serves the data, and `https://dagster.common-ground.nyc` manages the pipeline.

---

## Execution Order

```
Task 1: Create tunnel_net network     ← 1 minute, server
Task 2: Deploy pipeline               ← 5-10 minutes, rsync + build
Task 3: Verify Dagster through tunnel  ← 2 minutes, curl + browser
Task 4: Deploy website                 ← 2 minutes, independent (can parallel with 2-3)
Task 5: Test pipeline e2e              ← 10 minutes, Dagster UI
```

**Parallelizable:** Task 4 (website deploy) is independent of Tasks 1-3.

---

## Rollback

If the pipeline deploy breaks:
1. Stop pipeline: `ssh fattie@178.156.228.119 "cd /opt/dagster-pipeline && docker compose down"`
2. The infra compose (Postgres, MinIO, DuckDB MCP) is unaffected — it's a separate compose at `/opt/common-ground/`
3. The data is safe — DuckLake catalog in Postgres, parquet in MinIO
4. Dagster UI at `https://dagster.common-ground.nyc` will show 502 (expected when pipeline is down)
