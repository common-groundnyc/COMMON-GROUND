# Cloudflare Tunnels — Replace Exposed Ports with Zero-Trust Access

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all publicly exposed service ports on Hetzner with Cloudflare Tunnels, giving every service a clean `*.common-ground.nyc` subdomain with zero-trust auth for team dashboards.

**Architecture:** Install `cloudflared` on the Hetzner server as a Docker service. It creates an outbound-only connection to Cloudflare's edge — no inbound ports needed. Public services (MCP) get open access. Team services (Dagster, Duck-UI, xyOps) get Cloudflare Access with email OTP. This replaces Caddy entirely for auth and TLS. Downstream references (website code, MCP configs, deploy scripts, docs) all get updated to use the new URLs.

**Tech Stack:** Cloudflare Tunnel (`cloudflared`), Cloudflare Access (Zero Trust), Docker Compose

---

## What Changes for Your Workflow

| Before | After |
|--------|-------|
| Dagster UI at `http://178.156.228.119:3000` | `https://dagster.common-ground.nyc` (email OTP login) |
| MCP at `http://178.156.228.119:4213/mcp` | `https://mcp.common-ground.nyc/mcp` |
| DuckDB UI at `https://178.156.228.119:9999` (self-signed) | `https://duckdb.common-ground.nyc` (real cert) |
| Duck-UI at `http://178.156.228.119:5522` | `https://duck-ui.common-ground.nyc` (email OTP) |
| xyOps at `http://178.156.228.119:5580` | `https://xyops.common-ground.nyc` (email OTP) |
| MinIO Console at `https://178.156.228.119:9001` | `https://minio.common-ground.nyc` (email OTP) |
| Firewall allows ports 3000/4213/5522/5580/9999 | Close all — only 22/5432/9000 remain |
| Caddy container for auth | **Removed** — Cloudflare handles TLS + auth |

**Important:** `mcp.commonground.nyc` (no hyphen) doesn't exist as a domain. The website code currently references it but the actual domain is `common-ground.nyc`. This plan uses `mcp.common-ground.nyc` — the website references will be updated.

---

## Service Routing Map

| Subdomain | Internal target | Auth | Public? |
|-----------|----------------|------|---------|
| `mcp.common-ground.nyc` | `http://duckdb-server:4213` | None | Yes — users paste this into LLM configs. **NEVER** add to Access policy. |
| `duckdb.common-ground.nyc` | `https://duckdb-server:9999` | None | Yes — DuckDB UI has its own Auth0. Set "No TLS Verify" ON (self-signed). |
| `dagster.common-ground.nyc` | `http://dagster-webserver:3000` | Cloudflare Access (email OTP) | Team only. Dagster is in separate compose — needs shared `tunnel_net` network. |
| `duck-ui.common-ground.nyc` | `http://duck-ui:5522` | Cloudflare Access (email OTP) | Team only. Internal port is 5522 (host-mapped to 5580). |
| `xyops.common-ground.nyc` | `http://xyops:5522` | Cloudflare Access (email OTP) | Team only. Internal port is 5522 (host-mapped to 5580). |
| `minio.common-ground.nyc` | `https://minio:9001` | Cloudflare Access (email OTP) | Team only. Set "No TLS Verify" ON (self-signed). |

---

## File Structure

### Hetzner server (`/opt/common-ground/`)

| File | Action | Responsibility |
|------|--------|----------------|
| `docker-compose.yml` | Modify | Add `cloudflared` service, remove exposed ports from tunneled services |
| `.env` | Modify | Add `TUNNEL_TOKEN` |

### Pipeline repo (`~/Desktop/dagster-pipeline/`)

| File | Action | Responsibility |
|------|--------|----------------|
| `docker-compose.yml` | Modify | Remove Caddy service and volumes (tunnel replaces it) |
| `Caddyfile` | Delete | No longer needed |
| `infra/hetzner-docker-compose.yml` | Modify | Mirror of server compose (version control) |
| `infra/deploy.sh` | Modify | Update URLs in deploy output |
| `docs/LOGINS.md` | Modify | Update all URLs to subdomains |
| `docs/BETA-LAUNCH-RUNBOOK.md` | Modify | Update access table and procedures |
| `CLAUDE.md` | Modify | Update server section URLs |

### Website repo (`~/Desktop/common-ground-website/`)

| File | Action | Responsibility |
|------|--------|----------------|
| `src/app/connect/page.tsx` | Modify | Change `mcp.commonground.nyc` → `mcp.common-ground.nyc` |
| `src/components/light/wire-it-up.tsx` | Modify | Change `mcp.commonground.nyc` → `mcp.common-ground.nyc` |
| `src/components/light/get-it.tsx` | Modify | Change `mcp.commonground.nyc` → `mcp.common-ground.nyc` |

### Local config

| File | Action | Responsibility |
|------|--------|----------------|
| `~/.mcp.json` | Modify | Change DuckDB MCP URL to `https://mcp.common-ground.nyc/mcp` |

---

## Task 1: Create Cloudflare Tunnel

**Priority:** First — everything else depends on the tunnel existing.

**Files:**
- Cloudflare Zero Trust dashboard (web UI or API)

- [ ] **Step 1: Log into Cloudflare Zero Trust**

Go to https://one.dash.cloudflare.com/ → select the account that owns `common-ground.nyc`.

If you don't have Zero Trust set up yet, Cloudflare will prompt you to create a team name (e.g., `common-ground`). The free plan supports up to 50 users.

- [ ] **Step 2: Create a tunnel**

Navigate to: Networks → Tunnels → Create a tunnel

- Name: `common-ground-prod`
- Environment: Choose "Docker" as the connector
- Copy the tunnel token — it looks like `eyJhIjoiN2...` (long base64 string)

**Save this token** — it goes into the server's `.env` file.

- [ ] **Step 3: Configure public hostnames in the tunnel**

In the tunnel config UI, add these public hostnames:

| Public hostname | Service | Notes |
|-----------------|---------|-------|
| `mcp.common-ground.nyc` | `http://duckdb-server:4213` | No auth. **NEVER** add to Access policy — MCP clients can't handle auth challenges. |
| `duckdb.common-ground.nyc` | `https://duckdb-server:9999` | No auth (has own Auth0). Set "No TLS Verify" ON (self-signed cert). |
| `dagster.common-ground.nyc` | `http://dagster-webserver:3000` | Will add Access policy later. Reachable via shared `tunnel_net` Docker network. |
| `duck-ui.common-ground.nyc` | `http://duck-ui:5522` | Will add Access policy later. Internal port 5522 (was host-mapped to 5580). |
| `xyops.common-ground.nyc` | `http://xyops:5522` | Will add Access policy later. Internal port 5522 (was host-mapped to 5580). |
| `minio.common-ground.nyc` | `https://minio:9001` | Will add Access policy later. Set "No TLS Verify" ON (self-signed cert). |

**Self-signed certs:** DuckDB UI (9999) and MinIO Console (9001) use self-signed certs. Enable "No TLS Verify" in the tunnel config for those hostnames. This only affects the `cloudflared`→origin leg; users see valid Cloudflare-issued certs on the public side.

**Container DNS:** `cloudflared` resolves `duckdb-server`, `duck-ui`, `xyops`, `minio` via Docker's built-in DNS (same compose). For `dagster-webserver` (separate pipeline compose), both composes join the shared external `tunnel_net` network so `cloudflared` can resolve the container name. See Task 2 for the network setup.

- [ ] **Step 4: Verify DNS records were created**

Cloudflare automatically creates CNAME records for each public hostname pointing to the tunnel. Verify:

```bash
dig +short mcp.common-ground.nyc
# Expected: <tunnel-id>.cfargotunnel.com (CNAME)

dig +short dagster.common-ground.nyc
# Expected: <tunnel-id>.cfargotunnel.com (CNAME)
```

---

## Task 2: Deploy cloudflared on Hetzner

**Priority:** Second — the tunnel connector must run on the server.

**Files:**
- Modify: `/opt/common-ground/.env` (on Hetzner)
- Modify: `/opt/common-ground/docker-compose.yml` (on Hetzner)
- Modify: `~/Desktop/dagster-pipeline/infra/hetzner-docker-compose.yml` (local mirror)

- [ ] **Step 1: Add tunnel token to server .env**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
echo 'TUNNEL_TOKEN=eyJhIjoiN2...' | sudo tee -a /opt/common-ground/.env
# Replace with actual token from Task 1 Step 2
```

- [ ] **Step 2: Add cloudflared service to server docker-compose.yml**

SSH to server and add this service to `/opt/common-ground/docker-compose.yml`:

```yaml
  cloudflared:
    image: cloudflare/cloudflared:2025.4.1
    command: tunnel --no-autoupdate run
    environment:
      TUNNEL_TOKEN: ${TUNNEL_TOKEN}
    networks:
      - default
      - tunnel_net
    depends_on:
      - duckdb-server
      - minio
    deploy:
      resources:
        limits:
          memory: 256M
    healthcheck:
      test: ["CMD", "cloudflared", "tunnel", "info"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped

networks:
  tunnel_net:
    name: tunnel_net
    driver: bridge
```

**Cross-compose networking:** `cloudflared` and the Dagster webserver (in the pipeline compose) both attach to the external `tunnel_net` network. This lets `cloudflared` resolve `dagster-webserver:3000` by container name.

The pipeline's `docker-compose.yml` must also join this network:

```yaml
networks:
  tunnel_net:
    external: true

services:
  dagster-webserver:
    networks:
      - frontend
      - backend
      - tunnel_net  # so cloudflared can reach it
```

**Why not `host.docker.internal`:** That does not resolve on Linux by default. A shared external Docker network is the reliable cross-compose solution.

**Image pinned** to `2025.4.1` to avoid surprise breaking changes. Update periodically.

- [ ] **Step 3: Remove exposed ports from tunneled services**

In the same docker-compose.yml, remove (or bind to 127.0.0.1) the ports that are now tunneled:

```yaml
  duckdb-server:
    # REMOVE these lines:
    # ports:
    #   - "4213:4213"
    #   - "9999:9999"
    # REPLACE with localhost-only (for healthchecks and local debugging):
    ports:
      - "127.0.0.1:4213:4213"
      - "127.0.0.1:9999:9999"

  duck-ui:
    ports:
      - "127.0.0.1:5522:5522"

  xyops:
    ports:
      - "127.0.0.1:5580:5522"

  minio:
    # Keep 9000 open for pipeline S3 access (not tunneled)
    # Bind console to localhost only
    ports:
      - "9000:9000"
      - "127.0.0.1:9001:9001"
```

- [ ] **Step 4: Start cloudflared**

```bash
cd /opt/common-ground
sudo docker compose up -d cloudflared
sudo docker logs common-ground-cloudflared-1 --tail 10
# Expected: "Connection ... registered" messages, "Registered tunnel connection"
```

- [ ] **Step 5: Test the tunnel**

```bash
# From your local machine:
curl -s -o /dev/null -w "%{http_code}" https://mcp.common-ground.nyc/mcp -X POST
# Expected: 200 or 405 (Method Not Allowed) — means tunnel is working

curl -s -o /dev/null -w "%{http_code}" https://dagster.common-ground.nyc
# Expected: 200 (Dagster UI) or 302 (redirect) — if pipeline is deployed
# OR: 502 (Bad Gateway) — if pipeline isn't deployed yet (expected)
```

- [ ] **Step 6: Update local mirror**

Copy the updated compose file back:

```bash
scp -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119:/opt/common-ground/docker-compose.yml \
  ~/Desktop/dagster-pipeline/infra/hetzner-docker-compose.yml
```

---

## Task 3: Set Up Cloudflare Access Policies

**Priority:** Third — protect team-only dashboards.

**Files:**
- Cloudflare Zero Trust dashboard (web UI)

- [ ] **Step 1: Create an Access application for team dashboards**

Go to: https://one.dash.cloudflare.com/ → Access → Applications → Add an application

- Application type: Self-hosted
- Application name: `Common Ground — Team Dashboards`
- Application domain: `dagster.common-ground.nyc`
- Add additional hostnames:
  - `duck-ui.common-ground.nyc`
  - `xyops.common-ground.nyc`
  - `minio.common-ground.nyc`

- [ ] **Step 2: Create the access policy**

- Policy name: `Team members`
- Action: Allow
- Include rule: **Emails** — add your email address (and any team members)
- Authentication: One-time PIN (email OTP)

This means: when you visit `dagster.common-ground.nyc`, Cloudflare shows a login page. Enter your email, get a code, enter it. Session lasts 24 hours.

- [ ] **Step 3: Verify team access works**

Open `https://dagster.common-ground.nyc` in a browser:
- Expected: Cloudflare Access login page
- Enter your email → receive OTP → enter code
- Expected: Dagster UI loads (or 502 if pipeline not deployed yet)

- [ ] **Step 4: Verify public MCP access works (no auth)**

```bash
curl -s https://mcp.common-ground.nyc/mcp -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}'
# Expected: JSON response with server capabilities (no auth challenge)
```

---

## Task 4: Update Hetzner Firewall

**Priority:** After tunnel is verified working.

**Files:**
- Hetzner Cloud Firewall (API or dashboard)

- [ ] **Step 1: Remove tunneled ports from firewall**

Use `mcp__hetzner__set_firewall_rules` to update the `duckdb-firewall` (ID: 10638515). Remove ports that are now tunneled and keep only:

| Port | Protocol | Source | Purpose |
|------|----------|--------|---------|
| 22 | TCP | YOUR_IP/32, YOUR_IPv6/64 | SSH |
| 5432 | TCP | YOUR_IP/32, YOUR_IPv6/64 | Postgres (pipeline direct) |
| 9000 | TCP | YOUR_IP/32, YOUR_IPv6/64 | MinIO S3 API (pipeline direct) |
**Removed:** 80, 443, 3000 (Dagster), 4213 (MCP), 5522 (Duck-UI), 5580 (xyOps), 9001 (MinIO Console), 9999 (DuckDB UI)

**Why no 80/443:** Cloudflare Tunnels are outbound-only — Cloudflare never initiates inbound connections. Health checks run through the tunnel connection itself. No inbound HTTP/HTTPS ports needed.

- [ ] **Step 2: Do the same for France server** (ID: 10751352)

Same rules, adapted for whatever services run there.

- [ ] **Step 3: Verify locked down**

```bash
# From your machine — should timeout:
curl -s --connect-timeout 5 http://178.156.228.119:4213/mcp
# Expected: connection timeout

# Through tunnel — should work:
curl -s https://mcp.common-ground.nyc/mcp -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}'
# Expected: JSON response
```

---

## Task 5: Remove Caddy from Pipeline Compose

**Priority:** After tunnel is working — Caddy is no longer needed.

**Files:**
- Modify: `~/Desktop/dagster-pipeline/docker-compose.yml`
- Delete: `~/Desktop/dagster-pipeline/Caddyfile`

- [ ] **Step 1: Remove Caddy service and volumes from docker-compose.yml**

Remove the entire `caddy:` service block and the `caddy_data:` / `caddy_config:` volume declarations.

The pipeline compose should have only:
- `dagster-code`
- `dagster-webserver`
- `dagster-daemon`

The webserver can now bind to `0.0.0.0:3000` (not just `127.0.0.1`) since the firewall + tunnel handle access control:

```yaml
  dagster-webserver:
    ports:
      - "3000:3000"
```

- [ ] **Step 2: Delete Caddyfile**

```bash
rm ~/Desktop/dagster-pipeline/Caddyfile
```

- [ ] **Step 3: Verify compose is valid**

```bash
cd ~/Desktop/dagster-pipeline
docker compose config --quiet 2>/dev/null || python3 -c "import yaml; yaml.safe_load(open('docker-compose.yml'))"
```

---

## Task 6: Update Website MCP URLs

**Priority:** Before website deploy — users need the correct URL.

**Files:**
- Modify: `~/Desktop/common-ground-website/src/app/connect/page.tsx`
- Modify: `~/Desktop/common-ground-website/src/components/light/wire-it-up.tsx`
- Modify: `~/Desktop/common-ground-website/src/components/light/get-it.tsx`

- [ ] **Step 1: Update connect page**

In `src/app/connect/page.tsx`, replace all occurrences:

| Old | New |
|-----|-----|
| `mcp.commonground.nyc` | `mcp.common-ground.nyc` |

Three locations:
- Line 87: JSON config URL
- Line 102: Claude Code CLI command
- Line 114: "Point your MCP client at" text

- [ ] **Step 2: Update wire-it-up component**

In `src/components/light/wire-it-up.tsx`, replace all occurrences:

| Old | New |
|-----|-----|
| `mcp.commonground.nyc` | `mcp.common-ground.nyc` |

Two locations:
- Line 46: JSON config URL
- Line 65: Claude Code CLI command

- [ ] **Step 3: Update get-it component**

In `src/components/light/get-it.tsx`, replace all occurrences:

| Old | New |
|-----|-----|
| `mcp.commonground.nyc` | `mcp.common-ground.nyc` |

Three locations:
- Line 14: clipboard copy text
- Line 18: PostHog event property
- Line 62: display text (appears twice in ternary)

- [ ] **Step 4: Build and verify**

```bash
cd ~/Desktop/common-ground-website
npm run build
# Expected: build succeeds, no errors
```

- [ ] **Step 5: Deploy to Cloudflare**

```bash
npm run deploy
```

---

## Task 7: Update Local MCP Config

**Priority:** After tunnel is live — so your Claude Code session uses the new URL.

**Files:**
- Modify: `~/.mcp.json`

- [ ] **Step 1: Update DuckDB MCP URL**

In `~/.mcp.json`, change the `duckdb` entry:

```json
"duckdb": {
  "type": "http",
  "url": "https://mcp.common-ground.nyc/mcp"
}
```

Old value: `http://178.156.228.119:4213/mcp`

- [ ] **Step 2: Verify connection**

Restart Claude Code (or run `/mcp`) and verify the DuckDB MCP server connects through the tunnel without the OAuth 404 error (Cloudflare terminates TLS, so the client-to-tunnel connection is standard HTTPS).

**Note:** The OAuth stub we added to the MCP server is still useful — other MCP clients may connect directly. But through the Cloudflare tunnel, the error should not appear since Cloudflare handles the TLS handshake.

---

## Task 8: Update Documentation and References

**Priority:** Last — documents the final state.

**Files:**
- Modify: `~/Desktop/dagster-pipeline/docs/LOGINS.md`
- Modify: `~/Desktop/dagster-pipeline/docs/BETA-LAUNCH-RUNBOOK.md`
- Modify: `~/Desktop/dagster-pipeline/CLAUDE.md`
- Modify: `~/Desktop/dagster-pipeline/infra/deploy.sh`

- [ ] **Step 1: Update LOGINS.md**

Replace all IP:port references with subdomain URLs:

| Old | New |
|-----|-----|
| `http://178.156.228.119:4213` | `https://mcp.common-ground.nyc` |
| `https://178.156.228.119:9999` | `https://duckdb.common-ground.nyc` |
| `http://178.156.228.119:5522` | `https://duck-ui.common-ground.nyc` |
| `http://178.156.228.119:5580` | `https://xyops.common-ground.nyc` |
| `https://178.156.228.119:9001` | `https://minio.common-ground.nyc` |

Keep IP references for SSH and Postgres/MinIO S3 API (those stay on direct IP).

Add a section about Cloudflare Access:
```markdown
## Cloudflare Access (Team Dashboards)

Dagster, Duck-UI, xyOps, and MinIO Console are behind Cloudflare Access.
- Auth: email OTP (one-time code sent to your email)
- Session: 24 hours
- Add team members: https://one.dash.cloudflare.com/ → Access → Applications → Edit policy
```

- [ ] **Step 2: Update BETA-LAUNCH-RUNBOOK.md**

Update the access table:

```markdown
| Service | URL | Auth | Who |
|---------|-----|------|-----|
| Website | https://common-ground.nyc | Public | Everyone |
| MCP Server | https://mcp.common-ground.nyc/mcp | None | Everyone |
| DuckDB UI | https://duckdb.common-ground.nyc | Auth0 (built-in) | Everyone |
| Dagster UI | https://dagster.common-ground.nyc | Cloudflare Access (email OTP) | Team only |
| Duck-UI | https://duck-ui.common-ground.nyc | Cloudflare Access (email OTP) | Team only |
| xyOps | https://xyops.common-ground.nyc | Cloudflare Access (email OTP) | Team only |
| MinIO Console | https://minio.common-ground.nyc | Cloudflare Access (email OTP) | Team only |
```

Remove the "Caddy basic auth" section. Remove references to `DAGSTER_UI_PASSWORD`.

- [ ] **Step 3: Update CLAUDE.md**

In the Hetzner Server section, update:

```markdown
## Hetzner Server (178.156.228.119)

SSH: `ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119`

Services (via Cloudflare Tunnel):
- MCP: https://mcp.common-ground.nyc/mcp
- DuckDB UI: https://duckdb.common-ground.nyc
- Dagster UI: https://dagster.common-ground.nyc (Cloudflare Access)

Direct access (firewall-restricted):
- Postgres: 178.156.228.119:5432
- MinIO S3: 178.156.228.119:9000
```

- [ ] **Step 4: Update deploy.sh**

In `infra/deploy.sh`, update the output URLs:

```bash
echo "  Dagster UI:  https://dagster.common-ground.nyc"
echo "  MCP:         https://mcp.common-ground.nyc/mcp"
echo "  DuckDB UI:   https://duckdb.common-ground.nyc"
```

Also update `SERVER="root@..."` to `SERVER="fattie@178.156.228.119"`.

- [ ] **Step 5: Update xyOps base URL**

In `infra/hetzner-docker-compose.yml` (and on the server), update:

```yaml
  xyops:
    environment:
      XYOPS_base_app_url: "https://xyops.common-ground.nyc"
```

---

## Execution Order

```
Task 1: Create Cloudflare Tunnel     ← Cloudflare dashboard, creates tunnel + DNS
Task 2: Deploy cloudflared            ← Server-side, starts the connector
Task 3: Cloudflare Access Policies    ← Dashboard, protects team services
Task 4: Update Hetzner Firewall       ← Close ports that are now tunneled
Task 5: Remove Caddy                  ← Local repo, clean up unused service
Task 6: Update Website URLs           ← Fix mcp.commonground.nyc → mcp.common-ground.nyc
Task 7: Update Local MCP Config       ← Your ~/.mcp.json
Task 8: Update Docs                   ← LOGINS.md, runbook, CLAUDE.md, deploy.sh
```

**Parallelizable:** Tasks 5, 6, 7, 8 are all independent and can run in parallel after Task 4.

---

## Rollback

If the tunnel breaks:
1. Stop cloudflared: `sudo docker compose stop cloudflared`
2. Re-open ports in Hetzner firewall (add back 80, 443, 4213, 9999, 5522, 5580, 9001)
3. Services are still bound to `127.0.0.1` — change back to `0.0.0.0` in docker-compose
4. Restart services: `sudo docker compose up -d`
5. Revert `~/.mcp.json` to use `http://178.156.228.119:4213/mcp`
6. Caddy can be re-added if needed (Caddyfile is in git history)
