# Beta Launch Security Hardening — common-ground.nyc

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the Common Ground infrastructure (Dagster pipeline + website) for public beta launch at common-ground.nyc.

**Architecture:** Two repos — `dagster-pipeline` (data pipeline on Hetzner VPS) and `common-ground-website` (Next.js on Cloudflare Workers). The pipeline runs on a Hetzner server (178.156.228.119) with Postgres, MinIO, and DuckDB exposed. The website is static with one external API call (NYC geosearch). This plan locks down the server, hardens Docker, encrypts secrets, adds security headers, and documents everything for the team.

**Tech Stack:** Dagster + dlt + Docker Compose (pipeline), Next.js 16 + Cloudflare Workers (website), Hetzner Cloud (infra), SOPS + age (secrets), Caddy (reverse proxy), Grype (image scanning)

**Repos:**
- Pipeline: `~/Desktop/dagster-pipeline/`
- Website: `~/Desktop/common-ground-website/`
- Hetzner server: `root@178.156.228.119` (SSH key: `~/.ssh/id_ed25519_hetzner`)

---

## What Changes for Your Workflow

**Nothing breaks.** Here's what you'll notice:

| Before | After | Impact |
|--------|-------|--------|
| Open Dagster UI directly at `:3000` | Open at `https://dagster.common-ground.nyc` with password prompt | One-time login per browser session |
| Secrets in `.dlt/secrets.toml` plaintext | Secrets in `.env.secrets` (decrypted from encrypted `.env.secrets.enc` at deploy) | `sops --input-type dotenv --output-type dotenv .env.secrets.enc` to edit |
| `deploy.sh` rsyncs and restarts | `deploy.sh` decrypts secrets then rsyncs and restarts | One extra line in deploy script |
| Postgres connection unencrypted | Postgres connection encrypted (TLS) | Connection string gets `?sslmode=require` — invisible |
| MinIO on default creds | MinIO on rotated creds with scoped service account | New access key in `.env.secrets` |
| No firewall on Hetzner | Ports 3000/5432/9000/9001 restricted to your IPs | If your IP changes, update firewall rule |

**Credentials you'll need to remember:**
- Dagster UI: basic auth password (stored in `.env.secrets.enc`, shared with team)
- SSH: unchanged (key-based)
- Everything else: env vars injected by docker-compose, no manual entry

---

## File Structure

### Pipeline repo (`~/Desktop/dagster-pipeline/`)

| File | Action | Responsibility |
|------|--------|----------------|
| `docker-compose.yml` | Modify | Remove socket from code server, add networks, add Caddy service, replace secret mounts with env_file |
| `Caddyfile` | Create | Reverse proxy with basic auth for Dagster UI |
| `.env.secrets` | Create | Plaintext env vars (gitignored, decrypted at deploy) |
| `.env.secrets.enc` | Create | SOPS-encrypted secrets (committed to git) |
| `.sops.yaml` | Create | SOPS config — which age keys can decrypt |
| `.gitignore` | Modify | Add `.env.secrets`, `.env.secrets.enc` patterns |
| `src/dagster_pipeline/definitions.py` | Modify | Replace hardcoded `/Users/fattie2020/` paths with env vars |
| `CLAUDE.md` | Modify | Redact plaintext credentials, reference `.env.secrets` |
| `docs/BETA-LAUNCH-RUNBOOK.md` | Create | Team reference: logins, IPs, rotation procedures, emergency contacts |

### Website repo (`~/Desktop/common-ground-website/`)

| File | Action | Responsibility |
|------|--------|----------------|
| `next.config.ts` | Modify | Security headers (CSP, HSTS, X-Frame-Options, etc.) |
| `public/_headers` | Create | Static asset headers for Cloudflare Pages |
| `.env.local` | Modify | Remove dead `TWENTY_FIRST_API_KEY` |

### Hetzner server (`root@178.156.228.119`)

| File | Action | Responsibility |
|------|--------|----------------|
| `/opt/common-ground/docker-compose.yml` | Modify | Postgres SSL, MinIO TLS, bind ports to 127.0.0.1 |
| `/opt/postgres-ssl/server.crt` + `server.key` | Create | Self-signed TLS cert for Postgres |
| `/opt/minio-certs/public.crt` + `private.key` | Create | Self-signed TLS cert for MinIO |
| `/etc/ssh/sshd_config` | Modify | Disable root login, disable password auth |
| Hetzner Cloud Firewall | Create | Restrict ports to team IPs |

---

## Task 1: Hetzner Cloud Firewall

**Priority:** CRITICAL — this is the outer perimeter and blocks all unauthorized access at the hypervisor level (before Docker's iptables).

**Files:**
- Hetzner Cloud API (via MCP or `hcloud` CLI)

- [ ] **Step 1: Get your current public IP**

```bash
curl -s https://ifconfig.me
```

Save this IP — you'll use it in every firewall rule.

- [ ] **Step 2: Create the firewall via Hetzner MCP**

Use `mcp__hetzner__create_firewall` with name `beta-lockdown`.

- [ ] **Step 3: Set firewall rules**

Use `mcp__hetzner__set_firewall_rules` with these inbound rules:

| Port | Protocol | Source | Purpose |
|------|----------|--------|---------|
| 22 | TCP | YOUR_IP/32 | SSH |
| 80 | TCP | 0.0.0.0/0 | HTTP (Caddy redirect to HTTPS) |
| 443 | TCP | 0.0.0.0/0 | HTTPS (Caddy serves Dagster UI) |
| 3000 | TCP | YOUR_IP/32 | Dagster UI direct (fallback) |
| 5432 | TCP | YOUR_IP/32 | Postgres |
| 9000 | TCP | YOUR_IP/32 | MinIO API |
| 9001 | TCP | YOUR_IP/32 | MinIO Console |

Note: Ports 80/443 are open to all because Caddy handles TLS + auth. Direct port 3000 is restricted to your IP as a fallback.

- [ ] **Step 4: Apply firewall to server**

Use `mcp__hetzner__apply_firewall_to_resources` to attach `beta-lockdown` to the production server.

- [ ] **Step 5: Verify — test from another IP**

```bash
# From your IP — should work:
curl -s -o /dev/null -w "%{http_code}" http://178.156.228.119:3000
# Expected: 200 (or connection to Dagster)

# From a VPN/different IP — should timeout:
# Expected: connection refused / timeout
```

- [ ] **Step 6: Do the same for the France server (178.104.77.236)**

Repeat steps 2-5 for the France pipeline server. Same rules, same IPs.

---

## Task 2: SSH Hardening

**Priority:** CRITICAL — server access is the keys to the kingdom.

**Files:**
- Modify: `/etc/ssh/sshd_config` (on Hetzner server)

- [ ] **Step 1: SSH into server**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner root@178.156.228.119
```

- [ ] **Step 2: Create a non-root user**

```bash
adduser --disabled-password --gecos "" fattie
usermod -aG sudo fattie
mkdir -p /home/fattie/.ssh
cp /root/.ssh/authorized_keys /home/fattie/.ssh/authorized_keys
chown -R fattie:fattie /home/fattie/.ssh
chmod 700 /home/fattie/.ssh
chmod 600 /home/fattie/.ssh/authorized_keys
```

- [ ] **Step 3: Test SSH as new user (from a new terminal, don't close root session)**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
sudo whoami
# Expected: root
```

- [ ] **Step 4: Harden sshd_config**

```bash
sudo tee /etc/ssh/sshd_config.d/hardened.conf << 'EOF'
PermitRootLogin no
PasswordAuthentication no
ChallengeResponseAuthentication no
UsePAM yes
AllowUsers fattie
MaxAuthTries 3
LoginGraceTime 30
EOF
```

- [ ] **Step 5: Restart SSH and verify**

```bash
sudo systemctl restart sshd
# In a NEW terminal (keep current session open as fallback):
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
# Expected: success

ssh -i ~/.ssh/id_ed25519_hetzner root@178.156.228.119
# Expected: Permission denied
```

- [ ] **Step 6: Install fail2ban**

```bash
sudo apt update && sudo apt install -y fail2ban
sudo tee /etc/fail2ban/jail.local << 'EOF'
[sshd]
enabled = true
port = ssh
maxretry = 3
bantime = 3600
findtime = 600
EOF
sudo systemctl enable fail2ban
sudo systemctl restart fail2ban
```

- [ ] **Step 7: Enable unattended security upgrades**

```bash
sudo apt install -y unattended-upgrades
echo 'APT::Periodic::Update-Package-Lists "1";' | sudo tee /etc/apt/apt.conf.d/20auto-upgrades
echo 'APT::Periodic::Unattended-Upgrade "1";' | sudo tee -a /etc/apt/apt.conf.d/20auto-upgrades
```

- [ ] **Step 8: Update deploy.sh to use new user**

Update any references from `root@178.156.228.119` to `fattie@178.156.228.119`. The deploy script uses rsync + docker compose — `fattie` needs to be in the `docker` group:

```bash
sudo usermod -aG docker fattie
# Log out and back in for group to take effect
```

---

## Task 3: Postgres SSL

**Priority:** HIGH — encrypts all data in transit between pipeline and database.

**Files:**
- Create: `/opt/postgres-ssl/server.crt` and `server.key` (on Hetzner)
- Modify: `/opt/common-ground/docker-compose.yml` (on Hetzner)

- [ ] **Step 1: Generate self-signed cert on server**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119

sudo mkdir -p /opt/postgres-ssl
cd /opt/postgres-ssl

sudo openssl req -new -x509 -days 365 -nodes \
  -out server.crt -keyout server.key \
  -subj "/CN=common-ground-postgres"

sudo chmod 600 server.key
sudo chown 999:999 server.key server.crt
```

UID 999 is the `postgres` user inside the official Docker image.

- [ ] **Step 2: Update Postgres service in server docker-compose.yml**

Add SSL config to the Postgres service command and mount certs:

```yaml
services:
  postgres:
    image: postgres:16
    command: >
      -c ssl=on
      -c ssl_cert_file=/var/lib/postgresql/server.crt
      -c ssl_key_file=/var/lib/postgresql/server.key
    volumes:
      - pgdata:/var/lib/postgresql/data
      - /opt/postgres-ssl/server.crt:/var/lib/postgresql/server.crt:ro
      - /opt/postgres-ssl/server.key:/var/lib/postgresql/server.key:ro
```

- [ ] **Step 3: Restart Postgres**

```bash
cd /opt/common-ground
sudo docker compose up -d postgres
```

- [ ] **Step 4: Verify SSL is active**

```bash
sudo docker exec -it common-ground-postgres-1 psql -U dagster -d ducklake -c "SHOW ssl;"
# Expected: on

sudo docker exec -it common-ground-postgres-1 psql -U dagster -d ducklake -c "SELECT * FROM pg_stat_ssl WHERE pid = pg_backend_pid();"
# Expected: ssl = t, version = TLSv1.3
```

- [ ] **Step 5: Update connection string in pipeline secrets**

Add `?sslmode=require` to the catalog URL. The full URL becomes:

```
postgresql://dagster:<password>@178.156.228.119:5432/ducklake?sslmode=require
```

This will be set in the `.env.secrets` file created in Task 5.

---

## Task 4: MinIO Security

**Priority:** HIGH — MinIO holds all the parquet data files.

**Files:**
- Create: `/opt/minio-certs/public.crt` and `private.key` (on Hetzner)
- Modify: `/opt/common-ground/docker-compose.yml` (on Hetzner)

- [ ] **Step 1: Generate self-signed cert for MinIO**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119

sudo mkdir -p /opt/minio-certs
cd /opt/minio-certs

sudo openssl req -new -x509 -days 365 -nodes \
  -out public.crt -keyout private.key \
  -subj "/CN=common-ground-minio"

sudo chmod 600 private.key
sudo chown root:root private.key public.crt
```

- [ ] **Step 2: Mount certs in MinIO service**

```yaml
services:
  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
      - /opt/minio-certs/private.key:/root/.minio/certs/private.key:ro
      - /opt/minio-certs/public.crt:/root/.minio/certs/public.crt:ro
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
```

MinIO auto-enables TLS when it finds certs at `/root/.minio/certs/`.

- [ ] **Step 3: Rotate MinIO credentials**

Generate a new root password:

```bash
openssl rand -base64 24
# Save this — it goes into .env.secrets
```

Update the server's `.env` with the new `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`.

- [ ] **Step 4: Create scoped service account for pipeline**

```bash
# Install mc if not present
curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc && sudo mv mc /usr/local/bin/

# Set alias (use new root creds, --insecure for self-signed)
mc alias set cg https://127.0.0.1:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD --insecure

# Create a pipeline-only policy
cat > /tmp/pipeline-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject", "s3:GetBucketLocation"],
    "Resource": ["arn:aws:s3:::ducklake", "arn:aws:s3:::ducklake/*"]
  }]
}
EOF

mc admin policy create cg pipeline-rw /tmp/pipeline-policy.json

# Create service account
mc admin user svcacct add cg $MINIO_ROOT_USER \
  --policy /tmp/pipeline-policy.json
# Save the access key and secret key — they go into .env.secrets
```

- [ ] **Step 5: Restart MinIO and verify**

```bash
cd /opt/common-ground
sudo docker compose up -d minio

# Verify TLS
curl -sk https://127.0.0.1:9000/minio/health/live
# Expected: healthy
```

- [ ] **Step 6: Update pipeline secrets**

The `bucket_url` in dlt config changes from `s3://ducklake/data/` to `s3://ducklake/data/` (unchanged — the URL scheme stays `s3://`, TLS is handled by the endpoint config). But the endpoint URL and credentials change:

```
DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_ACCESS_KEY_ID=<new-service-account-key>
DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_SECRET_ACCESS_KEY=<new-service-account-secret>
DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__ENDPOINT_URL=https://178.156.228.119:9000
```

This goes into `.env.secrets` (Task 5).

---

## Task 5: Secrets Management (SOPS + age)

**Priority:** HIGH — replaces plaintext secrets.toml with encrypted, git-friendly secrets.

**Files:**
- Create: `~/Desktop/dagster-pipeline/.sops.yaml`
- Create: `~/Desktop/dagster-pipeline/.env.secrets`
- Create: `~/Desktop/dagster-pipeline/.env.secrets.enc`
- Modify: `~/Desktop/dagster-pipeline/.gitignore`
- Modify: `~/Desktop/dagster-pipeline/docker-compose.yml`

- [ ] **Step 1: Install SOPS and age**

```bash
brew install sops age
```

- [ ] **Step 2: Generate your age keypair**

```bash
mkdir -p ~/.config/sops/age
age-keygen -o ~/.config/sops/age/keys.txt
```

This outputs your public key (starts with `age1...`). Save it — you'll need it for `.sops.yaml`.

- [ ] **Step 3: Create `.sops.yaml` in pipeline root**

Create: `~/Desktop/dagster-pipeline/.sops.yaml`

```yaml
creation_rules:
  - path_regex: \.env\.secrets
    age: >-
      age1YOUR_PUBLIC_KEY_HERE
```

Replace with your actual public key from step 2. To add another team member later, comma-separate their public key.

- [ ] **Step 4: Create `.env.secrets` with all credentials as env vars**

Create: `~/Desktop/dagster-pipeline/.env.secrets`

Map every secret from `secrets.toml` to dlt env var format. The mapping is:

```bash
# Postgres catalog (with sslmode=require added)
DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG=postgresql://dagster:<NEW_PASSWORD>@178.156.228.119:5432/ducklake?sslmode=require

# MinIO storage credentials (use new scoped service account from Task 4)
DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_ACCESS_KEY_ID=<new-key>
DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_SECRET_ACCESS_KEY=<new-secret>
DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__ENDPOINT_URL=https://178.156.228.119:9000
DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__REGION=us-east-1
# Self-signed cert on MinIO — tell boto3/dlt to skip TLS verification
DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__CLIENT_KWARGS={"verify": false}

# Socrata app token
SOURCES__SOCRATA__APP_TOKEN=KECcMSAGIwfqQKOpYjsX5EVVO

# CourtListener
SOURCES__COURTLISTENER_SOURCE__API_TOKEN=<token>

# College Scorecard
SOURCES__COLLEGE_SCORECARD__API_KEY=<key>

# Census
SOURCES__CENSUS__API_KEY=<key>

# FEC
SOURCES__FEC__API_KEY=<key>

# NREL
SOURCES__NREL__API_KEY=<key>

# HUD (source code uses dlt.secrets["sources.hud.api_key"])
SOURCES__HUD__API_KEY=<key>

# Caddy basic auth password (for Dagster UI)
DAGSTER_UI_PASSWORD=<generate-with-openssl-rand>
```

**Important:** Do NOT copy secrets from this plan. Open `secrets.toml` directly and translate each key. For new Postgres/MinIO passwords, generate fresh ones:

```bash
openssl rand -base64 24  # for each new password
```

- [ ] **Step 5: Encrypt the secrets file**

```bash
cd ~/Desktop/dagster-pipeline
sops --encrypt --input-type dotenv --output-type dotenv .env.secrets > .env.secrets.enc
```

SOPS doesn't auto-detect `.env.secrets` as dotenv format, so the `--input-type dotenv --output-type dotenv` flags are required.

Verify it encrypted properly:

```bash
head -5 .env.secrets.enc
# Should show SOPS metadata, not plaintext
```

- [ ] **Step 6: Test decryption**

```bash
sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc | head -3
# Should show your plaintext env vars
```

- [ ] **Step 7: Update `.gitignore`**

Modify: `~/Desktop/dagster-pipeline/.gitignore`

Add at the end:

```
# Decrypted secrets (never commit)
.env.secrets
```

Note: `.env.secrets.enc` (encrypted) IS committed. `.env.secrets` (plaintext) is NOT.

- [ ] **Step 8: Verify secrets.toml is still gitignored**

```bash
cd ~/Desktop/dagster-pipeline
git check-ignore .dlt/secrets.toml
# Expected: .dlt/secrets.toml
```

- [ ] **Step 9: Deploy age key to Hetzner server**

The server needs the private key to decrypt at deploy time:

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "mkdir -p ~/.config/sops/age"
scp -i ~/.ssh/id_ed25519_hetzner ~/.config/sops/age/keys.txt fattie@178.156.228.119:~/.config/sops/age/keys.txt
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "chmod 600 ~/.config/sops/age/keys.txt"
```

---

## Task 6: Docker Compose Hardening

**Priority:** HIGH — fixes socket exposure, network isolation, and secrets injection.

**Files:**
- Modify: `~/Desktop/dagster-pipeline/docker-compose.yml`
- Modify: `~/Desktop/dagster-pipeline/src/dagster_pipeline/definitions.py`

- [ ] **Step 1: Read current docker-compose.yml**

Read: `~/Desktop/dagster-pipeline/docker-compose.yml` (already read above, but verify before editing)

- [ ] **Step 2: Rewrite docker-compose.yml**

Replace the entire file with this hardened version:

```yaml
volumes:
  dlt-pipelines:
    name: dagster-pipeline-dlt-state
  caddy_data:
  caddy_config:

networks:
  frontend:
    driver: bridge
  backend:
    driver: bridge

services:
  # Reverse proxy — TLS + basic auth for Dagster UI
  caddy:
    image: caddy:2-alpine
    container_name: caddy
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_data:/data
      - caddy_config:/config
    networks:
      - frontend
    deploy:
      resources:
        limits:
          memory: 256m
          cpus: '0.25'
    restart: unless-stopped

  # gRPC code server — loads user code, shared by webserver + daemon
  dagster-code:
    build: .
    container_name: dagster-code
    command: ["uv", "run", "dagster", "code-server", "start",
              "-m", "dagster_pipeline.definitions",
              "--host", "0.0.0.0", "--port", "4000"]
    init: true
    env_file:
      - .env.secrets
    volumes:
      - ./dagster-home:/dagster-home
      - ./src/dagster_pipeline:/app/src/dagster_pipeline
      - ./.dlt/config.toml:/app/.dlt/config.toml:ro
      - ./models:/app/models:ro
      - dlt-pipelines:/var/dlt/pipelines
    networks:
      - backend
    deploy:
      resources:
        limits:
          memory: 16g
          cpus: '4.0'
    restart: unless-stopped

  # Dagster webserver — UI + GraphQL API
  dagster-webserver:
    build: .
    container_name: dagster-webserver
    command: ["uv", "run", "dagster-webserver", "-w", "workspace.yaml", "-p", "3000", "-h", "0.0.0.0"]
    ports:
      - "127.0.0.1:3000:3000"
    init: true
    env_file:
      - .env.secrets
    volumes:
      - ./dagster-home:/dagster-home
    networks:
      - frontend
      - backend
    deploy:
      resources:
        limits:
          memory: 2g
          cpus: '1.0'
    depends_on:
      - dagster-code
    restart: unless-stopped

  # Dagster daemon — scheduler, sensors, run orchestration
  dagster-daemon:
    build: .
    container_name: dagster-daemon
    command: ["uv", "run", "dagster-daemon", "run", "-w", "workspace.yaml"]
    init: true
    env_file:
      - .env.secrets
    volumes:
      - ./dagster-home:/dagster-home
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      - backend
    deploy:
      resources:
        limits:
          memory: 8g
          cpus: '2.0'
    depends_on:
      - dagster-code
    restart: unless-stopped
```

Key changes from the original:
1. **Removed** `/var/run/docker.sock` from `dagster-code` (only daemon needs it)
2. **Removed** `~/Downloads:/root/Downloads:ro` mount (unnecessary, leaks home dir)
3. **Removed** `~/.dagster-home` → replaced with relative `./dagster-home`
4. **Removed** `.dlt/secrets.toml` volume mounts from all services
5. **Added** `env_file: [.env.secrets]` to all services (dlt reads env vars natively)
6. **Added** `caddy` service for reverse proxy with auth
7. **Added** `frontend` and `backend` networks
8. **Added** CPU limits alongside memory limits
9. **Reduced** daemon memory from 48g to 8g (step containers get their own limits)
10. **Bound** webserver port to `127.0.0.1:3000` (only accessible via Caddy or localhost)

- [ ] **Step 3: Update definitions.py — remove hardcoded paths**

Modify: `~/Desktop/dagster-pipeline/src/dagster_pipeline/definitions.py`

Replace the `container_kwargs.volumes` dict. Change from hardcoded `/Users/fattie2020/` paths to paths relative to the project on the server:

```python
"network": "dagster-pipeline_backend",  # must match compose network name
"container_kwargs": {
    "auto_remove": False,
    "mem_limit": "30g",
    "nano_cpus": 4_000_000_000,  # 4 CPUs
    "volumes": {
        "/opt/dagster-pipeline/dagster-home": {
            "bind": "/dagster-home",
            "mode": "rw",
        },
        "/opt/dagster-pipeline/models": {
            "bind": "/app/models",
            "mode": "ro",
        },
        "dagster-pipeline-dlt-state": {
            "bind": "/var/dlt/pipelines",
            "mode": "rw",
        },
    },
},
```

**Important changes:**
- Removed `/Users/fattie2020/.dagster-home` → `/opt/dagster-pipeline/dagster-home`
- Removed `/Users/fattie2020/Downloads` mount entirely
- Removed `secrets.toml` mount from step containers (secrets come via env vars now)
- Added `nano_cpus` for CPU limit on step containers

**Note:** The step containers need secrets too. Since `docker_executor` spawns them, you need to pass env vars through. Add to the executor config:

```python
"env_vars": [
    "DAGSTER_HOME=/dagster-home",
    # Destination credentials (DuckLake catalog + MinIO storage)
    "DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG",
    "DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_ACCESS_KEY_ID",
    "DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_SECRET_ACCESS_KEY",
    "DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__ENDPOINT_URL",
    "DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__REGION",
    "DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__CLIENT_KWARGS",
    # Source API tokens (all sources that run as step containers)
    "SOURCES__SOCRATA__APP_TOKEN",
    "SOURCES__COURTLISTENER_SOURCE__API_TOKEN",
    "SOURCES__COLLEGE_SCORECARD__API_KEY",
    "SOURCES__CENSUS_SOURCE__API_KEY",
    "SOURCES__FEC__API_KEY",
    "SOURCES__NREL__API_KEY",
    "SOURCES__HUD__API_KEY",
],
```

When you list an env var name without `=value`, Docker inherits it from the parent container's environment. Since the daemon has `env_file: [.env.secrets]`, the step containers will get those values.

- [ ] **Step 4: Verify the compose file is valid**

```bash
cd ~/Desktop/dagster-pipeline
docker compose config --quiet
# Expected: no output (means valid)
```

---

## Task 7: Caddy Reverse Proxy

**Priority:** HIGH — puts auth in front of the Dagster UI.

**Files:**
- Create: `~/Desktop/dagster-pipeline/Caddyfile`
- Add to: `docker-compose.yml` (already added in Task 6)

- [ ] **Step 1: Generate a bcrypt password hash**

```bash
# Generate a random password
openssl rand -base64 16
# Save this as DAGSTER_UI_PASSWORD in .env.secrets

# Generate bcrypt hash for Caddyfile
docker run --rm caddy:2-alpine caddy hash-password --plaintext "YOUR_PASSWORD_HERE"
# Save the output hash
```

- [ ] **Step 2: Create Caddyfile**

Create: `~/Desktop/dagster-pipeline/Caddyfile`

```
dagster.common-ground.nyc {
    basicauth {
        admin BCRYPT_HASH_HERE
    }
    reverse_proxy dagster-webserver:3000
}
```

Replace `BCRYPT_HASH_HERE` with the hash from step 1. Replace `admin` with your preferred username.

**Note:** If you don't have a DNS record for `dagster.common-ground.nyc` yet, use the IP temporarily:

```
:443 {
    tls internal
    basicauth {
        admin BCRYPT_HASH_HERE
    }
    reverse_proxy dagster-webserver:3000
}
```

`tls internal` generates a self-signed cert. Switch to the domain version once DNS is pointed.

- [ ] **Step 3: Add DNS record for dagster.common-ground.nyc**

In your DNS provider (probably Cloudflare since you manage common-ground.nyc there):
- Type: A
- Name: dagster
- Value: 178.156.228.119
- Proxy: OFF (DNS only — Caddy handles TLS, not Cloudflare)

- [ ] **Step 4: Add Caddy volumes to docker-compose.yml**

Already included in Task 6's compose file. Verify these volume declarations exist at the top:

```yaml
volumes:
  dlt-pipelines:
    name: dagster-pipeline-dlt-state
  caddy_data:
  caddy_config:
```

- [ ] **Step 5: Test locally**

```bash
cd ~/Desktop/dagster-pipeline
docker compose up caddy dagster-webserver dagster-code -d
curl -u admin:YOUR_PASSWORD https://dagster.common-ground.nyc
# Expected: Dagster UI HTML

curl https://dagster.common-ground.nyc
# Expected: 401 Unauthorized
```

---

## Task 8: Website Security Headers

**Priority:** HIGH — protects site visitors from XSS, clickjacking, MIME sniffing.

**Files:**
- Modify: `~/Desktop/common-ground-website/next.config.ts`
- Create: `~/Desktop/common-ground-website/public/_headers`
- Modify: `~/Desktop/common-ground-website/.env.local`

- [ ] **Step 1: Update next.config.ts with security headers**

Modify: `~/Desktop/common-ground-website/next.config.ts`

**Important:** The existing `next.config.ts` has PostHog proxy rewrites and `skipTrailingSlashRedirect`. We MUST preserve those. The site uses PostHog for analytics via a `/ingest` proxy path.

Replace entire file:

```typescript
import type { NextConfig } from "next";

const cspDirectives = [
  "default-src 'self'",
  "script-src 'self' 'unsafe-inline'",
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' blob: data:",
  "font-src 'self'",
  "connect-src 'self' https://geosearch.planninglabs.nyc https://us.i.posthog.com https://us-assets.i.posthog.com",
  "worker-src 'self' blob:",
  "object-src 'none'",
  "base-uri 'self'",
  "form-action 'self'",
  "frame-ancestors 'none'",
  "upgrade-insecure-requests",
].join("; ");

const securityHeaders = [
  { key: "Content-Security-Policy", value: cspDirectives },
  { key: "Strict-Transport-Security", value: "max-age=63072000; includeSubDomains; preload" },
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "X-Frame-Options", value: "DENY" },
  { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
  { key: "Permissions-Policy", value: "camera=(), microphone=(), geolocation=(), interest-cohort=()" },
];

const nextConfig: NextConfig = {
  async headers() {
    return [
      {
        source: "/(.*)",
        headers: securityHeaders,
      },
    ];
  },
  // PostHog proxy — routes analytics through our domain to avoid ad blockers
  async rewrites() {
    return [
      {
        source: "/ingest/static/:path*",
        destination: "https://us-assets.i.posthog.com/static/:path*",
      },
      {
        source: "/ingest/:path*",
        destination: "https://us.i.posthog.com/:path*",
      },
    ];
  },
  skipTrailingSlashRedirect: true,
};

export default nextConfig;
```

**Why `unsafe-inline` is required:**
- `style-src`: GSAP writes inline styles on every animation frame. No nonce workaround exists.
- `script-src`: Non-nonce approach requires it. The nonce approach would force dynamic rendering for all pages (kills CDN caching on a static marketing site — not worth it).

**PostHog domains in `connect-src`:** The `/ingest` rewrite proxies to `us.i.posthog.com` and `us-assets.i.posthog.com`. These must be in `connect-src` for the proxy to work. The `instrumentation-client.ts` initializes PostHog with `api_host: "/ingest"` which routes through the rewrite, but PostHog's JS client also makes direct calls to the `ui_host`.

- [ ] **Step 2: Create public/_headers for static assets**

Create: `~/Desktop/common-ground-website/public/_headers`

```
/*
  X-Content-Type-Options: nosniff
  X-Frame-Options: DENY
  Referrer-Policy: strict-origin-when-cross-origin
  Permissions-Policy: camera=(), microphone=(), geolocation=(), interest-cohort=()

/_next/static/*
  Cache-Control: public, max-age=31536000, immutable
```

This covers static assets served directly by Cloudflare Pages (bypassing the Worker).

- [ ] **Step 3: Remove dead API key from .env.local**

Modify: `~/Desktop/common-ground-website/.env.local`

Delete the entire file or clear its contents. The `TWENTY_FIRST_API_KEY` is not used anywhere in the codebase.

```bash
rm ~/Desktop/common-ground-website/.env.local
```

Also: rotate the key on TwentyFirst's service (if you have an account there). Even though it's unused, the key is exposed.

- [ ] **Step 4: Build and verify headers**

```bash
cd ~/Desktop/common-ground-website
npm run build:cf
npm run preview
# In another terminal:
curl -sI http://localhost:3000 | grep -E "(Content-Security|Strict-Transport|X-Frame|X-Content|Referrer|Permissions)"
```

Expected output:

```
Content-Security-Policy: default-src 'self'; script-src 'self' 'unsafe-inline'; ...
Strict-Transport-Security: max-age=63072000; includeSubDomains; preload
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Referrer-Policy: strict-origin-when-cross-origin
Permissions-Policy: camera=(), microphone=(), geolocation=(), interest-cohort=()
```

- [ ] **Step 5: Test that nothing breaks**

```bash
npm run dev
```

Open `http://localhost:3002` and verify:
- Address search still works (fetches from geosearch.planninglabs.nyc)
- GSAP animations play normally
- Lenis smooth scroll works
- Three.js renders (if on a page that uses it)
- No CSP errors in browser console (open DevTools > Console, filter for "CSP")

- [ ] **Step 6: Deploy to Cloudflare**

```bash
npm run deploy
```

Verify headers on production:

```bash
curl -sI https://common-ground.nyc | grep -E "(Content-Security|Strict-Transport|X-Frame|X-Content|Referrer|Permissions)"
```

- [ ] **Step 7: Enable HSTS in Cloudflare dashboard (belt + suspenders)**

In Cloudflare dashboard for common-ground.nyc:
1. SSL/TLS > Edge Certificates > Always Use HTTPS: ON
2. SSL/TLS > Edge Certificates > HSTS: Enable with max-age 6 months, includeSubDomains

This ensures HSTS is set even on responses served from Cloudflare's cache before hitting your Worker.

---

## Task 9: Redact CLAUDE.md

**Priority:** MEDIUM — contains plaintext credentials that could be accidentally committed.

**Files:**
- Modify: `~/Desktop/dagster-pipeline/CLAUDE.md`

- [ ] **Step 1: Read current CLAUDE.md**

Read: `~/Desktop/dagster-pipeline/CLAUDE.md`

- [ ] **Step 2: Redact credentials section**

Find the "Credentials" section and replace with:

```markdown
## Credentials

All secrets are managed via SOPS + age encryption. See `.env.secrets.enc` (encrypted, in git).

To edit secrets:
```bash
sops --input-type dotenv --output-type dotenv .env.secrets.enc
```

To decrypt for deployment:
```bash
sops --decrypt .env.secrets.enc > .env.secrets
```

Key names follow dlt's env var convention: `DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG`, etc.
```

- [ ] **Step 3: Also redact the "Hetzner Server" section SSH command**

Replace the SSH command with:

```markdown
## Hetzner Server

SSH: `ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119`

Note: Root login is disabled. Use `fattie` user with sudo.
```

---

## Task 10: Image Scanning with Grype

**Priority:** MEDIUM — catch known vulnerabilities in container images before deploy.

**Files:**
- Local machine only (no file changes)

- [ ] **Step 1: Install Grype**

```bash
brew install grype
```

- [ ] **Step 2: Scan the Dagster pipeline image**

```bash
cd ~/Desktop/dagster-pipeline
docker build -t dagster-pipeline:latest .
grype dagster-pipeline:latest --only-fixed
```

Review output. Focus on `Critical` and `High` severity. `--only-fixed` shows only vulnerabilities that have patches available.

- [ ] **Step 3: Fix critical/high vulnerabilities**

If Grype reports fixable Critical/High CVEs, update the base image or pin specific package versions in the Dockerfile.

- [ ] **Step 4: Add scan to deploy workflow**

Add to your deploy script (or run manually before each deploy):

```bash
grype dagster-pipeline:latest --fail-on critical
```

This exits non-zero if any Critical vulnerability is found, blocking the deploy.

---

## Task 11: Credential Rotation

**Priority:** HIGH — must happen AFTER Tasks 3-5 are complete (new secrets infra in place).

**Files:**
- Modify: `.env.secrets` and `.env.secrets.enc`
- Modify: Postgres on Hetzner
- Modify: MinIO on Hetzner

**Order of operations** (to avoid breaking the pipeline):

- [ ] **Step 1: Rotate Postgres password**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119

sudo docker exec -it common-ground-postgres-1 psql -U postgres -c \
  "ALTER ROLE dagster WITH PASSWORD 'NEW_PASSWORD_HERE';"
```

Generate the new password with `openssl rand -base64 24`.

- [ ] **Step 2: Update .env.secrets with new Postgres password**

```bash
cd ~/Desktop/dagster-pipeline
# Edit the encrypted file directly (SOPS opens in $EDITOR)
sops --input-type dotenv --output-type dotenv .env.secrets.enc
# Update the DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG line with new password
```

- [ ] **Step 3: MinIO credentials were already rotated in Task 4**

Verify the new service account keys are in `.env.secrets.enc`.

- [ ] **Step 4: Re-encrypt and deploy**

```bash
cd ~/Desktop/dagster-pipeline
sops --decrypt .env.secrets.enc > .env.secrets
# Deploy (your deploy script handles the rest)
```

- [ ] **Step 5: Restart pipeline and run a test materialization**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
cd /opt/dagster-pipeline
docker compose down && docker compose up -d
```

Then trigger a small test run from the Dagster UI (now behind Caddy auth) — materialize a single small asset to verify the full chain works.

- [ ] **Step 6: Verify end-to-end**

```bash
# Check Dagster UI loads behind auth
curl -u admin:PASSWORD https://dagster.common-ground.nyc
# Expected: 200

# Check Postgres SSL
docker exec dagster-code python -c "
import duckdb
con = duckdb.connect()
con.execute(\"INSTALL ducklake; LOAD ducklake;\")
con.execute(\"ATTACH 'ducklake:...' AS lake\")
print(con.execute('SELECT count(*) FROM information_schema.tables').fetchone())
"
```

---

## Task 12: Beta Launch Runbook

**Priority:** MEDIUM — team documentation so anyone can operate the system.

**Files:**
- Create: `~/Desktop/dagster-pipeline/docs/BETA-LAUNCH-RUNBOOK.md`

- [ ] **Step 1: Write the runbook**

Create: `~/Desktop/dagster-pipeline/docs/BETA-LAUNCH-RUNBOOK.md`

```markdown
# Common Ground — Beta Launch Runbook

## Access

| Service | URL | Auth | Who |
|---------|-----|------|-----|
| Website | https://common-ground.nyc | Public | Everyone |
| Dagster UI | https://dagster.common-ground.nyc | Basic auth (user: `admin`) | Team only |
| Hetzner SSH | `ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119` | SSH key | Team only |
| MinIO Console | https://178.156.228.119:9001 | Root creds in .env.secrets | Team only |
| Postgres | `178.156.228.119:5432` (sslmode=require) | Creds in .env.secrets | Pipeline only |

## Dagster UI Login

Username: `admin`
Password: stored in `.env.secrets.enc` as `DAGSTER_UI_PASSWORD`

To retrieve: `sops --decrypt .env.secrets.enc | grep DAGSTER_UI_PASSWORD`

## Editing Secrets

```bash
cd ~/Desktop/dagster-pipeline
sops --input-type dotenv --output-type dotenv .env.secrets.enc    # opens in $EDITOR, saves encrypted on close
```

To add a new team member's age key:
1. They run: `age-keygen` and send you the public key
2. Add their public key to `.sops.yaml`
3. Run: `sops updatekeys .env.secrets.enc`

## Deploying

```bash
cd ~/Desktop/dagster-pipeline
./deploy.sh   # rsyncs code + decrypted secrets, restarts docker compose
```

## Rotating Credentials

### Postgres
1. SSH to server
2. `docker exec -it common-ground-postgres-1 psql -U postgres -c "ALTER ROLE dagster WITH PASSWORD 'new'"`
3. `sops --input-type dotenv --output-type dotenv .env.secrets.enc` — update catalog URL
4. Deploy

### MinIO
1. SSH to server
2. `mc admin user svcacct add ...` (create new service account)
3. `sops --input-type dotenv --output-type dotenv .env.secrets.enc` — update access key + secret
4. Deploy
5. `mc admin user svcacct rm ...` (delete old service account)

### API Tokens (Socrata, CourtListener, etc.)
1. Generate new token on provider's website
2. `sops --input-type dotenv --output-type dotenv .env.secrets.enc` — update the relevant `SOURCES__*` var
3. Deploy

## Emergency: IP Changed

If your IP changes and you're locked out of Hetzner firewall:
1. Log into Hetzner Cloud Console (https://console.hetzner.cloud)
2. Firewalls > beta-lockdown > Edit rules
3. Update source IPs to your new IP
4. Or: use Hetzner Console (web-based terminal, always accessible)

## Emergency: Pipeline Stuck

```bash
ssh fattie@178.156.228.119
cd /opt/dagster-pipeline
docker compose logs --tail=50 dagster-daemon
docker compose restart dagster-daemon
```

## Emergency: Website Down

1. Check Cloudflare status: https://www.cloudflarestatus.com
2. Check deploy: `cd ~/Desktop/common-ground-website && npm run deploy`
3. Rollback: `npx wrangler rollback`

## SSL Cert Renewal

- Caddy: automatic (Let's Encrypt, no action needed)
- Postgres/MinIO: self-signed, expires in 365 days. Regenerate before expiry:
  ```bash
  ssh fattie@178.156.228.119
  cd /opt/postgres-ssl && sudo openssl req -new -x509 -days 365 ...
  cd /opt/minio-certs && sudo openssl req -new -x509 -days 365 ...
  docker compose restart postgres minio
  ```
```

---

## Execution Order

Tasks should be executed in this order (dependencies noted):

```
Task 1: Hetzner Firewall         ← do first, immediate protection
Task 2: SSH Hardening             ← do second, before anything else on server
Task 3: Postgres SSL              ← server-side, no pipeline changes yet
Task 4: MinIO Security            ← server-side, no pipeline changes yet
Task 5: Secrets Management        ← local setup, generates .env.secrets
Task 6: Docker Compose Hardening  ← depends on Task 5 (.env.secrets exists)
Task 7: Caddy Reverse Proxy       ← depends on Task 6 (in compose)
Task 8: Website Security Headers  ← independent, can run in parallel with 1-7
Task 9: Redact CLAUDE.md          ← quick, do after Task 5
Task 10: Image Scanning           ← independent, can run anytime
Task 11: Credential Rotation      ← depends on Tasks 3, 4, 5 (new infra in place)
Task 12: Runbook                  ← do last, documents final state
```

**Parallelizable:** Task 8 (website headers) is completely independent and can be done in parallel with everything else.

---

## Post-Launch Monitoring

After completing all tasks:

- [ ] Set a calendar reminder for SSL cert renewal (365 days from Task 3/4)
- [ ] Monitor Cloudflare Analytics for the website
- [ ] Check Dagster UI daily for failed runs
- [ ] Run `grype dagster-pipeline:latest` monthly
- [ ] Review Hetzner firewall rules monthly (remove stale IPs, add new team members)
