# Server Hardening — Fix Audit Findings

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all issues found during the server audit: broken SSH access patterns, missing infrastructure protections, Cloudflare tunnel SSH fallback, fail2ban whitelisting, and compose hygiene.

**Architecture:** Fix local config files first (deploy script, SSH config), then SSH into the server (after fail2ban expires or via Hetzner console) to fix server-side issues (fail2ban whitelist, cloudflared SSH tunnel, verify .env secrets). Finally, enable Hetzner API-level protections (delete protection, backups). The France server gets labels and a data volume.

**Tech Stack:** Hetzner Cloud API (via MCP), SSH, Docker Compose, fail2ban, Cloudflare Tunnel

---

## What Changes

| Before | After |
|--------|-------|
| `data-pipeline/deploy.sh` uses `root@` | Uses `fattie@` (matches dagster deploy script) |
| No SSH config entry for Hetzner | `~/.ssh/config` has `Host hetzner` with key + user |
| fail2ban bans your IP on any bad attempt | Your IP whitelisted in `ignoreip` |
| No SSH fallback when fail2ban bans you | Cloudflare SSH tunnel as backup path |
| Compose has default fallback secrets | Defaults removed — `.env` is required |
| No delete protection on servers/volume | Delete + rebuild protection enabled |
| No automated backups | Hetzner backups enabled on common-ground |
| cg-france has no volume or labels | Volume attached, labels added |

---

## File Structure

### Local files

| File | Action | Responsibility |
|------|--------|----------------|
| `~/Desktop/data-pipeline/deploy.sh` | Modify | Fix `root@` → `fattie@` |
| `~/.ssh/config` | Create | SSH alias with correct key, user, and tunnel fallback |
| `~/Desktop/dagster-pipeline/infra/hetzner-docker-compose.yml` | Modify | Remove default secret fallbacks |

### Server files (178.156.228.119)

| File | Action | Responsibility |
|------|--------|----------------|
| `/etc/fail2ban/jail.local` | Modify | Whitelist team IP |
| `/opt/common-ground/.env` | Verify | Confirm real values set for all secrets |
| `/opt/common-ground/docker-compose.yml` | Modify | Remove default secret fallbacks |

### Hetzner API

| Resource | Action | Responsibility |
|----------|--------|----------------|
| `common-ground` server | Enable | Delete + rebuild protection |
| `cg-france` server | Enable | Delete protection, backups, labels |
| `duckdb-data` volume | Enable | Delete protection |
| New volume for cg-france | Create | Data persistence |

---

## Task 1: Fix deploy script — root@ → fattie@

**Priority:** Do this first — prevents future fail2ban lockouts.

**Files:**
- Modify: `~/Desktop/data-pipeline/deploy.sh`

- [ ] **Step 1: Fix the user in deploy.sh**

Change line 4:

```bash
# OLD
SERVER="root@178.156.228.119"

# NEW
SERVER="fattie@178.156.228.119"
```

- [ ] **Step 2: Verify the fix**

```bash
grep 'SERVER=' ~/Desktop/data-pipeline/deploy.sh
```

Expected: `SERVER="fattie@178.156.228.119"`

- [ ] **Step 3: Commit**

```bash
cd ~/Desktop/data-pipeline
git add deploy.sh
git commit -m "fix: use fattie@ instead of root@ in deploy script

Root login is disabled on the server. Using root@ triggers fail2ban."
```

---

## Task 2: Create SSH config for Hetzner servers

**Priority:** Do this second — makes SSH reliable and provides Cloudflare tunnel fallback.

**Files:**
- Create or append: `~/.ssh/config` (currently does not exist)

- [ ] **Step 1: Create SSH config**

```
Host hetzner
    HostName 178.156.228.119
    User fattie
    IdentityFile ~/.ssh/id_ed25519_hetzner
    IdentitiesOnly yes

Host hetzner-tunnel
    HostName ssh.common-ground.nyc
    User fattie
    IdentityFile ~/.ssh/id_ed25519_hetzner
    IdentitiesOnly yes
    ProxyCommand cloudflared access ssh --hostname %h

Host france
    HostName 178.104.77.236
    User fattie
    IdentityFile ~/.ssh/id_ed25519_hetzner
    IdentitiesOnly yes
```

- [ ] **Step 2: Set correct permissions**

```bash
chmod 600 ~/.ssh/config
```

- [ ] **Step 3: Test direct SSH (if fail2ban has expired)**

```bash
ssh hetzner "echo connected"
```

Expected: `connected`

If still banned, skip — Task 4 will fix fail2ban.

---

## Task 3: Remove default secret fallbacks from compose files

**Priority:** Prevents accidentally running with well-known defaults.

**Files:**
- Modify: `~/Desktop/data-pipeline/docker-compose.yml` (local copy)
- Modify: `~/Desktop/dagster-pipeline/infra/hetzner-docker-compose.yml` (version-controlled mirror)

- [ ] **Step 1: Remove defaults in data-pipeline/docker-compose.yml**

Three changes:

In `data-pipeline/docker-compose.yml`, remove defaults at these locations:

**minio service:**
```yaml
# OLD
MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minioadmin}
# NEW
MINIO_ROOT_USER: ${MINIO_ROOT_USER}
```

**duckdb-server service:**
```yaml
# OLD
MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minioadmin}
# NEW
MINIO_ROOT_USER: ${MINIO_ROOT_USER}
```

**xyops service:**
```yaml
# OLD
XYOPS_secret_key: ${XYOPS_SECRET_KEY:-<default-removed>}
XYOPS_base_app_url: "http://178.156.228.119:5580"
# NEW
XYOPS_secret_key: ${XYOPS_SECRET_KEY}
XYOPS_base_app_url: "https://xyops.common-ground.nyc"
```

- [ ] **Step 2: Apply same default removals to hetzner-docker-compose.yml**

In `~/Desktop/dagster-pipeline/infra/hetzner-docker-compose.yml`, remove the `:-minioadmin` default from both minio and duckdb-server services, and remove the `:-<default>` from xyops. The URL is already correct in this file (`https://xyops.common-ground.nyc`).

- [ ] **Step 3: Remove minioadmin fallback from mcp_server.py**

In `~/Desktop/data-pipeline/duckdb-server/mcp_server.py`, find:
```python
os.environ.get("MINIO_ROOT_USER", "minioadmin")
```
Replace with:
```python
os.environ["MINIO_ROOT_USER"]
```
This ensures the server fails loudly if the env var is missing, rather than silently using the well-known default.

- [ ] **Step 4: Commit all files**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/hetzner-docker-compose.yml
git commit -m "fix: remove default secret fallbacks from compose

Forces .env to have real values for MINIO_ROOT_USER and XYOPS_SECRET_KEY."
```

```bash
cd ~/Desktop/data-pipeline
git add docker-compose.yml duckdb-server/mcp_server.py
git commit -m "fix: remove default secret fallbacks from compose and mcp_server"
```

---

## Task 4: SSH into server — fix fail2ban and verify secrets

**Priority:** Requires SSH access. Either wait ~1 hour for fail2ban to expire, or use Hetzner Cloud Console (web VNC).

**Prerequisite:** Task 2 completed (SSH config exists).

- [ ] **Step 1: Connect to server**

Try direct first:
```bash
ssh hetzner
```

If still banned, use Hetzner Cloud Console:
1. Go to https://console.hetzner.cloud
2. Select `common-ground` server
3. Click "Console" tab (web VNC)
4. Login as `fattie`

- [ ] **Step 2: Check current fail2ban config**

First, verify your current public IP (run this locally):
```bash
curl -4 -s ifconfig.me
```

Then on the server, check what exists:
```bash
cat /etc/fail2ban/jail.local 2>/dev/null || echo "File does not exist"
sudo fail2ban-client status sshd
```

- [ ] **Step 3: Add your IP to fail2ban whitelist**

If `jail.local` doesn't exist or is empty:
```bash
sudo tee /etc/fail2ban/jail.local << 'EOF'
[DEFAULT]
ignoreip = 127.0.0.1/8 ::1 90.119.124.77 2a01:cb1c:8188:c500::/64
EOF
```

If `jail.local` already has content (custom bantime, maxretry, etc.), only ADD the `ignoreip` line — don't overwrite existing settings:
```bash
sudo nano /etc/fail2ban/jail.local
# Add under [DEFAULT]: ignoreip = 127.0.0.1/8 ::1 90.119.124.77 2a01:cb1c:8188:c500::/64
```

Replace `90.119.124.77` with your actual IP from Step 2 if it has changed.

- [ ] **Step 4: Restart fail2ban and unban current IP**

```bash
sudo systemctl restart fail2ban
sudo fail2ban-client set sshd unbanip 90.119.124.77 2>/dev/null || true
sudo fail2ban-client status sshd
```

Expected: `Currently banned: 0` (or your IP not in the list).

- [ ] **Step 5: Verify .env has real secrets (not defaults)**

```bash
sudo grep -E 'MINIO_ROOT_USER|MINIO_ROOT_PASSWORD|XYOPS_SECRET_KEY|DAGSTER_PG_PASSWORD|TUNNEL_TOKEN' /opt/common-ground/.env | sed 's/=.*/=<redacted>/'
```

Expected: All 5 variables present. If any are missing, add them from the SOPS-encrypted secrets:

```bash
# On your local machine:
sops --decrypt --input-type dotenv --output-type dotenv ~/Desktop/dagster-pipeline/.env.secrets.enc | grep MINIO
# Then set on server
```

- [ ] **Step 6: Test SSH from local after unban**

```bash
ssh hetzner "hostname && docker ps --format '{{.Names}}' | sort"
```

Expected: Connection succeeds, lists all running containers.

---

## Task 5: Fix Cloudflare SSH tunnel

**Priority:** Provides backup SSH access when fail2ban or network issues block direct connection.

**Prerequisite:** Task 4 completed (server access restored).

- [ ] **Step 1: Check if SSH tunnel exists in Cloudflare dashboard**

```bash
ssh hetzner "docker exec common-ground-cloudflared-1 cloudflared tunnel info 2>&1 | head -20"
```

This shows what hostnames are routed through the tunnel.

- [ ] **Step 2: Check if ssh.common-ground.nyc is in the tunnel config**

Go to: https://one.dash.cloudflare.com/ → Networks → Tunnels → select the `common-ground` tunnel → Public Hostname tab.

Look for `ssh.common-ground.nyc`. If missing:
- Add hostname: `ssh.common-ground.nyc`
- Service: `ssh://localhost:22`
- Enable "Access" with email OTP policy

If present but broken, check:
- Is the service type `ssh://` (not `http://`)?
- Is the target `localhost:22`?
- Restart cloudflared: `ssh hetzner "cd /opt/common-ground && sudo docker compose restart cloudflared"`

- [ ] **Step 3: Test the tunnel**

```bash
ssh hetzner-tunnel "echo connected"
```

Expected: `connected` (may prompt for Cloudflare Access email OTP first time).

---

## Task 6: Enable Hetzner delete protection and backups

**Priority:** Infrastructure safety net. Can be done anytime via API.

- [ ] **Step 1: Enable delete + rebuild protection on common-ground**

Use Hetzner MCP tool or CLI:

```
mcp__hetzner__update_server(id=122765563, protection={"delete": true, "rebuild": true})
```

Or via hcloud CLI:
```bash
hcloud server enable-protection common-ground delete rebuild
```

- [ ] **Step 2: Enable delete protection on duckdb-data volume**

```
mcp__hetzner__resize_volume  # No direct protection tool — use Hetzner Console
```

Go to: https://console.hetzner.cloud → Volumes → `duckdb-data` → Protection → Enable delete protection.

- [ ] **Step 3: Enable backups on common-ground**

```bash
hcloud server enable-backup common-ground
```

Cost: 20% of server price (~€5/month for CCX33). Provides 7 rolling daily backups.

- [ ] **Step 4: Enable delete protection on cg-france**

```bash
hcloud server enable-protection cg-france delete rebuild
```

- [ ] **Step 5: Add labels to cg-france**

```bash
hcloud server add-label cg-france env=production
hcloud server add-label cg-france project=common-ground
hcloud server add-label cg-france role=pipeline-france
```

- [ ] **Step 6: Verify all protections**

```bash
hcloud server describe common-ground -o json | jq '.protection'
hcloud server describe cg-france -o json | jq '.protection'
hcloud volume describe duckdb-data -o json | jq '.protection'
```

Expected: All show `"delete": true`.

---

## Task 7: Add data volume to cg-france

**Priority:** Low — but important for data persistence. Server and volume must be in same location (nbg1).

- [ ] **Step 1: Create and attach volume**

```bash
hcloud volume create --name cg-france-data --size 100 --server cg-france --format ext4 --automount
```

This creates a 100GB volume and auto-mounts it (requires Hetzner agent on server).

- [ ] **Step 2: Verify mount on server**

```bash
ssh france "df -h | grep HC_Volume"
```

Expected: Volume mounted at `/mnt/HC_Volume_<id>`.

- [ ] **Step 3: Create a symlink for consistent paths**

Use the actual volume ID from Step 2's output (replace NNNNN):
```bash
ssh france "sudo ln -s /mnt/HC_Volume_NNNNN /mnt/data"
```

- [ ] **Step 4: Enable delete protection on the new volume**

```bash
hcloud volume enable-protection cg-france-data delete
```

---

## Execution Order & Dependencies

```
Task 1 (deploy.sh)          ← Immediate, no dependencies
Task 2 (SSH config)          ← Immediate, no dependencies
Task 3 (compose defaults)    ← Immediate, no dependencies
  ↓
Task 4 (fail2ban + secrets)  ← Needs SSH access (wait for ban to expire or use Hetzner Console)
  ↓
Task 5 (Cloudflare SSH)      ← Needs server access from Task 4
  ↓
Task 6 (Hetzner protections) ← Can run anytime via API, but logically after server access confirmed
Task 7 (France volume)       ← Independent, can run anytime via API
```

**Tasks 1, 2, 3** can all be done in parallel right now.
**Task 4** is blocked until fail2ban expires (~10 min default, check server config).
**Tasks 6, 7** can be done via Hetzner MCP tools without SSH.

---

## Verification Checklist

After all tasks complete:

- [ ] `ssh hetzner` connects without specifying key or user
- [ ] `ssh hetzner-tunnel` connects via Cloudflare as fallback
- [ ] `ssh france` connects without specifying key or user
- [ ] `grep 'root@' ~/Desktop/data-pipeline/deploy.sh` returns nothing
- [ ] `grep 'minioadmin' ~/Desktop/*/docker-compose.yml` returns nothing
- [ ] Both servers have delete protection enabled
- [ ] common-ground has automated backups enabled
- [ ] cg-france has labels and a data volume
- [ ] fail2ban whitelist includes team IP
