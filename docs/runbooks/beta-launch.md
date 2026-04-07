# Common Ground NYC — Beta Launch Runbook

Team reference for operating the Common Ground NYC infrastructure.

---

## Access

| Service | URL | Auth | Who |
|---------|-----|------|-----|
| Website | https://common-ground.nyc | Public | Everyone |
| MCP Server | https://mcp.common-ground.nyc/mcp | None | Everyone |
| DuckDB UI | https://duckdb.common-ground.nyc | Auth0 (built-in) | Everyone |
| Dagster UI | https://dagster.common-ground.nyc | Cloudflare Access (email OTP) | Team only |
| Duck-UI | https://duck-ui.common-ground.nyc | Cloudflare Access (email OTP) | Team only |
| xyOps | https://xyops.common-ground.nyc | Cloudflare Access (email OTP) | Team only |
| MinIO Console | https://minio.common-ground.nyc | Cloudflare Access (email OTP) | Team only |
| Hetzner SSH (prod) | `ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119` | SSH key | Team only |
| Hetzner SSH (france) | `ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.104.77.236` | SSH key | Team only |
| Postgres | `178.156.228.119:5432` (sslmode=require) | Creds in `.env.secrets` | Pipeline only |

---

## Dagster UI Login

Auth is handled by **Cloudflare Access** (email OTP). No username/password needed.

---

## Editing Secrets

All secrets are managed via **SOPS + age** encryption.

- **Edit in-place** (opens in `$EDITOR`):
  ```bash
  sops --input-type dotenv --output-type dotenv .env.secrets.enc
  ```

- **Decrypt to file:**
  ```bash
  sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc > .env.secrets
  ```

- **Add a team member:**
  1. They run `age-keygen` and send you their public key.
  2. Add the public key to `.sops.yaml`.
  3. Run `sops updatekeys .env.secrets.enc`.

- **Age key location on macOS:** `~/Library/Application Support/sops/age/keys.txt`

---

## Deploying

### Pipeline

Rsync code + decrypted `.env.secrets` to the server, then:

```bash
docker compose up -d
```

### Website

```bash
cd ~/Desktop/common-ground-website && npm run deploy
```

---

## Credential Rotation Procedures

### Postgres

1. SSH to server.
2. Change the password:
   ```bash
   docker exec -it common-ground-postgres-1 psql -U dagster -d ducklake -c "ALTER ROLE dagster WITH PASSWORD 'new'"
   ```
3. Update the catalog URL in secrets:
   ```bash
   sops --input-type dotenv --output-type dotenv .env.secrets.enc
   ```
4. Deploy.

### MinIO

1. SSH to server, use `mc` CLI to create a new service account.
2. Update `.env.secrets.enc` with the new access key + secret.
3. Deploy.
4. Delete the old service account.

### API Tokens

1. Generate a new token on the provider website.
2. Update the relevant `SOURCES__*` var in `.env.secrets.enc`.
3. Deploy.

---

## Firewall Management

- **Hetzner Cloud firewall** restricts ports to team IPs.
- **If your IP changes:** log into https://console.hetzner.cloud > Firewalls > `beta-lockdown` > Edit rules.
- **Emergency fallback:** Hetzner Console (web terminal) always works regardless of firewall.

---

## Emergency Procedures

### Pipeline Stuck

```bash
ssh fattie@178.156.228.119
cd /opt/common-ground
docker compose logs --tail=50 dagster-daemon
docker compose restart dagster-daemon
```

### Website Down

1. Check https://www.cloudflarestatus.com
2. Redeploy:
   ```bash
   cd ~/Desktop/common-ground-website && npm run deploy
   ```
3. Rollback:
   ```bash
   npx wrangler rollback
   ```

### Server Unresponsive

1. Use **Hetzner Console** (web terminal).
2. Or: Hetzner Cloud API to reboot.

---

## SSL Cert Renewal

| Component | Type | Expiry | Location |
|-----------|------|--------|----------|
| Caddy | Automatic (Let's Encrypt) | Auto-renewed | Managed by Caddy |
| Postgres | Self-signed | ~March 2027 | `/opt/postgres-ssl/` |
| MinIO | Self-signed | ~March 2027 | `/opt/minio-certs/` |

After regenerating self-signed certs:

```bash
docker compose restart postgres minio
```

---

## SSH Access

- Root login is **disabled** on both servers.
- Only user `fattie` can SSH.
- **fail2ban** active: 3 failed attempts = 1 hour ban.
- Unattended security upgrades enabled.
