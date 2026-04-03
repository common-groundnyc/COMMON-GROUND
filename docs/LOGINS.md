# Common Ground — Logins & Access

Quick reference. For full procedures see [BETA-LAUNCH-RUNBOOK.md](BETA-LAUNCH-RUNBOOK.md).

## Website

| | |
|-|-|
| URL | https://common-ground.nyc |
| Auth | None (public) |
| Deploy | `cd ~/Desktop/common-ground-website && npm run deploy` |
| Rollback | `npx wrangler rollback` |
| Dashboard | https://dash.cloudflare.com (common-ground.nyc zone) |

## Dagster UI

| | |
|-|-|
| URL | https://dagster.common-ground.nyc |
| Auth | Cloudflare Access (email OTP) |

## SSH — Production Server

| | |
|-|-|
| IP | 178.156.228.119 |
| User | `fattie` (root login disabled) |
| Command | `ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119` |
| Infra dir | `/opt/common-ground/` |
| Pipeline dir | `/opt/dagster-pipeline/` (after deploy) |

## SSH — France Server

| | |
|-|-|
| IP | 178.104.77.236 |
| User | `fattie` (root login disabled) |
| Command | `ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.104.77.236` |

## Postgres (DuckLake catalog)

| | |
|-|-|
| Host | 178.156.228.119:5432 |
| Database | `ducklake` |
| User | `dagster` |
| Password | In `.env.secrets.enc` → `DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG` |
| TLS | Required (`?sslmode=require`) |
| Container | `common-ground-postgres-1` |

## DuckDB MCP Server

| | |
|-|-|
| URL | https://mcp.common-ground.nyc |
| Auth | None (firewall-restricted to team IPs) |
| Container | `common-ground-duckdb-server-1` |

## DuckDB UI

| | |
|-|-|
| URL | https://duckdb.common-ground.nyc |
| Auth | Auth0 (built-in) |

## Duck-UI

| | |
|-|-|
| URL | https://duck-ui.common-ground.nyc |
| Auth | None (firewall-restricted) |

## xyOps Monitoring

| | |
|-|-|
| URL | https://xyops.common-ground.nyc |
| Auth | Secret key in server `.env` |

## Hetzner Cloud

| | |
|-|-|
| Console | https://console.hetzner.cloud |
| Prod server | `common-ground` (ID: 122765563) |
| France server | `cg-france` (ID: 124394203) |
| Firewall (prod) | `duckdb-firewall` (ID: 10638515) |
| Firewall (france) | `cg-france-firewall` (ID: 10751352) |

## Secrets Management

All secrets encrypted with SOPS + age. Never stored in plaintext in git.

```bash
# View a secret
sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc | grep KEYWORD

# Edit secrets (opens in $EDITOR)
sops --input-type dotenv --output-type dotenv .env.secrets.enc

# Decrypt for deploy
sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc > .env.secrets
```

Age key location: `~/Library/Application Support/sops/age/keys.txt`

## Firewall — Allowed IPs

Ports 22, 3000, 5432, 5522, 5580 restricted to:
- **IPv4:** 90.119.124.77
- **IPv6:** 2a01:cb1c:8188:c500::/64

To update: Hetzner Console > Firewalls > edit rules, or use `mcp__hetzner__set_firewall_rules`.

## Cloudflare Access (Team Dashboards)

Dagster, Duck-UI, and xyOps are behind Cloudflare Access.
- Auth: email OTP (one-time code sent to your email)
- Session: 24 hours
- Add team members: https://one.dash.cloudflare.com/ → Access → Applications → Edit policy
