# Infrastructure & DevOps Audit

**Auditor**: infra-engineer (cg-innovation-audit team)
**Date**: 2026-04-01
**Scope**: Single Hetzner server, Docker Compose, DuckDB/DuckLake, MinIO, Postgres, Cloudflare Tunnel

---

## 1. Current State Assessment

### Server Architecture

Single Hetzner dedicated server at `178.156.228.119` running everything:

| Service | Container | Memory Limit | CPU Limit | Purpose |
|---------|-----------|-------------|-----------|---------|
| dagster-code | gRPC code server | 24 GB | 8 cores | Asset execution (forkserver) |
| dagster-webserver | Web UI + GraphQL | 2 GB | 1 core | UI via Cloudflare Tunnel |
| dagster-daemon | Scheduler/sensors | 4 GB | 2 cores | Run orchestration |
| duckdb-server | FastMCP + DuckDB | 28 GB | unlimited | MCP API, DuckDB queries |
| postgres | PostgreSQL 16 | 2 GB | unlimited | DuckLake catalog + Dagster storage |
| minio | MinIO S3 | 1 GB | unlimited | Parquet file storage |
| duck-ui | DuckDB web UI | 512 MB | unlimited | SQL exploration |
| xyops | Monitoring | 256 MB | unlimited | Server monitoring dashboard |
| pg-backup | Postgres dump | 256 MB | unlimited | Daily pg_dump loop |

**Total memory allocated**: ~62 GB across containers. This is a tightly packed single-server deployment.

### Networking

- **External access**: Cloudflare Tunnel (`tunnel_net` external Docker network)
  - `dagster.common-ground.nyc` -- Dagster UI (behind Cloudflare Access)
  - `mcp.common-ground.nyc/mcp` -- FastMCP endpoint
  - `duckdb.common-ground.nyc` -- DuckDB UI
  - `xyops.common-ground.nyc` -- Monitoring
- **Internal**: Docker bridge networks (`frontend`, `backend`, `shared`)
- **Direct ports**: Postgres 5432, MinIO 9000/9001 (firewall-restricted to team IPs)

### Deployment

- `deploy.sh`: rsync + SSH + `docker compose up -d --build`
- No CI/CD pipeline -- manual script execution
- No rollback mechanism beyond re-deploying previous code
- No build cache optimization (full rebuild every deploy)
- Secrets: SOPS + age encryption (`.env.secrets.enc`) -- good practice

### Storage

- All data on a single volume at `/mnt/data/common-ground/`
  - Postgres data: `/mnt/data/common-ground/pgdata`
  - MinIO/parquet: `/mnt/data/common-ground/` (lakevol)
  - xyops: `/mnt/data/common-ground/xyops`
- No RAID information visible in config (single point of failure if disk-based)
- Backups: pg_dump every 24 hours, 7-day retention. **No MinIO/parquet backup.**

### Dagster Configuration

- **Storage**: SQLite (`/dagster-home/storage`) -- fine for single-instance but not scalable
- **Run launcher**: DefaultRunLauncher -- runs execute in the dagster-code container via multiprocess executor
- **Run retries**: enabled, max 2, only for infra crashes (not code errors) -- good
- **Run monitoring**: 15s poll, 6-hour max runtime, auto-cancel stuck runs -- good
- **Retention**: 30-day schedules, 7-day skipped sensors, 90-day successes -- reasonable

---

## 2. Risk Assessment

### Critical Risks

| Risk | Impact | Likelihood | Current Mitigation |
|------|--------|-----------|-------------------|
| **Server failure** | Total platform loss | Medium | Postgres backup only, no parquet backup |
| **Disk failure** | Data loss | Medium | No visible RAID or replication |
| **No rollback** | Extended downtime on bad deploy | High | None -- rsync + rebuild |
| **DuckLake catalog loss** | Parquet files become orphaned | High | pg_dump daily, but 24h RPO |
| **MinIO data loss** | All parquet data gone | High | **No backup at all** |
| **Single-threaded DuckDB** | API unavailable during heavy queries | Medium | None |
| **No monitoring/alerting** | Silent failures go unnoticed | High | xyops for basic metrics only |
| **No log aggregation** | Debugging requires SSH + docker logs | High | None |

### Security Assessment

**Strengths**:
- SOPS + age for secrets management (well-implemented)
- Cloudflare Tunnel eliminates exposed ports (no public IP needed)
- Cloudflare Access on Dagster UI (authentication layer)
- SSH restricted to team IPs + ed25519 keys
- Postgres SSL enabled (self-signed certs)
- Root login disabled

**Weaknesses**:
- Docker socket mounted in xyops container (`/var/run/docker.sock:ro`) -- container escape risk
- No WAF rules on MCP endpoint (public-facing API)
- No rate limiting on MCP endpoint (DuckDB resource exhaustion possible)
- No mTLS between Cloudflare and origin
- DuckDB server Dockerfile pulls from Bullseye repos for libssl1.1 -- legacy dependency
- `duck-ui` image unpinned (`ghcr.io/caioricciuti/duck-ui:latest`) -- supply chain risk
- MinIO console (port 9001) exposed on host -- should be internal only
- Self-signed SSL certs for DuckDB UI nginx proxy (not verified by clients)
- Gemini API key and GCP credentials mounted in duckdb-server container

---

## 3. Recommendations

### Priority 1: Disaster Recovery (Week 1-2)

**Problem**: MinIO parquet files have ZERO backup. A disk failure loses everything.

**Recommendation: Two-tier backup strategy**

1. **MinIO bucket replication** to a second Hetzner server or Hetzner Object Storage:
   - Use `mc mirror --watch` for continuous replication
   - Or configure MinIO server-side replication to a standby instance
   - Hetzner Object Storage is S3-compatible and costs ~EUR 5/TB/month

2. **Postgres catalog backup hardening**:
   - Current: pg_dump every 24h, 7-day retention -- 24-hour RPO is too high
   - Recommended: WAL archiving to MinIO or Hetzner Object Storage for point-in-time recovery
   - Use `pgBackRest` or `wal-g` for continuous archival (RPO ~minutes instead of 24h)
   - Or at minimum, increase pg_dump frequency to every 4 hours

3. **DuckLake-specific backup**:
   - The Postgres catalog is the single point of truth for which parquet files belong to which tables/versions
   - Losing the catalog means orphaned parquet files with no way to reconstruct the lake
   - Treat catalog backup as the MOST critical backup in the system

### Priority 2: Observability (Week 2-4)

**Problem**: No centralized logging, no metrics beyond basic xyops, no alerting.

**Recommendation: LGTM Stack (Loki + Grafana + Tempo + Mimir)**

Add to `hetzner-docker-compose.yml`:

```yaml
# Lightweight observability stack
otel-collector:
  image: otel/opentelemetry-collector-contrib:0.122.0
  # Routes: traces -> Tempo, logs -> Loki, metrics -> Prometheus

loki:
  image: grafana/loki:3.5.0
  # Docker log driver sends container logs here

grafana:
  image: grafana/grafana:12.0.0
  # Dashboards: container health, DuckDB query latency, Dagster run status

prometheus:
  image: prom/prometheus:3.4.0
  # Scrapes: container metrics, MinIO metrics, Postgres metrics
```

**Memory budget**: ~1.5 GB total for the observability stack.

**Key dashboards to build**:
- DuckDB query latency and memory usage (via PostHog events already being sent)
- Dagster run success/failure rates
- MinIO storage growth and I/O
- Container health and restart counts
- MCP endpoint request rates and error rates

**Alerting**: Grafana alerting to Slack/email for:
- Container restarts > 2 in 10 minutes
- Disk usage > 80%
- Dagster run failures
- DuckDB OOM events
- MCP endpoint 5xx rate > 5%

### Priority 3: CI/CD Pipeline (Week 3-4)

**Problem**: Manual `deploy.sh` with no testing, no rollback, no branch deployments.

**Recommendation: GitHub Actions pipeline**

```
PR opened -> lint + test -> build Docker images -> push to registry
PR merged -> build -> deploy to staging (if applicable) -> deploy to production
```

Concrete steps:

1. **GitHub Container Registry** for Docker images (free with GitHub)
   - Build and tag images in CI, pull on server (faster deploys, cached layers)
   - Current approach rebuilds on the server every time -- slow and wasteful

2. **Blue-green deployment**:
   - Deploy new containers alongside old ones
   - Health check passes -> swap traffic
   - Health check fails -> rollback automatically
   - Docker Compose profiles can handle this on a single server

3. **Pre-deploy validation**:
   - Run DuckDB server health check against new image
   - Verify Dagster can load code locations
   - Run MCP tool smoke tests

4. **Deployment script upgrade** (`deploy.sh` v2):
   ```bash
   # Pull pre-built images instead of building on server
   # Keep previous image tagged as :rollback
   # Health-check new containers before removing old ones
   # Automatic rollback on health check failure
   ```

### Priority 4: Orchestration Upgrade Path (Month 2-3)

**Problem**: Docker Compose works but has no service discovery, rolling updates, or multi-node scaling.

**Evaluation of options**:

| Option | Overhead | Complexity | Multi-node | Best For |
|--------|----------|-----------|-----------|----------|
| **Docker Compose** (current) | ~0 MB | Low | No | Single server, < 10 services |
| **Coolify** | ~500-700 MB | Low | Yes (Swarm) | Web apps, git-push deploys |
| **K3s** | ~450-500 MB | Medium | Yes | Full K8s ecosystem, HA |
| **Nomad** | ~60-80 MB | Medium | Yes | Lightweight, non-K8s orchestration |

**Recommendation**: Stay on Docker Compose for now. The platform has ~8 services on a single server -- orchestration is overkill at this scale.

**When to upgrade**: If any of these become true:
- Need a second server (e.g., separate compute for Dagster vs DuckDB serving)
- Need zero-downtime deployments (blue-green with health checks)
- Need auto-scaling for MCP API traffic
- Need multi-region or HA

**If upgrading**: Coolify is the lowest-friction path from Docker Compose:
- Supports raw Docker Compose files directly
- Adds git-push deploys, SSL, monitoring out of the box
- ~52k public instances by 2026 -- battle-tested
- Can later expand to Docker Swarm for multi-node

**K3s** is better if you need the Kubernetes ecosystem (Helm charts, CRDs, operators) but adds significant operational complexity for a small team.

### Priority 5: Security Hardening (Ongoing)

1. **Pin all Docker images** -- `duck-ui:latest` is a supply chain risk. Pin to specific digests.

2. **Remove Docker socket mount** from xyops -- or use a Docker socket proxy like `tecnativa/docker-socket-proxy` that limits API access.

3. **Add Cloudflare WAF rules** on MCP endpoint:
   - Rate limiting: 100 req/min per IP
   - Block known bad user agents
   - Challenge on suspicious patterns

4. **Enable mTLS** between Cloudflare Tunnel and origin:
   - Cloudflare can verify origin certificates
   - Prevents anyone who discovers the server IP from bypassing Cloudflare

5. **Remove libssl1.1 dependency** in DuckDB server Dockerfile:
   - Pulling from Bullseye repos in a Bookworm image is fragile
   - Investigate which dependency actually needs OpenSSL 1.1 and upgrade or replace it

6. **Secrets rotation schedule**:
   - Rotate MinIO credentials quarterly
   - Rotate Postgres password quarterly
   - Rotate Gemini API key if compromised
   - Automate with a cron job that updates SOPS and redeploys

7. **Network isolation**:
   - MinIO console (port 9001) should not be exposed on host -- access via Cloudflare Tunnel only
   - Postgres port should be firewalled (already done but verify regularly)

### Priority 6: DuckDB Scaling (Month 3+)

**Current bottleneck**: Single DuckDB process handles both MCP API queries and background processing.

**Options for read scaling**:

1. **Read replicas via file copy** (simplest):
   - DuckDB can open parquet files read-only from multiple processes
   - DuckLake catalog can be queried read-only by multiple DuckDB instances
   - Run 2-3 `duckdb-server` containers, each with its own DuckDB connection
   - Nginx load balances across them
   - **Caveat**: DuckLake catalog is in Postgres, which handles concurrent reads fine

2. **MotherDuck for read scaling** (managed):
   - Read scaling tokens spin up dedicated "Ducklings" for concurrent reads
   - Up to 16 replicas by default
   - Good if MCP traffic grows significantly
   - Cost: usage-based, likely $50-200/month for moderate traffic

3. **Connection pooling** (already partially done):
   - The MCP server already has a `CursorPool` with timeout and replacement
   - Ensure pool size matches expected concurrency
   - Monitor pool exhaustion events

### Priority 7: Edge/CDN for Data APIs (Month 3+)

**Cloudflare already provides CDN** for the tunnel endpoints. Additional optimizations:

1. **Cache MCP responses** for common queries:
   - Neighborhood data, school info, building lookups change infrequently
   - Add `Cache-Control` headers for safe-to-cache responses
   - Cloudflare will cache at edge (200+ PoPs worldwide)

2. **Cloudflare Workers** for API gateway logic:
   - Rate limiting, authentication, request validation at the edge
   - Reduces load on origin server
   - Can serve cached responses without hitting DuckDB

---

## 4. Infrastructure Roadmap

### Month 1: Foundation
- [ ] MinIO backup to Hetzner Object Storage (`mc mirror`)
- [ ] Postgres WAL archiving (pgBackRest or wal-g)
- [ ] Pin all Docker images to specific versions/digests
- [ ] GitHub Actions CI pipeline (build + test + push images)
- [ ] Basic Grafana + Prometheus for container metrics
- [ ] Remove Docker socket mount from xyops (use proxy)

### Month 2: Hardening
- [ ] LGTM observability stack fully deployed
- [ ] Alerting for critical events (disk, OOM, failures)
- [ ] Blue-green deployment with automatic rollback
- [ ] Cloudflare WAF rules on MCP endpoint
- [ ] mTLS between Cloudflare and origin
- [ ] Secrets rotation automation

### Month 3: Scaling
- [ ] DuckDB read replicas (2-3 instances behind load balancer)
- [ ] MCP response caching at Cloudflare edge
- [ ] Evaluate Coolify if multi-server needed
- [ ] Evaluate MotherDuck for managed read scaling
- [ ] Load testing and capacity planning

---

## 5. Cost Estimates

| Item | Monthly Cost | Notes |
|------|-------------|-------|
| Hetzner Object Storage (backup) | ~EUR 5-15 | 1-3 TB parquet backup |
| Second Hetzner server (DR) | ~EUR 40-80 | Optional standby |
| Grafana Cloud (free tier) | EUR 0 | 10k metrics, 50GB logs |
| GitHub Actions | EUR 0 | Free for public repos, 2000 min/month private |
| Cloudflare (current plan) | EUR 0-25 | Free tier likely sufficient |
| MotherDuck (if needed) | ~EUR 50-200 | Usage-based read scaling |

**Total incremental cost**: EUR 5-15/month for essential backup. EUR 50-100/month for full hardening.

---

## 6. Key Takeaways for Teammates

### For platform-architect
- DuckLake catalog in Postgres is the critical single point of failure -- backup strategy must prioritize this
- DuckDB read scaling is achievable via multiple containers reading from the same MinIO/Postgres backend
- Dagster's SQLite storage is fine for now but would need Postgres if scaling to multiple workers
- Docker Compose is sufficient at current scale -- don't over-engineer orchestration

### For ai-strategist
- DuckDB server has 28 GB memory limit -- AI features (embeddings, vector search) compete for this
- If adding LLM inference on-server, need a second Hetzner box or GPU instance
- Observability stack will help monitor AI feature performance (latency, memory, error rates)
- MCP endpoint needs rate limiting before exposing to more AI agents
- Edge caching could dramatically reduce load for repeated AI queries

---

## References

- [DuckLake Backup & Recovery](https://ducklake.select/docs/stable/duckdb/guides/backups_and_recovery)
- [MotherDuck Read Scaling](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/read-scaling/)
- [Duckstream - DuckDB replication tool](https://github.com/The-Singularity-Labs/duckstream)
- [MinIO Zero RPO Backup Strategy](https://blog.min.io/zero-rto-rpo-backup-and-restore/)
- [LGTM Observability Stack with OpenTelemetry](https://oneuptime.com/blog/post/2026-02-06-lgtm-stack-opentelemetry/view)
- [Docker Compose Observability Stack](https://oneuptime.com/blog/post/2026-02-06-docker-compose-observability-stack/view)
- [Dagster CI/CD with GitHub Actions](https://docs.dagster.io/deployment/dagster-plus/deploying-code/configuring-ci-cd)
- [Coolify v5 Sovereign PaaS](https://criztec.com/coolify-v5-sovereign-paas-2026-s-post-heroku-j5sr/)
- [Nomad vs K3s Comparison](https://tech.breakingcube.com/2026/03/21/nomad-vs-k3s-lightweight-orchestration-comparison/)
- [Cloudflare mTLS with Application Security](https://developers.cloudflare.com/learning-paths/mtls/mtls-app-security/)
- [Self-Hosted WAFs 2026](https://medium.com/@qcnvpnmsy0561/top-10-self-hosted-web-application-firewalls-waf-in-2026-64d6f01ba04e)
- [Cloudflare Tunnel Documentation](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/)
