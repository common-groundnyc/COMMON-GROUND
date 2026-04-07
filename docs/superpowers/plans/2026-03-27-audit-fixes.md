# Audit Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all CRITICAL and IMPORTANT findings from the security, infrastructure, and accessibility audits — 12 fixes across 3 codebases.

**Architecture:** 9 independent tasks across 3 repos. Tasks 1-8 can run in parallel. Task 9 deploys everything.

**Tech Stack:** Python (MCP server), Docker Compose, Next.js/React (website), CSS

---

### Task 1: Remove MinIO default username fallback
**Severity:** CRITICAL (security) | **Repo:** dagster-pipeline | **File:** `infra/duckdb-server/mcp_server.py`

- [ ] Change `os.environ.get("MINIO_ROOT_USER", "minioadmin")` → `os.environ.get("MINIO_ROOT_USER", "")` with warning log. Two locations: lifespan (~line 695) and `_catalog_connect()` (~line 12615).
- [ ] Commit: `security(mcp): remove default MinIO admin username fallback`

---

### Task 2: Pin Docker images + MinIO healthcheck
**Severity:** CRITICAL (infra) | **Repo:** dagster-pipeline | **File:** `infra/hetzner-docker-compose.yml`

- [ ] Pin `minio/minio` to dated release tag (check hub.docker.com for latest)
- [ ] Pin `ghcr.io/caioricciuti/duck-ui:latest` to specific version
- [ ] Add MinIO healthcheck: `curl -f http://localhost:9000/minio/health/live` (interval 30s, retries 3, start_period 30s)
- [ ] Update duckdb-server `depends_on` to `minio: condition: service_healthy`
- [ ] Commit: `infra: pin images, add MinIO healthcheck`

---

### Task 3: Add Postgres daily backup
**Severity:** CRITICAL (infra) | **Repo:** dagster-pipeline | **File:** `infra/hetzner-docker-compose.yml`

- [ ] Add `pg-backup` service: `postgres:17-alpine`, runs `pg_dump -h postgres -U dagster ducklake | gzip > /backups/ducklake_$(date +%Y%m%d_%H%M%S).sql.gz`, sleep 86400 loop, deletes backups older than 7 days
- [ ] Volume: `/mnt/data/common-ground/backups:/backups`
- [ ] Create backup dir on server: `ssh ... "sudo mkdir -p /mnt/data/common-ground/backups"`
- [ ] Commit: `infra: add daily Postgres backup with 7-day retention`

---

### Task 4: Pin pip packages in Dockerfile
**Severity:** IMPORTANT (infra) | **Repo:** dagster-pipeline | **File:** `infra/duckdb-server/Dockerfile`

- [ ] Get current versions: `ssh ... "sudo docker exec common-ground-duckdb-server-1 pip list --format=freeze"` — extract posthog, openpyxl, onnxruntime, numpy, pyarrow, aiohttp versions
- [ ] Pin all packages in the `pip install` line to their current versions
- [ ] Commit: `infra: pin all pip packages in Dockerfile`

---

### Task 5: Add ARIA to data health table
**Severity:** CRITICAL (a11y) | **Repo:** common-ground-website | **File:** `src/components/data-health-table.tsx`

- [ ] Add `aria-label="Search tables by name or schema"` to search input
- [ ] Add `aria-label="Filter by schema"` to select dropdown
- [ ] Add `aria-sort={sortKey === key ? (sortDir === "asc" ? "ascending" : "descending") : "none"}` to each sortable `<th>`
- [ ] Add `aria-label` to Prev/Next buttons: `"Previous page"` / `"Next page"`
- [ ] Add `role="grid" aria-label="Data lake table health status"` to `<table>`
- [ ] Wrap loading/error state in `<div aria-live="polite">`
- [ ] Commit: `a11y: add ARIA attributes to data health table`

---

### Task 6: Fix light-mode contrast
**Severity:** IMPORTANT (a11y) | **Repo:** common-ground-website | **File:** `src/app/globals.css`

- [ ] In `:root` block: `--muted-dim: #9CA3AF` → `--muted-dim: #6B7280` (contrast 2.8:1 → 4.8:1)
- [ ] In `:root` block: `--muted: #6B6B6B` → `--muted: #5C5C5C` (contrast 4.2:1 → 5.2:1)
- [ ] Do NOT change `[data-theme="dark"]` tokens (already pass WCAG AA)
- [ ] Commit: `a11y: fix light-mode contrast to pass WCAG AA`

---

### Task 7: Fix skip-to-content + nav aria
**Severity:** IMPORTANT (a11y) | **Repo:** common-ground-website | **Files:** `src/app/page.tsx`, `src/components/site-nav.tsx`

- [ ] Add `id="main-content"` to the home page's `<main>` or first `<section>` after nav
- [ ] Add `aria-label="Main navigation"` to SiteNav's `<nav>` element
- [ ] Commit: `a11y: fix skip-to-content target, add nav aria-label`

---

### Task 8: Remove three.js if unused
**Severity:** CRITICAL (perf) | **Repo:** common-ground-website | **File:** `package.json`

- [ ] Search: `grep -r "from 'three'\|from \"three\"\|@react-three" src/ --include="*.tsx" --include="*.ts"`
- [ ] If NO imports: `npm uninstall three @react-three/fiber @types/three`
- [ ] If imports found: wrap with `dynamic(() => import(...), { ssr: false })` for lazy loading
- [ ] Verify build: `npm run build` → `Compiled successfully`
- [ ] Commit: `perf: remove unused three.js (~600KB saved)` or `perf: lazy-load three.js`

---

### Task 9: Deploy everything

- [ ] Deploy MCP server:
```bash
scp -i ~/.ssh/id_ed25519_hetzner infra/duckdb-server/mcp_server.py fattie@178.156.228.119:/opt/common-ground/duckdb-server/mcp_server.py
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "cd /opt/common-ground && sudo docker compose build --no-cache duckdb-server && sudo docker compose up -d duckdb-server"
```
- [ ] Deploy infra:
```bash
scp -i ~/.ssh/id_ed25519_hetzner infra/hetzner-docker-compose.yml fattie@178.156.228.119:/opt/common-ground/docker-compose.yml
scp -i ~/.ssh/id_ed25519_hetzner infra/duckdb-server/Dockerfile fattie@178.156.228.119:/opt/common-ground/duckdb-server/Dockerfile
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "cd /opt/common-ground && sudo docker compose up -d"
```
- [ ] Deploy website: `cd ~/Desktop/common-ground-website && npm run deploy`
- [ ] Verify: catalog endpoint returns tables, website returns 200, backup service running
