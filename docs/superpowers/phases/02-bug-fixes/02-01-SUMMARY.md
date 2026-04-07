---
phase: 02-bug-fixes
plan: 01
subsystem: infra
tags: [duckdb, ducklake, minio, s3, duckpgq, docker]

requires:
  - phase: 01-data-audit
    provides: entity registry identifying all tables needing S3 access
provides:
  - DuckLake reads parquet from MinIO via S3 protocol
  - duckpgq extension loaded and all 7 property graphs created
  - Graph cache rebuilt from live data
affects: [02-bug-fixes, 03-ownership-rebuild, 04-corporate-web-rebuild]

tech-stack:
  added: []
  patterns: [reconnect-path-must-reapply-all-settings, minio-http-internal-only]

key-files:
  modified:
    - /opt/common-ground/duckdb-server/mcp_server.py
    - /opt/common-ground/duckdb-server/Dockerfile
    - /opt/common-ground/duckdb-server/start.sh
    - /opt/common-ground/docker-compose.yml

key-decisions:
  - "Removed CREATE SECRET — SET s3_* is the only approach DuckLake respects"
  - "MinIO switched to HTTP-only (removed TLS cert mounts) — internal-only, firewall-protected"
  - "FORCE INSTALL duckpgq FROM community ensures version matches DuckDB build"
  - "Reconnect path must re-apply ALL settings (S3, extensions) — warm-up always fails"

patterns-established:
  - "DuckLake warm-up always fails on information_schema query — reconnect path is the primary code path"
  - "Any new connection settings must be applied in BOTH initial setup AND reconnect path"

duration: 90min
completed: 2026-03-26
---

# Phase 2 Plan 1: Infrastructure Fixes Summary

**DuckLake S3 reads from MinIO fixed (root cause: reconnect path dropped S3 creds), duckpgq loaded via FORCE INSTALL, all 7 property graphs created from live data.**

## Performance

- **Duration:** ~90 min (extensive debugging)
- **Started:** 2026-03-26T01:15:00Z
- **Completed:** 2026-03-26T01:56:00Z
- **Tasks:** 2
- **Files modified:** 4 (remote)

## Accomplishments

- All 7 property graphs built from live lake data (first successful rebuild in days)
- 194K owners, 348K buildings, 10.7M violations, 417K corps, 311K political entities, 58K contractors, 197K officers
- Graph cache saved to Parquet for fast restarts
- duckpgq, rapidfuzz, anofox_forecast all loading correctly
- MCP tools returning live data

## Task Commits

No local git commits — all changes were remote on Hetzner server via SSH.

1. **Task 1: Fix S3 credentials** — remote edit of mcp_server.py (reconnect path + SET s3_*)
2. **Task 2: Fix duckpgq loading** — remote edit of mcp_server.py (FORCE INSTALL) + Dockerfile (ca-certificates)

## Files Created/Modified (Remote)

- `/opt/common-ground/duckdb-server/mcp_server.py` — S3 reconnect fix, FORCE INSTALL, posthog optional import
- `/opt/common-ground/duckdb-server/Dockerfile` — added ca-certificates, CURL_CA_BUNDLE env, posthog pip install
- `/opt/common-ground/duckdb-server/start.sh` — MinIO cert trust store setup
- `/opt/common-ground/docker-compose.yml` — removed MinIO TLS cert mounts

## Decisions Made

1. **Removed CREATE SECRET** — DuckLake ignores the httpfs secrets system entirely. Only `SET s3_*` global settings work, and they must be set on the active connection before ATTACH.
2. **MinIO HTTP-only** — Removed self-signed TLS certs. MinIO is internal-only (Docker network), protected by Hetzner firewall. DuckDB's libcurl couldn't verify self-signed certs anyway.
3. **FORCE INSTALL from community** — Ensures extension binary matches DuckDB version after any rebuild.
4. **Reconnect path is primary** — The warm-up check (`SELECT 1 FROM lake.information_schema.tables`) always fails because DuckLake doesn't expose `information_schema` that way. So the reconnect path runs EVERY startup. All initialization must happen there.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Postgres password auth failure after docker compose recreate**
- **Found during:** Task 1 (multiple restarts)
- **Issue:** `docker compose up -d` recreated Postgres container, resetting the password
- **Fix:** `ALTER USER dagster WITH PASSWORD '...'` to match .env
- **Committed in:** N/A (remote fix)

**2. [Rule 1 - Bug] stray 'n' character on line 568**
- **Found during:** Initial server investigation
- **Issue:** `n    # Disable compactor` — stray character broke indentation
- **Fix:** Removed stray character
- **Committed in:** N/A (remote fix)

**3. [Rule 1 - Bug] posthog module not installed**
- **Found during:** Server crash loop
- **Issue:** `import posthog` but posthog not in Dockerfile
- **Fix:** Made import optional with try/except, added posthog to Dockerfile pip install
- **Committed in:** N/A (remote fix)

---

**Total deviations:** 3 auto-fixed (all blocking), 0 deferred
**Impact on plan:** All fixes necessary to get server running. No scope creep.

## Issues Encountered

- DuckLake's S3 client is completely separate from DuckDB's httpfs extension — it ignores CREATE SECRET, SCOPE, PROVIDER settings
- MinIO with mounted TLS certs only serves HTTPS — DuckDB couldn't verify self-signed cert
- The warm-up check always fails, making the reconnect path the de facto primary startup path
- Multiple Postgres password resets needed after container recreation

## Next Phase Readiness

- Infrastructure fixed — S3 reads work, duckpgq loaded, all graphs built
- Ready for 02-02 (data bug fixes: property_history, graph_corps dedup, orphaned violations)
- MinIO HTTP-only change needs to be synced to local infra/ directory for deployment script

---
*Phase: 02-bug-fixes*
*Completed: 2026-03-26*
