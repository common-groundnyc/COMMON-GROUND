# MinIO → Local Filesystem Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate MinIO S3 layer — export parquet to local NVMe, update DuckLake catalog, remove MinIO. Expected 2-10x speedup on all queries.

**Architecture:** `mc mirror` exports raw parquet from MinIO to local directory. One Postgres UPDATE changes DuckLake's root `data_path`. MCP server drops S3 config. dlt pipeline switches to local writes. MinIO removed after 24h confidence period.

**Tech Stack:** MinIO client (`mc`), Postgres (`psql`), Docker, bash

**Spec:** `docs/superpowers/specs/2026-04-03-minio-removal-design.md`

---

## File Structure

| Action | File | What Changes |
|--------|------|-------------|
| Modify | `infra/hetzner-docker-compose.yml` | Remove minio service, remove duckdb-server dependency on minio |
| Modify | `infra/duckdb-server/mcp_server.py` | Remove S3 SECRET, remove http S3 settings |
| Modify | `.dlt/config.toml` (on server) | Change bucket_url from s3:// to file:// |
| Modify | `.env.secrets.enc` | Remove MINIO_ROOT_USER, MINIO_ROOT_PASSWORD |

---

### Task 1: Record baseline benchmarks

**Files:** None (operational)

All commands run from your local Mac via SSH to `fattie@178.156.228.119`.

- [ ] **Step 1: Run baseline benchmark via MCP**

```bash
cd ~/Desktop/dagster-pipeline && uv run python3 -c "
import asyncio, aiohttp, json, time
URL = 'https://mcp.common-ground.nyc/mcp'
TESTS = [
    ('COUNT hpd_violations', {'input': 'SELECT COUNT(*) FROM lake.housing.hpd_violations'}),
    ('PLUTO point lookup', {'input': \"SELECT address FROM lake.city_government.pluto WHERE LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = '1000670001' LIMIT 1\"}),
    ('mv_building_hub', {'input': \"SELECT address FROM lake.foundation.mv_building_hub WHERE bbl = '1000670001' LIMIT 1\"}),
    ('building full', {'input': 'SKIP'}),
    ('entity xray', {'input': 'SKIP'}),
]
async def q(s, l, a):
    if a.get('input') == 'SKIP': return l, 0, 'SKIP'
    p = {'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':'query','arguments':a}}
    h = {'Content-Type':'application/json','Accept':'application/json, text/event-stream'}
    t = time.monotonic()
    async with s.post(URL, json=p, headers=h, timeout=aiohttp.ClientTimeout(total=60)) as r:
        await r.text()
        return l, (time.monotonic()-t)*1000, 'OK'
async def main():
    async with aiohttp.ClientSession() as s:
        for l, a in TESTS:
            r = await q(s, l, a)
            print(f'  {r[0]:<25} {r[1]:>8.0f}ms  {r[2]}')
asyncio.run(main())
"
```

Also benchmark the full tools:
```bash
# building
curl -s --max-time 60 -X POST https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"building","arguments":{"identifier":"1000670001"}}}' \
  | grep -o '"query_time_ms":[0-9]*'

# entity
curl -s --max-time 120 -X POST https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"entity","arguments":{"name":"BLOOMBERG"}}}' \
  | grep -o '"query_time_ms":[0-9]*'
```

- [ ] **Step 2: Record results**

Save the output as a comment in this file. Example baseline (S3/MinIO):
```
COUNT hpd_violations:    ~350ms
PLUTO point lookup:      ~1700ms
mv_building_hub:         ~350ms
building (full):         ~3000ms
entity (xray):           ~27000ms
```

---

### Task 2: Resize Hetzner volume

**Files:** None (Hetzner console + SSH)

- [ ] **Step 1: Resize volume in Hetzner Cloud console**

1. Go to https://console.hetzner.cloud
2. Navigate to Volumes → find volume `HC_Volume_104936190` (250GB)
3. Click "Resize" → set to 500GB
4. Confirm (~€10/month additional)

The volume resize is instant and online — no downtime.

- [ ] **Step 2: Resize the filesystem**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
sudo resize2fs /dev/sdb
```

Expected output: `The filesystem on /dev/sdb is now XXXXX blocks long.`

- [ ] **Step 3: Verify new size**

```bash
df -h /mnt/HC_Volume_104936190
```

Expected: ~500GB total, ~300GB free.

---

### Task 3: Export data from MinIO

**Files:** None (operational)

- [ ] **Step 1: Install mc (MinIO client) on the host**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/
mc --version
```

Expected: `mc version RELEASE.2025-...`

- [ ] **Step 2: Configure mc alias**

```bash
# Get MinIO credentials from .env
source /opt/common-ground/.env 2>/dev/null || true
mc alias set myminio http://localhost:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
mc ls myminio/ducklake/data/ | head -5
```

Expected: lists schema directories (business, housing, education, etc.)

- [ ] **Step 3: Count source objects**

```bash
mc ls --recursive myminio/ducklake/data/ | wc -l
```

Save this number — you'll verify against it after the mirror.

- [ ] **Step 4: Create target directory**

```bash
sudo mkdir -p /mnt/data/common-ground/lake/data
sudo chown -R fattie:fattie /mnt/data/common-ground/lake
```

The duckdb-server container sees `/mnt/data/common-ground` as `/data/common-ground`, so the local path inside the container will be `/data/common-ground/lake/data/`.

- [ ] **Step 5: Run mc mirror**

```bash
mc mirror --preserve myminio/ducklake/data/ /mnt/data/common-ground/lake/data/
```

This exports all objects as raw parquet files. Streams data — low memory (~50-100MB).

**Monitor progress:** `mc mirror` shows a progress bar. For 194GB on local NVMe, expect 10-30 minutes.

**If interrupted:** `mc mirror` is resumable — just re-run and it skips already-copied files.

- [ ] **Step 6: Verify file count**

```bash
find /mnt/data/common-ground/lake/data/ -type f | wc -l
```

Compare to the count from Step 3. Must match.

- [ ] **Step 7: Verify a sample parquet file is readable**

```bash
# Quick test — read one parquet file header with DuckDB
docker exec common-ground-duckdb-server-1 python3 -c "
import duckdb
conn = duckdb.connect()
r = conn.execute(\"SELECT COUNT(*) FROM read_parquet('/data/common-ground/lake/data/housing/hpd_violations/*.parquet')\").fetchone()
print(f'hpd_violations via local parquet: {r[0]:,} rows')
"
```

Expected: same row count as the current `lake.housing.hpd_violations` table.

- [ ] **Step 8: Check disk usage**

```bash
du -sh /mnt/data/common-ground/lake/data/
df -h /mnt/HC_Volume_104936190
```

Verify: raw parquet size (should be less than MinIO's 194GB), and enough free space remains.

---

### Task 4: Switch DuckLake to local filesystem

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Stop the MCP server**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose stop duckdb-server"
```

- [ ] **Step 2: Update DuckLake catalog data_path in Postgres**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "docker exec common-ground-postgres-1 psql -U dagster -d ducklake -c \
    \"UPDATE lake.ducklake_metadata SET value = '/data/common-ground/lake/data/' WHERE key = 'data_path';\""
```

Expected: `UPDATE 1`

Verify:
```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "docker exec common-ground-postgres-1 psql -U dagster -d ducklake -c \
    \"SELECT key, value FROM lake.ducklake_metadata WHERE key = 'data_path';\""
```

Expected: `data_path | /data/common-ground/lake/data/`

- [ ] **Step 3: Remove S3/MinIO configuration from mcp_server.py**

In `infra/duckdb-server/mcp_server.py`, remove the primary S3 SECRET block (lines ~179-192):

```python
# DELETE these lines:
    # Configure S3 for MinIO via CREATE SECRET (survives DETACH/ATTACH cycles)
    minio_user = os.environ.get("MINIO_ROOT_USER", "").replace("'", "''")
    minio_pass = os.environ.get("MINIO_ROOT_PASSWORD", "").replace("'", "''")
    conn.execute(f"""
        CREATE OR REPLACE SECRET minio_s3 (
            TYPE s3,
            KEY_ID '{minio_user}',
            SECRET '{minio_pass}',
            ENDPOINT 'minio:9000',
            USE_SSL false,
            URL_STYLE 'path'
        )
    """)
    print("S3/MinIO credentials configured (SECRET)", flush=True)
```

Also remove the reconnect fallback S3 SECRET block (lines ~230-235):
```python
# DELETE these lines:
        # Reconfigure S3 via SECRET and performance tuning on new connection
        conn.execute(f"""
            CREATE OR REPLACE SECRET minio_s3 (
                TYPE s3, KEY_ID '{minio_user}', SECRET '{minio_pass}',
                ENDPOINT 'minio:9000', USE_SSL false, URL_STYLE 'path'
            )
        """)
```

Also remove the S3-specific HTTP settings (lines ~201-205):
```python
# DELETE these lines:
    # HTTP/S3 resilience — disable keep-alive to prevent stale 403s (known DuckDB bug)
    conn.execute("SET http_keep_alive = false")
    conn.execute("SET enable_http_metadata_cache = true")
    conn.execute("SET http_retries = 5")
    conn.execute("SET http_retry_wait_ms = 200")
```

Keep `minio_user` and `minio_pass` variable declarations if they're used elsewhere (search first). If only used for the SECRET, delete them too.

- [ ] **Step 4: Deploy and rebuild**

```bash
cd ~/Desktop/dagster-pipeline
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  infra/duckdb-server/mcp_server.py \
  fattie@178.156.228.119:/opt/common-ground/duckdb-server/mcp_server.py
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose build duckdb-server && docker compose up -d duckdb-server"
```

- [ ] **Step 5: Wait for startup and verify**

```bash
sleep 90 && ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "docker compose -f /opt/common-ground/docker-compose.yml logs duckdb-server --tail 10"
```

Verify: `DuckLake catalog attached` and `Uvicorn running` — no S3 errors.

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "perf: remove MinIO S3 layer — DuckLake reads parquet directly from local NVMe"
```

---

### Task 5: Run after-benchmarks

**Files:** None (operational)

- [ ] **Step 1: Run the same benchmark suite from Task 1**

Same queries, same tools. Record times.

- [ ] **Step 2: Compare before/after**

Expected improvement:
| Query | Before (S3) | After (local) | Speedup |
|-------|-------------|---------------|---------|
| COUNT hpd_violations | ~350ms | ~100ms | 3x |
| PLUTO point lookup | ~1700ms | ~200ms | 8x |
| mv_building_hub | ~350ms | ~50ms | 7x |
| building (full) | ~3000ms | ~500ms | 6x |
| entity (xray) | ~27000ms | ~5000ms | 5x |

- [ ] **Step 3: Test writes — materialize one asset**

```bash
cd ~/Desktop/dagster-pipeline
export CATALOG_URL="$(sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc 2>/dev/null | grep 'CATALOG=' | sed 's/^[^=]*=//' | sed 's/common-ground-postgres-1/178.156.228.119/')"

DAGSTER_HOME=/tmp/dagster-home \
  DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG="$CATALOG_URL" \
  uv run dagster asset materialize \
    --select 'foundation/mv_city_averages' \
    -m dagster_pipeline.definitions
```

Verify the new parquet file appears locally:
```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "find /mnt/data/common-ground/lake/data/foundation/mv_city_averages/ -type f -newer /tmp/.migration_marker 2>/dev/null | head -5"
```

**Note:** The Dagster write path also needs to stop using S3. If it still tries to write via MinIO, you need to update the DuckLakeResource or dlt config. The DuckLake ATTACH without S3 SECRET means writes also go to the local `data_path`. But dlt may have its own S3 config — check if the write fails and fix the dlt config.

---

### Task 6: Update dlt pipeline config (if needed)

**Files:**
- Modify: `.dlt/config.toml` (on server, not in git)
- Modify: `src/dagster_pipeline/resources/ducklake.py` (if it references S3)

- [ ] **Step 1: Check if dlt writes via S3 or via DuckLake catalog**

If dlt uses the DuckLake extension directly (via `ATTACH`), writes will automatically use the new local `data_path` from the catalog — no config change needed.

If dlt has its own S3 bucket_url config:
```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cat /opt/dagster-pipeline/.dlt/config.toml 2>/dev/null | grep -i bucket\|s3\|minio"
```

- [ ] **Step 2: Update if needed**

If dlt has S3-specific config, change `bucket_url` from `s3://ducklake` to the local path. The exact change depends on what Step 1 reveals.

- [ ] **Step 3: Run a full Dagster asset materialization test**

```bash
# Pick a small, fast asset
DAGSTER_HOME=/tmp/dagster-home uv run dagster asset materialize \
  --select 'foundation/address_lookup' -m dagster_pipeline.definitions
```

---

### Task 7: Remove MinIO from docker-compose (after 24h)

**Files:**
- Modify: `infra/hetzner-docker-compose.yml`

- [ ] **Step 1: Remove MinIO dependency from duckdb-server**

In `infra/hetzner-docker-compose.yml`, remove from the `duckdb-server` service:
```yaml
# DELETE these lines from duckdb-server.depends_on:
      minio:
        condition: service_healthy
```

Also remove the MINIO env vars from duckdb-server:
```yaml
# DELETE these lines from duckdb-server.environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
```

- [ ] **Step 2: Remove the MinIO service entirely**

Delete the entire `minio:` service block (lines ~104-127).

- [ ] **Step 3: Deploy updated docker-compose**

```bash
cd ~/Desktop/dagster-pipeline
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  infra/hetzner-docker-compose.yml \
  fattie@178.156.228.119:/opt/common-ground/docker-compose.yml
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose up -d"
```

MinIO container will stop. All other services continue.

- [ ] **Step 4: Delete MinIO's old data**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "sudo rm -rf /mnt/data/common-ground/ducklake/"
```

- [ ] **Step 5: Verify disk freed**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "df -h /mnt/HC_Volume_104936190"
```

Expected: ~194GB freed.

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/hetzner-docker-compose.yml infra/duckdb-server/mcp_server.py
git commit -m "infra: remove MinIO — DuckLake reads/writes parquet directly from NVMe

MinIO was only needed for Mac→server data sync. Now that ingestion runs
server-side via Docker, the S3 layer added ~2-5ms per file access with
zero benefit. Data exported via mc mirror, catalog updated to local path."
```

---

### Task 8: Update memory and clean up secrets

**Files:**
- Update: `~/.claude/projects/-Users-fattie2020/memory/project_commonground_stack.md`
- Modify: `.env.secrets.enc` (remove MINIO vars)

- [ ] **Step 1: Update project memory**

Update the stack description to reflect MinIO removal. Change:
```
Stack: Dagster → dlt → MinIO S3 (direct write) → DuckDB → DuckLake → FastMCP → users
```
to:
```
Stack: Dagster → dlt → local NVMe → DuckDB → DuckLake → FastMCP → users
```

- [ ] **Step 2: Remove MinIO secrets from SOPS**

```bash
cd ~/Desktop/dagster-pipeline
sops --input-type dotenv --output-type dotenv .env.secrets.enc
# Remove MINIO_ROOT_USER and MINIO_ROOT_PASSWORD lines
# Save and close
```

- [ ] **Step 3: Final verification**

Run the full benchmark suite one more time. All tools should work, all faster than the S3 baseline.

```bash
# Quick smoke test of all 15 MCP tools
cd ~/Desktop/dagster-pipeline
uv run python3 infra/duckdb-server/stress_test.py --phase audit
```
