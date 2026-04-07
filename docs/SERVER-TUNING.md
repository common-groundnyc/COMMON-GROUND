# Server Tuning — Hetzner 178.156.228.119

## Hardware

| Resource | Spec |
|----------|------|
| CPU | AMD EPYC Milan, 8 vCPUs |
| RAM | 30.6 GB |
| Boot disk | 226 GB SSD (`/dev/sda`, ext4) — OS, 199 GB free |
| Data volume | 100 GB SSD (`/dev/sdb`, ext4) — DuckLake + Postgres, 20 GB free (79% used) |
| OS | Ubuntu 24.04.3 LTS, kernel 6.8.0 |

## Current State (2026-03-15)

Data lake: **391M rows, 47 GB parquet** across 220 tables in 11 schemas.
Postgres catalog: **1.6 GB**.
Data volume: **74 GB used / 98 GB** (79%) — needs monitoring.

---

## 1. Docker Compose — Memory Limits

### Before & After

| Container | Old Limit | Actual Use | Problem | New Limit |
|-----------|-----------|------------|---------|-----------|
| duckdb-server | **1 GB** | 1.9 GB | OOM on compaction, merges, heavy queries. Root cause of crashes. | **None** (uncapped) |
| dagster-daemon | 16 GB | 218 MB | Wasting 15.8 GB of reservable memory | 4 GB |
| dagster-webserver | 4 GB | 231 MB | Fine but overset | 2 GB |
| duck-ui | **24 GB** | 33 MB | A web UI with 24 GB reserved | 512 MB |
| postgres | 4 GB | 154 MB | Catalog is 1.6 GB, 4 GB overkill | 2 GB |
| minio | 512 MB | 286 MB (56%) | Could OOM during bulk S3 uploads | 1 GB |

**Status**: Applied.

---

## 2. DuckDB Server Settings

Set in `mcp_server.py` at startup:

```python
conn.execute("SET threads = 6")            # 6 of 8 vCPUs (leave 2 for OS + other containers)
conn.execute("SET memory_limit = '20GB'")  # of 30 GB total
```

**Status**: Applied.

---

## 3. DuckLake Options

```sql
-- Not yet supported in DuckLake v0.4 (extension 7ea15644)
-- Available options: created_by, data_path, encrypted, version
-- compression and row_group_size are documented but require a newer DuckLake version
```

**Status**: Blocked — waiting for DuckLake extension update.

---

## 4. Postgres — DuckLake Catalog Tuning

Passed via `command:` in docker-compose.yml:

```
shared_buffers = 512MB          # Was 128 MB (default). 25% of 2 GB limit.
work_mem = 16MB                 # Was 4 MB. Helps complex catalog queries.
effective_cache_size = 1536MB   # Was 4 GB. Realistic for 2 GB container.
max_connections = 50            # Was 100. We don't need 100.
checkpoint_completion_target = 0.9
wal_buffers = 16MB
```

**Why**: Every DuckLake write creates a snapshot row and updates `ducklake_data_file`, `ducklake_column`, etc. With 20 concurrent dlt pipelines writing, Postgres is the serialization bottleneck.

**Status**: Applied.

---

## 5. Linux Kernel Tuning

### vm.swappiness: 60 → 10

```bash
# Current: 60 (aggressive swap). Bad for DuckDB — swapping analytical data kills performance.
# Set to 10: only swap under real memory pressure.
sysctl -w vm.swappiness=10
echo "vm.swappiness=10" >> /etc/sysctl.d/99-common-ground.conf
```

### vm.dirty_ratio / vm.dirty_background_ratio

```bash
# Current: 20/10 (defaults). Fine for our write pattern — dlt writes large parquet files
# in bursts then goes idle. No change needed.
```

### File descriptor limit: 1024 → 65536

```bash
# Current: 1024 (default). Too low for DuckDB + Postgres + MinIO + Docker.
# DuckDB opens multiple parquet files simultaneously during queries.
echo "* soft nofile 65536" >> /etc/security/limits.conf
echo "* hard nofile 65536" >> /etc/security/limits.conf
# Also set for systemd services:
mkdir -p /etc/systemd/system/docker.service.d
echo -e "[Service]\nLimitNOFILE=65536" > /etc/systemd/system/docker.service.d/limits.conf
systemctl daemon-reload && systemctl restart docker
```

### Transparent Huge Pages: madvise (OK)

```bash
# Current: [madvise]. This is correct for DuckDB — it uses madvise() for large allocations.
# 'always' would cause latency spikes from THP compaction. No change needed.
```

**Status**: Not yet applied.

---

## 6. Filesystem — noatime

Both disks mount with `relatime` (default). For a data pipeline server, `noatime` reduces unnecessary write I/O:

```bash
# /etc/fstab — add noatime to both mount entries
# For sda1 (boot): minimal impact
# For sdb (data volume): meaningful — DuckDB/MinIO read parquet files constantly,
#   each read updates atime metadata. noatime skips this.
# Change relatime → noatime in fstab, then:
mount -o remount,noatime /
mount -o remount,noatime /mnt/HC_Volume_104936190
```

**Status**: Not yet applied.

---

## 7. Docker Daemon

No `daemon.json` exists. Add one for log rotation and storage driver optimization:

```json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "50m",
    "max-file": "3"
  },
  "storage-driver": "overlay2",
  "default-ulimits": {
    "nofile": { "Name": "nofile", "Hard": 65536, "Soft": 65536 }
  }
}
```

**Why**: Without log rotation, container logs grow unbounded. The duckdb-server logs every MCP request. Over weeks, this fills disk.

**Status**: Not yet applied.

---

## 8. Firewall (ufw)

Current rules only allow SSH (22) and port 8642 (old DuckDB MCP port). Missing:

| Port | Service | Status |
|------|---------|--------|
| 22 | SSH | Allowed |
| 4213 | DuckDB MCP (current) | **Not allowed** — works because Docker bypasses ufw by default |
| 5432 | Postgres | **Not allowed** — same Docker bypass |
| 9000 | MinIO S3 | **Not allowed** — same |
| 9001 | MinIO Console | **Not allowed** — same |
| 3000 | Dagster UI | **Not allowed** — same |
| 8642 | Old MCP port | Allowed (stale rule) |

Docker publishes ports via iptables directly, bypassing ufw entirely. This means ufw gives a false sense of security — all published ports are world-accessible regardless of ufw rules.

**Fix options**:
1. **Accept Docker bypass** — remove the stale 8642 rule, document that Docker manages its own firewall
2. **Use Hetzner firewall** (cloud-level) — more reliable than host-level ufw with Docker

```bash
ufw delete allow 8642/tcp  # Remove stale rule
```

**Status**: Not yet applied.

---

## 9. MinIO

MinIO is lightweight by design. Single-server, single-drive — no erasure coding, no special tuning needed. The 1 GB memory limit (up from 512 MB) gives headroom for multipart upload buffering.

**Status**: Applied (memory limit).

---

## 10. FastMCP Server

Currently single-threaded via `python mcp_server.py`. For production:

```bash
# Multi-worker with uvicorn (requires stateless mode — already enabled)
FASTMCP_STATELESS_HTTP=true uvicorn app:app --host 0.0.0.0 --port 4213 --workers 2
```

**Why 2 workers**: Each MCP request triggers a DuckDB query using 6 threads. 2 workers × 6 threads = 12 across 8 vCPUs. More would cause contention.

Each worker gets its own DuckDB connection + ATTACH. DuckLake catalog (Postgres) and data (MinIO S3) are shared — the DuckDB process is just compute.

**Status**: Not yet applied — requires Dockerfile change to use uvicorn entrypoint.

---

## 11. Data Volume Capacity

```
/dev/sdb: 98 GB total, 74 GB used, 20 GB free (79%)
```

At 47 GB for the lake + 1.6 GB catalog + MinIO overhead, we're at 79%. Full 208-dataset load + incremental history will push past this. Options:

1. **Resize Hetzner volume** — can expand online via Hetzner Cloud console
2. **Clean up old parquet files** — `ducklake_cleanup_old_files` and `ducklake_cleanup_orphaned_files` after compaction
3. **Move to Hetzner Object Storage** — offload parquet from local volume to Hetzner S3 (cheaper, infinite)

**Status**: Monitor. Clean up orphaned files first.

---

## 12. DuckLake Compaction

All 30 multi-file tables compacted. Full results:

### Round 1 (1 GB memory limit — 19 tables)

| Table | Files | Rows | Time |
|-------|-------|------|------|
| pluto | 2 → 1 | 858K | 4.6s |
| campaign_contributions | 2 → 1 | 1.7M | 4.8s |
| sparcs_discharges_* (5) | 2 → 1 | ~2.1M ea | ~4.3s |
| dob_permit_issuance | 4 → 1 | 4.0M | 11.8s |
| parking_violations | 4 → 1 | 9.1M | 12.2s |
| nypd_complaints_historic | 5 → 1 | 9.5M | 9.9s |
| *(+ 8 more)* | | | |

### Round 2 (uncapped memory — 12 remaining tables)

| Table | Files | Rows | Time |
|-------|-------|------|------|
| property_valuation | 14 → 1 | 10.5M | 42.0s |
| hpd_violations | 8 → 1 | 10.8M | 17.1s |
| hpd_complaints | 7 → 1 | 15.9M | 12.8s |
| acris_master | 2 → 1 | 16.9M | 8.8s |
| n311_service_requests | 13 → 1 | 20.5M | 27.6s |
| oath_hearings | 17 → 1 | 21.5M | 28.6s |
| acris_parties | 6 → 1 | 46.2M | 19.4s |
| ed_flu_visits | 4 → 1 | 82.0M | 14.9s |
| *(+ 4 more)* | | | |

**Total**: 391M rows compacted in ~200s. `property_valuation` (140 columns) was the slowest at 42s.

**Status**: Complete. Schedule periodic compaction after loads.

---

## Applied vs Pending

| Change | Status |
|--------|--------|
| Docker memory limits | Applied |
| DuckDB threads=6, memory=20GB | Applied |
| Postgres shared_buffers, work_mem | Applied |
| MinIO 1GB limit | Applied |
| DuckLake compaction | Complete |
| DuckLake compression/row_group_size | Blocked (DuckLake v0.4) |
| vm.swappiness=10 | **Pending** |
| nofile=65536 | **Pending** |
| Filesystem noatime | **Pending** |
| Docker log rotation | **Pending** |
| Firewall cleanup | **Pending** |
| FastMCP uvicorn workers | **Pending** |
| Data volume monitoring | **Pending** |
