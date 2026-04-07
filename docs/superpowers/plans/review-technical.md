# Technical Risk Review — CG Platform Expansion

> **Reviewer**: Infrastructure Engineer
> **Date**: 2026-04-03
> **Status**: COMPLETE
> **Scope**: Platform roadmap, notification engine, Telegram bot, SQLRooms dashboard, CLI

---

## CRITICAL RISKS

### 1. DuckLake `table_changes()` is unreliable — the entire CDC strategy may not work

**Rating: CRITICAL**

The notification engine's primary change detection mechanism relies on `table_changes(table, start_snapshot, end_snapshot)`. Investigation reveals:

- **GitHub issue [#330](https://github.com/duckdb/ducklake/issues/330)**: `table_changes` does not follow its own spec. It shows the literal diff between two snapshots, not all cumulative changes. Checking between snapshots [0, 2] vs [0, 4] does not show expected intermediate changes.
- **Snapshot expiry is manual but compaction is automatic**: DuckLake never removes data unless you explicitly call `ducklake_expire_snapshots`, but `auto_compact = true` by default runs `ducklake_merge_adjacent_files` which [can't compact files from expired snapshots](https://github.com/duckdb/ducklake/issues/336). If snapshots are expired (even accidentally), `table_changes()` between those snapshots breaks.
- **49 federal tables use DROP + CREATE**: These destroy snapshot history entirely. The "row count fallback" catches the refresh event but loses all row-level detail.
- **DuckLake is pre-1.0**: Currently v0.4. The CDC feature is not battle-tested in production. No major project appears to depend on `table_changes()` for production notification delivery.

**Impact**: If `table_changes()` silently returns wrong results (as issue #330 shows), notifications silently stop or deliver incorrect data. Users won't know their watchlist is broken.

**Mitigation**:
1. Before building the notification engine, write a standalone test that verifies `table_changes()` accuracy across 5+ successive MERGE INTO operations on a test table. Validate that intermediate inserts, updates, and deletes are all captured.
2. Implement a checksum-based fallback: hash key columns before and after materialization, diff the hashes. This is heavier but reliable.
3. Consider storing change hashes during dlt loading (dlt has `_dlt_load_id` built in) rather than relying on DuckLake's snapshot mechanism.
4. Do not depend on `table_changes()` being production-ready until DuckLake reaches 1.0.

---

### 2. Storage layer has diverged — MinIO may no longer exist

**Rating: CRITICAL**

The platform roadmap assumes MinIO is running and available for:
- Public Parquet serving to DuckDB-WASM (`s3://public-agg/` bucket)
- RSS feed storage (`s3://cg-feeds/`)
- CDN backing via Cloudflare

But **CLAUDE.md explicitly states**: `Local NVMe (parquet files on server disk)`. The architecture diagram shows `Local NVMe` not MinIO. The MCP server prints `"Performance tuning applied (local filesystem, no S3)"`. The DuckLake catalog is attached via Postgres only — no S3 endpoint.

The `secrets.toml` still mentions "MinIO creds" but the actual stack comment in the memory file says: "Dagster → dlt → MinIO S3 (direct write) → DuckDB → DuckLake" — this appears to be stale. The live infrastructure uses local NVMe.

**Impact**: The entire SQLRooms dashboard strategy (DuckDB-WASM querying Parquet via HTTP range requests on MinIO) breaks if MinIO is not running. Without an HTTP-accessible object store, the browser has no way to fetch Parquet files. This is a **showstopper** for Phase 5.

**Mitigation**:
1. **Verify immediately**: SSH into Hetzner and run `docker ps | grep minio` or `curl http://localhost:9000/minio/health/cluster`. Determine if MinIO is running, stopped, or never deployed.
2. If MinIO is dead: either restart it for the `public-agg` bucket only, or serve Parquet files via nginx with byte-range support (simpler, no MinIO dependency).
3. If MinIO was never deployed: the Parquet export strategy needs a static file server. nginx with `Accept-Ranges: bytes` + CORS headers is sufficient. No need for a full object store.
4. Update CLAUDE.md and memory to reflect the actual storage layer.

---

### 3. Single Hetzner server — no redundancy for a notification platform

**Rating: CRITICAL**

Everything runs on one box (178.156.228.119):
- DuckDB/DuckLake (Postgres catalog + NVMe Parquet)
- MCP server (15 tools, all queries)
- Dagster pipeline
- The proposed Subscription API, Notification Engine, and Telegram Bot

**Impact**: When the server goes down (hardware failure, OS update, Docker restart), **all four surfaces die simultaneously**: website, bot, CLI, MCP. Users who depend on notifications will miss alerts with no way to know they missed them. A notification system that silently stops is worse than no notification system.

**Mitigation**:
1. **Health monitoring first**: Before adding notification services, add uptime monitoring (UptimeRobot or Hetzner Cloud monitoring) that alerts on Telegram/SMS when the server is unreachable. Cost: ~$0.
2. **Notification delivery receipt**: The bot should track the last successful notification delivery time per user. If no notification has been sent in 48 hours for a user with active realtime subscriptions, send a "still watching" heartbeat. This detects silent failures.
3. **Postgres backup**: The DuckLake catalog on Postgres is the irreplaceable piece. Daily pg_dump to Hetzner Object Storage ($0.01/GB/mo). Parquet files can be regenerated from the catalog.
4. **Long-term**: Consider a second node (Hetzner Cloud ~$4/mo) running just Postgres replica + the Telegram bot, so notifications survive a primary failure.

---

## HIGH RISKS

### 4. DuckDB single-writer contention — 3 services writing to the same database

**Rating: HIGH**

DuckDB supports multiple concurrent readers but only one writer at a time. The plan adds:
- **Dagster**: materializing assets (heavy writes)
- **Notification engine**: writing to `_change_log`, `_notification_queue`, `_delivery_log` (frequent writes)
- **Subscription API**: writing to `_subscriptions` (user-driven writes)
- **MCP server**: read-only queries (but connection pooling conflicts)

DuckLake moves coordination to Postgres, which helps, but [concurrent writes still fail](https://github.com/duckdb/ducklake/issues/233) in DuckLake. Issue #243 shows "Concurrent writes can fail on first write to table."

**Impact**: During a Dagster materialization run (which can take hours for 260 datasets), the notification engine and subscription API may fail to write. Notifications queue up but can't be recorded. Subscriptions created during a run may silently fail.

**Mitigation**:
1. Use Postgres directly for `_subscriptions`, `_notification_queue`, and `_delivery_log` — not DuckLake. These are OLTP workloads (small row writes, frequent updates). Postgres is already running and handles concurrent writes natively.
2. Only use DuckLake for `_change_log` and `_snapshot_bookmarks` (which are written by the Dagster sensor, serialized by nature since sensors run sequentially).
3. The CLI/Telegram plan doc (Section 4.4) already suggests Postgres for subscriptions — follow that design, not the notification engine doc which puts everything in DuckLake.

---

### 5. `data_watch` subscription type is a SQL injection vector

**Rating: HIGH**

The `data_watch` subscription type accepts a `filter_value` that is a **SQL WHERE fragment**. From the notification engine design:

> `data_watch` | SQL WHERE fragment | `{"table": "housing.hpd_violations"}` | Evaluate SQL predicate against row_data

This means user-provided SQL is evaluated against the change_log. Even if the change_log is read-only, crafted SQL can:
- Extract data from other tables via subqueries: `EXISTS (SELECT * FROM lake._subscriptions WHERE ...)`
- Cause denial of service via expensive queries: `(SELECT COUNT(*) FROM lake.housing.acris_master) > 0`
- Leak schema information via error messages

**Impact**: Any user (via CLI API key or Telegram) can submit arbitrary SQL that executes inside the notification matching engine with full DuckDB read access.

**Mitigation**:
1. **Remove `data_watch` entirely for launch**. The other 5 subscription types cover the vast majority of use cases.
2. If `data_watch` is needed later, implement a SQL expression allowlist: only permit column references, comparison operators, and literal values. Parse the expression with a SQL parser (e.g., `sqlglot`) and reject anything containing subqueries, function calls, or table references.
3. Never execute raw user SQL in the notification engine. Always parameterize.

---

### 6. SQLRooms is a low-traction single-maintainer project

**Rating: HIGH**

Research shows:
- **29 GitHub stars** (not 410 as claimed in the plan)
- Organization has **no public members** visible
- Last update: March 24, 2026 (recent, but single-org activity)
- No visible corporate backing or community adoption

**Impact**: If the maintainer goes inactive, the entire dashboard is built on an unmaintained framework. SQLRooms abstracts DuckDB-WASM + Kepler + Mosaic + CRDT — that's a lot of complexity behind a thin wrapper. When it breaks, you're debugging someone else's Zustand slice composition.

**Mitigation**:
1. **Evaluate the exit cost**: Before committing to SQLRooms, build the neighborhood overview view using raw DuckDB-WASM + deck.gl + Zustand directly. If it takes only 2-3 extra days, skip SQLRooms entirely.
2. If SQLRooms is used: pin exact versions, vendor the packages locally, and ensure you understand the store composition well enough to fork if abandoned.
3. The `@sqlrooms/ai` package should be skipped entirely — use the existing MCP server for NL queries instead of adding another AI integration layer.

---

### 7. Subscription data is sensitive — no data retention or privacy policy

**Rating: HIGH**

The `_subscriptions` table stores:
- Telegram user IDs
- What buildings they're watching (BBLs)
- What entities they're tracking (names)
- What keywords they're searching for

This is a **surveillance watchlist**. A journalist investigating their landlord, an activist tracking a developer, a tenant monitoring their building — their interests are exposed in a database. A data breach reveals who is watching whom.

**Impact**: Legal liability. If CG processes EU users' data (any Telegram user can message the bot), GDPR applies. Even without GDPR, storing investigation targets tied to identifiable users is a privacy risk.

**Mitigation**:
1. Define a data retention policy: auto-delete inactive subscriptions after 90 days. Delete all user data if a user runs `/deletedata`.
2. Hash or encrypt the `filter_value` at rest. The notification engine can decrypt during matching but the database at rest doesn't reveal what users are watching.
3. Add a privacy policy page linked from the bot's `/start` message and the CLI's first-run output.
4. Consider making the Telegram user ID one-way hashed (bot stores hash, not raw ID) where possible.

---

### 8. DuckDB-WASM on mobile is risky — memory, bandwidth, and battery

**Rating: HIGH**

The SQLRooms plan says "~75 MB maximum" for Parquet files, with "initial load ~5-10 MB." Research reveals:
- DuckDB-WASM binary itself is ~30 MB
- DuckDB uses [4 GB RAM to read a 120 MB Parquet file](https://github.com/duckdb/duckdb/issues/17262) in some cases
- Memory is not released after queries in WASM
- Mobile browsers on 2GB-RAM phones will crash
- SharedArrayBuffer requires COOP/COEP headers which break third-party embeds

**Impact**: The Telegram Mini App (which runs in a mobile WebView) will be unusable on low-end phones. This is the NYC civic data audience — not everyone has a flagship phone.

**Mitigation**:
1. The Telegram Mini App should NOT use DuckDB-WASM. Instead, make it a lightweight API client that fetches pre-rendered charts/data from the MCP server. The server already has all the data.
2. For the website `/explore`, use DuckDB-WASM only for desktop browsers. Add a `navigator.deviceMemory` check and fall back to server-side queries for devices with <4GB RAM.
3. Keep aggregate Parquet files under 5 MB each (not 10-50 MB as currently planned). Filter `agg_building_summary` aggressively.

---

## MEDIUM RISKS

### 9. Notification chain has no monitoring — silent failures

**Rating: MEDIUM**

The chain: `sensor → change_log → match → queue → fanout`. If any link breaks:
- Sensor fails silently (Dagster sensors don't alert on failure by default)
- Change_log accumulates unmatched rows forever
- Queue accumulates unsent notifications forever
- Fan-out fails to reach Telegram (429s, network errors)

No monitoring is described in any of the 5 planning docs.

**Mitigation**:
1. Add a Dagster `freshness_check` on `_notification_queue`: if pending items are older than 2 hours, alert.
2. Log notification pipeline metrics to PostHog: changes_detected, matches_found, notifications_sent, failures.
3. Add a `/status` command in the Telegram bot that shows "last notification sent: X ago" per user.

---

### 10. Telegram rate limits vs fan-out scale

**Rating: MEDIUM**

Telegram limits: 30 messages/second globally, ~1 message/second per chat. With 10K subscribers:
- A big data drop (e.g., quarterly HPD violations update: 50K new rows) could match thousands of subscriptions
- At 30 msg/sec: 10K notifications = 5.5 minutes
- At 1 msg/sec per chat (if many users watch the same building): could take much longer
- 429 errors during fan-out require retry logic that extends delivery time

The daily digest at 8 AM ET concentrates all daily subscribers into one burst.

**Mitigation**:
1. Spread daily digests over a 30-minute window (8:00-8:30 AM) with random jitter per user.
2. Implement a proper send queue with token-bucket rate limiter (30/sec global, 1/sec per chat).
3. For the forum supergroup, batch changes per ZIP topic — one message per topic per refresh, not one per change.

---

### 11. change_log stores full row JSON — storage growth

**Rating: MEDIUM**

`row_data JSON NOT NULL` stores the entire changed row as JSON. With 287 delta-merge tables:
- Average row size as JSON: ~500 bytes (small) to ~5KB (ACRIS with text fields)
- At 10K changes/day: ~50 MB/day, ~1.5 GB/month (manageable)
- At 100K changes/day: ~500 MB/day, ~15 GB/month (concerning on NVMe)
- ACRIS alone has 85M records — a full refresh would be catastrophic

The 30-day cleanup helps, but the accumulation between cleanups could be significant.

**Mitigation**:
1. Store only the key columns needed for matching (BBL, ZIP, name, table_key) plus a row hash — not the full row.
2. For the notification payload, fetch the full row on-demand from the source table at rendering time.
3. Set a hard cap on change_log entries per table per snapshot (e.g., 10K). If a table refresh produces more changes, log a summary entry instead of individual rows.

---

### 12. Operational complexity — 6 services for one person

**Rating: MEDIUM**

The plan adds 3 new Docker services (Subscription API, Notification Engine, Telegram Bot) to an existing stack of MCP server + Postgres + (possibly) MinIO. Plus Dagster runs from the Mac Studio.

Current service count after expansion: **6-7 services**, 5 planning docs, 294 tables, 15 MCP tools, 6 subscription types, 3 notification channels.

**Impact**: Deploy failures, version mismatches, config drift between services. The `docker-compose.yml` becomes the single most critical file in the project.

**Mitigation**:
1. **Merge the Subscription API into the MCP server**. FastMCP is built on FastAPI — mount the subscription CRUD routes directly on the existing server. This eliminates one service and one port.
2. **Merge the Notification Engine into Dagster**. It's already designed as Dagster sensors and assets. Don't create a separate service — run it as part of the existing Dagster deployment.
3. The Telegram bot is the only genuinely new service. Net new services: **1**, not 3.

---

### 13. API key security for CLI

**Rating: MEDIUM**

The CLI stores API keys in `~/.cg/config.toml` in plain text. Keys are "stored hashed (SHA-256)" on the server but transmitted in `Authorization: Bearer` headers.

Questions unanswered:
- How are keys generated? Self-service or admin-issued?
- Is there rate limiting per key?
- Can a key be revoked without server access?
- What access does a key grant? Read-only MCP tools? Write access to subscriptions?

**Mitigation**:
1. Keys should be generated via the CLI itself (`cg auth login` → generates key on server, returns it once).
2. Rate limit per key: 100 requests/minute (same as current MCP rate limiting).
3. Key rotation: `cg auth rotate` invalidates old key and generates new one.
4. Scope: keys grant MCP read access + subscription CRUD for that user only. No admin access.

---

## LOW RISKS

### 14. Mapbox token dependency

**Rating: LOW**

Kepler.gl requires a Mapbox access token. This is a runtime dependency on a third-party service with usage-based pricing.

**Mitigation**: Use MapLibre GL JS instead. It's free, open source, and compatible with deck.gl. The SQLRooms Kepler slice may require Mapbox, but if SQLRooms is dropped (see risk #6), this becomes moot.

---

### 15. aiogram + Bot API 9.5 `sendMessageDraft` is confirmed

**Rating: LOW**

Verified: aiogram 3.26+ supports `SendMessageDraft` for Bot API 9.5. This is implemented and documented. The Telegram bot architecture's LLM streaming plan is technically feasible.

---

### 16. Cross-identity linking is deferred but will be requested

**Rating: LOW**

CLI user (`apikey:...`) and Telegram user (`telegram:...`) are separate identities. The plan correctly defers linking. But users will ask "why can't I see my CLI subscriptions in Telegram?" within the first week of launch.

**Mitigation**: Add a `/link` command in the bot that accepts a CLI API key and merges identities. Implement in Phase 7, but design the subscription table to support it from day one (composite user_id with namespace prefix is already planned).

---

## Summary Table

| # | Risk | Rating | Category |
|---|------|--------|----------|
| 1 | DuckLake `table_changes()` is buggy and pre-1.0 | **CRITICAL** | Fragile assumption |
| 2 | Storage layer diverged — MinIO may not exist | **CRITICAL** | Infrastructure |
| 3 | Single server, no redundancy | **CRITICAL** | Single point of failure |
| 4 | DuckDB single-writer contention | **HIGH** | Scalability |
| 5 | `data_watch` SQL injection | **HIGH** | Security |
| 6 | SQLRooms is 29-star single-maintainer project | **HIGH** | Dependency risk |
| 7 | Subscription data is sensitive, no privacy policy | **HIGH** | Security / Privacy |
| 8 | DuckDB-WASM unusable on mobile phones | **HIGH** | Scalability |
| 9 | No monitoring on notification chain | **MEDIUM** | Operational |
| 10 | Telegram rate limits at scale | **MEDIUM** | Scalability |
| 11 | change_log JSON storage growth | **MEDIUM** | Scalability |
| 12 | 6+ services for one operator | **MEDIUM** | Operational |
| 13 | CLI API key lifecycle undefined | **MEDIUM** | Security |
| 14 | Mapbox token dependency | **LOW** | Dependency |
| 15 | aiogram Bot API 9.5 support confirmed | **LOW** | Verified OK |
| 16 | Cross-identity linking deferred | **LOW** | UX |

---

## Top 3 Actions Before Writing Any Code

1. **Verify MinIO status on Hetzner** — `docker ps | grep minio`. This determines whether Phase 5 (dashboard) is feasible as designed.
2. **Test `table_changes()` end-to-end** — write a standalone script that creates a test table, does 5 MERGE INTO operations, and verifies `table_changes()` returns all expected rows between each pair of snapshots.
3. **Move subscription/notification tables to Postgres** — don't fight DuckDB's single-writer model for OLTP workloads.

---

## Sources

- [DuckLake Data Change Feed docs](https://ducklake.select/docs/stable/duckdb/advanced_features/data_change_feed)
- [DuckLake Issue #330 — table_changes does not follow spec](https://github.com/duckdb/ducklake/issues/330)
- [DuckLake Issue #336 — compaction + expired snapshots](https://github.com/duckdb/ducklake/issues/336)
- [DuckLake Issue #233 — concurrent writes fail](https://github.com/duckdb/ducklake/issues/233)
- [DuckLake Expire Snapshots docs](https://ducklake.select/docs/stable/duckdb/maintenance/expire_snapshots)
- [DuckDB Concurrency docs](https://duckdb.org/docs/current/connect/concurrency)
- [DuckDB-WASM 4GB RAM for 120MB Parquet](https://github.com/duckdb/duckdb/issues/17262)
- [DuckDB-WASM memory not released](https://github.com/duckdb/duckdb-wasm/issues/1904)
- [SQLRooms GitHub](https://github.com/sqlrooms)
- [aiogram sendMessageDraft docs](https://docs.aiogram.dev/en/dev-3.x/api/methods/send_message_draft.html)
- [Telegram Bot API rate limits FAQ](https://core.telegram.org/bots/faq)
