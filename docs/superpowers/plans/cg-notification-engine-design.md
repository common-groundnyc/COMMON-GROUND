# CG Notification Engine — Change Detection + Fan-Out Design

> Status: DESIGN
> Author: pipeline-engineer
> Date: 2026-04-03

---

## 1. Architecture Overview

```
Dagster asset materialization
        │
        ▼
┌──────────────────────┐
│  change_detection    │  Dagster run_status_sensor (SUCCESS)
│  sensor              │  fires after every successful asset run
└──────┬───────────────┘
       │ writes to
       ▼
┌──────────────────────┐
│  lake._change_log    │  DuckLake table: what rows are new/updated
└──────┬───────────────┘
       │ read by
       ▼
┌──────────────────────┐
│  notification_match  │  Dagster asset: matches changes → subscriptions
│  asset               │  runs after change_log is populated
└──────┬───────────────┘
       │ writes to
       ▼
┌──────────────────────┐
│  lake._notification  │  Pending notifications queue table
│  _queue              │
└──────┬───────────────┘
       │ consumed by
       ▼
┌──────────────────────┐
│  notification_fanout │  Dagster asset: Telegram + Email + RSS
│  asset               │  batches per user, respects frequency prefs
└──────────────────────┘
```

---

## 2. Change Detection

### 2.1 Strategy: DuckLake `table_changes()` + Snapshot Bookmarks

DuckLake has native CDC via its `table_changes(table, start_snapshot, end_snapshot)` function. This returns all inserted/updated/deleted rows between two snapshots with three extra columns: `snapshot_id`, `rowid`, `change_type`.

**This is the primary detection mechanism.** No custom hashing or watermarking needed.

#### How it works

1. After each successful asset materialization, Dagster emits a `SUCCESS` run status event.
2. A `run_status_sensor` fires and:
   - Reads the last-processed snapshot ID from `lake._snapshot_bookmarks` for each table that was materialized in the run.
   - Queries `ducklake_snapshots('lake')` to get the current (latest) snapshot ID.
   - If current > last-processed, calls `table_changes('lake.schema.table', last_snapshot, current_snapshot)`.
   - Writes new/changed rows to `lake._change_log`.
   - Updates `lake._snapshot_bookmarks` with the current snapshot ID.

#### Schema: `lake._snapshot_bookmarks`

```sql
CREATE TABLE IF NOT EXISTS lake._snapshot_bookmarks (
    table_key       VARCHAR PRIMARY KEY,  -- 'schema.table_name'
    last_snapshot   BIGINT,               -- last processed snapshot_id
    processed_at    TIMESTAMP DEFAULT current_timestamp
);
```

#### Schema: `lake._change_log`

```sql
CREATE TABLE IF NOT EXISTS lake._change_log (
    id              BIGINT DEFAULT nextval('change_log_seq'),
    table_key       VARCHAR NOT NULL,      -- 'housing.hpd_violations'
    change_type     VARCHAR NOT NULL,      -- 'insert', 'update', 'delete'
    snapshot_id     BIGINT NOT NULL,
    row_data        JSON NOT NULL,          -- the changed row as JSON
    detected_at     TIMESTAMP DEFAULT current_timestamp,
    matched         BOOLEAN DEFAULT false   -- true after notification_match processes it
);
```

**Note:** `row_data` is JSON because different tables have different schemas. The notification matcher will extract relevant fields (address, BBL, name, ZIP) from this JSON per subscription type.

### 2.2 Fallback: Row Count Delta + Cursor Timestamp

For tables where `table_changes()` is impractical (full-replace assets that DROP + CREATE, breaking snapshot history), fall back to:

1. Compare `row_count` before and after materialization (already stored in `lake._pipeline_state`).
2. Use the cursor timestamp from `lake._pipeline_state.last_updated_at` as a "something changed" signal.
3. For full-replace tables, treat every materialization that changes row count as "all rows are new" — write a single change_log entry with `change_type = 'refresh'` and no row_data.

**Which tables use which strategy:**

| Strategy | Tables | Reason |
|----------|--------|--------|
| `table_changes()` | All Socrata delta-merge assets (~287) | They use MERGE INTO, preserving snapshot history |
| Row count fallback | Federal assets (~49) | They use full replace (DROP + CREATE) |
| Skip | Foundation/MV assets | Derived data, not user-facing changes |

### 2.3 Sensor Implementation

```python
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="change_detection",
    minimum_interval_seconds=60,
)
def change_detection_sensor(context: RunStatusSensorContext):
    """Detect new/changed rows after successful asset materialization."""
    # 1. Get which assets were materialized in this run
    run = context.dagster_run
    asset_keys = _get_materialized_asset_keys(context.instance, run.run_id)

    # 2. For each asset, detect changes
    conn = _connect_ducklake()
    try:
        for asset_key in asset_keys:
            table_key = f"{asset_key.path[0]}.{asset_key.path[1]}"

            if _is_delta_merge_table(table_key):
                _detect_via_table_changes(conn, table_key)
            elif _is_ingestion_table(table_key):
                _detect_via_row_count(conn, table_key)
            # else: skip foundation/MV tables
    finally:
        conn.close()
```

---

## 3. Subscription Model

### 3.1 Schema: `lake._subscriptions`

```sql
CREATE TABLE IF NOT EXISTS lake._subscriptions (
    id              VARCHAR PRIMARY KEY,   -- UUID
    user_id         VARCHAR NOT NULL,      -- telegram user ID or email
    sub_type        VARCHAR NOT NULL,      -- see types below
    filter_value    VARCHAR NOT NULL,      -- BBL, name, ZIP, SQL fragment, etc.
    filter_extra    JSON,                  -- optional: category, table list, etc.
    channels        JSON NOT NULL,         -- ["telegram", "email", "rss"]
    frequency       VARCHAR DEFAULT 'daily',  -- 'realtime', 'daily', 'weekly'
    active          BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT current_timestamp,
    last_notified   TIMESTAMP
);
```

### 3.2 Subscription Types

| Type | `filter_value` | `filter_extra` | Match Logic |
|------|---------------|----------------|-------------|
| `address_watch` | BBL (10-digit) | `{"address": "..."}` | Extract BBL from row_data, exact match |
| `entity_watch` | Person/company name | `{"fuzzy": true}` | Name columns in row_data, phonetic + fuzzy match via name_index |
| `zip_category` | ZIP code (5-digit) | `{"tables": ["hpd_violations","311"]}` | Extract ZIP from row_data, match table_key |
| `table_watch` | Table key | `{}` | Any change in that table_key |
| `keyword_watch` | Search term | `{"tables": [...]}` | Full-text search across row_data JSON |
| `data_watch` | SQL WHERE fragment | `{"table": "housing.hpd_violations"}` | Evaluate SQL predicate against row_data |

### 3.3 Matching Engine

The notification_match asset runs after the change_detection sensor populates `_change_log`. It:

1. Reads unmatched rows from `_change_log` (`WHERE matched = false`).
2. For each subscription type, runs a batch SQL query against the change_log:

```sql
-- address_watch: match by BBL
SELECT cl.id AS change_id, s.id AS sub_id, s.user_id, s.channels, s.frequency
FROM lake._change_log cl
JOIN lake._subscriptions s
  ON s.sub_type = 'address_watch'
  AND s.active = true
WHERE cl.matched = false
  AND (
    json_extract_string(cl.row_data, '$.bbl') = s.filter_value
    OR LPAD(TRY_CAST(TRY_CAST(json_extract_string(cl.row_data, '$.bbl') AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') = s.filter_value
  );

-- zip_category: match by ZIP + optional table filter
SELECT cl.id AS change_id, s.id AS sub_id, s.user_id, s.channels, s.frequency
FROM lake._change_log cl
JOIN lake._subscriptions s
  ON s.sub_type = 'zip_category'
  AND s.active = true
WHERE cl.matched = false
  AND (
    json_extract_string(cl.row_data, '$.zipcode') = s.filter_value
    OR json_extract_string(cl.row_data, '$.zip_code') = s.filter_value
    OR json_extract_string(cl.row_data, '$.zip') = s.filter_value
  )
  AND (
    s.filter_extra IS NULL
    OR s.filter_extra->>'tables' IS NULL
    OR cl.table_key IN (SELECT unnest(json_extract_string(s.filter_extra, '$.tables')::VARCHAR[]))
  );

-- entity_watch: match by name (fuzzy via phonetic index)
-- This requires joining against foundation.phonetic_index
SELECT cl.id AS change_id, s.id AS sub_id, s.user_id, s.channels, s.frequency
FROM lake._change_log cl
JOIN lake._subscriptions s
  ON s.sub_type = 'entity_watch'
  AND s.active = true
JOIN lake.foundation.phonetic_index pi
  ON double_metaphone(UPPER(s.filter_value)) = pi.dm_last
WHERE cl.matched = false
  AND cl.table_key = pi.source_table;

-- table_watch: any change in the table
SELECT cl.id AS change_id, s.id AS sub_id, s.user_id, s.channels, s.frequency
FROM lake._change_log cl
JOIN lake._subscriptions s
  ON s.sub_type = 'table_watch'
  AND s.active = true
WHERE cl.matched = false
  AND cl.table_key = s.filter_value;
```

3. Writes matches to `lake._notification_queue`.
4. Marks processed change_log rows as `matched = true`.

---

## 4. Notification Queue + Fan-Out

### 4.1 Schema: `lake._notification_queue`

```sql
CREATE TABLE IF NOT EXISTS lake._notification_queue (
    id              VARCHAR PRIMARY KEY,   -- UUID
    user_id         VARCHAR NOT NULL,
    sub_id          VARCHAR NOT NULL,      -- FK to _subscriptions
    change_ids      JSON NOT NULL,         -- array of change_log IDs
    channel         VARCHAR NOT NULL,      -- 'telegram', 'email', 'rss'
    frequency       VARCHAR NOT NULL,      -- 'realtime', 'daily', 'weekly'
    payload         JSON NOT NULL,         -- pre-rendered notification content
    status          VARCHAR DEFAULT 'pending',  -- 'pending', 'sent', 'failed', 'skipped'
    created_at      TIMESTAMP DEFAULT current_timestamp,
    sent_at         TIMESTAMP,
    attempts        INT DEFAULT 0,
    last_error      VARCHAR
);
```

### 4.2 Payload Format

All channels receive a standardized payload that the delivery layer renders per-channel:

```json
{
  "type": "address_watch",
  "title": "New HPD violation at 305 LINDEN BLVD",
  "summary": "Class C violation: Roach infestation in apartment 3A",
  "details": {
    "table": "housing.hpd_violations",
    "table_label": "HPD Violations",
    "change_type": "insert",
    "row_count": 1,
    "key_fields": {
      "bbl": "3037230001",
      "address": "305 LINDEN BLVD, Brooklyn",
      "violation_class": "C",
      "description": "Roach infestation"
    }
  },
  "subscription": {
    "id": "sub-uuid",
    "type": "address_watch",
    "filter": "3037230001",
    "label": "305 Linden Blvd, Brooklyn"
  },
  "links": {
    "building": "https://common-ground.nyc/building/3037230001",
    "manage": "https://common-ground.nyc/alerts"
  },
  "batch_count": 3,
  "batch_summary": "3 new violations since last check"
}
```

### 4.3 Batching + Rate Limiting

**Batching by frequency:**

| Frequency | Behavior |
|-----------|----------|
| `realtime` | Send within 5 minutes of detection. Max 10 notifications/user/hour. |
| `daily` | Aggregate all pending notifications for user, send at 8 AM ET. Single digest. |
| `weekly` | Aggregate all pending notifications for user, send Monday 8 AM ET. Single digest. |

**Rate limiting:**
- Hard cap: 50 notifications per user per day (across all channels).
- Per-channel caps: Telegram 30/day, Email 5/day, RSS unlimited.
- Overflow: notifications marked `skipped` with reason, included in next digest.

### 4.4 Delivery Implementations

#### Telegram
- Send via Telegram Bot API (`sendMessage` with HTML parse mode).
- For group topics: post to the appropriate topic thread ID.
- Inline keyboard: `[View Building]` `[Mute 24h]` `[Unsubscribe]`
- Digest: single message with collapsible sections per subscription.

#### Email
- Batch into HTML digest via Resend API (transactional email).
- Subject: "Common Ground Alert: {N} updates for your watchlist"
- Unsubscribe link in footer (one-click via signed URL).
- Only send for `daily` and `weekly` frequency (no realtime email).

#### RSS / Atom
- Per-subscription Atom feed files stored on MinIO at `s3://cg-feeds/{user_id}/{sub_id}.xml`.
- Updated after each fan-out run.
- Public URL via Cloudflare: `https://feeds.common-ground.nyc/{user_id}/{sub_id}.xml`
- Also: per-ZIP aggregate feeds at `https://feeds.common-ground.nyc/zip/{zip}.xml`

### 4.5 Fan-Out Asset

```python
@dg.asset(
    key=dg.AssetKey(["notifications", "fanout"]),
    group_name="notifications",
    deps=[dg.AssetKey(["notifications", "matched"])],
    automation_condition=dg.AutomationCondition.eager(),
)
def notification_fanout(context) -> dg.MaterializeResult:
    """Deliver pending notifications via Telegram, Email, RSS."""
    conn = _connect_ducklake()
    try:
        # 1. Read pending queue entries grouped by user + channel
        pending = _read_pending_grouped(conn)

        # 2. Apply rate limits
        deliverable = _apply_rate_limits(pending)

        # 3. Deliver per channel
        results = {"telegram": 0, "email": 0, "rss": 0, "skipped": 0}
        for batch in deliverable:
            if batch.channel == "telegram":
                _send_telegram(batch)
                results["telegram"] += 1
            elif batch.channel == "email":
                _send_email_digest(batch)
                results["email"] += 1
            elif batch.channel == "rss":
                _update_rss_feed(batch)
                results["rss"] += 1

        # 4. Mark sent
        _mark_sent(conn, deliverable)

        return dg.MaterializeResult(metadata={
            "telegram_sent": results["telegram"],
            "email_sent": results["email"],
            "rss_updated": results["rss"],
            "skipped": results["skipped"],
        })
    finally:
        conn.close()
```

### 4.6 Delivery Tracking

```sql
CREATE TABLE IF NOT EXISTS lake._delivery_log (
    id              VARCHAR PRIMARY KEY,
    queue_id        VARCHAR NOT NULL,       -- FK to _notification_queue
    channel         VARCHAR NOT NULL,
    status          VARCHAR NOT NULL,       -- 'delivered', 'failed', 'bounced'
    response_code   INT,
    response_body   VARCHAR,
    delivered_at    TIMESTAMP DEFAULT current_timestamp,
    retry_count     INT DEFAULT 0
);
```

Failed deliveries are retried with exponential backoff (3 attempts max). After 3 failures, the notification is marked `failed` and the user's channel preference is flagged for review.

---

## 5. Schedules

| Component | Trigger | Frequency |
|-----------|---------|-----------|
| `change_detection_sensor` | `run_status_sensor(SUCCESS)` | After every successful run |
| `notification_match` asset | `AutomationCondition.eager()` | Runs when `_change_log` has unmatched rows |
| `notification_fanout` asset | `AutomationCondition.eager()` | Runs when `_notification_queue` has pending items |
| `daily_digest_job` | Cron schedule `0 13 * * *` (8 AM ET) | Daily digest send |
| `weekly_digest_job` | Cron schedule `0 13 * * 1` (Monday 8 AM ET) | Weekly digest send |
| `change_log_cleanup` | Cron schedule `0 4 * * *` | Delete matched entries older than 30 days |

---

## 6. Parquet Export Assets (SQLRooms Dashboard)

### 6.1 Purpose

Pre-compute aggregate Parquet files that the SQLRooms client-side dashboard loads directly via HTTP range requests. The browser loads Parquet into DuckDB-WASM — no server-side query needed.

### 6.2 Aggregate Files (rebuild daily)

| File | ~Size | Grain | Key Columns | Source Tables |
|------|-------|-------|-------------|---------------|
| `agg_zip_overview.parquet` | 2 MB | ZIP (~200 rows) | zip, population, median_income, median_rent, total_complaints, total_violations, total_crimes, avg_restaurant_grade | housing, census, 311, DOB, NYPD |
| `agg_zip_timeseries.parquet` | 10 MB | ZIP x month x category | zip, year_month, category, count | complaints, violations, crimes, 311 — last 5 years |
| `agg_building_summary.parquet` | ~10 MB | BBL (~150-200K buildings with any violation or complaint) | bbl, address, zip, owner_name, violation_count, complaint_count, open_violations, last_inspection_date | DOB, HPD, ECB. Clean buildings excluded — dashboard falls back to MCP `building()` for on-demand detail. Only split by borough if >15 MB. |
| `agg_entity_network.parquet` | 5 MB | Entity | entity_name, roles, portfolio_size, total_violations, connection_count | entity resolution + ownership |
| `agg_election_results.parquet` | 3 MB | District x race x year | district, race, year, votes, turnout_pct | BOE election tables |

### 6.3 Geo Reference Files (one-time, from NYC Planning BYTES)

| File | ~Size | Content |
|------|-------|---------|
| `geo_zip_boundaries.parquet` | 2 MB | NYC ZIP code polygons as WKB geometry |
| `geo_council_districts.parquet` | 1 MB | City Council district polygons as WKB |
| `geo_precincts.parquet` | 1 MB | Police precinct polygons as WKB |
| `geo_community_districts.parquet` | 1 MB | Community district polygons as WKB |

### 6.4 Storage + Access

- **Bucket:** `s3://public-agg/` on MinIO (178.156.228.119:9000)
- **Policy:** Public-read (anonymous GET)
- **CORS:** Allow `GET`, `HEAD` from `*.common-ground.nyc` origins; expose `Range` header
- **Cache-Control:** `max-age=3600` + ETag for invalidation
- **CDN:** Cloudflare proxies `https://data.common-ground.nyc/agg/*.parquet`
- **Size target:** < 15 MB per file. If `agg_building_summary` exceeds this after violations-only filter, split by borough as last resort (adds frontend routing complexity).

### 6.5 Dagster Assets

Two separate assets: one for daily aggregates, one for static geo files.

```python
@dg.asset(
    key=dg.AssetKey(["dashboard", "aggregate_parquet_views"]),
    group_name="dashboard",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["foundation", "mv_building_hub"]),
        dg.AssetKey(["foundation", "mv_zip_stats"]),
        dg.AssetKey(["foundation", "mv_crime_precinct"]),
        dg.AssetKey(["foundation", "mv_corp_network"]),
    ],
)
def aggregate_parquet_views(context) -> dg.MaterializeResult:
    """Pre-compute aggregate Parquet files for SQLRooms dashboard."""
    conn = _connect_ducklake()
    try:
        exported = 0
        for agg_name, query in AGGREGATION_QUERIES.items():
            result = conn.execute(query).fetch_arrow_table()
            _upload_parquet_to_minio(result, f"agg/{agg_name}.parquet",
                                     bucket="public-agg")
            exported += 1
            context.log.info("Exported %s: %d rows, %.1f MB",
                            agg_name, result.num_rows,
                            result.nbytes / (1024 * 1024))

        return dg.MaterializeResult(metadata={
            "files_exported": dg.MetadataValue.int(exported),
        })
    finally:
        conn.close()


@dg.asset(
    key=dg.AssetKey(["dashboard", "geo_reference_files"]),
    group_name="dashboard",
    description="NYC boundary polygons as Parquet (one-time download from NYC Planning BYTES).",
    compute_kind="download",
)
def geo_reference_files(context) -> dg.MaterializeResult:
    """Download and convert NYC geo boundary files to Parquet on MinIO."""
    uploaded = 0
    for geo_name, source_url in GEO_SOURCES.items():
        gdf = _download_geojson(source_url)
        table = _geodataframe_to_wkb_arrow(gdf)
        _upload_parquet_to_minio(table, f"agg/{geo_name}.parquet",
                                 bucket="public-agg")
        uploaded += 1
        context.log.info("Uploaded %s: %d features", geo_name, table.num_rows)

    return dg.MaterializeResult(metadata={
        "files_uploaded": dg.MetadataValue.int(uploaded),
    })
```

### 6.6 Refresh Schedule

- **Aggregates:** Triggered by `AutomationCondition.eager()` when upstream MVs refresh. MVs refresh when ingestion assets complete. Net effect: dashboard Parquets update within ~30 min of new data landing.
- **Geo files:** Manual trigger only (boundaries change ~once per decade after redistricting).

---

## 7. Implementation Phases

### Phase 1: Change Detection (Week 1)
- Create `_snapshot_bookmarks` and `_change_log` tables
- Implement `change_detection_sensor` (run_status_sensor)
- Test with a single table (e.g., hpd_violations)
- Verify `table_changes()` works with current DuckLake version

### Phase 2: Subscription Model (Week 1-2)
- Create `_subscriptions` table
- Build subscription CRUD via MCP tool (so Telegram bot can create them)
- Implement `address_watch` and `table_watch` matchers first (simplest)

### Phase 3: Notification Matching (Week 2)
- Implement `notification_match` asset
- All 6 subscription types
- Test with synthetic subscriptions

### Phase 4: Fan-Out (Week 2-3)
- Telegram delivery (highest priority — users are already in Telegram)
- RSS feed generation
- Email digest (lower priority)
- Delivery tracking + retry

### Phase 5: Parquet Exports (Week 3)
- Implement aggregation queries
- MinIO upload with CORS
- Cloudflare CDN config

---

## 8. Key Design Decisions

1. **DuckLake `table_changes()` over custom CDC**: Native, zero-maintenance, captures exact row-level diffs. Only falls back to row-count for full-replace tables.

2. **JSON row_data in change_log**: Avoids needing a separate change_log table per schema. Trades query speed for flexibility. The matching queries use `json_extract_string()` which is fast enough for the expected volume (~10K changes/day).

3. **SQL-based matching over application code**: All subscription matching happens in DuckDB SQL joins. No Python loops over rows. Scales to thousands of subscriptions without code changes.

4. **Dagster automation over cron**: Using `AutomationCondition.eager()` chains the pipeline: materialization → change detection → matching → fan-out. Each step runs only when its input has new data.

5. **Batched digests by default**: Most users should be on `daily` frequency. Realtime is opt-in and rate-limited to prevent notification fatigue.

6. **Parquet exports as a Dagster asset**: Keeps the dashboard data pipeline visible in the Dagster UI alongside everything else. Auto-refreshes via eager automation.

---

## 9. Open Questions

1. **DuckLake snapshot retention**: How long does DuckLake keep snapshots? Need to verify `table_changes()` works across daily refresh cycles. If snapshots are garbage-collected, we need to process changes more frequently.

2. **Full-replace table notifications**: For federal assets (DROP + CREATE), we can't diff individual rows. Options: (a) skip notifications for these tables, (b) treat every refresh as "all new", (c) migrate federal assets to MERGE INTO.

3. **Entity matching performance**: Fuzzy name matching via phonetic index may be slow for high-volume change_log batches. May need a materialized lookup table that maps phonetic codes to subscription IDs.

4. **Subscription management UX**: The Telegram bot needs to create subscriptions. The NLP layer parses natural language → subscription type + filter. Need to define the handoff format between NLP and this engine.
