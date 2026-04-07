# Common Ground Platform Expansion — Unified Roadmap

> **Date**: 2026-04-03
> **Status**: PLAN
> **Sources**: 5 parallel research agents (telegram-architect, nlp-designer, pipeline-engineer, frontend-planner, cli-designer)

---

## Vision

Four surfaces, one data lake, zero accounts:

| Surface | Tech | Purpose |
|---------|------|---------|
| **Website** `/explore` | SQLRooms + DuckDB-WASM + Kepler maps | Visual data exploration with cross-filtering |
| **Telegram Bot** | aiogram 3.x + Claude Haiku + Forum Topics | Granular notifications + community discussion |
| **CLI** `cg` | typer + FastMCP Client + Rich | Power-user terminal access to all 15 MCP tools |
| **MCP Server** | FastMCP (existing) | AI-powered investigation (Claude, Cursor, etc.) |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    COMMON GROUND PLATFORM                        │
│                                                                  │
│  CLIENTS (zero accounts)                                         │
│  ┌──────────┐ ┌──────────────┐ ┌──────────────┐ ┌────────────┐ │
│  │  cg CLI   │ │ Telegram Bot │ │ Website      │ │ MCP Hosts  │ │
│  │  (typer)  │ │ (aiogram)    │ │ /explore     │ │ (Claude)   │ │
│  │           │ │ Mini App     │ │ SQLRooms     │ │            │ │
│  └─────┬─────┘ └──────┬───────┘ └──────┬───────┘ └─────┬──────┘ │
│        │              │               │                │         │
│        ▼              ▼               │                ▼         │
│  ┌─────────────────────────┐          │   ┌──────────────────┐  │
│  │ FastMCP Server (:4213)  │◄─────────┘   │ MinIO public-agg │  │
│  │ 15 tools, SSE           │              │ Parquet + CORS   │  │
│  └─────────┬───────────────┘              │ (DuckDB-WASM     │  │
│            │                               │  range requests) │  │
│  ┌─────────▼───────────────┐              └──────────────────┘  │
│  │ Subscription API (:8080)│                                     │
│  │ FastAPI, CRUD + auth    │                                     │
│  └─────────┬───────────────┘                                     │
│            │                                                      │
│  ┌─────────▼────────────────────────────────────────────────┐    │
│  │                    DATA LAYER (Hetzner)                    │    │
│  │  DuckDB/DuckLake ←→ Postgres (catalog + subs) ←→ MinIO  │    │
│  │  Lance (2.96M embeddings)    Graph Cache (40+ tables)    │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                   │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │  DAGSTER PIPELINE (Mac Studio → Hetzner)                   │   │
│  │  260 datasets → dlt → MinIO → DuckLake                     │   │
│  │  + change_detection_sensor → notification_match → fan-out  │   │
│  │  + parquet_exports (aggregates for SQLRooms)               │   │
│  └───────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions (Resolved Across Agents)

### 1. LLM Model Split
- **Haiku 4.5** for subscription parsing — $0.001/parse, strict tool use, ~800ms
- **Sonnet 4.6** for `/ask` free-form queries — streamed via `sendMessageDraft`
- Rationale: Haiku is sufficient for structured extraction. Sonnet needed for open-ended data questions.

### 2. Subscription Storage: DuckLake (not separate Postgres table)
- Pipeline engineer and telegram architect aligned on `lake._subscriptions` in DuckLake
- Notification engine can JOIN subscriptions against change_log in the same DuckDB connection
- Bot writes via MCP `query()` tool — no direct DB access needed

### 3. Change Detection: DuckLake `table_changes()` (native CDC)
- Primary: `table_changes(table, start_snapshot, end_snapshot)` for 287 delta-merge tables
- Fallback: row-count delta for 49 full-replace federal tables
- Runs via Dagster `run_status_sensor(SUCCESS)` — fires after every materialization

### 4. Notification Fan-Out: Dagster pushes directly
- Dagster fan-out asset calls Telegram `sendMessage` directly
- Bot is passive for notifications (no HTTP endpoint needed)
- Bot is active for subscription CRUD, topic management, LLM parsing

### 5. Dashboard: One React app, two shells
- SQLRooms core (DuckDB + Kepler + Mosaic + Recharts)
- Website: Next.js `/explore` with localStorage persistence
- Telegram: Mini App with CloudStorage persistence + compact layout
- Same components, different layout and persistence adapters

### 6. CLI: Thin MCP client, zero DuckDB dependency
- FastMCP `Client` class over SSE — already in the stack
- All 15 tools map 1:1 to CLI commands
- 3 dependencies total: typer + fastmcp + rich

---

## Phased Implementation

### Phase 0: Shared Infrastructure (Week 1)
> Foundation that all surfaces depend on

- [ ] **Subscription table** in DuckLake (`lake._subscriptions` + `lake._snapshot_bookmarks` + `lake._change_log`)
- [ ] **Subscription REST API** — FastAPI on :8080, CRUD endpoints, API key + Telegram auth
- [ ] **MinIO public-agg bucket** — CORS config, public read, separate from main lake
- [ ] **Dagster parquet_exports asset** — 9 aggregate Parquet files for SQLRooms dashboard
- [ ] Wire into docker-compose on Hetzner

### Phase 1: CLI MVP (Week 1-2, parallel with Phase 0)
> Fastest to ship, validates the MCP client pattern

- [ ] Scaffold `cg-cli` — typer + FastMCP Client + Rich
- [ ] Map all 15 MCP tools to CLI commands
- [ ] Output modes: table (Rich), json, csv, raw
- [ ] Config management (`~/.cg/config.toml`)
- [ ] `cg watch` — create subscriptions via REST API
- [ ] Publish to PyPI (`pipx install cg-cli`)

### Phase 2: Telegram Bot Core (Week 2-3)
> NL subscriptions + DM organization

- [ ] Register `@CommonGroundNYCBot` via BotFather
- [ ] aiogram 3.x skeleton — webhook, routers, middlewares
- [ ] LLM subscription parser (Haiku 4.5, strict tool use)
- [ ] Subscription CRUD via DuckLake (`create_subscription` tool)
- [ ] Confirmation cards with inline keyboards
- [ ] Preview on creation ("last 30 days would have sent N alerts")
- [ ] DM topics: "Your Alerts", "Your Watches", "Ask CG"
- [ ] `/mysubs` portfolio view
- [ ] Guided fallback (inline keyboard state machine, no LLM)
- [ ] Deploy as Docker container on Hetzner, webhook via Cloudflare Tunnel

### Phase 3: Change Detection + Notifications (Week 3-4)
> The engine that makes subscriptions live

- [ ] `change_detection_sensor` — DuckLake `table_changes()` + snapshot bookmarks
- [ ] `notification_match` asset — SQL joins change_log × subscriptions
- [ ] 6 subscription matchers: address_watch, entity_watch, zip_category, table_watch, keyword_watch, data_watch
- [ ] `notification_fanout` asset — Telegram delivery (sendMessage with HTML + inline keyboards)
- [ ] Batching: realtime (5 min, max 10/hr), daily digest (8 AM ET), weekly (Monday 8 AM)
- [ ] Rate limiting: 50/user/day hard cap
- [ ] Delivery tracking + retry (3 attempts, exponential backoff)

### Phase 4: Community Forum (Week 4-5)
> Per-ZIP topics + group discussion

- [ ] Create supergroup "Common Ground NYC"
- [ ] Auto-create Forum Topics per ZIP code (on first subscriber)
- [ ] Bot posts data changes to relevant ZIP topic
- [ ] Inline buttons: [View Building] [Track This Owner] [Discuss in {ZIP}]
- [ ] Bot responds to @mentions in group using MCP tools
- [ ] Contextual framing: "↑23% this quarter" not just raw counts

### Phase 5: SQLRooms Dashboard (Week 5-7)
> Interactive visual exploration on the website

- [ ] Install SQLRooms packages in CG website
- [ ] Room store: DuckDB + Kepler + Mosaic + Recharts + CRDT slices
- [ ] Data layer: DuckDB-WASM loads aggregate Parquets from MinIO via range requests
- [ ] NYC geo boundaries (ZIP, council district, precinct) as Parquet with WKB
- [ ] Neighborhood overview: map choropleth + stat cards + time series + bar charts
- [ ] Cross-filtering via Mosaic: click ZIP on map → all charts update
- [ ] CRDT persistence to localStorage (remember last ZIP, filters)
- [ ] Building detail view (hybrid: local aggregates + MCP API for full data)
- [ ] Election results view
- [ ] COOP/COEP headers for SharedArrayBuffer

### Phase 6: Telegram Mini App (Week 7-8)
> Same dashboard inside Telegram

- [ ] Compact mobile layout (single-column stack)
- [ ] Telegram environment detection + theme bridge
- [ ] CloudStorage adapter (1024 items × 4KB) for state persistence
- [ ] Deep links: `startapp=zip_11201`, `startapp=bbl_3012340001`
- [ ] Register Mini App with BotFather
- [ ] Touch interaction adjustments (larger targets, no hover)

### Phase 7: Polish + Advanced (Week 8+)
> The "wow" features

- [ ] `/ask` streaming via `sendMessageDraft` (Sonnet 4.6)
- [ ] Email digest channel (Resend/SES)
- [ ] RSS/Atom per-ZIP feeds on MinIO
- [ ] `@sqlrooms/ai` natural language query in dashboard
- [ ] OPFS caching for offline repeat visits
- [ ] Homebrew tap for CLI
- [ ] PWA install prompt on website with Web Push notifications

---

## Effort Estimates

| Phase | Calendar | Work | Dependencies |
|-------|----------|------|-------------|
| P0: Shared infra | 1 week | 3-4 days | None |
| P1: CLI | 1 week | 3-4 days | P0 (subscription API) |
| P2: Telegram bot | 2 weeks | 8-10 days | P0 (subscription table) |
| P3: Notifications | 2 weeks | 6-8 days | P0 + P2 (subscriptions exist) |
| P4: Community | 1 week | 3-4 days | P2 + P3 (bot + notifications) |
| P5: Dashboard | 2 weeks | 10-12 days | P0 (Parquet exports) |
| P6: Mini App | 1-2 weeks | 5-7 days | P2 + P5 (bot + dashboard) |
| P7: Polish | Ongoing | — | All |

**Critical path**: P0 → P2 → P3 → P4 (Telegram notifications live in ~5 weeks)
**Parallel track**: P0 → P1 (CLI ships in ~2 weeks) and P0 → P5 (dashboard in ~3 weeks)

---

## Cost Projections

| Component | Monthly Cost | At 1K users | At 10K users |
|-----------|-------------|-------------|--------------|
| Haiku parsing | $0.50 | $4.75 | $47.50 |
| Sonnet `/ask` | $2.00 | $20 | $200 |
| Hetzner server | Already running | — | — |
| Telegram API | Free | Free | Free |
| MinIO storage | Already running | — | — |
| Email (Resend) | — | $3 | $15 |
| **Total marginal** | **~$2.50** | **~$28** | **~$263** |

---

## Open Questions

1. **DuckLake snapshot retention** — verify `table_changes()` works across daily refresh cycles. If snapshots are GC'd, sensor needs higher frequency.
2. **Kepler.gl vs deck.gl** — Kepler is higher-level but 2MB bundle. Benchmark against deck.gl + Mosaic.
3. **Mapbox vs MapLibre** — Kepler requires Mapbox token. MapLibre is free. Check SQLRooms compatibility.
4. **Building-level data** — 1M BBL rows (~50MB). Load into WASM or always fetch from MCP? Hybrid recommended.
5. **Federal asset notifications** — full-replace tables can't diff rows. Options: skip, treat as "all new", or migrate to MERGE INTO.
6. **Cross-identity linking** — CLI user and Telegram user are separate identities. Allow linking later?

---

## Source Documents

All 5 agent research plans are in this directory:
- `cg-telegram-bot-architecture.md` — Bot framework, routers, Forum Topics, deployment
- `cg-subscription-parser-design.md` — LLM parsing, tool use, cost analysis, UX flows
- `cg-notification-engine-design.md` — Change detection, matching SQL, fan-out, Parquet exports
- `cg-sqlrooms-dashboard-plan.md` — SQLRooms packages, data layer, layouts, Telegram Mini App
- `cg-cli-and-platform-design.md` — CLI commands, subscription API, unified architecture
