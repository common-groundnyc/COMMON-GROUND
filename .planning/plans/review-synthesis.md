# CG Platform Review Synthesis — Amendments to Roadmap

> **Date**: 2026-04-06
> **Sources**: strategist, tech-auditor, civic-advisor
> **Status**: APPROVED AMENDMENTS

---

## Executive Summary

The data layer is strong. The product layer needs restructuring: wrong channel priority, wrong sequencing, missing safeguards. Three critical technical risks must be resolved before writing any feature code.

---

## Critical Pre-Work (Before Any Feature Code)

### C1. Verify MinIO / Storage Status
- SSH into Hetzner, check if MinIO is running
- CLAUDE.md now says "Local NVMe" — if MinIO is gone, dashboard Parquet-over-HTTP strategy needs rework
- Options if no MinIO: (a) reinstall MinIO for public-agg bucket only, (b) serve Parquet via nginx/caddy from NVMe, (c) use Cloudflare R2

### C2. Test DuckLake `table_changes()` End-to-End
- DuckLake GitHub #330 shows cumulative snapshot behavior is buggy
- Write a standalone test: create table, insert rows, take snapshot, insert more, call table_changes()
- If broken: fall back to `_dlt_load_id` watermark comparison (dlt already tracks this)

### C3. Move Subscriptions + Notifications to Postgres
- DuckDB is single-writer — 3 services competing for write access will deadlock
- Subscriptions are OLTP (frequent small writes), not analytics
- Use existing Postgres on Hetzner, new `cg` database alongside `ducklake`

---

## Revised Sequencing

| Phase | What | Calendar | Rationale |
|-------|------|----------|-----------|
| **P0** | Pre-work: verify infra (C1-C3) | 2-3 days | Don't build on broken foundations |
| **P1** | Website dashboard (`/explore`) | 2 weeks | Discovery surface. Top of funnel. Visual proof of value. |
| **P2** | Channel-agnostic notification engine + email/SMS | 2 weeks | Widest reach. Serves actual affected populations. |
| **P3** | Telegram bot (NL subscriptions, `/ask`) | 2 weeks | Power-user channel. Builds on P2 engine. |
| **P4** | CLI (`cg`) | 1 week | Smallest audience, ships fast on P2 API. |
| **P5** | Anti-stalking, privacy policy, Spanish i18n | 1 week | Required before public launch. |
| **P6** | Partner recruitment + soft launch | Ongoing | In-person at Legal Aid offices, tenant org meetings. |
| **P7** | Community features (Forum Topics) | Only after 1K users | Requires moderation plan + community manager. |

---

## Features Cut / Deferred

| Feature | Verdict | Reason |
|---------|---------|--------|
| Forum Topics (per-ZIP community) | **DEFER to 1K+ users** | Moderation liability, dead forum risk |
| `data_watch` (arbitrary SQL WHERE) | **CUT** | SQL injection vector |
| `keyword_watch` | **DEFER** | Imprecise, slow on JSON. Focus address + entity first |
| DuckDB-WASM in Telegram Mini App | **CHANGE** to server-rendered | 30MB crashes phones |
| RSS feeds | **DEFER** | Tiny audience |
| Homebrew tap | **DEFER** | pipx sufficient |

---

## Features Added

| Feature | Phase | Reason |
|---------|-------|--------|
| SMS delivery (Twilio) | P2 | Reach non-tech populations. NotifyNYC model. |
| Email delivery (Resend/SES) | P2 | Universal reach, no app install. |
| WhatsApp Business API | P3+ | 40% of NYC is foreign-born, WhatsApp dominant. |
| Anti-stalking protections | P5 | Subscription caps per entity, audit logs, pattern detection |
| Privacy policy + data retention | P5 | Watchlists are sensitive. Legal requirement. |
| Uptime monitoring + heartbeat | P0 | Silent failure is unacceptable for notifications |
| Spanish language support | P5 | 24% of NYC speaks Spanish at home |
| Partner recruitment plan | P6 | First 100 users from institutions, not organic |

---

## Notification Engine: Channel-Agnostic Design

The original plan had Telegram as the primary delivery channel. The revised design makes the engine channel-agnostic from day one:

```
Dagster sensor → change_log → match → notification_queue
                                            │
                                    ┌───────┼───────┐───────┐
                                    ▼       ▼       ▼       ▼
                                  Email    SMS   Telegram  Webhook
                                (Resend) (Twilio) (Bot API) (CLI)
```

Each subscription stores a `channels` array. The fan-out asset dispatches to all selected channels. Users pick their preferred channel when subscribing — no platform lock-in.

---

## User Personas (Explicit)

### Primary: Tenant Organizer (~500 in NYC)
- Uses `address_watch` on buildings they're organizing
- Needs SMS or WhatsApp (not Telegram)
- Spanish/Chinese language support critical
- Discovery: through tenant union meetings + legal aid referrals

### Secondary: Legal Aid Attorney (~2,000 in NYC)
- Uses `entity_watch` on landlords they're litigating against
- Uses `address_watch` for case buildings
- Comfortable with email + web dashboard
- Discovery: through Legal Aid Society, Brooklyn Legal Services

### Tertiary: Investigative Journalist (~100 in NYC)
- Uses `entity_watch` for campaign finance + corporate filings
- Uses `network` tools for follow-the-money investigations
- Comfortable with CLI + Telegram
- Discovery: through THE CITY, City Limits, ProPublica Local

### Quaternary: City Council Staff (~50)
- Uses dashboard for district-level oversight
- Uses `neighborhood` views for constituent inquiries
- Discovery: through direct outreach to council offices

---

## Anti-Stalking Protections (Required Before Launch)

1. **Subscription caps**: Max 10 `entity_watch` subscriptions per user. Max 20 `address_watch`.
2. **Audit log**: All subscription CRUD is logged with timestamp, user_id, target.
3. **Pattern detection**: Flag users watching 5+ addresses in the same building (potential landlord surveillance).
4. **Cooling period**: New entity_watch subscriptions have 24-hour delay before first notification.
5. **Abuse reporting**: Inline button on every notification for targets to report monitoring.

---

## Technical Architecture Amendments

1. **Subscriptions in Postgres** (not DuckLake) — OLTP workload, avoids DuckDB write contention
2. **CDC fallback**: If `table_changes()` fails testing, use `_dlt_load_id` watermark (dlt already tracks last load)
3. **Parquet serving**: If MinIO is gone, serve aggregates via Caddy/nginx from NVMe with CORS headers. Or use Cloudflare R2.
4. **Telegram Mini App**: Server-rendered via MCP API, not DuckDB-WASM. Website gets the full WASM experience.
5. **Health monitoring**: Add /health endpoint to all services + external uptime check (UptimeRobot or similar)

---

## First 100 Users — Recruitment Plan

| Organization | Contact Method | Users | What They Get |
|-------------|---------------|-------|---------------|
| Brooklyn Legal Services | In-person at staff meeting | 20 | address_watch + entity_watch for active cases |
| Met Council on Housing | In-person at organizer training | 30 | address_watch via SMS for buildings they're organizing |
| Crown Heights Tenant Union | In-person at monthly meeting | 20 | address_watch + neighborhood dashboard |
| THE CITY newsroom | Email to data editor | 10 | entity_watch + CLI for investigations |
| City Council Housing Committee | Email to chief of staff | 10 | dashboard + neighborhood views |
| CASA / community boards | In-person | 10 | neighborhood dashboard + SMS alerts |

---

## Source Documents

- `review-strategic.md` — Product strategy critique (strategist)
- `review-technical.md` — Technical risk assessment (tech-auditor)
- `review-civic.md` — Civic impact + ethics analysis (civic-advisor)
- `cg-platform-roadmap.md` — Original roadmap (amended by this document)
