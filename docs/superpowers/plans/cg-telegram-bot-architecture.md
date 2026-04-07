# Common Ground — Telegram Bot Architecture

> **Status**: Planning
> **Date**: 2026-04-03
> **Author**: Telegram Bot Architect Agent

---

## 1. Overview

A Telegram bot that turns the Common Ground NYC open data lake (294 tables, 60M+ rows) into a living notification and community discussion platform. Users subscribe to granular data changes via natural language, receive alerts in organized topics, and explore dashboards via an embedded Mini App.

### Design Goals

1. **Natural-language subscriptions** — "tell me when a new violation is filed at 305 Linden Blvd"
2. **Per-ZIP community topics** — forum supergroup with auto-created topic per ZIP code
3. **DM topics** — personal alerts organized by category in private chat
4. **Mini App dashboard** — SQLRooms embedded in Telegram for interactive data exploration
5. **LLM streaming** — use `sendMessageDraft` (Bot API 9.5) for real-time LLM response streaming

---

## 2. Technology Decisions

| Component | Choice | Rationale |
|---|---|---|
| Bot framework | **aiogram 3.26+** | Async-native, full Bot API 9.5 support, routers/middlewares/FSM, `sendMessageDraft` |
| Web server | **aiohttp** (via aiogram webhook) | Built-in aiogram integration, serves webhook + Mini App static |
| LLM | **Claude Sonnet 4.6** via Anthropic API | Parses natural-language subscriptions into structured filters |
| Subscription store | **DuckLake** (`lake._subscriptions` table) | Shared with notification engine; bot writes via MCP |
| Notification queue | **DuckLake** (`lake._notification_queue`) | Engine writes payloads; Dagster fan-out asset calls Telegram API directly |
| Change detection | **Dagster sensor + fan-out asset** | Engine detects changes AND pushes to Telegram — bot is passive receiver |
| Bot-local state | **Postgres** (existing, 178.156.228.119:5432) | `telegram` schema for users, topic mappings, DM preferences |
| Data queries | **FastMCP server** (existing, :4213) | Bot calls MCP tools for data retrieval, same as Claude clients |
| Deployment | **Docker** on Hetzner | Same compose stack, webhook via Cloudflare Tunnel |

---

## 3. Component Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        TELEGRAM CLOUD                            │
│                                                                  │
│   Users ←→ Bot API ←→ Webhook (HTTPS)                           │
│                           │                                      │
│   Mini App (iframe) ──────┼── SQLRooms static files              │
└───────────────────────────┼──────────────────────────────────────┘
                            │
                    ┌───────▼───────┐
                    │  aiohttp      │ :8443 (webhook + static)
                    │  server       │
                    └───────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
     ┌────────▼──┐  ┌──────▼─────┐  ┌────▼────────┐
     │ Dispatcher │  │ Notifier   │  │ Mini App    │
     │ (aiogram)  │  │ Service    │  │ API routes  │
     │            │  │            │  │             │
     │ Routers:   │  │ Fan-out    │  │ /webapp/*   │
     │  /start    │  │ queue      │  │ auth verify │
     │  /sub      │  │ rate limit │  │ deep links  │
     │  /unsub    │  │ batch send │  │             │
     │  /my       │  │            │  │             │
     │  /ask      │  │            │  │             │
     └────┬───────┘  └──────┬─────┘  └─────────────┘
          │                 │
     ┌────▼─────────────────▼──────┐
     │       Service Layer          │
     │                              │
     │  SubscriptionService         │
     │  LLMParserService            │
     │  NotificationService         │
     │  TopicManagerService         │
     │  MCPClientService            │
     └──────────┬──────────────────┘
                │
     ┌──────────▼──────────────────┐
     │       Data Layer             │
     │                              │
     │  Postgres (telegram schema)  │
     │  FastMCP (data queries)      │
     │  Anthropic API (LLM)        │
     └─────────────────────────────┘

     ┌─────────────────────────────┐
     │   DAGSTER (separate container)│
     │                              │
     │  change_detection_sensor     │
     │       │                      │
     │       ▼                      │
     │  notify_subscribers_job      │
     │       │                      │
     │       ▼                      │
     │  HTTP POST → Notifier        │
     └─────────────────────────────┘
```

---

## 3A. Integration Contract with Notification Engine (UPDATED 2026-04-03)

> This section supersedes the original Dagster sensor design in section 8.
> Based on payload spec from the pipeline engineer.

### Push Model

The Dagster fan-out asset calls the Telegram Bot API **directly** via `sendMessage`.
The bot does NOT poll for notifications and does NOT expose an internal HTTP endpoint.
The bot's responsibility is: (1) subscription CRUD, (2) topic management, (3) message rendering.

### Subscription Storage

Subscriptions live in **DuckLake** at `lake._subscriptions`, NOT in Postgres.
The bot writes subscriptions via MCP `query()` tool. Required fields:

| Field | Type | Example |
|---|---|---|
| `user_id` | BIGINT | Telegram user ID |
| `sub_type` | TEXT | `address_watch`, `zip_digest`, `entity_watch`, `table_watch` |
| `filter_value` | TEXT | BBL `3037230001`, ZIP `11201`, entity name, table name |
| `channels` | TEXT[] | Must include `"telegram"` |
| `frequency` | TEXT | `realtime`, `daily`, `weekly` |

### Notification Payload (from `lake._notification_queue.payload`)

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

### What the Bot Renders

The Dagster fan-out asset calls `sendMessage` with the rendered HTML. The bot's `NotificationRenderer` service formats the payload:

- `title` as `<b>bold header</b>`
- `summary` as body text
- `batch_summary` when `batch_count > 1` (digest mode)
- Inline keyboard: `[View Building]` (links.building) | `[Mute 24h]` | `[Unsubscribe]`
- Route to correct `message_thread_id` based on subscription → topic mapping

### Frequency Modes

| Mode | Behavior | Rate Cap |
|---|---|---|
| `realtime` | Delivered within 5 min of materialization | Max 10/user/hour |
| `daily` | Single digest at 8 AM ET | 1 message |
| `weekly` | Single digest Monday 8 AM ET | 1 message |

### Bot Responsibilities vs Engine Responsibilities

| Concern | Owner |
|---|---|
| Change detection | **Engine** (Dagster sensor) |
| Subscription matching | **Engine** (reads `_subscriptions`) |
| Payload construction | **Engine** (builds JSON in `_notification_queue`) |
| HTML rendering | **Engine** (fan-out asset renders + calls sendMessage) |
| Subscription CRUD (user-facing) | **Bot** (writes to `_subscriptions` via MCP) |
| Topic management (create/map) | **Bot** (Telegram API calls) |
| LLM parsing (NL → filter) | **Bot** (Claude Sonnet 4.6) |
| `/ask` streaming queries | **Bot** (sendMessageDraft) |
| Callback handling (Mute/Unsub buttons) | **Bot** (updates `_subscriptions` via MCP) |

---

## 4. aiogram Architecture — Routers, Middlewares, Handlers

### 4.1 Router Tree

aiogram 3.x uses a hierarchical router system. Events propagate from parent to child until a handler matches.

```
Dispatcher (root router)
├── auth_router          — /start, registration, onboarding FSM
├── subscription_router  — /sub, /unsub, /my, /edit
├── query_router         — /ask (free-form data questions)
├── admin_router         — /stats, /broadcast, /topic_create
├── group_router         — group message handling, topic routing
└── error_router         — catch-all error handler
```

```python
# bot/routers/__init__.py
from aiogram import Router

auth_router = Router(name="auth")
subscription_router = Router(name="subscriptions")
query_router = Router(name="queries")
admin_router = Router(name="admin")
group_router = Router(name="group")

# In main dispatcher setup:
dp.include_routers(
    auth_router,
    subscription_router,
    query_router,
    admin_router,
    group_router,
)
```

### 4.2 Middleware Stack

Middlewares execute in order for every update. Inner middlewares only run when filters pass.

```
Outer middlewares (run on ALL updates):
  1. RateLimitMiddleware     — per-user cooldown (1 msg/sec)
  2. AuthMiddleware          — load user from DB, inject into handler data
  3. LoggingMiddleware       — structured logging with user_id, chat_id
  4. ErrorCaptureMiddleware  — catch exceptions, send user-friendly error, log full trace

Inner middlewares (run when handler matched):
  5. ThrottleMiddleware      — per-handler rate limiting (e.g., /ask max 5/min)
```

```python
from aiogram import BaseMiddleware
from aiogram.types import Update

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, rate: float = 1.0):
        self.rate = rate
        self._timestamps: dict[int, float] = {}

    async def __call__(self, handler, event: Update, data: dict):
        user_id = event.event.from_user.id if event.event and event.event.from_user else None
        if user_id:
            now = time.monotonic()
            last = self._timestamps.get(user_id, 0)
            if now - last < self.rate:
                return  # silently drop
            self._timestamps[user_id] = now
        return await handler(event, data)
```

### 4.3 FSM for Subscription Creation

Multi-step subscription creation uses aiogram's Finite State Machine.

```
States:
  IDLE → AWAITING_DESCRIPTION → AWAITING_CONFIRMATION → IDLE

Flow:
  User: "tell me when new HPD violations at 305 Linden Blvd"
  Bot:  [LLM parses → structured filter] → shows confirmation card
  User: [clicks Confirm button]
  Bot:  [saves subscription] → "Done! You'll get alerts in your Violations topic."
```

```python
from aiogram.fsm.state import State, StatesGroup

class SubscriptionStates(StatesGroup):
    awaiting_description = State()
    awaiting_confirmation = State()
```

---

## 5. Data Flows

### 5.1 Subscription Creation (User → Bot → DB)

```
User sends: "alert me about new DOB permits in 11201"
        │
        ▼
  subscription_router catches message
        │
        ▼
  LLMParserService.parse(text) → Claude Sonnet 4.6
        │
        ▼
  Returns structured subscription:
  {
    "sub_type": "zip_digest",
    "filter_value": "11201",
    "frequency": "realtime",
    "description": "New DOB permits in ZIP 11201"
  }
        │
        ▼
  Bot sends confirmation card with InlineKeyboard:
  ┌────────────────────────────────────────┐
  │ 📋 New Subscription                    │
  │                                        │
  │ Table: DOB Permits                     │
  │ Filter: ZIP = 11201                    │
  │ Trigger: New rows added                │
  │                                        │
  │ [✓ Confirm]  [✏ Edit]  [✗ Cancel]     │
  └────────────────────────────────────────┘
        │
        ▼ (user clicks Confirm)
  SubscriptionService.create(user_id, filter)
        │
        ▼
  INSERT INTO lake._subscriptions via MCP query() tool:
    (user_id, sub_type, filter_value, channels=['telegram'], frequency)
        │
        ▼
  TopicManagerService.ensure_dm_topic(user_id, "Housing")
        │
        ▼
  Bot: "Subscribed! Alerts will appear in your Housing topic."
```

### 5.2 Notification Pipeline (Dagster → Telegram — Push Model)

> See section 3A for the full integration contract.

```
  Dagster materializes housing.dob_permits
        │
        ▼
  change_detection_sensor (Dagster — owned by pipeline engineer)
    - Compares row counts / max timestamps
    - Detects: 47 new rows in housing.dob_permits
    - Matches against lake._subscriptions
    - Writes payloads to lake._notification_queue
        │
        ▼
  fan_out_notifications asset (Dagster — owned by pipeline engineer)
    - Reads pending notifications from queue
    - Renders HTML from payload (title, summary, inline keyboard)
    - Calls Telegram Bot API sendMessage DIRECTLY
    - Rate limiting: max 10/user/hour for realtime
    - Respects frequency mode (realtime/daily/weekly)
        │
        ▼
  Messages delivered to DM topics + group topics
    - Bot receives callback_query when user clicks buttons
    - Bot handles [Mute 24h] and [Unsubscribe] via MCP writes
```

**Bot's role in notifications**: The bot does NOT send notifications.
The bot (1) provides topic_id mappings so the engine knows where to route,
and (2) handles callback buttons after the message is delivered.

### 5.3 Mini App Launch (Deep Link → WebApp)

```
  User clicks: t.me/CommonGroundNYCBot?startapp=zip_11201
        │
        ▼
  Telegram opens Mini App iframe with launch params:
    tgWebAppStartParam = "zip_11201"
        │
        ▼
  SQLRooms dashboard loads at:
    https://bot.common-ground.nyc/webapp/?start=zip_11201
        │
        ▼
  Frontend JS:
    1. Reads Telegram.WebApp.initData
    2. Sends to /webapp/auth for verification
    3. Bot verifies hash using bot token
    4. Returns JWT for dashboard API calls
        │
        ▼
  Dashboard renders neighborhood overview for ZIP 11201
    - Queries FastMCP via /webapp/api/query proxy
    - neighborhood("11201") → violations, complaints, safety stats
```

---

## 6. Database Schema

### 6.1 DuckLake Tables (shared with notification engine)

These are owned by the pipeline engineer. The bot reads/writes via MCP `query()`.

```sql
-- Subscriptions (bot writes, engine reads)
-- Located at: lake._subscriptions
-- Fields the bot MUST set when creating:
--   user_id       BIGINT      -- Telegram user ID
--   sub_type      TEXT        -- address_watch | zip_digest | entity_watch | table_watch
--   filter_value  TEXT        -- BBL, ZIP, entity name, or table name
--   channels      TEXT[]      -- must include 'telegram'
--   frequency     TEXT        -- realtime | daily | weekly

-- Notification queue (engine writes, engine reads for fan-out)
-- Located at: lake._notification_queue
-- Bot does NOT read this table — engine pushes directly to Telegram
```

### 6.2 Postgres Tables (bot-local state, `telegram` schema)

```sql
CREATE SCHEMA IF NOT EXISTS telegram;

-- Registered bot users
CREATE TABLE telegram.users (
    user_id       BIGINT PRIMARY KEY,       -- Telegram user ID
    username      TEXT,
    first_name    TEXT,
    language_code TEXT DEFAULT 'en',
    registered_at TIMESTAMPTZ DEFAULT now(),
    is_active     BOOLEAN DEFAULT true
);

-- Topic mappings (bot creates topics, engine needs thread IDs for routing)
CREATE TABLE telegram.user_topics (
    user_id       BIGINT REFERENCES telegram.users(user_id),
    category      TEXT NOT NULL,             -- housing, safety, civic, etc.
    topic_id      INTEGER NOT NULL,          -- message_thread_id in DM
    created_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, category)
);

-- Group topics (per-ZIP community forums)
CREATE TABLE telegram.group_topics (
    topic_id      INTEGER PRIMARY KEY,       -- message_thread_id
    group_chat_id BIGINT NOT NULL,           -- supergroup chat ID
    zip_code      TEXT NOT NULL,
    topic_name    TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- Topic routing view (engine queries this to get thread IDs for sendMessage)
CREATE VIEW telegram.topic_routing AS
SELECT s.user_id, s.sub_type, s.filter_value,
       ut.topic_id AS dm_topic_id,
       gt.topic_id AS group_topic_id
FROM lake._subscriptions s
LEFT JOIN telegram.user_topics ut
  ON ut.user_id = s.user_id AND ut.category = (
    CASE WHEN s.sub_type IN ('address_watch') THEN 'housing'
         WHEN s.sub_type = 'zip_digest' THEN 'neighborhood'
         ELSE 'general' END
  )
LEFT JOIN telegram.group_topics gt
  ON gt.zip_code = s.filter_value AND s.sub_type = 'zip_digest';

CREATE INDEX idx_user_topics ON telegram.user_topics(user_id);
CREATE INDEX idx_group_topics_zip ON telegram.group_topics(zip_code);
```

---

## 7. LLM Subscription Parser

### 7.1 Design

The LLM receives the user's natural-language text plus a schema summary (table names, columns, sample values) and returns a structured subscription filter.

```python
SYSTEM_PROMPT = """You are Common Ground's subscription parser.
Given a user's natural-language alert request, return a JSON object matching
the lake._subscriptions schema:
{
  "sub_type": "address_watch" | "zip_digest" | "entity_watch" | "table_watch",
  "filter_value": "BBL or ZIP or entity name or table name",
  "frequency": "realtime" | "daily" | "weekly",
  "description": "human-readable label for the subscription"
}

Available schemas and tables:
{schema_summary}

Rules:
- Match the user's intent to the closest table
- Use column names exactly as they appear in the schema
- ZIP codes are always 5-digit strings
- Borough names: MANHATTAN, BROOKLYN, BRONX, QUEENS, STATEN ISLAND
- If ambiguous, return multiple candidate subscriptions and ask user to pick
"""
```

### 7.2 Schema Summary Generation

Pre-compute a compressed schema summary from the DuckLake catalog:

```python
async def build_schema_summary(mcp_client: MCPClientService) -> str:
    """Build a compressed schema summary for the LLM context."""
    result = await mcp_client.call("query", sql="""
        SELECT table_schema, table_name,
               string_agg(column_name, ', ' ORDER BY ordinal_position) as columns
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'ducklake')
        GROUP BY table_schema, table_name
        ORDER BY table_schema, table_name
    """)
    return result  # ~4KB compressed, fits easily in context
```

### 7.3 Streaming Response with sendMessageDraft

For `/ask` queries (free-form data questions), stream the LLM response:

```python
@query_router.message(Command("ask"))
async def handle_ask(message: Message, bot: Bot):
    question = message.text.removeprefix("/ask").strip()
    if not question:
        await message.reply("What would you like to know? e.g. /ask worst landlords in 11201")
        return

    draft_id = message.message_id  # unique per conversation

    async for chunk in llm_service.stream(question):
        await bot.send_message_draft(
            chat_id=message.chat.id,
            draft_id=draft_id,
            text=chunk,
            message_thread_id=message.message_thread_id,
        )

    # Final message replaces the draft
    await message.reply(llm_service.last_full_response)
```

---

## 8. Change Detection — Dagster Integration

> **Owner**: Pipeline engineer. The bot does NOT implement change detection.
> See section 3A for the full integration contract.

### 8.1 What the Engine Handles (not bot code)

- Dagster sensor detects new materializations via `_dlt_load_id` watermarks
- Matches changed rows against `lake._subscriptions`
- Constructs notification payloads in `lake._notification_queue`
- Fan-out asset renders HTML and calls Telegram `sendMessage` directly
- Respects frequency modes: realtime (max 10/user/hr), daily (8 AM ET), weekly (Mon 8 AM ET)

### 8.2 What the Bot Must Provide to the Engine

1. **Topic routing data** — The engine needs `message_thread_id` values to route messages to the correct DM topic or group topic. The bot exposes this via the `telegram.topic_routing` view (see section 6.2).

2. **Bot token** — The engine calls `sendMessage` directly, so it needs the `TELEGRAM_BOT_TOKEN` in its env. This is a shared secret in `.env.secrets`.

3. **Callback button schema** — The engine embeds inline keyboard buttons in messages. The callback data format must be agreed:
   - `mute:{subscription_id}:{hours}` — e.g. `mute:sub-uuid:24`
   - `unsub:{subscription_id}` — e.g. `unsub:sub-uuid`
   - `view:{link_type}:{id}` — e.g. `view:building:3037230001`

### 8.3 Bot Callback Handler

```python
@subscription_router.callback_query(F.data.startswith("mute:"))
async def handle_mute(callback: CallbackQuery):
    _, sub_id, hours = callback.data.split(":")
    await mcp_client.call("query", sql=f"""
        UPDATE lake._subscriptions
        SET muted_until = now() + INTERVAL '{int(hours)} hours'
        WHERE id = '{sub_id}' AND user_id = {callback.from_user.id}
    """)
    await callback.answer(f"Muted for {hours}h")
    await callback.message.edit_reply_markup(reply_markup=None)

@subscription_router.callback_query(F.data.startswith("unsub:"))
async def handle_unsub(callback: CallbackQuery):
    _, sub_id = callback.data.split(":")
    await mcp_client.call("query", sql=f"""
        UPDATE lake._subscriptions SET is_active = false
        WHERE id = '{sub_id}' AND user_id = {callback.from_user.id}
    """)
    await callback.answer("Unsubscribed")
    await callback.message.edit_text(
        callback.message.text + "\n\n<i>Subscription removed.</i>",
        parse_mode="HTML",
    )
```

---

## 9. Forum Topics — Group + DM Organization

### 9.1 Community Group (Per-ZIP Topics)

One supergroup for Common Ground NYC with forum topics enabled.

```
Common Ground NYC (supergroup, forum mode)
├── General           — announcements, onboarding
├── 10001 — Midtown   — alerts for ZIP 10001
├── 10002 — Kips Bay  — alerts for ZIP 10002
├── 11201 — Downtown BK — alerts for ZIP 11201
├── ...
└── Meta              — bot commands, feedback
```

Topics are created on-demand when the first subscription targets a new ZIP:

```python
class TopicManagerService:
    async def ensure_group_topic(self, zip_code: str) -> int:
        """Get or create a forum topic for a ZIP code."""
        existing = await self.db.fetchrow(
            "SELECT topic_id FROM telegram.group_topics WHERE zip_code = $1",
            zip_code
        )
        if existing:
            return existing["topic_id"]

        neighborhood = await self.mcp.call("neighborhood", zip=zip_code)
        topic_name = f"{zip_code} — {neighborhood['name']}"

        result = await self.bot.create_forum_topic(
            chat_id=self.group_chat_id,
            name=topic_name,
            icon_color=0x6FB9F0,  # blue
        )

        await self.db.execute(
            """INSERT INTO telegram.group_topics (topic_id, group_chat_id, zip_code, topic_name)
               VALUES ($1, $2, $3, $4)""",
            result.message_thread_id, self.group_chat_id, zip_code, topic_name
        )

        return result.message_thread_id
```

### 9.2 DM Topics (Personal Alert Categories)

Bot API 9.3+ allows bots to create forum topics in private chats. Each user gets topics by category:

```
User's DM with @CommonGroundNYCBot (forum topics enabled)
├── Housing       — HPD violations, DOB permits, evictions
├── Safety        — crimes, crashes, shootings
├── Civic         — contracts, permits, budget
├── Schools       — DOE updates, scores
└── My Searches   — saved /ask queries
```

```python
CATEGORY_MAP = {
    "housing": ["housing.*"],
    "safety": ["public_safety.*"],
    "civic": ["city_government.*"],
    "education": ["education.*"],
    "health": ["health.*"],
    "environment": ["environment.*"],
    "transportation": ["transportation.*"],
}

async def ensure_dm_topic(self, user_id: int, category: str) -> int:
    """Get or create a DM topic for a user's alert category."""
    # Check if user already has this topic
    existing = await self.db.fetchrow(
        """SELECT dm_topic_id FROM telegram.subscriptions
           WHERE user_id = $1 AND dm_topic_id IS NOT NULL
           LIMIT 1""",  # simplified — real impl groups by category
        user_id
    )
    if existing:
        return existing["dm_topic_id"]

    result = await self.bot.create_forum_topic(
        chat_id=user_id,
        name=category.title(),
    )
    return result.message_thread_id
```

---

## 10. Mini App Integration

### 10.1 Launch Flow

```
Entry points:
  1. Menu button → opens default dashboard
  2. Deep link: t.me/CommonGroundNYCBot?startapp=zip_11201
  3. Inline button in notification → opens detail view
  4. /explore command → opens with user's subscribed ZIPs
```

### 10.2 Authentication

```python
# bot/webapp/auth.py
import hashlib, hmac, json
from urllib.parse import parse_qs

async def verify_webapp_data(init_data: str, bot_token: str) -> dict | None:
    """Verify Telegram WebApp initData using HMAC-SHA256."""
    parsed = parse_qs(init_data)
    received_hash = parsed.pop("hash", [None])[0]
    if not received_hash:
        return None

    # Sort and join remaining params
    data_check_string = "\n".join(
        f"{k}={v[0]}" for k, v in sorted(parsed.items())
    )

    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(received_hash, calculated_hash):
        return None

    user_data = json.loads(parsed.get("user", ["{}"])[0])
    return user_data
```

### 10.3 SQLRooms Dashboard

The Mini App embeds SQLRooms — the same dashboard planned for the CG website. It connects to FastMCP for data queries.

```
Mini App Architecture:
  ┌─────────────────────────────────┐
  │  Telegram WebView (iframe)      │
  │                                 │
  │  ┌───────────────────────────┐  │
  │  │  SQLRooms React App       │  │
  │  │                           │  │
  │  │  - Neighborhood overview  │  │
  │  │  - Building detail        │  │
  │  │  - Entity search          │  │
  │  │  - Custom SQL explorer    │  │
  │  │                           │  │
  │  │  API: /webapp/api/query   │  │
  │  │  Auth: JWT from initData  │  │
  │  └───────────────────────────┘  │
  └─────────────────────────────────┘
         │
         ▼
  aiohttp proxy → FastMCP (:4213)
```

Deep link parameter mapping:

| Start Param | Dashboard View |
|---|---|
| `zip_11201` | Neighborhood overview for 11201 |
| `bbl_3002920001` | Building detail for BBL |
| `entity_john_smith` | Entity search for name |
| (none) | Home / search page |

---

## 11. Notification Message Rendering

> The Dagster fan-out asset renders and sends these messages. The bot does NOT send notifications.
> This section documents the **agreed rendering format** so both sides stay consistent.

### 11.1 Single Alert (realtime mode, from payload)

Given the notification payload from section 3A:

```html
<b>New HPD violation at 305 LINDEN BLVD</b>

Class C violation: Roach infestation in apartment 3A

<i>HPD Violations — 1 new record</i>

[View Building]  [Mute 24h]  [Unsubscribe]
```

Inline keyboard (rendered by engine):
```python
InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="View Building", url=payload["links"]["building"]),
        InlineKeyboardButton(text="Mute 24h", callback_data=f"mute:{sub_id}:24"),
        InlineKeyboardButton(text="Unsubscribe", callback_data=f"unsub:{sub_id}"),
    ]
])
```

### 11.2 Batch Alert (batch_count > 1)

```html
<b>3 new violations since last check</b>

305 LINDEN BLVD, Brooklyn — HPD Violations

<i>3 new records across 1 address</i>

[View Building]  [Mute 24h]  [Unsubscribe]
```

When `batch_count > 1`, the engine uses `batch_summary` as the header instead of `title`.

### 11.3 Daily/Weekly Digest

For `daily` and `weekly` frequency, the engine batches all pending notifications per user into a single message. Format TBD — will be defined during implementation.

### 11.4 Rate Limiting (engine-side)

| Constraint | Value | Enforced By |
|---|---|---|
| Telegram global limit | 30 msg/sec per bot token | Engine rate limiter |
| Per-DM chat | 1 msg/sec | Engine rate limiter |
| Per-group | 20 msg/min | Engine rate limiter |
| Realtime cap | 10 notifications/user/hour | Engine frequency filter |
| `TelegramRetryAfter` | Backoff per `retry_after` | Engine retry logic |

---

## 12. Project Structure

```
infra/telegram-bot/
├── Dockerfile
├── pyproject.toml              # aiogram 3.26+, anthropic, asyncpg, aiohttp
├── bot/
│   ├── __init__.py
│   ├── main.py                 # Entry point: webhook setup, aiohttp app
│   ├── config.py               # Settings from env vars
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── auth.py             # /start, registration
│   │   ├── subscriptions.py    # /sub, /unsub, /my, /edit
│   │   ├── queries.py          # /ask (LLM streaming)
│   │   ├── admin.py            # /stats, /broadcast
│   │   └── group.py            # Group message handling
│   ├── middlewares/
│   │   ├── __init__.py
│   │   ├── rate_limit.py
│   │   ├── auth.py
│   │   ├── logging.py
│   │   └── error_capture.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── subscription.py     # CRUD for subscriptions
│   │   ├── llm_parser.py       # NL → structured filter
│   │   ├── notification.py     # Fan-out with rate limiting
│   │   ├── topic_manager.py    # Group + DM topic lifecycle
│   │   └── mcp_client.py       # FastMCP tool caller
│   ├── webapp/
│   │   ├── auth.py             # initData verification
│   │   ├── routes.py           # /webapp/api/* proxy to MCP
│   │   └── static/             # SQLRooms build output
│   ├── db/
│   │   ├── __init__.py
│   │   ├── pool.py             # asyncpg connection pool
│   │   └── migrations/
│   │       └── 001_telegram_schema.sql
│   └── fsm/
│       ├── __init__.py
│       └── subscription_flow.py  # FSM states for sub creation
└── tests/
    ├── test_llm_parser.py
    ├── test_topic_manager.py
    ├── test_callback_handlers.py
    └── test_webapp_auth.py
```

---

## 13. Deployment

### 13.1 Docker Compose Addition

```yaml
# Added to existing docker-compose.yml
  cg-telegram-bot:
    build: ./infra/telegram-bot
    container_name: cg-telegram-bot
    env_file:
      - .env.secrets
    environment:
      - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
      - TELEGRAM_WEBHOOK_URL=https://bot.common-ground.nyc/webhook
      - TELEGRAM_GROUP_CHAT_ID=${TELEGRAM_GROUP_CHAT_ID}
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
      - DATABASE_URL=postgresql://dagster:${DAGSTER_PG_PASSWORD}@postgres:5432/ducklake
      - MCP_URL=http://duckdb-mcp:4213
    ports:
      - "8443:8443"
    networks:
      - backend
      - tunnel_net
      - shared
    deploy:
      resources:
        limits:
          memory: 1g
          cpus: '1.0'
    depends_on:
      - postgres
    restart: unless-stopped
```

### 13.2 Cloudflare Tunnel

Add route: `bot.common-ground.nyc → localhost:8443`

Two paths:
- `/webhook` → aiogram webhook handler
- `/webapp/*` → Mini App static + API

### 13.3 Webhook Setup

```python
# bot/main.py
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

async def on_startup(bot: Bot):
    await bot.set_webhook(
        url=f"{config.WEBHOOK_URL}/webhook",
        allowed_updates=["message", "callback_query", "my_chat_member"],
        drop_pending_updates=True,
    )
    # Set menu button for Mini App
    await bot.set_chat_menu_button(
        menu_button=MenuButtonWebApp(
            text="Explore Data",
            web_app=WebAppInfo(url=f"{config.WEBHOOK_URL}/webapp/"),
        )
    )

def create_app() -> web.Application:
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()

    # Register routers
    dp.include_routers(auth_router, subscription_router, query_router, admin_router, group_router)

    # Register middlewares
    dp.update.outer_middleware(RateLimitMiddleware())
    dp.update.outer_middleware(AuthMiddleware())

    dp.startup.register(on_startup)

    app = web.Application()

    # Webhook handler
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path="/webhook")

    # Mini App routes
    app.router.add_post("/webapp/auth", webapp_auth_handler)
    app.router.add_post("/webapp/api/query", webapp_query_proxy)
    app.router.add_static("/webapp/", path="bot/webapp/static/")

    # No internal notification endpoint — engine pushes directly to Telegram API
    setup_application(app, dp, bot=bot)
    return app

if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=8443)
```

---

## 14. Telegram Bot API 9.5 Features Used

| Feature | Bot API Version | Usage |
|---|---|---|
| `sendMessageDraft` | 9.3 (general: 9.5) | Stream LLM responses for `/ask` queries |
| Forum topics in private chats | 8.0+ | DM alert categories (Housing, Safety, etc.) |
| `createForumTopic` in DMs | 8.0+ | Auto-create user topic categories |
| Inline keyboards | 6.0+ | Subscription confirm/edit/cancel buttons |
| `MenuButtonWebApp` | 6.1+ | Mini App launch from bot menu |
| Deep links with `startapp` | 6.1+ | Direct Mini App views (ZIP, BBL, entity) |
| `message_thread_id` | 8.0+ | Route messages to specific forum topics |

---

## 15. Open Questions & Dependencies

### Questions for Team Lead

1. **SQLRooms build** — Is the SQLRooms dashboard being developed as a separate agent's task? The bot needs the static build output to serve via Mini App.

2. **Anthropic API key** — Is there an existing key in `.env.secrets`, or does a new one need to be provisioned for the bot?

3. **Bot token** — Has `@CommonGroundNYCBot` been registered with @BotFather? Need the token + forum topics enabled in bot settings.

4. **Community group** — Should we create the supergroup now, or wait until bot is functional? Need a supergroup with forum topics enabled.

### Resolved (from pipeline engineer)

- ~~Dagster sensor~~ — Engine handles change detection + notification delivery. Bot is passive.
- ~~Notification endpoint~~ — No internal HTTP endpoint needed. Engine calls Telegram API directly.
- ~~Subscription storage~~ — Uses `lake._subscriptions`, not a separate Postgres table.

### New Integration Questions (for pipeline engineer)

1. **Topic routing** — How does the engine get `message_thread_id` for each user's DM topic? Options: (a) bot writes topic IDs back to `_subscriptions`, (b) engine queries `telegram.topic_routing` view in Postgres, (c) bot exposes a lightweight HTTP endpoint for topic lookups.

2. **Callback data format** — Need to agree on callback_data strings for inline buttons. Proposed: `mute:{sub_id}:{hours}`, `unsub:{sub_id}`, `view:{type}:{id}`.

3. **Bot token sharing** — Engine needs `TELEGRAM_BOT_TOKEN` to call sendMessage. Add to engine's env alongside bot's env in `.env.secrets`.

### Dependencies on Other Agents

| Dependency | Agent | What We Need |
|---|---|---|
| SQLRooms dashboard build | Website/Dashboard agent | Static HTML/JS/CSS bundle for Mini App |
| MCP server stability | MCP server (existing) | Reliable tool calls for data queries + subscription writes |
| Notification payload format | Pipeline engineer | **RESOLVED** — payload spec received |
| Cloudflare Tunnel route | Infra/DevOps | `bot.common-ground.nyc` route |

### Implementation Priority

| Phase | Scope | Estimate |
|---|---|---|
| **P0** | Bot skeleton: /start, /sub, /my, webhook, Postgres schema | Foundation |
| **P1** | LLM parser + subscription CRUD + DM topics | Core subscription flow |
| **P2** | Dagster sensor + notification fan-out | Change detection |
| **P3** | Group forum topics (per-ZIP) + digests | Community features |
| **P4** | Mini App + SQLRooms integration | Dashboard |
| **P5** | sendMessageDraft streaming for /ask | LLM streaming |

---

## 16. Security Considerations

1. **Webhook verification** — Only accept requests from Telegram IPs (aiogram's built-in IP filter middleware)
2. **WebApp auth** — HMAC-SHA256 verification of `initData` before any API access
3. **Shared bot token** — Engine and bot both use `TELEGRAM_BOT_TOKEN`; stored in `.env.secrets`, never in code
4. **No secrets in messages** — Never include API keys, DB credentials, or internal URLs in bot responses
5. **Rate limiting** — Per-user rate limits prevent abuse; global limits prevent Telegram API bans
6. **SQL injection** — All MCP queries use parameterized filters; no raw SQL from user input
7. **Input validation** — Validate all user input (ZIP codes, addresses) before passing to LLM or DB
