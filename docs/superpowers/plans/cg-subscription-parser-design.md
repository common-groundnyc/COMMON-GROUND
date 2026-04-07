# CG Telegram Bot — Subscription Parser Design

> NLP/LLM system for parsing natural language into structured data subscriptions.
> Designed for Common Ground NYC open data platform (334 tables, 12 schemas).
>
> **Aligned with notification engine schema** (from pipeline-engineer).

---

## 1. Architecture Overview

```
User text message
       │
       ▼
┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Telegram     │────>│  Parser          │────>│  Subscription   │
│  Handler      │     │  (2 stages)      │     │  Store          │
│  (aiogram)    │<────│                  │     │  (Postgres)     │
└──────────────┘     │  1. Haiku 4.5    │     └─────────────────┘
       │              │     (intent +    │
       ▼              │      slots)      │
  Confirmation        │  2. MCP resolve  │
  card + inline       │     (BBL, name)  │
  keyboard            └──────────────────┘
```

**Two-stage parse**: Haiku extracts intent + slots, then MCP tools resolve addresses/entities.

**Model choice**: Claude Haiku 4.5 (`claude-haiku-4-5-20251001`)
- $1/M input, $5/M output tokens
- Strict tool use for guaranteed schema conformance
- Prompt caching drops repeated system prompt cost to $0.10/M (90% savings)

---

## 2. Subscription Data Model (Engine-Aligned)

### 2.1 LLM Output Format

The LLM must produce this exact structure (matches notification engine):

```json
{
  "sub_type": "address_watch",
  "filter_value": "3037230001",
  "filter_extra": {"address": "305 LINDEN BLVD, Brooklyn"},
  "channels": ["telegram"],
  "frequency": "daily"
}
```

### 2.2 Subscription Types

| Type | `filter_value` | `filter_extra` | Example User Input |
|------|---------------|----------------|-------------------|
| `address_watch` | BBL (10-digit) or address | `{"address": "305 Linden Blvd"}` | "Watch 305 Linden Blvd for new violations" |
| `entity_watch` | Person or company name | `{"fuzzy": true, "role": "landlord"}` | "Alert me about John Smith" or "Track ABC Realty LLC" |
| `zip_category` | ZIP code (5-digit) | `{"tables": ["hpd_violations", "311"]}` | "Notify me about housing complaints in 11226" |
| `table_watch` | Table key (schema.table) | `{}` | "Tell me when new restaurant inspection data comes in" |
| `keyword_watch` | Search term | `{"tables": ["hpd_violations"]}` | "Alert me about lead paint violations" |
| `data_watch` | SQL WHERE fragment | `{"table": "housing.hpd_violations"}` | "Notify me about Class C violations in 11226 with more than 5 units" |

### 2.3 Watchable Tables (Primary)

| Domain | Tables |
|--------|--------|
| Housing | `hpd_violations`, `hpd_complaints`, `dob_ecb_violations`, `hpd_litigation`, `dob_permits` |
| Safety | `nypd_complaints_historic`, `nypd_arrests_historic`, `nypd_shooting_incidents` |
| Health | `restaurant_inspections` |
| 311 | `n311_service_requests` |
| Business | `nys_corporations` (new filings) |
| City Gov | `pluto` (property changes) |

Full list: 294 tables across 10 schemas in `sources/datasets.py`.

### 2.4 Category-to-Table Mapping

Used by the parser to translate user-friendly terms into table lists:

```python
CATEGORY_TABLES: dict[str, list[str]] = {
    # Housing
    "violations":           ["hpd_violations", "dob_ecb_violations"],
    "complaints":           ["hpd_complaints", "n311_service_requests"],
    "housing complaints":   ["hpd_complaints", "hpd_violations"],
    "permits":              ["dob_permits"],
    "litigation":           ["hpd_litigation"],

    # Safety
    "crime":                ["nypd_complaints_historic"],
    "arrests":              ["nypd_arrests_historic"],
    "shootings":            ["nypd_shooting_incidents"],

    # Health
    "restaurant grades":    ["restaurant_inspections"],
    "restaurants":          ["restaurant_inspections"],

    # 311
    "311":                  ["n311_service_requests"],
    "noise":                ["n311_service_requests"],
    "sanitation":           ["n311_service_requests"],

    # Business / Corp
    "corporate filings":    ["nys_corporations"],
    "new businesses":       ["nys_corporations"],

    # City Gov
    "property changes":     ["pluto"],
}
```

---

## 3. Two-Stage Parser Design

### 3.1 Stage 1: LLM Intent + Slot Extraction

Haiku extracts the **type** and **raw slots** (unresolved address, raw name, etc.).
It does NOT resolve addresses to BBLs — that happens in stage 2.

**Approach**: Strict tool use (`strict: true`). The LLM gets a `parse_subscription` tool.
If it can parse, it calls the tool. If ambiguous, it responds with a clarifying question.

### 3.2 System Prompt (~1,400 tokens)

```
You are the Common Ground NYC subscription assistant. Users describe what NYC
open data they want to monitor. Parse their request into a structured
subscription by calling the parse_subscription tool.

RULES:
1. Always call parse_subscription when you can extract enough information.
2. If the request is ambiguous, respond with a clarifying question — do NOT
   guess. Examples of ambiguity: no location, unclear what data type.
3. Normalize addresses: expand abbreviations (Ave→Avenue, St→Street, Blvd→Boulevard).
4. Normalize boroughs: BK→BROOKLYN, BX→BRONX, QN→QUEENS, SI→STATEN ISLAND, MN→MANHATTAN.
5. For entity names, preserve exact user spelling.
6. Default frequency is "daily" unless user says "weekly" or "immediately"/"real-time" (→ "realtime").
7. This is NYC-only data. If the user asks about another city, say so.
8. For keyword_watch, extract the key search term (e.g., "lead paint", "elevator", "gas leak").
9. For data_watch, construct a valid SQL WHERE fragment. Use standard column names.

SUBSCRIPTION TYPES:
- address_watch: User wants to monitor a specific NYC address or building.
  filter_value = the address string (will be resolved to BBL later)
  filter_extra = {"address": "<normalized address, borough>"}

- entity_watch: User wants to monitor a person or company.
  filter_value = the name
  filter_extra = {"fuzzy": true/false, "role": "landlord"|"developer"|"politician"|null}

- zip_category: User wants to monitor a ZIP code for a category of data.
  filter_value = 5-digit ZIP code
  filter_extra = {"tables": [<table names from the category>]}

- table_watch: User wants to know when new data appears in a specific table.
  filter_value = schema.table_name
  filter_extra = {}

- keyword_watch: User wants alerts when a keyword appears in new records.
  filter_value = the keyword/phrase
  filter_extra = {"tables": [<relevant tables>]}

- data_watch: User wants alerts matching a specific condition (complex filter).
  filter_value = SQL WHERE fragment (e.g., "class = 'C' AND zip = '11226'")
  filter_extra = {"table": "<schema.table>"}

CATEGORY → TABLE MAPPING:
  violations → hpd_violations, dob_ecb_violations
  complaints → hpd_complaints, n311_service_requests
  housing complaints → hpd_complaints, hpd_violations
  permits → dob_permits
  crime → nypd_complaints_historic
  arrests → nypd_arrests_historic
  shootings → nypd_shooting_incidents
  restaurant grades → restaurant_inspections
  311 → n311_service_requests
  corporate filings → nys_corporations
  property changes → pluto
  litigation → hpd_litigation
```

### 3.3 Tool Definition

```python
PARSE_TOOL = {
    "name": "parse_subscription",
    "description": "Parse a natural language subscription request into structured data",
    "strict": True,
    "input_schema": {
        "type": "object",
        "properties": {
            "sub_type": {
                "type": "string",
                "enum": ["address_watch", "entity_watch", "zip_category",
                         "table_watch", "keyword_watch", "data_watch"],
            },
            "filter_value": {
                "type": "string",
                "description": "Primary filter: address, name, ZIP, table key, keyword, or SQL WHERE"
            },
            "filter_extra": {
                "type": "object",
                "description": "Type-specific extra params",
                "additionalProperties": True
            },
            "frequency": {
                "type": "string",
                "enum": ["daily", "weekly", "realtime"],
            },
            "description": {
                "type": "string",
                "description": "Human-readable summary for the confirmation card"
            }
        },
        "required": ["sub_type", "filter_value", "filter_extra", "frequency", "description"]
    }
}
```

### 3.4 Stage 2: MCP Resolution

After Haiku returns the parsed subscription, we resolve ambiguous values using MCP tools:

```python
async def resolve_subscription(parsed: dict) -> dict:
    """Resolve raw LLM output into engine-ready subscription using MCP tools."""
    sub_type = parsed["sub_type"]
    result = {**parsed, "channels": ["telegram"]}

    match sub_type:
        case "address_watch":
            # Resolve address → BBL using building() MCP tool
            address = parsed["filter_value"]
            bbl = await mcp_building_resolve(address)
            if bbl:
                result["filter_value"] = bbl  # 10-digit BBL
                result["filter_extra"]["address"] = address  # keep original
                result["filter_extra"]["bbl_resolved"] = True
            else:
                result["filter_extra"]["bbl_resolved"] = False
                # Keep address as filter_value — engine can still match by address

        case "entity_watch":
            # Verify entity exists using entity() MCP tool
            name = parsed["filter_value"]
            matches = await mcp_entity_verify(name)
            if matches == 0:
                result["filter_extra"]["verified"] = False
                result["filter_extra"]["warning"] = "No exact matches found — will use fuzzy matching"
                result["filter_extra"]["fuzzy"] = True
            else:
                result["filter_extra"]["verified"] = True
                result["filter_extra"]["match_count"] = matches

        case "zip_category":
            # Validate ZIP is a real NYC ZIP
            zip_code = parsed["filter_value"]
            if not is_nyc_zip(zip_code):
                raise ValueError(f"ZIP {zip_code} is not in NYC")

    # Strip 'description' from engine payload (used only for confirmation card)
    result.pop("description", None)
    return result
```

### 3.5 MCP Tool Integration

```python
async def mcp_building_resolve(address: str) -> str | None:
    """Call building() MCP tool to resolve address → BBL."""
    try:
        result = await mcp_client.call_tool("building", {"address": address})
        # Extract BBL from building profile result
        if result and "bbl" in result:
            return result["bbl"]
    except Exception:
        pass
    return None

async def mcp_entity_verify(name: str) -> int:
    """Call entity() MCP tool to check how many matches exist."""
    try:
        result = await mcp_client.call_tool("entity", {"name": name})
        return result.get("match_count", 0)
    except Exception:
        return 0
```

---

## 4. Multi-Turn Handling

| Scenario | Approach |
|----------|----------|
| **Ambiguous input** | LLM responds with text question instead of calling tool. Bot shows question. |
| **Modification** ("make it weekly") | Include current subscription JSON in user message. LLM returns updated params. |
| **Multi-intent** ("watch building AND track owner") | LLM calls tool twice (parallel tool use). Bot creates 2 subs, 2 confirmation cards. |
| **Deletion** ("stop watching 305 Linden") | Button-driven from `/mysubs`. No LLM needed. |
| **Refinement** ("also add DOB permits") | Include current sub in context. LLM returns updated `filter_extra.tables`. |

### Conversation Memory

Stateless per parse. No conversation history stored. Modifications include the existing subscription as context in the user message:

```
Current subscription: {"sub_type": "address_watch", "filter_value": "3037230001", ...}

User says: make it weekly and also track permits
```

---

## 5. UX Flow

### 5.1 Happy Path

```
User: "Watch my building at 305 Linden Blvd Brooklyn for violations"
                          │
                          ▼
              ┌─── Stage 1: Haiku ────┐
              │ sub_type: address_watch│
              │ filter_value:          │
              │   "305 LINDEN BLVD,   │
              │    BROOKLYN"           │
              │ frequency: daily       │
              └───────────────────────┘
                          │
                          ▼
              ┌─── Stage 2: MCP ──────┐
              │ building("305 LINDEN  │
              │   BLVD, BROOKLYN")    │
              │ → BBL: 3014200001     │
              └───────────────────────┘
                          │
                          ▼
         ┌─────────────────────────────────┐
         │ New Subscription                │
         │                                 │
         │ HPD + DOB violations at         │
         │ 305 Linden Blvd, Brooklyn       │
         │ (BBL: 3014200001)               │
         │                                 │
         │ Frequency: Daily                │
         │ Preview: 12 alerts in last 30d  │
         │                                 │
         │ [Confirm]  [Edit]  [Cancel]     │
         └─────────────────────────────────┘
```

### 5.2 Preview: "Last 30 Days" Alert Count

Run the subscription's query against the DuckDB lake for the last 30 days:

```python
async def compute_preview(sub: dict) -> int:
    """Count how many alerts this sub would have triggered in the last 30 days."""
    match sub["sub_type"]:
        case "address_watch":
            bbl = sub["filter_value"]
            counts = await parallel_queries([
                ("SELECT COUNT(*) FROM lake.housing.hpd_violations WHERE bbl = ? AND inspection_date >= CURRENT_DATE - 30", [bbl]),
                ("SELECT COUNT(*) FROM lake.housing.dob_ecb_violations WHERE bbl = ? AND issue_date >= CURRENT_DATE - 30", [bbl]),
            ])
            return sum(r[0][0] for r in counts)
        case "zip_category":
            zip_code = sub["filter_value"]
            tables = sub["filter_extra"].get("tables", [])
            # build queries per table with date filters
            ...
        case "keyword_watch":
            keyword = sub["filter_value"]
            tables = sub["filter_extra"].get("tables", [])
            # ILIKE search per table
            ...
```

Uses the same DuckDB connection pool as the MCP server. Parallel queries, ~200ms.

### 5.3 Guided Fallback

When Haiku can't parse (returns text instead of tool call):

```
User: "tell me about stuff"
Bot:  "I can set up a data alert for you. What would you like to monitor?"

     [Building/Address]  [Person/Company]
     [Neighborhood/ZIP]  [Browse Data Types]
```

Each button starts a deterministic slot-filling state machine (no LLM):
- Building → "What's the address?" → "What data? (violations/complaints/permits)" → confirm
- Person → "Name?" → "Role? (landlord/developer/any)" → confirm
- ZIP → "ZIP code?" → "Category? (crime/health/housing/311)" → confirm

### 5.4 `/mysubs` Portfolio View

```
Your Subscriptions (3 active)

1. [building] 305 Linden Blvd — violations
   Daily | 12 alerts this month
   [Pause] [Edit] [Delete]

2. [person] ABC Realty LLC — all activity
   Daily | 3 alerts this month
   [Pause] [Edit] [Delete]

3. [zip] 11201 — restaurant grades
   Weekly | 8 alerts this month
   [Pause] [Edit] [Delete]
```

---

## 6. Cost Analysis

### 6.1 Tokens Per Parse

| Component | Tokens |
|-----------|--------|
| System prompt (cached after first call) | ~1,400 |
| Tool definition (cached) | ~350 |
| User message | ~50 |
| **Total input** | **~1,800** |
| LLM output (tool call JSON) | ~150 |
| **Total output** | **~150** |

With prompt caching (system prompt + tool def):
- Cached input: 1,750 tokens @ $0.10/M = $0.000175
- Fresh input: 50 tokens @ $1.00/M = $0.00005
- Output: 150 tokens @ $5.00/M = $0.00075
- **Total per parse: ~$0.001**

### 6.2 Cost at Scale

| Users | Parses/month (est. 5/user) | LLM Cost | MCP Queries |
|-------|---------------------------|----------|-------------|
| 100   | 500                       | $0.50    | ~200 (address/entity resolves) |
| 1,000 | 5,000                     | $4.75    | ~2,000 |
| 10,000| 50,000                    | $47.50   | ~20,000 |

MCP queries are free (our own DuckDB server). The only external cost is Anthropic API.

### 6.3 Cost Optimization

1. **Prompt caching**: System prompt + tool def identical across requests. 90% input reduction.

2. **Pre-filter regex**: Skip LLM for simple patterns (~30-40% of requests):
   ```
   "watch <address> for <category>" → address_watch
   "track <name>"                   → entity_watch
   "alerts for <zip>"               → zip_category
   ```

3. **No LLM for button actions**: Pause/delete/frequency-toggle via inline keyboard, direct DB update.

4. **Batch API**: For scheduled digest notifications, use Anthropic's batch API (50% discount).

### 6.4 Latency Budget

| Step | Time |
|------|------|
| Haiku parse (TTFT + generation) | ~500-900ms |
| MCP resolve (building/entity) | ~200-500ms |
| Preview query (DuckDB) | ~200ms |
| **Total** | **~900-1,600ms** |

Acceptable for a Telegram interaction. The "typing..." indicator covers the wait.

---

## 7. Implementation: Parse Function

```python
from dataclasses import dataclass
from typing import Literal

import anthropic

SubType = Literal["address_watch", "entity_watch", "zip_category",
                  "table_watch", "keyword_watch", "data_watch"]
Frequency = Literal["daily", "weekly", "realtime"]

@dataclass(frozen=True)
class ParsedSubscription:
    sub_type: SubType
    filter_value: str
    filter_extra: dict
    frequency: Frequency
    description: str  # human-readable, for confirmation card only


@dataclass(frozen=True)
class ClarifyingQuestion:
    text: str


client = anthropic.Anthropic()

SYSTEM_PROMPT = """..."""  # ~1,400 tokens, see section 3.2
PARSE_TOOL = {...}        # see section 3.3

async def parse_subscription_text(
    user_text: str,
    existing_sub: dict | None = None,
) -> ParsedSubscription | ClarifyingQuestion:
    """Parse user text into a subscription, or return a clarifying question.

    Stage 1 only — does NOT resolve BBL/entity. Call resolve_subscription() after.
    """
    if len(user_text) > 500:
        user_text = user_text[:500]

    content = user_text
    if existing_sub:
        content = f"Current subscription: {json.dumps(existing_sub)}\n\nUser says: {user_text}"

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        system=[{
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        tools=[PARSE_TOOL],
        messages=[{"role": "user", "content": content}],
    )

    for block in response.content:
        if block.type == "tool_use" and block.name == "parse_subscription":
            return ParsedSubscription(**block.input)

    text_blocks = [b.text for b in response.content if b.type == "text"]
    return ClarifyingQuestion(text=" ".join(text_blocks))
```

### Pre-Filter (Skip LLM)

```python
import re

_WATCH_ADDR = re.compile(
    r"(?:watch|monitor|track|alert)\s+(?:my\s+)?(?:building\s+)?(?:at\s+)?"
    r"(.+?(?:st|street|ave|avenue|blvd|boulevard|rd|road|pl|place|dr|drive)\b.*?)"
    r"(?:\s+for\s+(.+))?$",
    re.IGNORECASE,
)

_ZIP_PATTERN = re.compile(
    r"(?:alerts?|notify|track|watch)\s+.*?\b(\d{5})\b.*?(?:for\s+)?(.+)?$",
    re.IGNORECASE,
)

def try_regex_parse(text: str) -> ParsedSubscription | None:
    """Fast-path for obvious patterns. Returns None to fall through to LLM."""
    m = _WATCH_ADDR.match(text.strip())
    if m:
        addr = m.group(1).strip().rstrip(",")
        cats = m.group(2)
        tables = resolve_category_tables(cats) if cats else ["hpd_violations", "hpd_complaints"]
        return ParsedSubscription(
            sub_type="address_watch",
            filter_value=addr,
            filter_extra={"address": addr},
            frequency="daily",
            description=f"Watching {addr} for {cats or 'violations + complaints'}",
        )

    m = _ZIP_PATTERN.match(text.strip())
    if m:
        zip_code = m.group(1)
        cats = m.group(2)
        tables = resolve_category_tables(cats) if cats else ["hpd_complaints", "n311_service_requests"]
        return ParsedSubscription(
            sub_type="zip_category",
            filter_value=zip_code,
            filter_extra={"tables": tables},
            frequency="daily",
            description=f"Watching ZIP {zip_code} for {cats or 'complaints'}",
        )

    return None  # LLM needed
```

---

## 8. Edge Cases

| Edge Case | Handling |
|-----------|----------|
| Address not in PAD / no BBL | Keep address as `filter_value`, set `bbl_resolved: false`. Show warning in confirmation. |
| Entity matches too many | `entity()` returns match count. If > 50, ask user to narrow: add borough, LLC/Inc, role. |
| Non-NYC data request | Haiku responds with "Common Ground covers NYC only." |
| Multi-intent | "Watch building AND track owner" → 2 tool calls → 2 confirmation cards. |
| Frequency ambiguity | "immediately" / "real-time" / "ASAP" → `realtime`. "every week" → `weekly`. Default: `daily`. |
| Off-topic / profanity | Haiku deflects naturally. No special handling. |
| Very long message | Truncate to 500 chars before LLM. Log full message. |
| Anthropic rate limit | Queue + exponential backoff. Show "Processing..." typing indicator. |
| Duplicate subscription | Hash `sub_type + filter_value + filter_extra`. Warn if same user has matching active sub. |

---

## 9. Key Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Model | Haiku 4.5 | $0.001/parse, ~800ms, strict tool use |
| Parse approach | Strict tool use | Guaranteed schema, no retries |
| Resolution | 2-stage (LLM → MCP) | LLM extracts intent; MCP resolves BBL/entity. Clean separation. |
| Address → BBL | `building()` MCP tool | Same resolver the MCP server uses. Handles abbreviations, boroughs. |
| Entity verify | `entity()` MCP tool | Confirms name exists, gets match count for disambiguation. |
| Prompt caching | Yes | 90% input cost savings |
| Pre-filter regex | Yes (~30-40%) | Skip LLM for obvious patterns |
| Conversation state | Stateless per parse | Modifications include current sub in message context |
| Guided fallback | Inline keyboard FSM | No LLM cost for fallback |
| Frequency | daily / weekly / realtime | realtime = engine polls at highest cadence |
| Output format | Engine-native JSON | `sub_type` / `filter_value` / `filter_extra` / `channels` / `frequency` |
