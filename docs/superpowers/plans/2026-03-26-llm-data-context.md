# LLM Data Context & Analytics Suggestions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Help LLMs understand what's in the NYC data lake so they proactively suggest interesting analytics, investigations, and comparisons to keep users engaged.

**Architecture:** Three changes to the existing MCP server: (1) expand the `INSTRUCTIONS` string from a 400-char routing guide to a ~2K-char data guide with schema highlights, row counts, question suggestions, and analytics angles; (2) add a `suggest_explorations` tool that returns pre-computed interesting findings from the data; (3) add MCP prompts as user-invocable slash commands for common investigation workflows. Each task starts with Exa research to pull latest best practices.

**Tech Stack:** FastMCP 3.1.1 (instructions, tools, prompts), DuckDB, Python 3.12

---

## Research Mandate

Every task starts with targeted Exa web search before coding. The implementer MUST use `mcp__exa__web_search_exa` at the start of each task.

---

## What Changes

| Before | After |
|--------|-------|
| 400-char routing-only instructions | ~2K-char data guide with schema highlights + suggested questions |
| No way for LLM to know what's interesting | `suggest_explorations` tool returns pre-computed highlights |
| No user-invocable investigation templates | 3 MCP prompts as slash commands |
| Schema descriptions are terse one-liners | Rich descriptions with row counts, key tables, question angles |

---

## File Structure

### Modified files

| File | Change |
|------|--------|
| `infra/duckdb-server/mcp_server.py:2319-2342` | Replace `INSTRUCTIONS` with expanded data guide |
| `infra/duckdb-server/mcp_server.py:340-353` | Expand `SCHEMA_DESCRIPTIONS` with richer descriptions |
| `infra/duckdb-server/mcp_server.py` (new tool) | Add `suggest_explorations` tool |
| `infra/duckdb-server/mcp_server.py` (new prompts) | Add 3 MCP prompts |

All changes are in a single file — the MCP server is a monolith and all tools/prompts/instructions live there.

---

## Task 1: Expand INSTRUCTIONS for rich data context

**Priority:** Highest impact, simplest change. The `instructions` field is auto-injected into every LLM's system prompt on connect.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:2323-2342`

- [ ] **Step 1: Research — what makes good MCP server instructions**

Search with Exa:
1. `motherduck mcp_server_instructions.md "instructions" 2026` — how MotherDuck structures their instructions
2. `"mcp server" instructions best practices length 2026` — recommended length and structure
3. `fastmcp instructions "InitializeResult" truncation limit` — is there a size limit?

Key constraint: keep under 2,500 chars. Some clients may truncate long instructions. MotherDuck's is ~2K.

- [ ] **Step 2: Expand `SCHEMA_DESCRIPTIONS` with richer context**

Replace the existing `SCHEMA_DESCRIPTIONS` dict at line 340:

```python
SCHEMA_DESCRIPTIONS = {
    "business": "BLS employment stats, ACS census demographics, business licenses, M/WBE certs. 15 tables. Ask: which industries are growing? where are minority-owned businesses concentrated?",
    "city_government": "PLUTO lot data (every parcel in NYC), city payroll, OATH hearings, zoning, facilities. 25 tables, 3M+ rows. Ask: who owns this lot? what's the zoning? how much do city employees earn?",
    "economics": "FRED metro economic indicators, IRS income by ZIP. 5 tables. Ask: how does income vary by neighborhood? what's the median household income in this ZIP?",
    "education": "DOE school surveys, test scores, enrollment, NYSED data, College Scorecard. 20 tables. Ask: how do schools compare? what's the graduation rate? which schools are overcrowded?",
    "environment": "Air quality, tree census (680k trees), energy benchmarking, FEMA flood zones, EPA enforcement. 18 tables. Ask: what's the air quality here? which buildings waste the most energy?",
    "financial": "CFPB consumer complaints with full narratives (searchable via text_search). 1 table, 1.2M rows. Ask: what financial companies get the most complaints? what are people complaining about?",
    "health": "Restaurant inspections (27k restaurants, letter grades), rat inspections, community health indicators. 10 tables. Ask: is this restaurant safe? where are the worst rat problems?",
    "housing": "HPD complaints/violations, DOB permits, evictions, NYCHA, HMDA mortgages, ACRIS transactions (85M records since 1966). 40 tables, 30M+ rows. THE richest schema. Ask: who owns this building? how many violations? when was it last sold? who's the worst landlord?",
    "public_safety": "NYPD crimes/arrests/shootings, motor vehicle collisions, hate crimes. 12 tables, 10M+ rows. Ask: is this neighborhood safe? what crimes are most common? how do precincts compare?",
    "recreation": "Parks, pools, permits, events. 8 tables. Ask: what parks are nearby? what events are happening?",
    "social_services": "311 service requests (30M+ rows), food assistance, childcare. 8 tables. Ask: what do people complain about? where are food deserts?",
    "transportation": "MTA ridership, parking tickets (40M+), traffic speeds, street conditions. 15 tables. Ask: which subway stations are busiest? where do people get the most parking tickets?",
}
```

- [ ] **Step 3: Replace `INSTRUCTIONS` with expanded data guide**

Replace the `INSTRUCTIONS` string at line 2323:

```python
INSTRUCTIONS = """\
NYC open data lake — 294 tables, 12 schemas, 60M+ rows of public records.

WHAT'S HERE: Every public dataset about NYC — housing violations, property sales, landlord networks, restaurant inspections, crime stats, school performance, 311 complaints, campaign donations, corporate filings, and more. Data goes back to 1966 for property transactions.

ROUTING — pick the FIRST match:
* BUILDING by BBL → building_profile, landlord_watchdog, building_story
* PERSON/COMPANY by name → entity_xray, person_crossref
* LANDLORD/OWNER → landlord_network, worst_landlords, llc_piercer
* NEIGHBORHOOD by ZIP → neighborhood_portrait, neighborhood_compare
* CRIME/SAFETY by precinct → safety_report
* CORRUPTION/INFLUENCE → pay_to_play, shell_detector, flipper_detector
* FIND DATA by keyword → data_catalog, then list_tables
* CUSTOM QUERY → sql_query (read-only, all tables in 'lake' database)
* DON'T KNOW WHAT TO EXPLORE → suggest_explorations

POWER FEATURES:
* Graph tools trace ownership networks across LLCs, property transactions, and corporate filings
* text_search does full-text search across CFPB complaint narratives and restaurant violations
* person_crossref links a name across every dataset (property, donations, businesses, violations)

SUGGEST INTERESTING THINGS: When the user is exploring or says "what's interesting", call suggest_explorations to get pre-computed highlights — worst landlords, biggest property flips, corporate shell networks, neighborhood comparisons. Use these as conversation starters.

Too many tools? Use search_tools(query) to find domain-specific tools.
BBL: 10 digits = borough(1) + block(5) + lot(4). Example: 1000670001
ZIP: 5 digits. Manhattan 100xx, Brooklyn 112xx, Bronx 104xx.
"""
```

- [ ] **Step 4: Verify the change**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
from mcp_server import INSTRUCTIONS, SCHEMA_DESCRIPTIONS
print(f'Instructions: {len(INSTRUCTIONS)} chars')
print(f'Schema descriptions: {len(SCHEMA_DESCRIPTIONS)} schemas')
for s, d in sorted(SCHEMA_DESCRIPTIONS.items()):
    print(f'  {s}: {len(d)} chars')
"
```

Expected: Instructions ~1,500-2,000 chars. Each schema description 100-250 chars.

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: expand MCP instructions with rich data context

Schema descriptions now include row counts, key tables, and example questions.
Instructions guide LLMs to suggest analytics and interesting explorations."
```

---

## Task 2: Add `suggest_explorations` tool

**Priority:** High — gives LLMs concrete "interesting things" to suggest. Pre-computes highlights on startup so the tool returns instantly.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (add tool + startup queries)

- [ ] **Step 1: Research — how other MCP servers suggest analytics**

Search with Exa:
1. `"mcp server" "suggest" analytics exploration tool 2026` — any precedent for this?
2. `duckdb "pre-computed" statistics summary "interesting" query` — how to find interesting data patterns
3. `fastmcp tool "lifespan_context" pre-compute startup` — how to store precomputed data in FastMCP lifespan

- [ ] **Step 2: Add exploration queries to `app_lifespan`**

In the `app_lifespan` function (around line 2306, before the `yield`), add queries that pre-compute interesting highlights. Add the results to the lifespan context dict.

Find the `yield` statement in `app_lifespan` (line 2306):

```python
    try:
        yield {
            "db": conn, "catalog": catalog,
            "graph_ready": graph_ready,
            "marriage_parquet": MARRIAGE_PARQUET if marriage_available else None,
            "posthog_enabled": bool(ph_key),
        }
```

Before the `try: yield`, add:

```python
    # Pre-compute exploration highlights (runs once at startup)
    explorations = _build_explorations(conn)
```

And add `"explorations": explorations` to the yield dict.

- [ ] **Step 3: Implement `_build_explorations()` helper**

Add this function in the DB helpers section (after line 401):

```python
def _build_explorations(db):
    """Pre-compute interesting data highlights for suggest_explorations tool."""
    highlights = []

    queries = [
        (
            "Worst landlords by open violations",
            """SELECT o.owner_name, COUNT(DISTINCT b.bbl) AS buildings,
                      COUNT(*) FILTER (WHERE v.violationstatus = 'Open') AS open_violations,
                      COUNT(*) AS total_violations
               FROM main.graph_owners o
               JOIN main.graph_owns ow ON o.owner_id = ow.owner_id
               JOIN main.graph_buildings b ON ow.bbl = b.bbl
               JOIN main.graph_violations v ON b.bbl = v.bbl
               WHERE o.owner_name IS NOT NULL
               GROUP BY o.owner_name
               ORDER BY open_violations DESC LIMIT 5""",
            "These landlords have the most open housing violations across their portfolios.",
            "Try: worst_landlords() or entity_xray(name)",
        ),
        (
            "Biggest property flips",
            """SELECT bbl,
                      MAX(TRY_CAST(document_amt AS DOUBLE)) - MIN(TRY_CAST(document_amt AS DOUBLE)) AS profit,
                      MIN(TRY_CAST(document_date AS DATE)) AS first_sale,
                      MAX(TRY_CAST(document_date AS DATE)) AS last_sale
               FROM lake.housing.acris_master
               WHERE doc_type IN ('DEED', 'DEEDO')
                 AND TRY_CAST(document_amt AS DOUBLE) > 100000
               GROUP BY bbl HAVING COUNT(*) >= 2
               ORDER BY profit DESC LIMIT 5""",
            "Properties with the biggest buy-sell price differences.",
            "Try: flipper_detector() or property_history(bbl)",
        ),
        (
            "Most complained-about restaurants",
            """SELECT camis, dba, zipcode, COUNT(*) AS violations,
                      COUNT(*) FILTER (WHERE violation_code LIKE '04%') AS critical
               FROM lake.health.restaurant_inspections
               GROUP BY camis, dba, zipcode
               ORDER BY critical DESC LIMIT 5""",
            "Restaurants with the most critical inspection violations.",
            "Try: restaurant_lookup(name) or text_search('mice kitchen', corpus='restaurants')",
        ),
        (
            "Neighborhoods with most 311 complaints",
            """SELECT incident_zip, complaint_type, COUNT(*) AS complaints
               FROM lake.social_services.n311_service_requests
               WHERE incident_zip IS NOT NULL AND incident_zip != ''
               GROUP BY incident_zip, complaint_type
               ORDER BY complaints DESC LIMIT 5""",
            "What NYC residents complain about most, by ZIP.",
            "Try: neighborhood_portrait(zip) or neighborhood_compare([zip1, zip2])",
        ),
        (
            "Corporate shell networks",
            """SELECT COUNT(*) AS total_active_corps,
                      COUNT(DISTINCT dos_process_name) AS distinct_agents
               FROM lake.business.nys_corporations
               WHERE current_entity_status = 'Active'
                 AND jurisdiction = 'NEW YORK'
                 AND dos_process_name IS NOT NULL""",
            "Active NYC corporations and how many share registered agents — a signal for shell company networks.",
            "Try: shell_detector() or corporate_web(name)",
        ),
    ]

    for title, sql, description, follow_up in queries:
        try:
            with _db_lock:
                result = db.execute(sql).fetchall()
            highlights.append({
                "title": title,
                "description": description,
                "sample": str(result[:3]) if result else "No data",
                "follow_up": follow_up,
            })
        except Exception as e:
            highlights.append({
                "title": title,
                "description": f"(query failed: {e})",
                "sample": "",
                "follow_up": "",
            })

    return highlights
```

- [ ] **Step 4: Add the `suggest_explorations` tool**

Add after the `data_catalog` tool (around line 2698):

```python
@mcp.tool(annotations=READONLY, tags={"discovery"})
def suggest_explorations(ctx: Context) -> str:
    """Get pre-computed interesting findings from the NYC data lake — worst landlords, property flips, shell company networks, restaurant violations, and more. Call this when the user wants to explore, says 'what's interesting', 'surprise me', or needs conversation starters. Returns highlights with follow-up tool suggestions."""
    explorations = ctx.lifespan_context.get("explorations", [])
    if not explorations:
        return "No pre-computed explorations available. Try data_catalog() to browse schemas."

    lines = ["Here are some interesting things in the NYC data lake:\n"]
    for i, exp in enumerate(explorations, 1):
        lines.append(f"**{i}. {exp['title']}**")
        lines.append(exp["description"])
        if exp.get("sample"):
            lines.append(f"Preview: {exp['sample']}")
        if exp.get("follow_up"):
            lines.append(f"Dig deeper: {exp['follow_up']}")
        lines.append("")
    return "\n".join(lines)
```

- [ ] **Step 5: Add `suggest_explorations` to ALWAYS_VISIBLE**

Add `"suggest_explorations"` to the `ALWAYS_VISIBLE` list (line 2348) so it's always shown to the LLM:

```python
ALWAYS_VISIBLE = [
    # Data discovery (always needed)
    "sql_query",
    "data_catalog",
    "list_schemas",
    "suggest_explorations",    # <-- ADD THIS
    # Building-centric ...
```

- [ ] **Step 6: Test locally**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
# Verify the tool is registered and instructions reference it
from mcp_server import INSTRUCTIONS, ALWAYS_VISIBLE
assert 'suggest_explorations' in INSTRUCTIONS
assert 'suggest_explorations' in ALWAYS_VISIBLE
print('OK: suggest_explorations in instructions and always_visible')
"
```

- [ ] **Step 7: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add suggest_explorations tool with pre-computed highlights

Pre-computes worst landlords, biggest flips, shell networks, restaurant
violations, and 311 complaints at startup. Returns highlights with
follow-up tool suggestions for user engagement."
```

---

## Task 3: Add MCP prompts as slash commands

**Priority:** Medium — gives users easy starting points. These appear as `/mcp__duckdb__prompt_name` in Claude Code.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (add prompts)

- [ ] **Step 1: Research — FastMCP prompt API**

Search with Exa:
1. `fastmcp @mcp.prompt decorator example 2026` — exact syntax
2. `fastmcp prompt "PromptResult" "Message" return type` — what to return
3. `"Claude Code" mcp prompt slash command /mcp__` — how prompts appear in Claude Code

- [ ] **Step 2: Add 3 MCP prompts**

Add after the last tool definition (before the PostHog middleware section):

```python
# ---------------------------------------------------------------------------
# Prompts — user-invocable investigation templates
# ---------------------------------------------------------------------------


@mcp.prompt(
    name="investigate_building",
    description="Deep investigation of a NYC building — violations, ownership, complaints, transactions, and enforcement history",
)
def investigate_building(bbl: str) -> str:
    return f"""Investigate building BBL {bbl} thoroughly:

1. Start with building_profile({bbl}) for the overview
2. Check landlord_network({bbl}) to see the ownership network
3. Look at enforcement_web({bbl}) for multi-agency enforcement
4. Pull property_history({bbl}) for the full transaction chain
5. Run building_story({bbl}) for the narrative summary

After gathering all data, write a clear summary highlighting:
- Who owns it and through what corporate structure
- Its violation and complaint history (improving or worsening?)
- Any red flags (frequent sales, liens, AEP program)
- How it compares to similar buildings nearby"""


@mcp.prompt(
    name="compare_neighborhoods",
    description="Compare 2-3 NYC neighborhoods across safety, schools, housing, environment, and quality of life",
)
def compare_neighborhoods(zip_codes: str) -> str:
    zips = [z.strip() for z in zip_codes.split(",")]
    zip_list = ", ".join(zips)
    return f"""Compare these NYC neighborhoods: {zip_list}

1. Start with neighborhood_compare({zips}) for the side-by-side stats
2. For each ZIP, pull neighborhood_portrait(zip) for the full picture
3. Check safety_report for the closest precinct to each
4. Look at environmental_justice for environmental burden

Build a comparison that covers:
- Safety (crime rates, types of crime)
- Housing (rent, violations, landlord quality)
- Schools (if residential)
- Environment (air quality, flood risk, green space)
- Services (311 responsiveness, nearby facilities)

End with a clear recommendation based on the data."""


@mcp.prompt(
    name="follow_the_money",
    description="Trace a person or company's full NYC footprint — property, politics, corporations, violations, and financial connections",
)
def follow_the_money(name: str) -> str:
    return f"""Investigate "{name}" across all NYC public records:

1. Start with entity_xray('{name}') for the full cross-reference
2. Check corporate_web('{name}') for shell company networks
3. Run pay_to_play('{name}') for political donation chains
4. Look at transaction_network('{name}') for property deals
5. Try llc_piercer('{name}') if they operate through LLCs

Build a profile covering:
- All properties connected to this entity
- Corporate structure (LLCs, officers, registered agents)
- Political connections (donations, contracts, lobbying)
- Violation and enforcement history
- Any patterns that suggest conflicts of interest"""
```

- [ ] **Step 3: Test prompt registration locally**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
from mcp_server import mcp
# List registered prompts
import asyncio
async def check():
    prompts = mcp._prompt_manager.list_prompts()
    for p in prompts:
        print(f'  {p.name}: {p.description[:60]}...')
    assert len(prompts) >= 3, f'Expected 3 prompts, got {len(prompts)}'
    print('OK: 3 prompts registered')
asyncio.run(check())
"
```

- [ ] **Step 4: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add MCP prompts for building investigation, neighborhood comparison, and entity tracing

Available as slash commands in Claude Code:
- /mcp__duckdb__investigate_building <bbl>
- /mcp__duckdb__compare_neighborhoods <zips>
- /mcp__duckdb__follow_the_money <name>"
```

---

## Task 4: Deploy and validate

**Files:**
- Deploy: `infra/duckdb-server/` to Hetzner

- [ ] **Step 1: Deploy to Hetzner**

```bash
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
    --exclude '__pycache__' --exclude '.git' --exclude 'tests' \
    ~/Desktop/dagster-pipeline/infra/duckdb-server/ \
    fattie@178.156.228.119:/opt/common-ground/duckdb-server/

ssh hetzner "cd /opt/common-ground && sudo docker compose up -d --build duckdb-server"
```

- [ ] **Step 2: Verify instructions are injected**

Reconnect to the MCP server (restart Claude Code or refresh MCP connection). Check the system reminder — the `## duckdb` section should show the expanded instructions including "WHAT'S HERE", "POWER FEATURES", and "SUGGEST INTERESTING THINGS".

- [ ] **Step 3: Test `suggest_explorations` tool**

Call the tool via MCP:
```
mcp__duckdb__suggest_explorations()
```

Expected: Returns 5 highlights with titles, descriptions, previews, and follow-up suggestions.

- [ ] **Step 4: Test prompts appear as slash commands**

In Claude Code, type `/mcp__duckdb__` and check autocomplete shows:
- `investigate_building`
- `compare_neighborhoods`
- `follow_the_money`

- [ ] **Step 5: Verify container is healthy**

```bash
ssh hetzner "docker logs common-ground-duckdb-server-1 --tail 20 2>&1"
ssh hetzner "docker ps --filter name=duckdb-server --format '{{.Names}} {{.Status}}'"
```

---

## Execution Order & Dependencies

```
Task 1 (instructions + schema descriptions)  ← Can deploy independently
  ↓
Task 2 (suggest_explorations tool)            ← References instructions text
  ↓
Task 3 (MCP prompts)                          ← Independent but deploy together
  ↓
Task 4 (deploy + validate)                    ← Needs all code committed
```

Tasks 1-3 modify the same file sequentially. Task 4 deploys everything.

---

## Verification Checklist

After all tasks complete:

- [ ] `INSTRUCTIONS` is 1,500-2,500 chars with schema highlights and question suggestions
- [ ] `SCHEMA_DESCRIPTIONS` includes row counts, key tables, and example questions for all 12 schemas
- [ ] `suggest_explorations` returns 5 pre-computed highlights with follow-up tool suggestions
- [ ] `suggest_explorations` is in `ALWAYS_VISIBLE` list
- [ ] 3 MCP prompts registered: `investigate_building`, `compare_neighborhoods`, `follow_the_money`
- [ ] Instructions mention `suggest_explorations` for exploration
- [ ] Container starts without errors
- [ ] Prompts appear as `/mcp__duckdb__*` slash commands in Claude Code
