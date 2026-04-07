# Verbatim Response System — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Claude present full, unsummarized tool output by combining MCP server instructions, tool descriptions, response-embedded presentation directives, and structured data that triggers Claude's native interactive visualization.

**Architecture:** Three layers work together: (1) server-level `instructions` tell Claude how to present Common Ground data globally, (2) each tool's docstring includes presentation guidance per the 6-component rubric, (3) every tool response embeds a presentation directive prefix that instructs Claude at the moment it decides how to render. Structured content returns clean numeric data to trigger Claude's native interactive visuals (charts, tables).

**Tech Stack:** Python, FastMCP 3.1.1 (no upgrade needed), no new dependencies

**Research basis:** Community best practices (MCP Tool Design article, Reddit r/mcp "every tool response is a prompt"), Claude Interactive Visuals (March 12, 2026), MCP 6-component description rubric.

---

## Task 1: Add presentation directives to MCP server instructions

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:1842-1877`

The `INSTRUCTIONS` string is injected into Claude's system context at connection time. Adding presentation rules here affects ALL 14 tools globally.

- [ ] **Step 1: Update the INSTRUCTIONS string**

Replace the current `INSTRUCTIONS` constant with a version that adds a `PRESENTATION` section after the existing `WORKFLOWS` section:

```python
INSTRUCTIONS = """Common Ground -- NYC open data lake. 294 tables, 60M+ rows, 14 schemas.

ROUTING -- pick the FIRST match:
* Street address or "where I live"       -> address_report()
* Address + specific question (violations, history) -> building()
* BBL (10-digit number)                  -> building()
* Person or company name                 -> entity()
* Cop by name                            -> entity(role="cop")
* Judge by name                          -> entity(role="judge")
* Birth/death/marriage records           -> entity(role="vitals")
* ZIP code or "this neighborhood"        -> neighborhood()
* Compare ZIPs                           -> neighborhood(view="compare")
* Restaurants in an area                 -> neighborhood(view="restaurants")
* Landlord portfolio or slumlord score   -> network(type="ownership")
* LLC piercing or shell companies        -> network(type="corporate")
* Campaign donations or lobbying         -> network(type="political")
* Worst landlords ranking                -> network(type="worst")
* School by name, DBN, or ZIP            -> school()
* "Find complaints about X" or concepts  -> semantic_search()
* Crime, crashes, shootings              -> safety()
* Health data, COVID, hospitals          -> health()
* Court cases, settlements, hearings     -> legal()
* City contracts, permits, jobs, budget  -> civic()
* Parking tickets, MTA, traffic          -> transit()
* Childcare, shelters, food pantries     -> services()
* "What can I explore?" or unsure        -> suggest()
* Custom SQL or "what tables have X"     -> query()
* Download or export data               -> query(format="xlsx")

WORKFLOWS -- chain tools for deep investigations:
* Landlord investigation: building() -> entity() -> network(type="ownership")
* Follow the money: entity() -> network(type="political") -> civic(view="contracts")
* School comparison: school("02M475,02M001")
* Health equity: health("10456") -> services("10456") -> neighborhood("10456")

PRESENTATION -- how to show Common Ground data to users:
* Every tool returns a complete, pre-formatted report. Present ALL sections to the user.
* Do NOT summarize, compress, or omit sections. Each section has independent data the user needs.
* Use interactive visualizations (charts, tables) when data contains numeric comparisons, rankings, or percentiles.
* Percentile bars and rankings should be shown visually, not described in prose.
* Preserve the tool's section headers and structure — they are designed for readability.
* When a report has a "Drill deeper" footer, show it — those are actionable tool calls the user can request.
* If the response includes a PRESENTATION directive, follow it exactly.

This is NYC-only data. Do not query for national/federal statistics."""
```

- [ ] **Step 2: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add PRESENTATION section to MCP server instructions"
```

---

## Task 2: Update address_report tool description (6-component rubric)

**Files:**
- Modify: `infra/duckdb-server/tools/address_report.py:31-36`

The tool docstring is the LLM's primary decision surface. Apply the 6-component rubric: Purpose, Guidelines, Limitations, Parameters, Length, Examples.

- [ ] **Step 1: Replace the docstring**

```python
    """Complete 360-degree report for any NYC address — building profile,
    violations with citywide percentile rankings, neighborhood demographics,
    crime stats, school quality, health indicators, environmental risk,
    civic representation, nearby services, and fun facts.

    GUIDELINES: Use as the FIRST lookup for any address question. Returns
    10 independent sections, each with quantitative data. Present the FULL
    report to the user — every section contains unique metrics they need.
    Use interactive charts/tables for violation percentiles and demographic
    comparisons. Show the drill-deeper suggestions at the end.

    LIMITATIONS: NYC addresses only. Does not cover transaction history
    (use building(view="history")), enforcement timeline (use
    building(view="enforcement")), or ownership networks (use network()).

    RETURNS: Pre-formatted multi-section report with percentile bars,
    structured numeric data for visualization, and drill-deeper tool calls."""
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/tools/address_report.py
git commit -m "feat: apply 6-component description rubric to address_report"
```

---

## Task 3: Add presentation directive to address_report response

**Files:**
- Modify: `infra/duckdb-server/tools/_address_format.py:99-130`

Embed a presentation instruction at the top of the report text. Claude reads `content` at the moment it decides how to present — this is the strongest lever.

- [ ] **Step 1: Add presentation prefix to assemble_report**

In the `assemble_report` function, add a presentation directive block before the header. Find the line `lines: list[str] = []` (line ~121) and add the directive immediately after:

```python
    lines: list[str] = []
    rule = "━" * W

    # Presentation directive — Claude reads this before deciding how to render
    lines.append("PRESENTATION: This is a complete 10-section report with quantitative data.")
    lines.append("Show ALL sections to the user. Use interactive tables or charts for")
    lines.append("violation counts, percentile rankings, and neighborhood demographics.")
    lines.append("Do not summarize or omit any section. Show the drill-deeper footer.")
    lines.append("")
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/tools/_address_format.py
git commit -m "feat: add presentation directive prefix to address_report response"
```

---

## Task 4: Enrich address_report structured_content for native visualization

**Files:**
- Modify: `infra/duckdb-server/tools/address_report.py:66-76`

Return clean, labeled numeric data in `structured_content` so Claude's native Interactive Visuals can render charts/tables from it.

- [ ] **Step 1: Build richer structured_content**

Replace the current structured_content block (lines 66-76) with:

```python
    # Step 4: structured content — clean numeric data for Claude's native visualization
    from tools._address_format import _get, _get_all

    v = _get(results, "building_hpd_violations")
    pv = _get(results, "pctile_violations")
    c = _get(results, "building_hpd_complaints")
    pc = _get(results, "pctile_complaints")
    dob = _get(results, "building_dob_violations")
    acs = _get(results, "neighborhood_acs")
    crimes = _get(results, "safety_crimes")
    shootings = _get(results, "safety_shootings")

    structured = {
        "bbl": bbl,
        "address": ctx_data.get("address", address),
        "zipcode": ctx_data.get("zip", ""),
        "borough": ctx_data.get("borough", ""),
        "building": {
            "year_built": ctx_data.get("yearbuilt"),
            "stories": ctx_data.get("numfloors"),
            "units": ctx_data.get("unitsres"),
            "assessed_value": ctx_data.get("assesstot"),
            "owner": ctx_data.get("ownername"),
            "zoning": ctx_data.get("zoning"),
        },
        "violations": {
            "hpd_total": v.get("total", 0) if v else 0,
            "hpd_open": v.get("open_cnt", 0) if v else 0,
            "hpd_class_c": v.get("class_c", 0) if v else 0,
            "hpd_percentile": round((pv.get("pctile") or 0) * 100) if pv else None,
            "complaints_total": c.get("total", 0) if c else 0,
            "complaints_percentile": round((pc.get("pctile") or 0) * 100) if pc else None,
            "dob_total": dob.get("total", 0) if dob else 0,
        },
        "neighborhood": {
            "population": acs.get("total_population") if acs else None,
            "median_income": acs.get("median_household_income") if acs else None,
            "poverty_rate": acs.get("poverty_rate") if acs else None,
            "median_rent": acs.get("median_gross_rent") if acs else None,
        },
        "safety": {
            "crimes_ytd": crimes.get("total") if crimes else None,
            "felonies": crimes.get("felonies") if crimes else None,
            "shootings_12mo": shootings.get("total") if shootings else None,
        },
        "visualization_hint": "Show violations as a ranked bar or percentile gauge. "
                              "Show neighborhood demographics as a summary card. "
                              "Show safety stats as a comparison table.",
    }

    return ToolResult(content=report, structured_content=structured)
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/tools/address_report.py
git commit -m "feat: enrich address_report structured_content for native visualization"
```

---

## Task 5: Update building tool description (6-component rubric)

**Files:**
- Modify: `infra/duckdb-server/tools/building.py:1987`

- [ ] **Step 1: Replace the docstring**

```python
    """Look up any NYC building by address or BBL. Returns violations,
    enforcement actions, landlord info, and property history.

    GUIDELINES: Use for any question about a specific building, address,
    or property. Present the FULL response — every field contains data
    the user needs. Use interactive tables for violation lists and charts
    for percentile comparisons.

    LIMITATIONS: Do NOT use for person lookups (use entity), neighborhood
    questions without a specific address (use neighborhood), or ownership
    network traversal (use network).

    RETURNS: Building profile with violations, enrichments (landmark, tax,
    SRO, facade), and citywide percentile ranking. Default view='full'
    returns the complete profile. Other views: story, block, similar,
    enforcement, history, flippers."""
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/tools/building.py
git commit -m "feat: apply 6-component description rubric to building tool"
```

---

## Task 6: Add presentation directive to building _view_full response

**Files:**
- Modify: `infra/duckdb-server/tools/building.py:674-686`

- [ ] **Step 1: Add presentation prefix to the summary string**

Replace the summary construction (lines 674-686) with a version that prepends a presentation directive:

```python
    summary = (
        "PRESENTATION: Show this complete building profile to the user. "
        "Use interactive charts for violation counts and percentile ranking. "
        "Do not omit any field — every metric is independently valuable.\n\n"
        f"BBL {row['bbl']}: {row['address']}, {row['zip']}\n"
        f"  {row['stories']} stories, {row['total_units']} units"
        f" ({row['managementprogram'] or 'N/A'})\n"
        f"  HPD Violations: {row['total_violations']} total,"
        f" {row['open_violations']} open"
        f" (latest: {row['latest_violation_date'] or 'N/A'})\n"
        f"  HPD Complaints: {row['total_complaints']} total,"
        f" {row['open_complaints']} open"
        f" (latest: {row['latest_complaint_date'] or 'N/A'})\n"
        f"  DOB/ECB Violations: {row['total_dob_violations']}"
        f" (latest: {row['latest_dob_date'] or 'N/A'})"
    )
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/tools/building.py
git commit -m "feat: add presentation directive to building full view"
```

---

## Task 7: Add presentation directives to all remaining tools

**Files:**
- Modify: `infra/duckdb-server/tools/entity.py`
- Modify: `infra/duckdb-server/tools/neighborhood.py`
- Modify: `infra/duckdb-server/tools/network.py`
- Modify: `infra/duckdb-server/tools/school.py`
- Modify: `infra/duckdb-server/tools/semantic_search.py`
- Modify: `infra/duckdb-server/tools/safety.py`
- Modify: `infra/duckdb-server/tools/health.py`
- Modify: `infra/duckdb-server/tools/legal.py`
- Modify: `infra/duckdb-server/tools/civic.py`
- Modify: `infra/duckdb-server/tools/transit.py`
- Modify: `infra/duckdb-server/tools/services.py`
- Modify: `infra/duckdb-server/tools/query.py`

For each tool, two changes:

**A) Update the docstring** to follow the 6-component rubric with presentation guidance. Pattern:

```python
    """[Purpose — what it does, what it returns]

    GUIDELINES: [When to use it. Present the FULL response to the user.
    Use interactive tables/charts for numeric data. Do not summarize.]

    LIMITATIONS: [What it cannot do, when to use a different tool]

    RETURNS: [What the response shape looks like]"""
```

**B) Add a presentation directive prefix** to the `content` string in the tool's return. Pattern:

```python
    content = (
        "PRESENTATION: Show this complete [tool name] report. "
        "Use interactive tables for [specific data type]. "
        "Do not omit any section.\n\n"
        + existing_content
    )
```

- [ ] **Step 1: Update entity.py** — docstring + response prefix

Find the main `entity()` function docstring and the place where it returns `ToolResult(content=..., ...)`. Update docstring to 6-component rubric. Prepend `"PRESENTATION: Show all entity matches in a table. Each row is a distinct data source. Do not summarize — show every match.\n\n"` to the content string.

- [ ] **Step 2: Update neighborhood.py** — docstring + response prefix

Prepend `"PRESENTATION: Show this complete neighborhood profile. Use interactive charts for demographic data and comparisons. Do not omit any section.\n\n"` to content.

- [ ] **Step 3: Update network.py** — docstring + response prefix

Prepend `"PRESENTATION: Show the complete ownership/corporate/political network. Use a table for all entities. Do not summarize the network — every connection matters.\n\n"` to content.

- [ ] **Step 4: Update school.py** — docstring + response prefix

Prepend `"PRESENTATION: Show the complete school profile with test scores and demographics. Use tables for score breakdowns. Do not omit any metric.\n\n"` to content.

- [ ] **Step 5: Update semantic_search.py** — docstring + response prefix

Prepend `"PRESENTATION: Show ALL search results in a table. Each row is a distinct violation/complaint match. Do not summarize — the user needs to scan results.\n\n"` to content.

- [ ] **Step 6: Update safety.py, health.py, legal.py, civic.py, transit.py, services.py** — docstring + response prefix for each

Same pattern: 6-component docstring, presentation prefix telling Claude to show the full report with interactive tables/charts.

- [ ] **Step 7: Update query.py** — docstring + response prefix

Prepend `"PRESENTATION: Show the complete query results as an interactive table. All rows, all columns. Do not summarize SQL results.\n\n"` to content.

- [ ] **Step 8: Commit all remaining tool updates**

```bash
git add infra/duckdb-server/tools/*.py
git commit -m "feat: add presentation directives and 6-component descriptions to all tools"
```

---

## Task 8: Deploy and test in Claude Desktop

**Files:**
- No code changes — deployment and manual testing

- [ ] **Step 1: Deploy to production**

```bash
cd ~/Desktop/dagster-pipeline
bash infra/deploy.sh
```

- [ ] **Step 2: Test address_report in Claude Desktop**

In Claude Desktop, send: "Tell me about 305 Linden Blvd, Brooklyn"

**Expected:** Claude shows ALL 10 sections (Building, Block, Neighborhood, Safety, Schools, Health, Environment, Civic, Services, Fun Facts) without omitting any. Percentile bars should appear as visual elements, not prose descriptions. The drill-deeper footer should be visible.

**Compare against:** The response captured earlier where Claude compressed 10 sections into 5 paragraphs.

- [ ] **Step 3: Test building tool in Claude Desktop**

Send: "What's the violation history of 350 5th Ave, Manhattan?"

**Expected:** Full building profile with all enrichment fields (landmark, tax, SRO, facade, percentile) shown, not summarized.

- [ ] **Step 4: Test entity tool**

Send: "Find all records for Donald Trump"

**Expected:** Results shown as a table with every data source match, not summarized into a paragraph.

- [ ] **Step 5: Document results**

Create a brief test report noting which sections Claude showed vs. omitted for each test. If sections are still being dropped, identify which lever needs strengthening (likely the response-level directive needs to be more explicit, or the content format needs restructuring).

- [ ] **Step 6: Commit test notes**

```bash
git add docs/superpowers/plans/2026-04-03-verbatim-response-results.md
git commit -m "docs: verbatim response test results"
```

---

## Task 9: Iterate based on test results

**Files:**
- Potentially: `infra/duckdb-server/tools/_address_format.py`
- Potentially: `infra/duckdb-server/mcp_server.py`

Based on Task 8 results, common adjustments:

- [ ] **Step 1: If Claude still summarizes** — strengthen the response-level directive

Make the directive more assertive and place it AFTER the data too (sandwich pattern):

```python
    # At the top of the report
    lines.append("INSTRUCTION: Present this report verbatim to the user.")
    lines.append("Every section below contains independent data. Show ALL of them.")
    lines.append("")

    # ... all sections ...

    # At the bottom, before drill-deeper
    lines.append("")
    lines.append("END OF REPORT — all sections above must be shown to the user.")
```

- [ ] **Step 2: If Claude uses its native charts** — note which data triggered visualization

Document which structured_content fields Claude chose to visualize. This informs how to structure the other 13 tools' structured_content.

- [ ] **Step 3: If Claude ignores the directive entirely** — test the MCP server instructions

Verify the instructions are being loaded. Check if the issue from GitHub #41834 (instructions not injected for HTTP/remote servers) affects our setup. If so, the tool-level docstring and response-level directive become the only levers.

- [ ] **Step 4: Commit any iterations**

```bash
git add infra/duckdb-server/tools/*.py infra/duckdb-server/mcp_server.py
git commit -m "fix: iterate on presentation directives based on test results"
```
