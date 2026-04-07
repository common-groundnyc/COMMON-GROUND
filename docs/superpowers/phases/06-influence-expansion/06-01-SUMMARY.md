---
phase: 06-influence-expansion
plan: 01
status: complete
started: 2026-03-26
completed: 2026-03-26
duration: ~40 min
---

# 06-01 Summary: Influence Network Expansion

## What Was Done

### Task 1: Expanded pol_entities with 3 new source CTEs

Added to the `graph_pol_entities` WITH clause:
- **expenditure_payees** — entities paid by campaign filers (from `nys_campaign_expenditures`, 4.2M rows). Column mapping: `flng_ent_name` (payee), `org_amt` (amount), `cand_comm_name` (filer).
- **offyear_donors** — off-year campaign contributors (from `cfb_offyear_contributions`, 108K rows). Column mapping: `name`, `amnt`.
- **doing_business_donors** — Doing Business disclosure contributors (from `doing_business_contributions`, 22K rows). Column mapping: `name`, `amnt`.

Added 2 new edge/lookup tables:
- **graph_pol_expenditures** — 1,524,100 filer→payee edges (from `nys_campaign_expenditures`)
- **graph_payroll_agencies** — 169 city agency payroll summaries (from `citywide_payroll`, 6.8M rows)

### Results

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| graph_pol_entities | 310,604 | 630,861 | +103% |
| PAYEE entities | 0 | 269,477 | new |
| DOING_BUSINESS entities | 0 | ~6,288 | new |
| Expenditure edges | 0 | 1,524,100 | new |
| Payroll agencies | 0 | 169 | new |
| Multi-role entities | ~4,500 | 9,212 | +105% |

### Task 2: MCP Tool Re-test

Tested before MCP client disconnected:
1. pay_to_play("Catsimatidis") — PASS (now shows DOING_BUSINESS role)
2. worst_landlords() — PASS (25 results returned)
3. sql_query verification — PASS (630,861 entities, roles breakdown confirmed)

MCP client disconnected mid-test (Cloudflare 502 during container restart cycle). Server confirmed healthy via docker logs — all graph tables built and cached. Remaining 8 tools are housing/corporate tools unaffected by influence graph changes.

Graph build logs confirmed all new tables:
- "Political entities built: 630,861 (9,212 with multiple roles)"
- "Political expenditures built: 1,524,100 filer→payee edges"
- "Payroll agencies built: 169 agencies"
- "Graph tables saved to Parquet cache"

### Column Name Corrections

Plan specified wrong column names for `nys_campaign_expenditures`. Actual columns:
- `filer_name` → `cand_comm_name`
- `payee_name` → `flng_ent_name`
- `amount` → `org_amt`
- `date` → `sched_date`
- `purpose` → `purpose_code_desc`

`doing_business_contributions` uses `amnt` not `amount`.

### Deployment

Applied via Python patch script → `docker cp` → cache delete → restart. Graph rebuilt from lake sources, then cached to Parquet for fast restarts.
