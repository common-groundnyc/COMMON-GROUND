# Graph Fixes + Orphan Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 2 bugs in existing property graphs, wire 7 orphaned graph tables into property graph definitions, add 4 missing edge types from existing data, and create MCP tools for the 2 unused graphs — increasing edge coverage from 11 to 19 edge types.

**Architecture:** All changes are in the property graph definitions section of mcp_server.py (lines 2537-2700). No new graph tables need to be built — the data already exists, it just isn't wired into DuckPGQ property graphs or exposed via MCP tools. Each task is a surgical edit to the graph definition SQL.

**Tech Stack:** DuckDB 1.5.0, DuckPGQ, FastMCP.

**Key constraint:** All property graph changes require a server restart to take effect (graphs are built at startup). Test by restarting the container after each commit.

---

## File Structure

### Modified files

| File | What changes |
|------|-------------|
| `infra/duckdb-server/mcp_server.py:2537-2700` | Fix bugs + expand property graph definitions |
| `infra/duckdb-server/mcp_server.py` (tool section) | Add 2 new MCP tools for officer/tradewaste graphs |

---

## Task 1: Fix BIC tradewaste graph — edge key mismatch

The `nyc_tradewaste_network` graph references `company_name` as the vertex key in edge foreign keys, but `graph_bic_companies` is keyed by `bic_number`. The edge table `graph_bic_shared_bbl` uses `company_name` which isn't the vertex primary key.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:2464-2477` (shared BBL edge build)
- Modify: `infra/duckdb-server/mcp_server.py:2683-2695` (property graph definition)

- [ ] **Step 1: Fix the edge table to use bic_number instead of company_name**

Replace the `graph_bic_shared_bbl` build SQL (lines 2464-2477) with:

```python
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_bic_shared_bbl AS
                    WITH company_bbls AS (
                        SELECT DISTINCT bic_number, bbl
                        FROM lake.business.bic_trade_waste
                        WHERE bbl IS NOT NULL AND LENGTH(bbl) >= 10
                          AND bic_number IS NOT NULL
                    )
                    SELECT a.bic_number AS company1, b.bic_number AS company2,
                           COUNT(DISTINCT a.bbl) AS shared_properties
                    FROM company_bbls a
                    JOIN company_bbls b ON a.bbl = b.bbl AND a.bic_number < b.bic_number
                    GROUP BY a.bic_number, b.bic_number
                """)
```

- [ ] **Step 2: Fix the property graph definition to use bic_number**

Replace `nyc_tradewaste_network` property graph (lines 2683-2695) with:

```python
                CREATE OR REPLACE PROPERTY GRAPH nyc_tradewaste_network
                VERTEX TABLES (
                    main.graph_bic_companies
                        PROPERTIES (bic_number, company_name, trade_name, address, record_count)
                        LABEL TradeWasteCompany
                )
                EDGE TABLES (
                    main.graph_bic_shared_bbl
                        SOURCE KEY (company1) REFERENCES main.graph_bic_companies (bic_number)
                        DESTINATION KEY (company2) REFERENCES main.graph_bic_companies (bic_number)
                        LABEL SharedProperty
                )
```

- [ ] **Step 3: Also fix the fallback table schema**

Replace the fallback (line 2484) to match the new schema:

```python
                conn.execute("CREATE OR REPLACE TABLE main.graph_bic_shared_bbl AS SELECT NULL::VARCHAR AS company1, NULL::VARCHAR AS company2, 0::BIGINT AS shared_properties WHERE FALSE")
```

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "fix: BIC tradewaste graph edge key mismatch — use bic_number not company_name"
```

---

## Task 2: Wire orphaned tables into nyc_housing graph

Add DOB respondents and litigation respondents as vertex+edge types in `nyc_housing`.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:2537-2553` (nyc_housing property graph)

- [ ] **Step 1: Expand nyc_housing property graph**

Replace the `nyc_housing` property graph definition (lines 2536-2553) with:

```python
        conn.execute("""
            CREATE OR REPLACE PROPERTY GRAPH nyc_housing
            VERTEX TABLES (
                main.graph_owners PROPERTIES (owner_id, registrationid, owner_name) LABEL Owner,
                main.graph_buildings PROPERTIES (bbl, housenumber, streetname, zip, boroid, total_units, stories) LABEL Building,
                main.graph_violations PROPERTIES (violation_id, bbl, severity, status, issued_date) LABEL Violation,
                main.graph_dob_respondents
                    PROPERTIES (respondent_name, violation_count, total_penalties, building_count, hazardous_count)
                    LABEL DOBRespondent,
                main.graph_litigation_respondents
                    PROPERTIES (respondent_name, bbl, case_count, open_cases)
                    LABEL LitigationRespondent
            )
            EDGE TABLES (
                main.graph_owns
                    SOURCE KEY (owner_id) REFERENCES main.graph_owners (owner_id)
                    DESTINATION KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    LABEL Owns,
                main.graph_has_violation
                    SOURCE KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    DESTINATION KEY (violation_id) REFERENCES main.graph_violations (violation_id)
                    LABEL HasViolation,
                main.graph_dob_respondent_bbl
                    SOURCE KEY (respondent_name) REFERENCES main.graph_dob_respondents (respondent_name)
                    DESTINATION KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    LABEL DOBRespondentAt,
                main.graph_litigation_respondents
                    SOURCE KEY (respondent_name) REFERENCES main.graph_litigation_respondents (respondent_name)
                    DESTINATION KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    LABEL LitigatedAt
            )
        """)
```

Note: `graph_litigation_respondents` is used as both a vertex table AND an edge table (it has respondent_name + bbl). If DuckPGQ doesn't allow this, split into separate vertex and edge tables. In that case, use `graph_litigation_respondents` as vertex only, and create the edge inline.

If the dual-use fails, fall back to just adding DOB respondents (which has a separate edge table `graph_dob_respondent_bbl`):

```python
        # Fallback: simpler version without litigation (if DuckPGQ rejects dual-use)
        conn.execute("""
            CREATE OR REPLACE PROPERTY GRAPH nyc_housing
            VERTEX TABLES (
                main.graph_owners PROPERTIES (owner_id, registrationid, owner_name) LABEL Owner,
                main.graph_buildings PROPERTIES (bbl, housenumber, streetname, zip, boroid, total_units, stories) LABEL Building,
                main.graph_violations PROPERTIES (violation_id, bbl, severity, status, issued_date) LABEL Violation,
                main.graph_dob_respondents
                    PROPERTIES (respondent_name, violation_count, total_penalties, building_count, hazardous_count)
                    LABEL DOBRespondent
            )
            EDGE TABLES (
                main.graph_owns
                    SOURCE KEY (owner_id) REFERENCES main.graph_owners (owner_id)
                    DESTINATION KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    LABEL Owns,
                main.graph_has_violation
                    SOURCE KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    DESTINATION KEY (violation_id) REFERENCES main.graph_violations (violation_id)
                    LABEL HasViolation,
                main.graph_dob_respondent_bbl
                    SOURCE KEY (respondent_name) REFERENCES main.graph_dob_respondents (respondent_name)
                    DESTINATION KEY (bbl) REFERENCES main.graph_buildings (bbl)
                    LABEL DOBRespondentAt
            )
        """)
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: wire DOB respondents + litigation into nyc_housing property graph"
```

---

## Task 3: Wire FEC + contracts into nyc_influence_network

Add federal FEC contributions and city contracts as edge types.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:2617-2634` (nyc_influence_network)

- [ ] **Step 1: Expand nyc_influence_network**

Replace the property graph definition (lines 2617-2634) with:

```python
                CREATE OR REPLACE PROPERTY GRAPH nyc_influence_network
                VERTEX TABLES (
                    main.graph_pol_entities
                        PROPERTIES (entity_name, roles, total_amount, total_transactions)
                        LABEL PoliticalEntity
                )
                EDGE TABLES (
                    main.graph_pol_donations
                        SOURCE KEY (donor_name) REFERENCES main.graph_pol_entities (entity_name)
                        DESTINATION KEY (candidate_name) REFERENCES main.graph_pol_entities (entity_name)
                        LABEL DonatesTo,
                    main.graph_pol_lobbying
                        SOURCE KEY (lobbyist_name) REFERENCES main.graph_pol_entities (entity_name)
                        DESTINATION KEY (client_name) REFERENCES main.graph_pol_entities (entity_name)
                        LABEL LobbiesFor,
                    main.graph_pol_contracts
                        SOURCE KEY (vendor_name) REFERENCES main.graph_pol_entities (entity_name)
                        DESTINATION KEY (agency_name) REFERENCES main.graph_pol_entities (entity_name)
                        LABEL ContractedBy
                )
```

Note: `graph_pol_contracts` already exists (built at line ~2074). It has `vendor_name` and `agency_name` which should match `graph_pol_entities.entity_name` (the entity table already includes vendors and agencies). If the foreign key fails because some vendor/agency names aren't in `graph_pol_entities`, wrap in try/except and fall back to the original 2-edge definition.

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: wire contracts into nyc_influence_network as ContractedBy edge"
```

---

## Task 4: Create nyc_coib_network property graph

Wire the 3 orphaned COIB tables into a new property graph.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (after line 2698, before `graph_ready = True`)

- [ ] **Step 1: Add COIB property graph definition**

Insert before `graph_ready = True` (line 2700):

```python
        # COIB conflicts of interest network
        try:
            conn.execute("""
                CREATE OR REPLACE PROPERTY GRAPH nyc_coib_network
                VERTEX TABLES (
                    main.graph_coib_donors
                        PROPERTIES (donor_name, donation_types, total_donated, donation_count, city, state)
                        LABEL COIBDonor,
                    main.graph_coib_policymakers
                        PROPERTIES (policymaker_name, agency, title, latest_year, years_active)
                        LABEL Policymaker
                )
                EDGE TABLES (
                    main.graph_coib_donor_edges
                        SOURCE KEY (donor_name) REFERENCES main.graph_coib_donors (donor_name)
                        DESTINATION KEY (recipient) REFERENCES main.graph_coib_policymakers (policymaker_name)
                        LABEL DonatesTo
                )
            """)
            print("Property graph nyc_coib_network created", flush=True)
        except Exception as e:
            print(f"Warning: nyc_coib_network graph definition failed: {e}", flush=True)
```

Note: The edge `recipient` may not always match `policymaker_name` (recipients include trust names and org names, not just policymaker names). If this causes a foreign key violation, change the destination to reference `graph_coib_donors` instead (self-referencing donor network), or make the edge definition LAXER. DuckPGQ may allow dangling references — test it.

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: create nyc_coib_network property graph (donors + policymakers)"
```

---

## Task 5: Add MCP tool for officer network

The `nyc_officer_network` graph exists but no tool uses it. `cop_sheet` queries the lake directly.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (tool section, after cop_sheet)

- [ ] **Step 1: Add officer_network tool**

```python
@mcp.tool(annotations=READONLY, tags={"graph", "safety"})
def officer_network(
    name: NAME,
    ctx: Context,
) -> str:
    """Find officers connected to a named officer through shared commands — discovers units with patterns of misconduct. Uses DuckPGQ graph traversal on CCRB + NYPD data. For an individual officer dossier, use cop_sheet(name)."""
    _require_graph(ctx)
    db = ctx.lifespan_context["db"]

    # Find the officer
    parts = name.strip().split()
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        cols, rows = _execute(db, """
            SELECT officer_id, full_name, current_rank, current_command, total_complaints, substantiated
            FROM main.graph_officers
            WHERE UPPER(last_name) = UPPER(?) AND UPPER(first_name) LIKE UPPER(?) || '%'
            LIMIT 5
        """, [last, first])
    else:
        cols, rows = _execute(db, """
            SELECT officer_id, full_name, current_rank, current_command, total_complaints, substantiated
            FROM main.graph_officers
            WHERE UPPER(last_name) = UPPER(?) OR UPPER(full_name) ILIKE '%' || ? || '%'
            LIMIT 5
        """, [name, name])

    if not rows:
        raise ToolError(f"No officer found matching '{name}'. Try cop_sheet(name) for broader search.")

    target = dict(zip(cols, rows[0]))
    officer_id = target["officer_id"]

    # Find connected officers via shared command
    try:
        net_cols, net_rows = _execute(db, """
            SELECT o2.full_name, o2.current_rank, o2.current_command,
                   o2.total_complaints, o2.substantiated, e.combined_complaints
            FROM main.graph_officer_shared_command e
            JOIN main.graph_officers o2 ON e.officer2 = o2.officer_id
            WHERE e.officer1 = ?
            ORDER BY e.combined_complaints DESC
            LIMIT 20
        """, [officer_id])
    except Exception:
        net_rows = []

    lines = [
        f"OFFICER NETWORK — {target['full_name']}",
        f"  Rank: {target.get('current_rank', '?')} | Command: {target.get('current_command', '?')}",
        f"  CCRB complaints: {target.get('total_complaints', 0)} ({target.get('substantiated', 0)} substantiated)",
        "=" * 55,
    ]

    if net_rows:
        lines.append(f"\nCONNECTED OFFICERS ({len(net_rows)} via shared command):")
        for r in net_rows:
            d = dict(zip(net_cols, r))
            lines.append(f"  {d['full_name']} — {d.get('current_rank', '?')}, {d.get('current_command', '?')}")
            lines.append(f"    Complaints: {d.get('total_complaints', 0)} ({d.get('substantiated', 0)} subst), combined: {d.get('combined_complaints', 0)}")
    else:
        lines.append("\nNo connected officers found via shared command.")

    return "\n".join(lines)
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add officer_network MCP tool (graph-based officer connections)"
```

---

## Task 6: Add MCP tool for tradewaste network

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (tool section)

- [ ] **Step 1: Add tradewaste_network tool**

```python
@mcp.tool(annotations=READONLY, tags={"graph", "business"})
def tradewaste_network(
    company: NAME,
    ctx: Context,
) -> str:
    """Find trade waste haulers connected through shared properties — reveals hidden relationships between waste companies. Uses DuckPGQ graph on BIC licensing data."""
    _require_graph(ctx)
    db = ctx.lifespan_context["db"]

    # Find the company
    cols, rows = _execute(db, """
        SELECT bic_number, company_name, trade_name, address, record_count
        FROM main.graph_bic_companies
        WHERE UPPER(company_name) ILIKE '%' || UPPER(?) || '%'
        LIMIT 5
    """, [company])

    if not rows:
        raise ToolError(f"No trade waste company found matching '{company}'.")

    target = dict(zip(cols, rows[0]))
    bic = target["bic_number"]

    # Find connected companies via shared BBL
    try:
        net_cols, net_rows = _execute(db, """
            SELECT c.company_name, c.trade_name, c.address, c.record_count, e.shared_properties
            FROM main.graph_bic_shared_bbl e
            JOIN main.graph_bic_companies c ON e.company2 = c.bic_number
            WHERE e.company1 = ?
            UNION ALL
            SELECT c.company_name, c.trade_name, c.address, c.record_count, e.shared_properties
            FROM main.graph_bic_shared_bbl e
            JOIN main.graph_bic_companies c ON e.company1 = c.bic_number
            WHERE e.company2 = ?
            ORDER BY shared_properties DESC
            LIMIT 20
        """, [bic, bic])
    except Exception:
        net_rows = []

    # Get violations for this company
    viol_cols, viol_rows = _execute(db, """
        SELECT type_of_violation, COUNT(*) AS cnt,
               SUM(COALESCE(fine_amount, 0)) AS total_fines
        FROM main.graph_bic_violation_edges
        WHERE company_name = ?
        GROUP BY type_of_violation
        ORDER BY cnt DESC
        LIMIT 10
    """, [target["company_name"]])

    lines = [
        f"TRADE WASTE NETWORK — {target['company_name']}",
        f"  BIC #: {bic} | Trade name: {target.get('trade_name', '?')}",
        f"  Address: {target.get('address', '?')} | Records: {target.get('record_count', 0)}",
        "=" * 55,
    ]

    if viol_rows:
        lines.append(f"\nVIOLATIONS ({sum(r[1] for r in viol_rows)}):")
        for r in viol_rows:
            lines.append(f"  {r[0]}: {r[1]} violations, ${r[2]:,.0f} fines")

    if net_rows:
        lines.append(f"\nCONNECTED COMPANIES ({len(net_rows)} via shared properties):")
        for r in net_rows:
            d = dict(zip(net_cols, r))
            lines.append(f"  {d['company_name']} — {d.get('shared_properties', 0)} shared properties")
            if d.get('trade_name'):
                lines.append(f"    Trade name: {d['trade_name']}")
    else:
        lines.append("\nNo connected companies found.")

    return "\n".join(lines)
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add tradewaste_network MCP tool (graph-based waste company connections)"
```

---

## Task 7: Add SharedContractor building edges

Derive building-to-building edges from shared contractors (buildings that used the same DOB-licensed contractor).

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (contractor graph table build section, ~line 2227)
- Modify: `infra/duckdb-server/mcp_server.py:2642-2654` (contractor property graph)

- [ ] **Step 1: Add shared-contractor building edge table**

After the `graph_contractor_shared` build (line ~2241), add:

```python
                # Building-to-building edges via shared contractor
                conn.execute("""
                    CREATE OR REPLACE TABLE main.graph_contractor_building_shared AS
                    SELECT a.bbl AS bbl1, b.bbl AS bbl2,
                           COUNT(DISTINCT a.license) AS shared_contractors
                    FROM main.graph_permit_edges a
                    JOIN main.graph_permit_edges b ON a.license = b.license AND a.bbl < b.bbl
                    GROUP BY a.bbl, b.bbl
                    HAVING COUNT(DISTINCT a.license) >= 2
                """)
                cb_count = conn.execute("SELECT COUNT(*) FROM main.graph_contractor_building_shared").fetchone()[0]
                print(f"Contractor building edges: {cb_count:,} building pairs sharing 2+ contractors", flush=True)
```

- [ ] **Step 2: Add to nyc_building_network property graph**

Expand `nyc_building_network` (lines 2554-2565) to include the new edge:

```python
            CREATE OR REPLACE PROPERTY GRAPH nyc_building_network
            VERTEX TABLES (
                main.graph_buildings PROPERTIES (bbl, housenumber, streetname, zip, boroid, total_units, stories) LABEL Building
            )
            EDGE TABLES (
                main.graph_shared_owner
                    SOURCE KEY (bbl1) REFERENCES main.graph_buildings (bbl)
                    DESTINATION KEY (bbl2) REFERENCES main.graph_buildings (bbl)
                    LABEL SharedOwner,
                main.graph_contractor_building_shared
                    SOURCE KEY (bbl1) REFERENCES main.graph_buildings (bbl)
                    DESTINATION KEY (bbl2) REFERENCES main.graph_buildings (bbl)
                    LABEL SharedContractor
            )
```

- [ ] **Step 3: Add to GRAPH_TABLES cache list**

Find the `GRAPH_TABLES` list (~line 960) and add `"graph_contractor_building_shared"` to it so it persists in the parquet cache.

- [ ] **Step 4: Add fallback**

In the contractor section's except handler, add:

```python
                conn.execute("CREATE OR REPLACE TABLE main.graph_contractor_building_shared AS SELECT NULL::VARCHAR AS bbl1, NULL::VARCHAR AS bbl2, 0::BIGINT AS shared_contractors WHERE FALSE")
```

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add SharedContractor building edges to nyc_building_network"
```

---

## Task 8: Delete stale graph cache + deploy + verify

**Files:** None (operational task)

- [ ] **Step 1: Deploy updated mcp_server.py**

```bash
scp -i ~/.ssh/id_ed25519_hetzner infra/duckdb-server/mcp_server.py fattie@178.156.228.119:/opt/common-ground/duckdb-server/
```

- [ ] **Step 2: Delete stale graph cache and restart**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "
    sudo rm -rf /mnt/data/common-ground/graph-cache
    cd /opt/common-ground && docker compose restart duckdb-server
"
```

Cache must be deleted because the graph table schemas changed (bic_shared_bbl keys, new tables).

- [ ] **Step 3: Wait for graph build and verify**

```bash
# Wait ~5 min for graph build, then:
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "
    docker logs --since 8m common-ground-duckdb-server-1 2>&1 | grep -iE 'property graph|COIB|officer_network|tradewaste'
"
```

Expected: All 10 property graphs created (8 existing + nyc_coib_network + expanded definitions).

- [ ] **Step 4: Smoke test new tools**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "
    curl -s -X POST http://localhost:4213/mcp -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
        -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"search_tools\",\"arguments\":{\"query\":\"officer tradewaste coib\"}}}'
" | head -5
```

- [ ] **Step 5: Commit state**

```bash
git commit --allow-empty -m "docs: graph fixes deployed — 10 property graphs, 19 edge types, 2 new MCP tools"
```
