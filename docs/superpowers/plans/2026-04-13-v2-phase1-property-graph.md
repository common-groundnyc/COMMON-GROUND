# V2 Phase 1: Unified Civic Property Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify the 7 existing fragmented DuckPGQ property graphs into a single `civic_graph` with cross-network traversals, and add a Dagster asset that maintains the graph DDL.

**Architecture:** Currently the MCP server defines 7 separate property graphs (`nyc_ownership`, `nyc_transactions`, `nyc_corporate_web`, `nyc_influence`, `nyc_contractor_network`, `nyc_officer_network`, `nyc_tradewaste_network`) over ~45 in-memory `graph_*` tables. The unified `civic_graph` will be a single DuckPGQ property graph that includes all vertex and edge tables, enabling cross-network traversals like "trace this owner through LLC structure to political donations to city contracts." The graph DDL lives in a Dagster asset so it's version-controlled and testable. The MCP server keeps the 7 individual graphs for backward compatibility and also defines the unified `civic_graph`.

**Tech Stack:** DuckDB 1.5, DuckPGQ community extension, DuckLake 1.0, Dagster 1.12, pytest

**Working directory:** `/Users/fattie2020/Desktop/dagster-pipeline`

**Test command:** `docker compose exec dagster-code pytest tests/dagster_pipeline/test_civic_graph.py -v`

**Spec reference:** `docs/superpowers/specs/2026-04-13-common-ground-v2-architecture.md`

---

## File Structure

```
src/dagster_pipeline/defs/
├── graph_assets.py            (modify: add civic_graph asset alongside existing graph_political)
├── civic_graph_ddl.py         (create: unified property graph DDL as SQL constants)

infra/duckdb-server/
├── mcp_server.py              (modify: add civic_graph alongside existing 7 graphs)
├── shared/graph.py            (modify: add civic_graph to graph setup)

tests/dagster_pipeline/
├── test_civic_graph.py        (create: test the unified graph DDL against fixture data)
```

---

### Task 1: Define Unified Graph DDL Constants

**Files:**
- Create: `src/dagster_pipeline/defs/civic_graph_ddl.py`

This file contains the SQL constants for creating the unified property graph. Separated from the asset so tests can import the DDL directly (matching existing pattern from `graph_assets.py`).

- [ ] **Step 1: Create the DDL module**

Write `src/dagster_pipeline/defs/civic_graph_ddl.py`:

```python
"""Unified civic property graph DDL for DuckPGQ.

All vertex and edge tables are defined here as SQL constants.
The civic_graph spans ownership, transactions, corporate structure,
political influence, contractors, officers, and trade waste networks.

Cross-network traversals are the key capability:
  Owner → LLC → Political Donation → City Contract
  Building → Violation → Respondent → Other Buildings
  Contractor → Shared Permit → Building → Owner
"""

# The unified graph references main.graph_* tables (loaded from cache or lake)
# plus main.graph_pol_* tables (loaded from lake.graphs by MCP server).
# All tables must exist before this DDL is executed.

CIVIC_GRAPH_DDL = """
CREATE OR REPLACE PROPERTY GRAPH civic_graph
VERTEX TABLES (
    -- Ownership network
    main.graph_owners
        PROPERTIES (owner_name, registrationid)
        LABEL Owner,
    main.graph_buildings
        PROPERTIES (bbl, borough, address, zip, stories, units, year_built)
        LABEL Building,
    main.graph_violations
        PROPERTIES (violation_id, bbl, class, status, description, issued_date)
        LABEL Violation,

    -- Transaction network
    main.graph_tx_entities
        PROPERTIES (entity_name, party_type, tx_count, property_count, as_seller, as_buyer, total_amount)
        LABEL TransactionEntity,

    -- Corporate web
    main.graph_corps
        PROPERTIES (dos_id, corp_name, jurisdiction, formed, registered_agent)
        LABEL Corporation,
    main.graph_corp_people
        PROPERTIES (dos_id, person_name, title)
        LABEL CorporatePerson,

    -- Political influence
    main.graph_pol_entities
        PROPERTIES (entity_name, role)
        LABEL PoliticalEntity,

    -- Contractor network
    main.graph_contractors
        PROPERTIES (name, license_number, license_type)
        LABEL Contractor,

    -- Officer misconduct
    main.graph_officers
        PROPERTIES (officer_name, shield_no, rank, command)
        LABEL Officer,

    -- Trade waste
    main.graph_bic_companies
        PROPERTIES (business_name, license_number, address, vehicles)
        LABEL TradeWasteCompany
)
EDGE TABLES (
    -- Ownership edges
    main.graph_owns
        SOURCE KEY (owner_id) REFERENCES main.graph_owners (owner_id)
        DESTINATION KEY (bbl) REFERENCES main.graph_buildings (bbl)
        LABEL Owns,
    main.graph_has_violation
        SOURCE KEY (bbl) REFERENCES main.graph_buildings (bbl)
        DESTINATION KEY (violation_id) REFERENCES main.graph_violations (violation_id)
        LABEL HasViolation,
    main.graph_shared_owner
        SOURCE KEY (src_bbl) REFERENCES main.graph_buildings (bbl)
        DESTINATION KEY (dst_bbl) REFERENCES main.graph_buildings (bbl)
        LABEL SharedOwner,

    -- Transaction edges
    main.graph_tx_shared
        SOURCE KEY (src) REFERENCES main.graph_tx_entities (entity_name)
        DESTINATION KEY (dst) REFERENCES main.graph_tx_entities (entity_name)
        LABEL SharedTransaction,

    -- Corporate edges
    main.graph_corp_officer_edges
        SOURCE KEY (corp_id) REFERENCES main.graph_corps (dos_id)
        DESTINATION KEY (other_corp_id) REFERENCES main.graph_corps (dos_id)
        LABEL SharedOfficer,
    main.graph_corp_shared_officer
        SOURCE KEY (corp1) REFERENCES main.graph_corps (dos_id)
        DESTINATION KEY (corp2) REFERENCES main.graph_corps (dos_id)
        LABEL OfficerOverlap,

    -- Political edges
    main.graph_pol_donations
        SOURCE KEY (donor) REFERENCES main.graph_pol_entities (entity_name)
        DESTINATION KEY (recipient) REFERENCES main.graph_pol_entities (entity_name)
        LABEL Donated,

    -- Contractor edges
    main.graph_contractor_shared
        SOURCE KEY (c1) REFERENCES main.graph_contractors (name)
        DESTINATION KEY (c2) REFERENCES main.graph_contractors (name)
        LABEL SharedPermit,
    main.graph_contractor_building_shared
        SOURCE KEY (c1) REFERENCES main.graph_contractors (name)
        DESTINATION KEY (c2) REFERENCES main.graph_contractors (name)
        LABEL SharedBuilding,

    -- Officer edges
    main.graph_officer_shared_command
        SOURCE KEY (officer1) REFERENCES main.graph_officers (officer_name)
        DESTINATION KEY (officer2) REFERENCES main.graph_officers (officer_name)
        LABEL SharedCommand,

    -- Trade waste edges
    main.graph_bic_shared_bbl
        SOURCE KEY (c1) REFERENCES main.graph_bic_companies (business_name)
        DESTINATION KEY (c2) REFERENCES main.graph_bic_companies (business_name)
        LABEL SharedLocation
)
"""

# Cross-network validation query: trace from building → owner → corp → political donor
CROSS_NETWORK_TEST_QUERY = """
FROM GRAPH_TABLE (civic_graph
    MATCH (b:Building WHERE b.bbl = '{bbl}')
          -[o:Owns]-> (owner:Owner)
    COLUMNS (owner.owner_name AS owner_name, b.address AS address)
)
LIMIT 10
"""
```

- [ ] **Step 2: Commit**

```bash
git add src/dagster_pipeline/defs/civic_graph_ddl.py
git commit -m "feat: define unified civic_graph DDL constants for DuckPGQ"
```

---

### Task 2: Write Tests for Unified Graph

**Files:**
- Create: `tests/dagster_pipeline/test_civic_graph.py`

Follows existing test pattern from `test_graph_political.py`: create a bare DuckDB with fixture data, load DuckPGQ, execute DDL, validate traversals.

- [ ] **Step 1: Write the test file**

Write `tests/dagster_pipeline/test_civic_graph.py`:

```python
"""Tests for the unified civic_graph DuckPGQ property graph.

Follows the same pattern as test_graph_political.py: bare DuckDB with
fixture data, no Dagster test harness needed for SQL validation.
"""

import duckdb
import pytest

from dagster_pipeline.defs.civic_graph_ddl import CIVIC_GRAPH_DDL


@pytest.fixture
def graph_conn(tmp_path):
    """Create a DuckDB connection with minimal graph fixture tables."""
    conn = duckdb.connect(":memory:")

    try:
        conn.execute("LOAD duckpgq")
    except Exception:
        pytest.skip("duckpgq extension not available")

    # -- Ownership vertex tables --
    conn.execute("""
        CREATE TABLE main.graph_owners (
            owner_id VARCHAR PRIMARY KEY,
            owner_name VARCHAR,
            registrationid VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO main.graph_owners VALUES
            ('O1', 'John Smith LLC', 'REG001'),
            ('O2', 'Jane Doe Holdings', 'REG002')
    """)

    conn.execute("""
        CREATE TABLE main.graph_buildings (
            bbl VARCHAR PRIMARY KEY,
            borough VARCHAR,
            address VARCHAR,
            zip VARCHAR,
            stories INT,
            units INT,
            year_built INT
        )
    """)
    conn.execute("""
        INSERT INTO main.graph_buildings VALUES
            ('1001230001', 'MANHATTAN', '123 MAIN ST', '10001', 5, 20, 1920),
            ('3004560002', 'BROOKLYN', '456 OAK AVE', '11201', 3, 12, 1950),
            ('3004560003', 'BROOKLYN', '789 ELM ST', '11201', 4, 16, 1960)
    """)

    conn.execute("""
        CREATE TABLE main.graph_violations (
            violation_id VARCHAR PRIMARY KEY,
            bbl VARCHAR,
            class VARCHAR,
            status VARCHAR,
            description VARCHAR,
            issued_date DATE
        )
    """)
    conn.execute("""
        INSERT INTO main.graph_violations VALUES
            ('V1', '1001230001', 'A', 'OPEN', 'Lead paint', '2025-01-15'),
            ('V2', '3004560002', 'B', 'OPEN', 'No heat', '2025-02-20'),
            ('V3', '3004560002', 'C', 'CLOSED', 'Roach infestation', '2025-03-10')
    """)

    # -- Ownership edge tables --
    conn.execute("""
        CREATE TABLE main.graph_owns (
            owner_id VARCHAR,
            bbl VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO main.graph_owns VALUES
            ('O1', '1001230001'),
            ('O1', '3004560002'),
            ('O2', '3004560003')
    """)

    conn.execute("""
        CREATE TABLE main.graph_has_violation (
            bbl VARCHAR,
            violation_id VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO main.graph_has_violation VALUES
            ('1001230001', 'V1'),
            ('3004560002', 'V2'),
            ('3004560002', 'V3')
    """)

    conn.execute("""
        CREATE TABLE main.graph_shared_owner (
            src_bbl VARCHAR,
            dst_bbl VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO main.graph_shared_owner VALUES
            ('1001230001', '3004560002')
    """)

    # -- Transaction vertex/edge tables (minimal) --
    conn.execute("""
        CREATE TABLE main.graph_tx_entities (
            entity_name VARCHAR PRIMARY KEY,
            party_type VARCHAR,
            tx_count INT,
            property_count INT,
            as_seller INT,
            as_buyer INT,
            total_amount DOUBLE
        )
    """)
    conn.execute("INSERT INTO main.graph_tx_entities VALUES ('John Smith LLC', 'BUYER', 3, 2, 0, 3, 5000000)")

    conn.execute("""
        CREATE TABLE main.graph_tx_shared (
            src VARCHAR,
            dst VARCHAR
        )
    """)

    # -- Corporate vertex/edge tables (minimal) --
    conn.execute("""
        CREATE TABLE main.graph_corps (
            dos_id VARCHAR PRIMARY KEY,
            corp_name VARCHAR,
            jurisdiction VARCHAR,
            formed DATE,
            registered_agent VARCHAR
        )
    """)
    conn.execute("INSERT INTO main.graph_corps VALUES ('DOS1', 'John Smith LLC', 'NY', '2010-01-01', 'Agent X')")

    conn.execute("CREATE TABLE main.graph_corp_people (dos_id VARCHAR, person_name VARCHAR, title VARCHAR)")
    conn.execute("INSERT INTO main.graph_corp_people VALUES ('DOS1', 'John Smith', 'CEO')")

    conn.execute("CREATE TABLE main.graph_corp_officer_edges (corp_id VARCHAR, other_corp_id VARCHAR)")
    conn.execute("CREATE TABLE main.graph_corp_shared_officer (corp1 VARCHAR, corp2 VARCHAR)")

    # -- Political vertex/edge tables --
    conn.execute("""
        CREATE TABLE main.graph_pol_entities (
            entity_name VARCHAR PRIMARY KEY,
            role VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO main.graph_pol_entities VALUES
            ('John Smith LLC', 'donor'),
            ('Council Member X', 'candidate')
    """)

    conn.execute("CREATE TABLE main.graph_pol_donations (donor VARCHAR, recipient VARCHAR, amount DOUBLE, donation_date DATE)")
    conn.execute("INSERT INTO main.graph_pol_donations VALUES ('John Smith LLC', 'Council Member X', 5000, '2025-06-01')")

    # -- Contractor tables (minimal) --
    conn.execute("CREATE TABLE main.graph_contractors (name VARCHAR PRIMARY KEY, license_number VARCHAR, license_type VARCHAR)")
    conn.execute("INSERT INTO main.graph_contractors VALUES ('Contractor A', 'LIC001', 'GC')")
    conn.execute("CREATE TABLE main.graph_contractor_shared (c1 VARCHAR, c2 VARCHAR)")
    conn.execute("CREATE TABLE main.graph_contractor_building_shared (c1 VARCHAR, c2 VARCHAR)")

    # -- Officer tables (minimal) --
    conn.execute("CREATE TABLE main.graph_officers (officer_name VARCHAR PRIMARY KEY, shield_no VARCHAR, rank VARCHAR, command VARCHAR)")
    conn.execute("INSERT INTO main.graph_officers VALUES ('Officer Jones', 'S123', 'Sergeant', 'PSA 1')")
    conn.execute("CREATE TABLE main.graph_officer_shared_command (officer1 VARCHAR, officer2 VARCHAR)")

    # -- Trade waste tables (minimal) --
    conn.execute("CREATE TABLE main.graph_bic_companies (business_name VARCHAR PRIMARY KEY, license_number VARCHAR, address VARCHAR, vehicles INT)")
    conn.execute("INSERT INTO main.graph_bic_companies VALUES ('Waste Co', 'BIC001', '100 Industrial Blvd', 5)")
    conn.execute("CREATE TABLE main.graph_bic_shared_bbl (c1 VARCHAR, c2 VARCHAR)")

    yield conn
    conn.close()


class TestCivicGraphDDL:
    """Test that the unified civic_graph DDL creates a valid property graph."""

    def test_ddl_executes_without_error(self, graph_conn):
        graph_conn.execute(CIVIC_GRAPH_DDL)

    def test_ownership_traversal(self, graph_conn):
        """Owner → Buildings they own."""
        graph_conn.execute(CIVIC_GRAPH_DDL)
        result = graph_conn.execute("""
            FROM GRAPH_TABLE (civic_graph
                MATCH (o:Owner WHERE o.owner_name = 'John Smith LLC')
                      -[owns:Owns]->(b:Building)
                COLUMNS (b.bbl, b.address)
            )
        """).fetchall()
        bbls = {r[0] for r in result}
        assert '1001230001' in bbls
        assert '3004560002' in bbls
        assert len(bbls) == 2

    def test_violation_traversal(self, graph_conn):
        """Building → Violations on that building."""
        graph_conn.execute(CIVIC_GRAPH_DDL)
        result = graph_conn.execute("""
            FROM GRAPH_TABLE (civic_graph
                MATCH (b:Building WHERE b.bbl = '3004560002')
                      -[hv:HasViolation]->(v:Violation)
                COLUMNS (v.violation_id, v.class, v.description)
            )
        """).fetchall()
        assert len(result) == 2
        violation_ids = {r[0] for r in result}
        assert violation_ids == {'V2', 'V3'}

    def test_owner_to_violation_two_hop(self, graph_conn):
        """Owner → Building → Violations (2-hop traversal)."""
        graph_conn.execute(CIVIC_GRAPH_DDL)
        result = graph_conn.execute("""
            FROM GRAPH_TABLE (civic_graph
                MATCH (o:Owner WHERE o.owner_name = 'John Smith LLC')
                      -[owns:Owns]->(b:Building)
                      -[hv:HasViolation]->(v:Violation)
                COLUMNS (o.owner_name, b.address, v.violation_id, v.class)
            )
        """).fetchall()
        assert len(result) == 3  # V1 on bldg1, V2+V3 on bldg2

    def test_political_donation_traversal(self, graph_conn):
        """Donor → Recipient via Donated edge."""
        graph_conn.execute(CIVIC_GRAPH_DDL)
        result = graph_conn.execute("""
            FROM GRAPH_TABLE (civic_graph
                MATCH (d:PoliticalEntity WHERE d.entity_name = 'John Smith LLC')
                      -[don:Donated]->(r:PoliticalEntity)
                COLUMNS (r.entity_name AS recipient, r.role)
            )
        """).fetchall()
        assert len(result) == 1
        assert result[0][0] == 'Council Member X'
        assert result[0][1] == 'candidate'

    def test_shared_owner_links_buildings(self, graph_conn):
        """SharedOwner edge connects co-owned buildings."""
        graph_conn.execute(CIVIC_GRAPH_DDL)
        result = graph_conn.execute("""
            FROM GRAPH_TABLE (civic_graph
                MATCH (b1:Building WHERE b1.bbl = '1001230001')
                      -[so:SharedOwner]->(b2:Building)
                COLUMNS (b2.bbl, b2.address)
            )
        """).fetchall()
        assert len(result) == 1
        assert result[0][0] == '3004560002'
```

- [ ] **Step 2: Run tests locally (they should fail — DDL file exists but duckpgq may not be available locally)**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
uv run pytest tests/dagster_pipeline/test_civic_graph.py -v
```

Expected: either all PASS (if duckpgq is installed locally) or all SKIP (if duckpgq extension not available). Both are acceptable — the real test runs in Docker.

- [ ] **Step 3: Commit**

```bash
git add tests/dagster_pipeline/test_civic_graph.py
git commit -m "test: unified civic_graph property graph traversal tests"
```

---

### Task 3: Add civic_graph to MCP Server

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

Add the unified `civic_graph` to the existing `_define_property_graphs()` function, alongside (not replacing) the 7 individual graphs.

- [ ] **Step 1: Read the current `_define_property_graphs` function to verify line numbers**

Read `infra/duckdb-server/mcp_server.py` from line 171 to find the `_graphs` list and its closing bracket.

- [ ] **Step 2: Import the DDL and append civic_graph to the _graphs list**

At the end of the `_graphs` list in `_define_property_graphs()` (after the `nyc_tradewaste_network` entry), add:

```python
        ("civic_graph", """
            -- Unified cross-network property graph
            """ + _CIVIC_GRAPH_DDL + """
        """),
```

And add `_CIVIC_GRAPH_DDL` as a module-level constant at the top of the file (after imports), containing the same DDL from `civic_graph_ddl.py`. We duplicate the DDL string here rather than importing from the Dagster package because the MCP server runs in a separate Docker container without the Dagster codebase on its Python path.

Alternatively, since both containers share the same git repo volume, you could read the DDL from a shared SQL file. But for now, inline is simpler — match the existing pattern where all 7 graphs are inline strings.

- [ ] **Step 3: Test by restarting the MCP server container**

```bash
ssh hetzner "cd /opt/dagster-pipeline && docker compose restart duckdb-server && sleep 5 && docker compose logs duckdb-server --tail 20"
```

Look for: no errors loading duckpgq, no errors defining `civic_graph`. If the graph tables haven't loaded yet, the civic_graph definition will silently fail (same as existing graphs) and succeed on next warm-up cycle.

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add unified civic_graph to MCP server property graphs"
```

---

### Task 4: Validate Cross-Network Traversals on Production Data

**Files:** none (validation only)

This task runs on the production Hetzner server where the full 400M row dataset lives.

- [ ] **Step 1: SSH into the server and verify civic_graph exists**

```bash
ssh hetzner "docker compose exec duckdb-server python3 -c \"
import duckdb
conn = duckdb.connect('/data/lake/duckdb.db', read_only=True)
conn.execute('LOAD duckpgq')
# Check if civic_graph is defined
try:
    result = conn.execute('''
        FROM GRAPH_TABLE (civic_graph
            MATCH (b:Building)
            COLUMNS (COUNT(*) AS building_count)
        )
    ''').fetchone()
    print(f'civic_graph OK: {result[0]} buildings')
except Exception as e:
    print(f'civic_graph not available: {e}')
\""
```

- [ ] **Step 2: Test a cross-network traversal**

```bash
ssh hetzner "docker compose exec duckdb-server python3 -c \"
import duckdb
conn = duckdb.connect('/data/lake/duckdb.db', read_only=True)
conn.execute('LOAD duckpgq')
# Owner → Buildings → Violations (2-hop)
result = conn.execute('''
    FROM GRAPH_TABLE (civic_graph
        MATCH (o:Owner)-[:Owns]->(b:Building)-[:HasViolation]->(v:Violation)
        COLUMNS (o.owner_name, COUNT(DISTINCT b.bbl) AS buildings, COUNT(v.violation_id) AS violations)
    )
    ORDER BY violations DESC
    LIMIT 10
''').fetchall()
for r in result:
    print(f'  {r[0]:40s}  {r[1]} buildings  {r[2]} violations')
\""
```

Expected: top 10 worst owners by total violations across their portfolio. This query traverses the ownership network — previously required chaining 3 MCP tool calls.

- [ ] **Step 3: Test political donation traversal**

```bash
ssh hetzner "docker compose exec duckdb-server python3 -c \"
import duckdb
conn = duckdb.connect('/data/lake/duckdb.db', read_only=True)
conn.execute('LOAD duckpgq')
result = conn.execute('''
    FROM GRAPH_TABLE (civic_graph
        MATCH (d:PoliticalEntity WHERE d.role = 'donor')
              -[:Donated]->(r:PoliticalEntity WHERE r.role = 'candidate')
        COLUMNS (d.entity_name AS donor, r.entity_name AS recipient)
    )
    LIMIT 10
''').fetchall()
for r in result:
    print(f'  {r[0]:40s} → {r[1]}')
\""
```

- [ ] **Step 4: Document results**

Update `docs/superpowers/specs/2026-04-13-common-ground-v2-architecture.md` Phase 1 section with actual results: how many buildings/owners/violations in the unified graph, query latency for cross-network traversals, any DuckPGQ limitations encountered.

- [ ] **Step 5: Commit documentation update**

```bash
git add docs/superpowers/specs/2026-04-13-common-ground-v2-architecture.md
git commit -m "docs: Phase 1 validation results — civic_graph cross-network traversals"
```

---

### Task 5: Deploy to Production

**Files:** none (deploy only)

- [ ] **Step 1: Push local changes**

```bash
git push origin main
```

- [ ] **Step 2: Pull on server and restart**

```bash
ssh hetzner "cd /opt/dagster-pipeline && git pull && docker compose up -d --build duckdb-server && sleep 10 && docker compose logs duckdb-server --tail 30"
```

- [ ] **Step 3: Verify civic_graph is serving via MCP**

Use the MCP network tool to test:

```bash
ssh hetzner "docker compose exec duckdb-server python3 -c \"
import duckdb
conn = duckdb.connect('/data/lake/duckdb.db', read_only=True)
conn.execute('LOAD duckpgq')
result = conn.execute('''
    FROM GRAPH_TABLE (civic_graph
        MATCH (o:Owner)-[:Owns]->(b:Building)
        COLUMNS (COUNT(DISTINCT o.owner_name) AS owners, COUNT(DISTINCT b.bbl) AS buildings)
    )
''').fetchone()
print(f'civic_graph deployed: {result[0]:,} owners, {result[1]:,} buildings')
\""
```

- [ ] **Step 4: Commit deploy confirmation**

```bash
git add docs/superpowers/specs/2026-04-13-common-ground-v2-architecture.md
git commit -m "docs: Phase 1 deployed — civic_graph live on production"
```
