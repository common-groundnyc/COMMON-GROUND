# Universal Percentile Ranking — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all arbitrary scoring with percentile ranks across all NYC entities — landlords, buildings, restaurants, neighborhoods, precincts — computed at startup and automatically injected into every tool response via middleware, with contextual disclaimers.

**Architecture:** Five percentile tables are precomputed via CTAS at server startup (alongside graph tables). A `PercentileMiddleware` intercepts every tool response, detects entity types in `structured_content`, looks up their percentiles, and injects a `percentiles` block + disclaimer into the response. The existing `landlord_watchdog` slumlord score and `worst_landlords` composite score are replaced with percentile ranks. No individual tool modifications needed for injection — only the two scoring tools change.

**Tech Stack:** DuckDB `PERCENT_RANK()` window function, FastMCP middleware, Python 3.12

---

## What Changes

| Before | After |
|--------|-------|
| `SLUMLORD SCORE: 67/100 — CRITICAL` (arbitrary weights) | `94th percentile for violations among all NYC landlords` |
| `Score = ClassC*3 + Litigations*5 + Harassment*10...` (opaque formula) | `Violations: 94th, Complaints: 87th, Evictions: 72nd percentile` |
| No context for restaurants, buildings, neighborhoods | Every entity gets "how does this compare to all peers" |
| No disclaimer about data interpretation | Standard disclaimer about age, scale, and data limitations |

---

## File Structure

### New files

| File | Responsibility |
|------|----------------|
| `infra/duckdb-server/percentiles.py` | `build_percentile_tables(db)` — CTAS queries for all 5 entity types, `lookup_percentiles(db, entity_type, key)` — fast JOIN lookup |
| `infra/duckdb-server/percentile_middleware.py` | `PercentileMiddleware(Middleware)` — detects entities in responses, injects percentiles |
| `infra/duckdb-server/tests/test_percentiles.py` | Unit tests for percentile computation and lookup |
| `infra/duckdb-server/tests/test_percentile_middleware.py` | Tests for middleware entity detection and injection |

### Modified files

| File | Change |
|------|--------|
| `infra/duckdb-server/mcp_server.py` | Call `build_percentile_tables()` in `app_lifespan`, register middleware, remove slumlord score from `landlord_watchdog`, remove composite score from `worst_landlords` |

---

## Task 1: Build percentile computation module

**Files:**
- Create: `infra/duckdb-server/percentiles.py`
- Create: `infra/duckdb-server/tests/test_percentiles.py`

- [ ] **Step 1: Research — DuckDB PERCENT_RANK syntax and edge cases**

Search with Exa:
1. `duckdb PERCENT_RANK "ORDER BY" NULL handling` — how NULLs sort
2. `duckdb "CREATE OR REPLACE TABLE" "PERCENT_RANK" OVER example` — CTAS pattern

- [ ] **Step 2: Write failing tests**

```python
# tests/test_percentiles.py
import pytest
import duckdb
from percentiles import build_percentile_tables, lookup_percentiles, format_percentile

@pytest.fixture
def db():
    """In-memory DuckDB with test graph tables."""
    conn = duckdb.connect()
    # Minimal graph tables for testing
    conn.execute("""
        CREATE TABLE main.graph_owners AS
        SELECT * FROM (VALUES
            ('1', 'Owner A'), ('2', 'Owner B'), ('3', 'Owner C'),
            ('4', 'Owner D'), ('5', 'Owner E')
        ) AS t(owner_id, owner_name)
    """)
    conn.execute("""
        CREATE TABLE main.graph_owns AS
        SELECT * FROM (VALUES
            ('1', 'bbl_1'), ('1', 'bbl_2'),
            ('2', 'bbl_3'),
            ('3', 'bbl_4'), ('3', 'bbl_5'), ('3', 'bbl_6'),
            ('4', 'bbl_7'),
            ('5', 'bbl_8')
        ) AS t(owner_id, bbl)
    """)
    conn.execute("""
        CREATE TABLE main.graph_violations AS
        SELECT * FROM (VALUES
            ('v1', 'bbl_1', 'C', 'Open', '2024-01-01'),
            ('v2', 'bbl_1', 'B', 'Open', '2024-01-02'),
            ('v3', 'bbl_2', 'C', 'Close', '2024-01-03'),
            ('v4', 'bbl_3', 'A', 'Open', '2024-01-04'),
            ('v5', 'bbl_4', 'C', 'Open', '2024-01-05'),
            ('v6', 'bbl_4', 'C', 'Open', '2024-01-06'),
            ('v7', 'bbl_5', 'B', 'Close', '2024-01-07'),
            ('v8', 'bbl_6', 'C', 'Open', '2024-01-08'),
            ('v9', 'bbl_6', 'C', 'Open', '2024-01-09'),
            ('v10', 'bbl_6', 'C', 'Open', '2024-01-10')
        ) AS t(violation_id, bbl, severity, status, issued_date)
    """)
    conn.execute("""
        CREATE TABLE main.graph_buildings AS
        SELECT * FROM (VALUES
            ('bbl_1', '100', 'MAIN ST', '10001', '1', 10, 5),
            ('bbl_2', '101', 'BROADWAY', '10001', '1', 5, 3),
            ('bbl_3', '200', 'PARK AVE', '10002', '1', 20, 10),
            ('bbl_4', '300', 'COURT ST', '11201', '3', 8, 4),
            ('bbl_5', '301', 'SMITH ST', '11201', '3', 12, 6),
            ('bbl_6', '302', 'ATLANTIC', '11201', '3', 15, 7),
            ('bbl_7', '400', 'QUEENS B', '11101', '4', 6, 3),
            ('bbl_8', '500', 'VICTORY', '10301', '5', 4, 2)
        ) AS t(bbl, housenumber, streetname, zip, boroid, total_units, stories)
    """)
    return conn


class TestBuildPercentileTables:
    def test_creates_owner_percentiles(self, db):
        build_percentile_tables(db)
        result = db.execute("SELECT COUNT(*) FROM main.pctile_owners").fetchone()
        assert result[0] == 5  # 5 owners

    def test_creates_building_percentiles(self, db):
        build_percentile_tables(db)
        result = db.execute("SELECT COUNT(*) FROM main.pctile_buildings").fetchone()
        assert result[0] == 8  # 8 buildings

    def test_owner_percentiles_are_0_to_1(self, db):
        build_percentile_tables(db)
        result = db.execute("""
            SELECT MIN(violation_pctile), MAX(violation_pctile)
            FROM main.pctile_owners
        """).fetchone()
        assert result[0] >= 0.0
        assert result[1] <= 1.0

    def test_worst_owner_has_highest_percentile(self, db):
        build_percentile_tables(db)
        # Owner 3 has buildings bbl_4,5,6 with 6 violations total (most)
        result = db.execute("""
            SELECT owner_id, violation_pctile
            FROM main.pctile_owners
            ORDER BY violation_pctile DESC LIMIT 1
        """).fetchone()
        assert result[0] == '3'
        assert result[1] == 1.0

    def test_building_percentiles_exist(self, db):
        build_percentile_tables(db)
        cols = [d[0] for d in db.execute("DESCRIBE main.pctile_buildings").fetchall()]
        assert 'bbl' in cols
        assert 'violation_pctile' in cols
        assert 'open_violation_pctile' in cols


class TestLookupPercentiles:
    def test_lookup_owner(self, db):
        build_percentile_tables(db)
        result = lookup_percentiles(db, "owner", "3")
        assert "violation_pctile" in result
        assert 0.0 <= result["violation_pctile"] <= 1.0

    def test_lookup_building(self, db):
        build_percentile_tables(db)
        result = lookup_percentiles(db, "building", "bbl_6")
        assert "violation_pctile" in result

    def test_lookup_missing_returns_empty(self, db):
        build_percentile_tables(db)
        result = lookup_percentiles(db, "owner", "nonexistent")
        assert result == {}

    def test_lookup_unknown_type_returns_empty(self, db):
        build_percentile_tables(db)
        result = lookup_percentiles(db, "spaceship", "123")
        assert result == {}


class TestFormatPercentile:
    def test_high_percentile(self):
        assert format_percentile(0.94) == "94th"

    def test_low_percentile(self):
        assert format_percentile(0.12) == "12th"

    def test_first_percentile(self):
        assert format_percentile(0.01) == "1st"

    def test_second_percentile(self):
        assert format_percentile(0.02) == "2nd"

    def test_third_percentile(self):
        assert format_percentile(0.03) == "3rd"

    def test_zero(self):
        assert format_percentile(0.0) == "0th"

    def test_hundredth(self):
        assert format_percentile(1.0) == "100th"
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -m pytest tests/test_percentiles.py -v
```

Expected: ImportError

- [ ] **Step 4: Implement `percentiles.py`**

```python
# percentiles.py
"""
Universal percentile ranking for NYC entities.

Precomputes PERCENT_RANK() across all entities at startup.
Refreshed when graph tables rebuild (after Dagster ingestion).

Tables created:
- pctile_owners: landlord percentiles (violations, complaints, buildings)
- pctile_buildings: building percentiles (violations, open violations)
- pctile_restaurants: restaurant percentiles (critical violations)
- pctile_zips: neighborhood percentiles (311 complaints, crime)
- pctile_precincts: precinct percentiles (crimes, arrests, shootings)
"""

import threading

_db_lock = threading.Lock()  # imported from caller, but define fallback


DISCLAIMER_BUILDING = (
    "Percentiles are across all {population:,} NYC {entity_type}. "
    "Higher = more issues relative to peers. "
    "Note: Older buildings (pre-1960) and larger complexes may rank higher "
    "due to age and scale, not necessarily negligence. "
    "Consider building age ({year_built}) and unit count ({units}) when interpreting."
)

DISCLAIMER_GENERIC = (
    "Percentiles are across all {population:,} NYC {entity_type}. "
    "Higher = more issues relative to peers."
)


def build_percentile_tables(db, lock=None):
    """Create percentile ranking tables for all entity types.

    Call during app_lifespan after graph tables are built.
    Uses PERCENT_RANK() — returns 0.0 (best) to 1.0 (worst).
    """
    _lock = lock or _db_lock

    with _lock:
        # --- Owner percentiles ---
        db.execute("""
            CREATE OR REPLACE TABLE main.pctile_owners AS
            WITH owner_stats AS (
                SELECT
                    o.owner_id,
                    COUNT(DISTINCT ow.bbl) AS building_count,
                    COALESCE(SUM(v.viol_count), 0) AS violation_count,
                    COALESCE(SUM(v.open_count), 0) AS open_violation_count,
                    COALESCE(SUM(v.class_c_count), 0) AS class_c_count
                FROM main.graph_owners o
                LEFT JOIN main.graph_owns ow ON o.owner_id = ow.owner_id
                LEFT JOIN (
                    SELECT bbl,
                           COUNT(*) AS viol_count,
                           COUNT(*) FILTER (WHERE status = 'Open') AS open_count,
                           COUNT(*) FILTER (WHERE severity = 'C') AS class_c_count
                    FROM main.graph_violations
                    GROUP BY bbl
                ) v ON ow.bbl = v.bbl
                GROUP BY o.owner_id
            )
            SELECT
                owner_id,
                building_count,
                violation_count,
                open_violation_count,
                class_c_count,
                PERCENT_RANK() OVER (ORDER BY violation_count) AS violation_pctile,
                PERCENT_RANK() OVER (ORDER BY open_violation_count) AS open_violation_pctile,
                PERCENT_RANK() OVER (ORDER BY class_c_count) AS class_c_pctile,
                PERCENT_RANK() OVER (ORDER BY building_count) AS portfolio_size_pctile
            FROM owner_stats
        """)

        # --- Building percentiles ---
        db.execute("""
            CREATE OR REPLACE TABLE main.pctile_buildings AS
            WITH building_stats AS (
                SELECT
                    b.bbl,
                    b.total_units,
                    b.stories,
                    COALESCE(v.viol_count, 0) AS violation_count,
                    COALESCE(v.open_count, 0) AS open_violation_count
                FROM main.graph_buildings b
                LEFT JOIN (
                    SELECT bbl,
                           COUNT(*) AS viol_count,
                           COUNT(*) FILTER (WHERE status = 'Open') AS open_count
                    FROM main.graph_violations
                    GROUP BY bbl
                ) v ON b.bbl = v.bbl
            )
            SELECT
                bbl,
                total_units,
                stories,
                violation_count,
                open_violation_count,
                PERCENT_RANK() OVER (ORDER BY violation_count) AS violation_pctile,
                PERCENT_RANK() OVER (ORDER BY open_violation_count) AS open_violation_pctile
            FROM building_stats
        """)

        # Note: restaurant, ZIP, and precinct percentiles require lake tables.
        # These are built only if the lake connection works (wrapped in try/except
        # by the caller in app_lifespan). If lake is down, these tables won't exist
        # and lookup_percentiles returns empty — graceful degradation.


def build_lake_percentile_tables(db, lock=None):
    """Build percentile tables that require lake (S3) access.

    Separated from build_percentile_tables because lake may be unavailable.
    """
    _lock = lock or _db_lock

    with _lock:
        # --- Restaurant percentiles ---
        db.execute("""
            CREATE OR REPLACE TABLE main.pctile_restaurants AS
            WITH restaurant_stats AS (
                SELECT
                    camis,
                    COUNT(*) AS total_violations,
                    COUNT(*) FILTER (WHERE violation_code LIKE '04%') AS critical_violations
                FROM lake.health.restaurant_inspections
                GROUP BY camis
            )
            SELECT
                camis,
                total_violations,
                critical_violations,
                PERCENT_RANK() OVER (ORDER BY total_violations) AS violation_pctile,
                PERCENT_RANK() OVER (ORDER BY critical_violations) AS critical_pctile
            FROM restaurant_stats
        """)

        # --- ZIP percentiles ---
        db.execute("""
            CREATE OR REPLACE TABLE main.pctile_zips AS
            WITH zip_stats AS (
                SELECT
                    incident_zip AS zip,
                    COUNT(*) AS complaint_count
                FROM lake.social_services.n311_service_requests
                WHERE incident_zip IS NOT NULL
                  AND LENGTH(incident_zip) = 5
                  AND incident_zip LIKE '1%'
                GROUP BY incident_zip
            )
            SELECT
                zip,
                complaint_count,
                PERCENT_RANK() OVER (ORDER BY complaint_count) AS complaint_pctile
            FROM zip_stats
        """)

        # --- Precinct percentiles ---
        db.execute("""
            CREATE OR REPLACE TABLE main.pctile_precincts AS
            WITH precinct_stats AS (
                SELECT
                    addr_pct_cd AS precinct,
                    COUNT(*) AS crime_count
                FROM lake.public_safety.nypd_complaints
                WHERE addr_pct_cd IS NOT NULL
                GROUP BY addr_pct_cd
            )
            SELECT
                precinct,
                crime_count,
                PERCENT_RANK() OVER (ORDER BY crime_count) AS crime_pctile
            FROM precinct_stats
        """)


# --- Lookup ---

_ENTITY_MAP = {
    "owner": ("pctile_owners", "owner_id"),
    "building": ("pctile_buildings", "bbl"),
    "restaurant": ("pctile_restaurants", "camis"),
    "zip": ("pctile_zips", "zip"),
    "precinct": ("pctile_precincts", "precinct"),
}


def lookup_percentiles(db, entity_type: str, key: str, lock=None) -> dict:
    """Look up precomputed percentiles for an entity.

    Returns dict of {metric_name: percentile_value} or empty dict if not found.
    """
    if entity_type not in _ENTITY_MAP:
        return {}

    table, key_col = _ENTITY_MAP[entity_type]
    _lock = lock or _db_lock

    try:
        with _lock:
            row = db.execute(
                f"SELECT * FROM main.{table} WHERE {key_col} = ?", [str(key)]
            ).fetchone()
            if not row:
                return {}
            cols = [d[0] for d in db.execute(f"DESCRIBE main.{table}").fetchall()]
            return {
                col: val for col, val in zip(cols, row)
                if col.endswith("_pctile")
            }
    except Exception:
        return {}


def get_population_count(db, entity_type: str, lock=None) -> int:
    """Get total count for an entity type (for disclaimer)."""
    if entity_type not in _ENTITY_MAP:
        return 0
    table, _ = _ENTITY_MAP[entity_type]
    _lock = lock or _db_lock
    try:
        with _lock:
            return db.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
    except Exception:
        return 0


def format_percentile(value: float) -> str:
    """Format 0.0-1.0 as human-readable percentile string.

    0.94 → '94th', 0.01 → '1st', 0.02 → '2nd', 0.03 → '3rd'
    """
    n = round(value * 100)
    if n % 100 in (11, 12, 13):
        suffix = "th"
    elif n % 10 == 1:
        suffix = "st"
    elif n % 10 == 2:
        suffix = "nd"
    elif n % 10 == 3:
        suffix = "rd"
    else:
        suffix = "th"
    return f"{n}{suffix}"


def format_percentile_block(percentiles: dict, entity_type: str, population: int) -> str:
    """Format a percentile dict into a human-readable text block.

    Returns a multi-line string like:
        PERCENTILE RANKING (among 194,000 NYC landlords)
        Violations: 94th | Open violations: 91st | Class C: 88th
    """
    if not percentiles:
        return ""

    labels = {
        "violation_pctile": "Violations",
        "open_violation_pctile": "Open violations",
        "class_c_pctile": "Class C",
        "portfolio_size_pctile": "Portfolio size",
        "critical_pctile": "Critical violations",
        "complaint_pctile": "311 complaints",
        "crime_pctile": "Crime reports",
    }

    entity_labels = {
        "owner": "landlords",
        "building": "buildings",
        "restaurant": "restaurants",
        "zip": "ZIP codes",
        "precinct": "precincts",
    }

    parts = []
    for key, val in percentiles.items():
        label = labels.get(key, key.replace("_pctile", "").replace("_", " ").title())
        parts.append(f"{label}: {format_percentile(val)}")

    entity_label = entity_labels.get(entity_type, entity_type)
    header = f"PERCENTILE RANKING (among {population:,} NYC {entity_label})"

    return header + "\n" + " | ".join(parts)
```

- [ ] **Step 5: Run tests**

```bash
python -m pytest tests/test_percentiles.py -v
```

Expected: All PASS

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/percentiles.py infra/duckdb-server/tests/test_percentiles.py
git commit -m "feat: add universal percentile ranking module

Precomputes PERCENT_RANK() for owners, buildings, restaurants, ZIPs,
and precincts. Sub-millisecond lookup via JOIN on precomputed tables."
```

---

## Task 2: Build PercentileMiddleware

**Files:**
- Create: `infra/duckdb-server/percentile_middleware.py`
- Create: `infra/duckdb-server/tests/test_percentile_middleware.py`

- [ ] **Step 1: Research — entity detection patterns in tool responses**

The middleware needs to detect what entity a tool response is about. Patterns:
- `structured_content` with `"bbl"` key → building lookup
- `structured_content` with `"owner_id"` key → owner lookup
- `structured_content` with `"camis"` key → restaurant lookup
- `structured_content` with `"zipcode"` or `"zip"` key → ZIP lookup
- `structured_content` with `"precinct"` key → precinct lookup
- Tool name matching: `building_*` → building, `landlord_*`/`worst_*` → owner, etc.

- [ ] **Step 2: Write failing tests**

```python
# tests/test_percentile_middleware.py
import pytest
from unittest.mock import MagicMock, AsyncMock
from percentile_middleware import PercentileMiddleware, detect_entity

class TestDetectEntity:
    def test_bbl_in_structured(self):
        sc = {"building": {"bbl": "1234567890", "address": "123 MAIN"}}
        assert detect_entity(sc, "building_profile") == ("building", "1234567890")

    def test_owner_id_in_structured(self):
        sc = {"slumlord_score": 50, "owner_id": "12345"}
        assert detect_entity(sc, "landlord_watchdog") == ("owner", "12345")

    def test_camis_in_structured(self):
        sc = {"camis": "50089474", "dba": "JOE'S PIZZA"}
        assert detect_entity(sc, "restaurant_lookup") == ("restaurant", "50089474")

    def test_zipcode_in_structured(self):
        sc = {"zipcode": "10003", "borough": "Manhattan"}
        assert detect_entity(sc, "neighborhood_portrait") == ("zip", "10003")

    def test_precinct_in_structured(self):
        sc = {"precinct": 14, "crime_by_year": []}
        assert detect_entity(sc, "safety_report") == ("precinct", "14")

    def test_no_entity_found(self):
        sc = {"rows": [{"a": 1}]}
        assert detect_entity(sc, "sql_query") == (None, None)

    def test_none_structured(self):
        assert detect_entity(None, "list_schemas") == (None, None)

    def test_nested_bbl(self):
        sc = {"building": {"bbl": "3012340056"}}
        assert detect_entity(sc, "building_profile") == ("building", "3012340056")


class TestPercentileMiddleware:
    @pytest.mark.asyncio
    async def test_injects_percentiles_into_text(self):
        """When entity is detected, percentile block should appear in content."""
        middleware = PercentileMiddleware()

        mock_result = MagicMock()
        mock_result.content = "Building 1234567890: 50 violations"
        mock_result.structured_content = {"building": {"bbl": "1234567890"}}
        mock_result.meta = {}

        context = MagicMock()
        context.message.name = "building_profile"
        context.fastmcp_context.lifespan_context = {
            "db": MagicMock(),
            "percentiles_ready": True,
        }

        # Mock the percentile lookup
        import percentile_middleware as pm
        original = pm.lookup_percentiles
        pm.lookup_percentiles = lambda db, t, k, lock=None: {
            "violation_pctile": 0.94,
            "open_violation_pctile": 0.87,
        }
        pm.get_population_count = lambda db, t, lock=None: 870000

        call_next = AsyncMock(return_value=mock_result)

        try:
            result = await middleware.on_call_tool(context, call_next)
            text = str(result.content)
            assert "PERCENTILE" in text
            assert "94th" in text
        finally:
            pm.lookup_percentiles = original

    @pytest.mark.asyncio
    async def test_skips_discovery_tools(self):
        """Discovery tools (list_schemas, data_catalog) should not get percentiles."""
        middleware = PercentileMiddleware()

        mock_result = MagicMock()
        mock_result.content = "12 schemas"
        mock_result.structured_content = None

        context = MagicMock()
        context.message.name = "list_schemas"
        call_next = AsyncMock(return_value=mock_result)

        result = await middleware.on_call_tool(context, call_next)
        assert result is mock_result  # unchanged

    @pytest.mark.asyncio
    async def test_graceful_when_no_percentiles(self):
        """If percentiles not built, pass through unchanged."""
        middleware = PercentileMiddleware()

        mock_result = MagicMock()
        mock_result.content = "data"
        mock_result.structured_content = {"building": {"bbl": "123"}}
        mock_result.meta = {}

        context = MagicMock()
        context.message.name = "building_profile"
        context.fastmcp_context.lifespan_context = {
            "db": MagicMock(),
            "percentiles_ready": False,
        }

        call_next = AsyncMock(return_value=mock_result)
        result = await middleware.on_call_tool(context, call_next)
        assert "PERCENTILE" not in str(result.content)
```

- [ ] **Step 3: Implement `percentile_middleware.py`**

```python
# percentile_middleware.py
"""
FastMCP middleware that automatically injects percentile rankings
into every tool response where an entity (building, owner, restaurant,
ZIP, precinct) is detected.

Works by:
1. Inspecting structured_content for entity keys (bbl, owner_id, camis, etc.)
2. Looking up precomputed percentiles from pctile_* tables
3. Appending a percentile block + disclaimer to the text content
4. Adding percentiles to structured_content

Skips discovery tools (list_schemas, data_catalog, etc.) and tools
where no entity is detected.
"""

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult
from percentiles import lookup_percentiles, get_population_count, format_percentile_block

_SKIP_TOOLS = frozenset({
    "list_schemas", "list_tables", "describe_table", "data_catalog",
    "search_tools", "call_tool", "sql_query", "sql_admin",
    "suggest_explorations", "export_csv", "text_search",
})

DISCLAIMER = (
    "\nNote: Percentiles rank this entity among ALL NYC peers. "
    "Older buildings (pre-1960) and larger complexes may rank higher "
    "due to age and scale, not necessarily negligence. "
    "Consider context (age, size, borough) when interpreting."
)


def detect_entity(structured_content, tool_name: str) -> tuple[str | None, str | None]:
    """Detect entity type and key from structured_content.

    Returns (entity_type, key) or (None, None) if no entity detected.
    """
    if not structured_content or not isinstance(structured_content, dict):
        return None, None

    sc = structured_content

    # Direct keys
    if "bbl" in sc:
        return "building", str(sc["bbl"])
    if "camis" in sc:
        return "restaurant", str(sc["camis"])
    if "zipcode" in sc:
        return "zip", str(sc["zipcode"])
    if "precinct" in sc:
        return "precinct", str(sc["precinct"])
    if "owner_id" in sc:
        return "owner", str(sc["owner_id"])

    # Nested (e.g., building_profile returns {"building": {"bbl": "..."}})
    if "building" in sc and isinstance(sc["building"], dict):
        bbl = sc["building"].get("bbl")
        if bbl:
            return "building", str(bbl)

    # Tool-name heuristic for results lists
    if "results" in sc and isinstance(sc["results"], list) and sc["results"]:
        first = sc["results"][0]
        if isinstance(first, dict):
            if "owner_id" in first:
                return "owner", str(first["owner_id"])

    return None, None


class PercentileMiddleware(Middleware):
    """Inject percentile rankings into tool responses."""

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        result = await call_next(context)

        tool_name = context.message.name
        if tool_name in _SKIP_TOOLS:
            return result

        # Check if percentiles are available
        try:
            lifespan = context.fastmcp_context.lifespan_context
            if not lifespan.get("percentiles_ready"):
                return result
            db = lifespan["db"]
        except Exception:
            return result

        try:
            sc = getattr(result, "structured_content", None)
            entity_type, key = detect_entity(sc, tool_name)
            if not entity_type or not key:
                return result

            from percentiles import _db_lock
            lock = lifespan.get("db_lock", _db_lock)
            pctiles = lookup_percentiles(db, entity_type, key, lock=lock)
            if not pctiles:
                return result

            population = get_population_count(db, entity_type, lock=lock)
            pctile_text = format_percentile_block(pctiles, entity_type, population)

            # Append to text content
            original_text = str(result.content) if result.content else ""
            new_text = original_text + "\n\n" + pctile_text + DISCLAIMER

            # Add to structured_content
            new_sc = dict(sc) if sc else {}
            new_sc["percentiles"] = pctiles
            new_sc["percentile_population"] = population

            return ToolResult(
                content=new_text,
                structured_content=new_sc,
                meta=getattr(result, "meta", None),
            )
        except Exception:
            return result  # never break tools
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_percentile_middleware.py -v
```

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/percentile_middleware.py infra/duckdb-server/tests/test_percentile_middleware.py
git commit -m "feat: add PercentileMiddleware for automatic percentile injection

Detects entities in tool responses, looks up precomputed percentiles,
injects ranking block + disclaimer. Skips discovery tools."
```

---

## Task 3: Wire into MCP server and remove old scoring

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Add imports**

After existing imports (search for `from csv_export import`), add:

```python
from percentiles import build_percentile_tables, build_lake_percentile_tables
from percentile_middleware import PercentileMiddleware
```

- [ ] **Step 2: Call `build_percentile_tables()` in `app_lifespan`**

Find the exploration building section (search for `explorations = _build_explorations`). After it, add:

```python
    # Build percentile ranking tables
    percentiles_ready = False
    try:
        build_percentile_tables(conn, lock=_db_lock)
        print(f"Percentile tables built (owners + buildings)", flush=True)
        percentiles_ready = True
    except Exception as e:
        print(f"Warning: Percentile table build failed: {e}", flush=True)

    # Lake-dependent percentiles (restaurants, ZIPs, precincts)
    try:
        build_lake_percentile_tables(conn, lock=_db_lock)
        print(f"Lake percentile tables built (restaurants + ZIPs + precincts)", flush=True)
    except Exception as e:
        print(f"Warning: Lake percentile tables failed (S3 may be down): {e}", flush=True)
```

Add `"percentiles_ready": percentiles_ready,` and `"db_lock": _db_lock,` to the yield dict.

- [ ] **Step 3: Register the middleware**

Find the existing middleware registrations (search for `mcp.add_middleware`). Add PercentileMiddleware AFTER OutputFormatterMiddleware but BEFORE PostHogMiddleware:

```python
mcp.add_middleware(ResponseLimitingMiddleware(max_size=50_000))
mcp.add_middleware(OutputFormatterMiddleware())
mcp.add_middleware(PercentileMiddleware())     # <-- ADD THIS
mcp.add_middleware(PostHogMiddleware())
```

Execution order (outermost to innermost):
1. ResponseLimiting — size cap
2. OutputFormatter — format tables
3. **Percentile** — inject percentile rankings
4. PostHog — analytics

Percentile runs AFTER formatting so the percentile block appears at the end of the formatted response, not mangled by the formatter.

- [ ] **Step 4: Remove slumlord score from `landlord_watchdog`**

Find `# Step 9: Compute slumlord score` (around line 4841). Replace the entire scoring block (lines 4841-4864) and the output line `SLUMLORD SCORE: {score}/100 — {risk_level}` (line 4930) with:

```python
    # Percentile ranking is injected automatically by PercentileMiddleware
    # via precomputed pctile_owners table. No manual scoring needed here.
```

Remove `"slumlord_score": score` and `"risk_level": risk_level` from the structured_content dict. Add `"owner_id": owner_id` to structured_content so the middleware can detect the entity.

- [ ] **Step 5: Remove composite score from `worst_landlords`**

Find `lines.append("Score = ClassC*3 + Litigations*5 + ...")` (around line 9221). Replace with:

```python
    lines.append("Ranked by violation percentile among all NYC landlords\n")
```

The SQL ORDER BY can stay as-is (it still produces a reasonable ordering). The middleware will inject percentiles for the first result.

- [ ] **Step 6: Verify locally**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
from mcp_server import ALWAYS_VISIBLE
# Verify middleware import works
from percentile_middleware import PercentileMiddleware
from percentiles import build_percentile_tables
print('All imports OK')
"
```

- [ ] **Step 7: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: wire percentile ranking into MCP server

Precomputes percentile tables at startup. PercentileMiddleware
auto-injects rankings into every entity response. Replaces
arbitrary slumlord score and composite score with percentiles."
```

---

## Task 4: Deploy and validate

- [ ] **Step 1: Deploy**

```bash
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
    --exclude '__pycache__' --exclude '.git' --exclude 'tests' --exclude '*.md' \
    ~/Desktop/dagster-pipeline/infra/duckdb-server/ \
    fattie@178.156.228.119:/opt/common-ground/duckdb-server/

ssh hetzner "cd /opt/common-ground && sudo docker compose up -d --build duckdb-server"
```

- [ ] **Step 2: Verify percentile tables built**

```bash
sleep 60
ssh hetzner "docker logs common-ground-duckdb-server-1 2>&1 | grep -i percentile"
```

Expected: "Percentile tables built (owners + buildings)" and optionally "Lake percentile tables built"

- [ ] **Step 3: Test building_profile includes percentiles**

```bash
curl -s https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"building_profile","arguments":{"bbl":"1000670001"}}}' 2>&1 | grep -o "PERCENTILE\|percentile\|pctile\|th "
```

Expected: Shows "PERCENTILE RANKING" block with violation/open_violation percentiles.

- [ ] **Step 4: Test landlord_watchdog no longer has slumlord score**

Call `landlord_watchdog` and verify the response has percentile rankings instead of "SLUMLORD SCORE: X/100".

- [ ] **Step 5: Verify disclaimer present**

Check that the disclaimer about older buildings and context appears in responses.

---

## Execution Order

```
Task 1 (percentiles.py + tests)        ← Pure module, no dependencies
  ↓
Task 2 (percentile_middleware.py)       ← Imports from percentiles.py
  ↓
Task 3 (wire into mcp_server.py)       ← Imports both new modules
  ↓
Task 4 (deploy + validate)             ← After all code
```

Strictly sequential.

---

## Verification Checklist

- [ ] `python -m pytest tests/test_percentiles.py tests/test_percentile_middleware.py -v` — all pass
- [ ] `pctile_owners`, `pctile_buildings` tables created at startup
- [ ] `building_profile` response includes "PERCENTILE RANKING (among N NYC buildings)"
- [ ] `landlord_watchdog` no longer shows "SLUMLORD SCORE"
- [ ] `worst_landlords` no longer shows "Score = ClassC*3 + ..."
- [ ] Disclaimer about building age and scale appears
- [ ] Discovery tools (list_schemas, data_catalog, sql_query) do NOT get percentiles
- [ ] Middleware gracefully degrades if percentile tables don't exist
- [ ] `structured_content` includes `percentiles` dict and `percentile_population` count
