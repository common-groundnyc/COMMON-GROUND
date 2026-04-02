"""semantic_search() super tool — absorbs text_search, semantic_search,
fuzzy_entity_search, and suggest_explorations into one dispatch."""

import re
import time
from typing import Annotated, Literal

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import execute, safe_query
from shared.formatting import make_result
from shared.lance import vector_expand_names, lance_route_entity
from shared.types import VS_NAME_DISTANCE, VS_CATALOG_DISTANCE, MAX_LLM_ROWS

# ---------------------------------------------------------------------------
# Valid source filters for Lance indices
# ---------------------------------------------------------------------------

VALID_DESC_SOURCES = frozenset({"311", "restaurant", "hpd", "oath"})
VALID_ENTITY_SOURCES = frozenset({"owner", "corp_officer", "donor", "tx_party"})

# Keywords that signal financial/consumer complaints
_FINANCIAL_KEYWORDS = re.compile(
    r"\b(credit|bank|debt|loan|mortgage|foreclos|identity.?theft|fraud|billing"
    r"|collection|student.?loan|payday|cfpb|financial|money|interest.?rate"
    r"|credit.?card|credit.?report|account)\b",
    re.IGNORECASE,
)

# Keywords that signal violations/inspections
_VIOLATION_KEYWORDS = re.compile(
    r"\b(violation|inspection|restaurant|health|roach|mice|rat|pest|mold"
    r"|bedbug|lead|asbestos|elevator|heat|hot.?water|plumbing|fire.?escape"
    r"|building|landlord|hpd|oath|hearing|fine|penalty|code)\b",
    re.IGNORECASE,
)

# Heuristic: query looks like a person/company name (2+ capitalized words, no complaint keywords)
_NAME_PATTERN = re.compile(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+$")



def semantic_search(
    query: Annotated[str, Field(
        description="Natural language search query, finds similar content even with different words, e.g. 'apartments with mold problems', 'noise complaints late night'",
        examples=["apartments with mold problems", "noise complaints late night", "identity theft credit report", "rat infestation kitchen"],
    )],
    domain: Annotated[
        Literal["auto", "complaints", "violations", "entities", "explore"],
        Field(
            default="auto",
            description="'auto' routes to the best corpus based on query content. 'complaints' searches 311 and CFPB complaints. 'violations' searches HPD, restaurant, and OATH violations. 'entities' does fuzzy name matching across all tables. 'explore' returns pre-computed interesting findings.",
        )
    ] = "auto",
    limit: Annotated[int, Field(
        description="Maximum results 1-100, e.g. 20",
        ge=1, le=100,
    )] = 20,
    ctx: Context = None,
) -> ToolResult:
    """Search across all NYC data using natural language, powered by Lance vector index and DuckDB hybrid search. Finds similar content even when exact keywords do not match. Use when looking for complaints, violations, or entities by concept rather than exact lookup. Do NOT use for specific building or person lookups (use building or entity) or structured SQL queries (use query). Default auto-routes to the best corpus based on the query."""
    if domain == "explore":
        return _explore(ctx)

    stripped = query.strip()
    if not stripped:
        raise ToolError("Search query cannot be empty")

    if domain == "auto":
        domain = _detect_domain(stripped)

    if domain == "entities":
        return _entity_search(stripped, limit, ctx)
    if domain == "complaints":
        return _complaint_search(stripped, limit, ctx)
    if domain == "violations":
        return _violation_search(stripped, limit, ctx)

    # Fallback: try both complaints and violations
    return _combined_search(stripped, limit, ctx)


# ---------------------------------------------------------------------------
# Auto-routing
# ---------------------------------------------------------------------------


def _detect_domain(query: str) -> str:
    """Detect the best domain for a query based on keyword heuristics."""
    if _NAME_PATTERN.match(query):
        return "entities"
    if _FINANCIAL_KEYWORDS.search(query) and not _VIOLATION_KEYWORDS.search(query):
        return "complaints"
    if _VIOLATION_KEYWORDS.search(query) and not _FINANCIAL_KEYWORDS.search(query):
        return "violations"
    # Ambiguous — run both
    return "combined"


# ---------------------------------------------------------------------------
# Domain: complaints (311 + CFPB)
# ---------------------------------------------------------------------------


def _complaint_search(query: str, limit: int, ctx: Context) -> ToolResult:
    pool = ctx.lifespan_context["pool"]
    t0 = time.time()
    pattern = f"%{query}%"
    all_results: list[tuple] = []

    # CFPB financial complaints
    try:
        _, rows_c = execute(
            pool,
            """
            SELECT 'CFPB' AS source, complaint_id AS id, product AS category,
                   company AS entity, date_received AS date,
                   LEFT(complaint_what_happened, 250) AS excerpt
            FROM lake.financial.cfpb_complaints_ny
            WHERE complaint_what_happened ILIKE ?
               OR product ILIKE ?
               OR issue ILIKE ?
            ORDER BY date_received DESC
            LIMIT ?
            """,
            [pattern, pattern, pattern, min(limit, 100)],
        )
        all_results.extend(rows_c)
    except Exception:
        pass

    # 311 service requests (keyword search)
    try:
        _, rows_311 = execute(
            pool,
            """
            SELECT '311' AS source, unique_key AS id, complaint_type AS category,
                   agency_name AS entity, created_date AS date,
                   LEFT(descriptor, 250) AS excerpt
            FROM lake.social_services.n311_service_requests
            WHERE descriptor ILIKE ?
               OR complaint_type ILIKE ?
            ORDER BY created_date DESC
            LIMIT ?
            """,
            [pattern, pattern, min(limit, 100)],
        )
        all_results.extend(rows_311)
    except Exception:
        pass

    # Semantic enhancement via Lance
    semantic_suggestions = _semantic_enhance(query, ctx, source_filter="WHERE source IN ('cfpb', '311')")

    elapsed = round((time.time() - t0) * 1000)
    return _format_text_results(query, all_results, limit, elapsed, "complaints", semantic_suggestions)


# ---------------------------------------------------------------------------
# Domain: violations (HPD + restaurant + OATH)
# ---------------------------------------------------------------------------


def _violation_search(query: str, limit: int, ctx: Context) -> ToolResult:
    pool = ctx.lifespan_context["pool"]
    t0 = time.time()
    pattern = f"%{query}%"
    all_results: list[tuple] = []

    # Restaurant inspection violations
    try:
        _, rows_r = execute(
            pool,
            """
            SELECT 'Restaurant' AS source,
                   camis AS id,
                   cuisine_description AS category,
                   dba || ' (' || boro || ' ' || zipcode || ')' AS entity,
                   inspection_date AS date,
                   LEFT(violation_description, 250) AS excerpt
            FROM lake.health.restaurant_inspections
            WHERE violation_description ILIKE ?
               OR dba ILIKE ?
               OR cuisine_description ILIKE ?
            ORDER BY inspection_date DESC
            LIMIT ?
            """,
            [pattern, pattern, pattern, min(limit, 100)],
        )
        all_results.extend(rows_r)
    except Exception:
        pass

    # HPD violations (keyword search)
    try:
        _, rows_hpd = execute(
            pool,
            """
            SELECT 'HPD' AS source,
                   violationid AS id,
                   novdescription AS category,
                   boroid || '-' || block || '-' || lot AS entity,
                   inspectiondate AS date,
                   LEFT(novdescription, 250) AS excerpt
            FROM lake.housing.hpd_violations
            WHERE novdescription ILIKE ?
            ORDER BY inspectiondate DESC
            LIMIT ?
            """,
            [pattern, min(limit, 100)],
        )
        all_results.extend(rows_hpd)
    except Exception:
        pass

    # OATH/ECB hearings (keyword search)
    try:
        _, rows_oath = execute(
            pool,
            """
            SELECT 'OATH' AS source,
                   ticket_number AS id,
                   violation_description AS category,
                   respondent_name AS entity,
                   violation_date AS date,
                   LEFT(violation_description, 250) AS excerpt
            FROM lake.city_government.oath_ecb_hearings
            WHERE violation_description ILIKE ?
               OR respondent_name ILIKE ?
            ORDER BY violation_date DESC
            LIMIT ?
            """,
            [pattern, pattern, min(limit, 100)],
        )
        all_results.extend(rows_oath)
    except Exception:
        pass

    # Semantic enhancement via Lance
    semantic_suggestions = _semantic_enhance(query, ctx, source_filter="WHERE source IN ('restaurant', 'hpd', 'oath')")

    elapsed = round((time.time() - t0) * 1000)
    return _format_text_results(query, all_results, limit, elapsed, "violations", semantic_suggestions)


# ---------------------------------------------------------------------------
# Domain: entities (Lance fuzzy name matching)
# ---------------------------------------------------------------------------


def _entity_search(name: str, limit: int, ctx: Context) -> ToolResult:
    embed_fn = ctx.lifespan_context.get("embed_fn")
    if embed_fn is None:
        raise ToolError(
            "Fuzzy entity search is unavailable — embedding model not loaded. "
            "Use entity_xray() for exact name matching instead."
        )

    t0 = time.time()
    vec = embed_fn(name)
    emb_conn = ctx.lifespan_context.get("emb_conn")
    if not emb_conn:
        raise ToolError("Embedding database not available")

    capped = min(limit, 50)
    try:
        result = emb_conn.execute(
            "SELECT sources, name, "
            "GREATEST(0, 1.0 / (1.0 + array_cosine_distance(embedding, ?::FLOAT[]))) AS similarity "
            "FROM entity_names ORDER BY array_cosine_distance(embedding, ?::FLOAT[]) LIMIT ?",
            [vec.tolist(), vec.tolist(), capped],
        )
        cols = [d[0] for d in result.description]
        rows = result.fetchall()
    except Exception as e:
        raise ToolError(f"Fuzzy entity search failed: {e}")

    elapsed = round((time.time() - t0) * 1000)

    if not rows:
        return ToolResult(
            content=f"No similar entities found for '{name}'. Try a shorter name or check spelling.",
            meta={"query_time_ms": elapsed},
        )

    lines = [f"Top {len(rows)} similar entities for '{name}' ({elapsed}ms)\n"]
    for i, row in enumerate(rows, 1):
        rec = dict(zip(cols, row))
        pct = round(rec["similarity"] * 100, 1)
        lines.append(f"{i}. [{rec['sources']}] {rec['name']} — {pct}% similarity")

    lines.append(
        "\nUse entity_xray(name) to get the full X-ray for any matched entity."
    )

    content = "\n".join(lines)
    structured = [dict(zip(cols, row)) for row in rows]

    return ToolResult(
        content=content,
        structured_content=structured,
        meta={"query_time_ms": elapsed, "result_count": len(rows), "domain": "entities"},
    )


# ---------------------------------------------------------------------------
# Domain: explore (pre-computed findings)
# ---------------------------------------------------------------------------


EXPLORATION_QUERIES = [
    (
        "Worst landlords by open violations",
        """SELECT o.owner_name, COUNT(DISTINCT b.bbl) AS buildings,
                  COUNT(*) FILTER (WHERE v.status = 'Open') AS open_violations,
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
        """WITH deed_sales AS (
               SELECT (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                      TRY_CAST(m.document_amt AS DOUBLE) AS price,
                      TRY_CAST(m.document_date AS DATE) AS sale_date
               FROM lake.housing.acris_master m
               JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
               WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
                 AND TRY_CAST(m.document_amt AS DOUBLE) > 50000
                 AND TRY_CAST(m.document_date AS DATE) >= '2015-01-01'
           ),
           ranked AS (
               SELECT bbl, price, sale_date,
                      LAG(price) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_price,
                      LAG(sale_date) OVER (PARTITION BY bbl ORDER BY sale_date) AS prev_date
               FROM deed_sales
           )
           SELECT bbl,
                  prev_date AS buy_date, sale_date AS sell_date,
                  prev_price AS buy_price, price AS sell_price,
                  (price - prev_price) AS profit,
                  ((price - prev_price) / prev_price * 100)::INT AS profit_pct
           FROM ranked
           WHERE prev_price IS NOT NULL
             AND price > prev_price
             AND DATEDIFF('month', prev_date, sale_date) <= 24
             AND DATEDIFF('month', prev_date, sale_date) > 0
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
        "Try: restaurant_lookup(name) or semantic_search('mice kitchen', domain='violations')",
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
        """SELECT COUNT(*) AS total_corps,
                  COUNT(DISTINCT dos_process_name) AS distinct_agents
           FROM lake.business.nys_corporations
           WHERE jurisdiction = 'NEW YORK'
             AND dos_process_name IS NOT NULL""",
        "Active NYC corporations and how many share registered agents — a signal for shell company networks.",
        "Try: shell_detector() or corporate_web(name)",
    ),
]


def _explore(ctx: Context) -> ToolResult:
    # Try pre-computed explorations from lifespan context first
    explorations = ctx.lifespan_context.get("explorations", [])

    if not explorations:
        # Compute on demand
        pool = ctx.lifespan_context["pool"]
        explorations = _build_explorations(pool)

    if not explorations:
        return ToolResult(content="No pre-computed explorations available. Try query(domain='catalog') to browse schemas.")

    lines = ["Here are some interesting things in the NYC data lake:\n"]
    for i, exp in enumerate(explorations, 1):
        lines.append(f"**{i}. {exp['title']}**")
        lines.append(exp["description"])
        if exp.get("sample"):
            lines.append(f"Preview: {exp['sample']}")
        if exp.get("follow_up"):
            lines.append(f"Dig deeper: {exp['follow_up']}")
        lines.append("")

    return ToolResult(content="\n".join(lines))


def _build_explorations(pool: object) -> list[dict]:
    """Compute interesting data highlights by running exploration queries."""
    highlights = []
    for title, sql, description, follow_up in EXPLORATION_QUERIES:
        try:
            _, rows = safe_query(pool, sql)
            highlights.append({
                "title": title,
                "description": description,
                "sample": str(rows[:3]) if rows else "No data",
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


# ---------------------------------------------------------------------------
# Combined search (auto-routing fallback)
# ---------------------------------------------------------------------------


def _combined_search(query: str, limit: int, ctx: Context) -> ToolResult:
    """Run both complaints and violations, merge the best results."""
    pool = ctx.lifespan_context["pool"]
    t0 = time.time()
    pattern = f"%{query}%"
    all_results: list[tuple] = []

    # CFPB
    try:
        _, rows_c = execute(
            pool,
            """
            SELECT 'CFPB' AS source, complaint_id AS id, product AS category,
                   company AS entity, date_received AS date,
                   LEFT(complaint_what_happened, 250) AS excerpt
            FROM lake.financial.cfpb_complaints_ny
            WHERE complaint_what_happened ILIKE ?
               OR product ILIKE ?
               OR issue ILIKE ?
            ORDER BY date_received DESC
            LIMIT ?
            """,
            [pattern, pattern, pattern, min(limit, 100)],
        )
        all_results.extend(rows_c)
    except Exception:
        pass

    # Restaurant violations
    try:
        _, rows_r = execute(
            pool,
            """
            SELECT 'Restaurant' AS source,
                   camis AS id,
                   cuisine_description AS category,
                   dba || ' (' || boro || ' ' || zipcode || ')' AS entity,
                   inspection_date AS date,
                   LEFT(violation_description, 250) AS excerpt
            FROM lake.health.restaurant_inspections
            WHERE violation_description ILIKE ?
               OR dba ILIKE ?
               OR cuisine_description ILIKE ?
            ORDER BY inspection_date DESC
            LIMIT ?
            """,
            [pattern, pattern, pattern, min(limit, 100)],
        )
        all_results.extend(rows_r)
    except Exception:
        pass

    # Semantic enhancement
    semantic_suggestions = _semantic_enhance(query, ctx)

    elapsed = round((time.time() - t0) * 1000)
    return _format_text_results(query, all_results, limit, elapsed, "all", semantic_suggestions)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _semantic_enhance(query: str, ctx: Context, source_filter: str = "") -> list[tuple[str, str, int]]:
    """Run vector similarity to find related description categories."""
    embed_fn = ctx.lifespan_context.get("embed_fn")
    emb_conn = ctx.lifespan_context.get("emb_conn")
    if not embed_fn or not emb_conn:
        return []
    try:
        query_vec = embed_fn(query)
        where = f"WHERE source = '{source_filter}'" if source_filter and source_filter in VALID_DESC_SOURCES else ""
        rows = emb_conn.execute(
            f"SELECT source, description, array_cosine_distance(embedding, ?::FLOAT[]) AS distance "
            f"FROM description_embeddings {where} "
            f"ORDER BY distance LIMIT 5",
            [query_vec.tolist()],
        ).fetchall()
        return [
            (src, desc, round(max(0, 1.0 / (1.0 + dist)) * 100))
            for src, desc, dist in rows
            if dist < VS_CATALOG_DISTANCE
        ]
    except Exception:
        return []


def _format_text_results(
    query: str,
    all_results: list[tuple],
    limit: int,
    elapsed: int,
    domain_label: str,
    semantic_suggestions: list[tuple[str, str, int]],
) -> ToolResult:
    """Format keyword search results into a ToolResult."""
    if not all_results:
        msg = f"No results for '{query}'. Try broader terms."
        if semantic_suggestions:
            msg += "\n\nRelated categories (semantic match):"
            for src, desc, sim in semantic_suggestions:
                msg += f"\n  [{src}] ({sim}% similar) {desc}"
        return ToolResult(content=msg, meta={"query_time_ms": elapsed})

    truncated = all_results[:limit]
    out_cols = ["source", "id", "category", "entity", "date", "excerpt"]

    lines = [
        f"Found {len(truncated)} results for '{query}' ({elapsed}ms)",
        f"Domain: {domain_label}",
    ]

    if semantic_suggestions:
        lines.append("\nRelated categories (semantic match):")
        for src, desc, sim in semantic_suggestions:
            lines.append(f"  [{src}] ({sim}% similar) {desc}")

    summary = "\n".join(lines)
    return make_result(summary, out_cols, truncated, {"query_time_ms": elapsed, "domain": domain_label})
