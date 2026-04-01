# infra/duckdb-server/tools/suggest.py
"""suggest() super tool — onboarding, discovery, and tool recommendations."""

from typing import Annotated
from pydantic import Field
from fastmcp import Context
from fastmcp.tools.tool import ToolResult

from shared.types import VS_CATALOG_DISTANCE


# Pre-built exploration suggestions (curated findings)
_EXPLORATIONS = [
    {
        "title": "Worst landlords in NYC",
        "call": "network(name='', type='worst')",
        "desc": "Ranked by violations, litigation, and rent stabilization losses",
    },
    {
        "title": "Gentrification pressure in East Harlem",
        "call": "neighborhood('10029', view='gentrification')",
        "desc": "Displacement signals: rising rents, eviction rates, demographic shifts",
    },
    {
        "title": "NYPD misconduct patterns",
        "call": "entity(name='', role='cop')",
        "desc": "Officers with most CCRB complaints, federal lawsuits, and settlements",
    },
    {
        "title": "Shell company networks",
        "call": "network(name='', type='corporate')",
        "desc": "LLCs with shared officers hiding property ownership",
    },
    {
        "title": "School performance gaps",
        "call": "school('district 7')",
        "desc": "South Bronx schools: test scores, absenteeism, and survey results",
    },
]

# Topic -> tool recommendations (at least 10 domains)
_TOPIC_MAP = {
    "corruption": [
        ("network(type='political')", "Campaign donation and lobbying networks"),
        ("entity(role='cop')", "Police misconduct records"),
        ("legal(view='settlements')", "Settlement payments by the city"),
        ("civic(view='contracts')", "City contracts and vendor payments"),
    ],
    "housing": [
        ("building('address')", "Building profile with violations and landlord"),
        ("network(type='ownership')", "Landlord portfolio and ownership graph"),
        ("neighborhood('zip', view='gentrification')", "Displacement and gentrification signals"),
        ("services(view='shelter')", "Shelters and housing assistance"),
    ],
    "safety": [
        ("safety('precinct')", "Crime and arrest trends by precinct"),
        ("safety(view='crashes')", "Motor vehicle collision data"),
        ("entity(role='cop')", "Individual officer misconduct"),
        ("legal(view='claims')", "Claims against the city for injuries"),
    ],
    "education": [
        ("school('name or DBN')", "Individual school performance"),
        ("school('district N')", "District-wide aggregates"),
        ("school('zip')", "Schools near a location"),
    ],
    "health": [
        ("health('zip')", "Community health profile"),
        ("health(view='covid')", "COVID data by ZIP"),
        ("health(view='facilities')", "Hospitals and clinics nearby"),
        ("health(view='environmental')", "Lead, asthma, and air quality"),
    ],
    "environment": [
        ("health(view='environmental')", "Lead, asthma, air quality by ZIP"),
        ("neighborhood('zip', view='environment')", "Climate risk and flood zones"),
        ("semantic_search('pollution')", "Environmental complaints and violations"),
        ("health(view='inspections')", "EPA ECHO facility inspections"),
    ],
    "transportation": [
        ("transit('zip')", "Ridership, traffic volume, and infrastructure"),
        ("transit(view='parking')", "Parking violations and enforcement"),
        ("transit(view='crashes')", "Motor vehicle collisions"),
        ("safety(view='crashes')", "Crash injuries and fatalities"),
    ],
    "legal": [
        ("legal(view='litigation')", "Civil litigation against the city"),
        ("legal(view='settlements')", "Settlement payments"),
        ("legal(view='hearings')", "OATH administrative hearings"),
        ("entity(role='judge')", "Judge hearing history and patterns"),
    ],
    "services": [
        ("services('zip')", "All social services near a location"),
        ("services(view='childcare')", "Licensed childcare providers"),
        ("services(view='food')", "SNAP centers, farmers markets, food programs"),
        ("services(view='shelter')", "DHS shelter census and locations"),
    ],
    "money": [
        ("network(type='political')", "Campaign donation networks"),
        ("civic(view='jobs')", "City employee salaries"),
        ("civic(view='contracts')", "City contracts and vendor payments"),
        ("civic(view='budget')", "Revenue and expense budget data"),
    ],
    "finance": [
        ("network(type='political')", "Campaign donation networks"),
        ("civic(view='jobs')", "City employee salaries"),
        ("civic(view='contracts')", "City contracts and vendor payments"),
        ("civic(view='budget')", "Revenue and expense budget data"),
    ],
}


def suggest(
    topic: Annotated[str, Field(
        description="Topic to explore, or empty for a general overview, e.g. 'corruption', 'rent stabilization', 'environmental justice'",
        examples=["corruption", "rent stabilization", "environmental justice", "worst landlords", "school quality"],
    )] = "",
    ctx: Context = None,
) -> ToolResult:
    """Discover what the NYC data lake can tell you and get guided to the right tools. Returns curated findings, tool recommendations, and ready-to-use example calls. Use when unsure which tool fits, when exploring a new topic, or when onboarding a new user. This tool never returns raw data, only recommendations. For actual data retrieval, use the recommended tool instead."""
    if not topic:
        return _overview()
    return _topic_guide(topic, ctx)


def _overview() -> ToolResult:
    """General overview with curated findings."""
    lines = [
        "Common Ground NYC Data Lake: 294 tables, 60M+ rows across 14 schemas.",
        "",
        "Try these explorations:",
    ]
    for exp in _EXPLORATIONS:
        lines.append(f"  {exp['title']}: {exp['call']}")
        lines.append(f"    {exp['desc']}")
    lines.append("")
    lines.append("Or ask about a topic: suggest('corruption'), suggest('housing'), suggest('health')")
    return ToolResult(content="\n".join(lines))


def _topic_guide(topic: str, ctx: Context | None) -> ToolResult:
    """Topic-specific tool recommendations. Uses Lance if available for semantic matching."""
    topic_lower = topic.lower().strip()

    # Direct match in topic map
    for key, recommendations in _TOPIC_MAP.items():
        if key in topic_lower:
            lines = [f"Tools for '{topic}':", ""]
            for call, desc in recommendations:
                lines.append(f"  {call}")
                lines.append(f"    {desc}")
            return ToolResult(content="\n".join(lines))

    # Try Lance semantic matching against catalog embeddings
    embed_fn = ctx.lifespan_context.get("embed_fn") if ctx else None
    if embed_fn:
        try:
            emb_conn = ctx.lifespan_context.get("emb_conn")
            if not emb_conn:
                raise Exception("Embedding DB unavailable")
            query_vec = embed_fn(topic)
            rows = emb_conn.execute(
                "SELECT schema_name, table_name, description, "
                "array_cosine_distance(embedding, ?::FLOAT[]) AS dist "
                "FROM catalog_embeddings "
                "WHERE dist < ? ORDER BY dist LIMIT 5",
                [query_vec.tolist(), VS_CATALOG_DISTANCE],
            ).fetchall()
            if rows:
                lines = [f"Tables related to '{topic}':", ""]
                for row in rows:
                    schema, table, desc, dist = row
                    lines.append(f"  lake.{schema}.{table}")
                    lines.append(f"    {desc}")
                lines.append("")
                lines.append(
                    "Use query(input='SELECT * FROM lake.schema.table LIMIT 20') to explore."
                )
                return ToolResult(content="\n".join(lines))
        except Exception:
            pass

    # Fallback
    return ToolResult(
        content=(
            f"No specific guide for '{topic}'. Try:\n"
            f"  semantic_search('{topic}') to find related content\n"
            f"  query(input='{topic}', mode='catalog') to search table names\n"
            f"  suggest() for a general overview"
        )
    )
