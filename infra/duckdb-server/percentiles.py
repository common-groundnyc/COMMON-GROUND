"""Universal percentile ranking module for the DuckDB MCP server."""

DISCLAIMER = (
    "\nNote: Percentiles rank this entity among ALL NYC peers. "
    "Older buildings (pre-1960) and larger complexes may rank higher "
    "due to age and scale, not necessarily negligence. "
    "Consider context (age, size, borough) when interpreting."
)

_ENTITY_TABLE_MAP = {
    "owner": ("pctile_owners", "owner_id"),
    "building": ("pctile_buildings", "bbl"),
    "restaurant": ("pctile_restaurants", "camis"),
    "zip": ("pctile_zips", "zip"),
    "precinct": ("pctile_precincts", "precinct"),
}

_LABEL_MAP = {
    "violation_pctile": "Violations",
    "open_violation_pctile": "Open violations",
    "class_c_pctile": "Class C",
    "portfolio_size_pctile": "Portfolio size",
    "critical_pctile": "Critical violations",
    "complaint_pctile": "311 complaints",
    "crime_pctile": "Crime reports",
}

_ENTITY_LABEL_MAP = {
    "owner": "landlords",
    "building": "buildings",
    "restaurant": "restaurants",
    "zip": "ZIP codes",
    "precinct": "precincts",
}


def build_percentile_tables(db):
    """Create pctile_owners and pctile_buildings using PERCENT_RANK()."""
    db.execute("DROP TABLE IF EXISTS main.pctile_owners")
    db.execute("""
        CREATE TABLE main.pctile_owners AS
            WITH owner_stats AS (
                SELECT
                    o.owner_id,
                    COUNT(DISTINCT ow.bbl) AS building_count,
                    COALESCE(SUM(CASE WHEN v.violation_id IS NOT NULL THEN 1 ELSE 0 END), 0) AS violation_count,
                    COALESCE(SUM(CASE WHEN v.status = 'open' THEN 1 ELSE 0 END), 0) AS open_violation_count,
                    COALESCE(SUM(CASE WHEN v.class = 'C' THEN 1 ELSE 0 END), 0) AS class_c_count
                FROM main.graph_owners o
                LEFT JOIN main.graph_owns ow ON o.owner_id = ow.owner_id
                LEFT JOIN main.graph_violations v ON ow.bbl = v.bbl
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

    db.execute("DROP TABLE IF EXISTS main.pctile_buildings")
    db.execute("""
        CREATE TABLE main.pctile_buildings AS
            WITH building_stats AS (
                SELECT
                    b.bbl,
                    b.units,
                    b.stories,
                    COALESCE(COUNT(v.violation_id), 0) AS violation_count,
                    COALESCE(SUM(CASE WHEN v.status = 'open' THEN 1 ELSE 0 END), 0) AS open_violation_count
                FROM main.graph_buildings b
                LEFT JOIN main.graph_violations v ON b.bbl = v.bbl
                GROUP BY b.bbl, b.units, b.stories
            )
            SELECT
                bbl,
                units,
                stories,
                violation_count,
                open_violation_count,
                PERCENT_RANK() OVER (ORDER BY violation_count) AS violation_pctile,
                PERCENT_RANK() OVER (ORDER BY open_violation_count) AS open_violation_pctile
            FROM building_stats
        """)


def build_lake_percentile_tables(db):
    """Create percentile tables requiring lake (S3/MinIO) access."""
    db.execute("DROP TABLE IF EXISTS main.pctile_restaurants")
    db.execute("""
        CREATE TABLE main.pctile_restaurants AS
            WITH stats AS (
                SELECT
                    camis,
                    COUNT(*) AS violation_count,
                    SUM(CASE WHEN violation_code LIKE '04%' THEN 1 ELSE 0 END) AS critical_count
                FROM lake.health.restaurant_inspections
                GROUP BY camis
            )
            SELECT
                camis,
                violation_count,
                critical_count,
                PERCENT_RANK() OVER (ORDER BY violation_count) AS violation_pctile,
                PERCENT_RANK() OVER (ORDER BY critical_count) AS critical_pctile
            FROM stats
        """)

    db.execute("DROP TABLE IF EXISTS main.pctile_zips")
    db.execute("""
            CREATE TABLE main.pctile_zips AS
            WITH stats AS (
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
            FROM stats
        """)

    db.execute("DROP TABLE IF EXISTS main.pctile_precincts")
    db.execute("""
            CREATE TABLE main.pctile_precincts AS
            WITH all_complaints AS (
                SELECT addr_pct_cd
                FROM lake.public_safety.nypd_complaints_historic
                UNION ALL
                SELECT addr_pct_cd
                FROM lake.public_safety.nypd_complaints_ytd
            ),
            stats AS (
                SELECT
                    addr_pct_cd AS precinct,
                    COUNT(*) AS crime_count
                FROM all_complaints
                GROUP BY addr_pct_cd
            )
            SELECT
                precinct,
                crime_count,
                PERCENT_RANK() OVER (ORDER BY crime_count) AS crime_pctile
            FROM stats
        """)


def lookup_percentiles(pool, entity_type, key):
    """Return dict of {metric_pctile: float} for the entity, or {} if not found."""
    if entity_type not in _ENTITY_TABLE_MAP:
        return {}
    table, key_col = _ENTITY_TABLE_MAP[entity_type]
    try:
        with pool.cursor() as cur:
            rows = cur.execute(
                f"SELECT * FROM main.{table} WHERE {key_col} = ?", [key]
            ).fetchall()
            if not rows:
                return {}
            cols = [c[0] for c in cur.execute(f"DESCRIBE main.{table}").fetchall()]
            record = dict(zip(cols, rows[0]))
            return {k: v for k, v in record.items() if k.endswith("_pctile")}
    except Exception:
        return {}


def get_population_count(pool, entity_type):
    """Return total row count for the entity's percentile table, or 0 on error."""
    if entity_type not in _ENTITY_TABLE_MAP:
        return 0
    table, _ = _ENTITY_TABLE_MAP[entity_type]
    try:
        with pool.cursor() as cur:
            return cur.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
    except Exception:
        return 0


def format_percentile(value: float) -> str:
    """Convert 0.0-1.0 to ordinal string: 0.94 -> '94th'."""
    n = round(value * 100)
    if 11 <= (n % 100) <= 13:
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
    """Format a dict of percentiles into a human-readable text block."""
    if not percentiles:
        return ""

    entity_label = _ENTITY_LABEL_MAP.get(entity_type, entity_type + "s")
    pop_str = f"{population:,}"
    header = f"PERCENTILE RANKING (among {pop_str} NYC {entity_label})"

    parts = []
    for key, label in _LABEL_MAP.items():
        if key in percentiles:
            parts.append(f"{label}: {format_percentile(percentiles[key])}")

    metrics_line = " | ".join(parts)
    return f"{header}\n{metrics_line}"
