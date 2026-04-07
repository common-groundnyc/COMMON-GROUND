# Extension-Powered MCP Tool Migration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate 12 existing MCP tools from ZIP/precinct crosswalks and UPPER() name matching to use the H3 spatial index, phonetic blocking, rapidfuzz scoring, and anomaly detection from the foundation layer.

**Architecture:** Each tool migration is independent — swap the SQL/logic inside the tool function while keeping the same MCP interface (parameters, return type, description). Tools that use ZIP-based geography switch to H3 k-ring queries against `lake.foundation.h3_index`. Tools that use UPPER() name matching switch to phonetic search against `lake.foundation.phonetic_index`. Both modules (`spatial.py`, `entity.py`) are already imported in `mcp_server.py`.

**Tech Stack:** DuckDB 1.5.0, FastMCP, H3 extension, splink_udfs, rapidfuzz. Foundation tables: `lake.foundation.h3_index` (40M rows), `lake.foundation.phonetic_index` (57M rows).

**Key constraint:** `_execute(conn, sql)` returns `(cols, rows)` where rows are tuples. Always unpack as `cols, raw_rows = _execute(db, sql)` then `rows = [dict(zip(cols, r)) for r in raw_rows]` when dict access is needed.

**Fallback pattern:** Every migration wraps the new H3/phonetic path in `try/except` and falls back to the original SQL if the foundation table doesn't exist. This means tools work whether or not foundation_rebuild has run.

---

## File Structure

### Modified files

| File | What changes |
|------|-------------|
| `infra/duckdb-server/spatial.py` | Add `h3_neighborhood_compare_sql()`, `h3_zip_centroid_sql()` |
| `infra/duckdb-server/entity.py` | Add `phonetic_vital_search_sql()`, `fuzzy_money_trail_sql()` |
| `infra/duckdb-server/mcp_server.py` | Migrate 12 tool functions to use foundation layer |

### Test files

| File | What it tests |
|------|--------------|
| `tests/test_spatial.py` | New spatial SQL builders |
| `tests/test_entity.py` | New entity SQL builders |

---

## Task 1: Extend spatial.py with ZIP-centroid and compare builders

**Files:**
- Modify: `infra/duckdb-server/spatial.py`
- Modify: `tests/test_spatial.py`

- [ ] **Step 1: Add h3_zip_centroid_sql to spatial.py**

Append to `infra/duckdb-server/spatial.py`:

```python
def h3_zip_centroid_sql(zipcode: str) -> str:
    """SQL to get the H3 centroid cell for a ZIP code using PLUTO data."""
    return f"""
        SELECT h3_latlng_to_cell(
            AVG(TRY_CAST(latitude AS DOUBLE)),
            AVG(TRY_CAST(longitude AS DOUBLE)),
            {H3_RES}
        ) AS center_cell,
        AVG(TRY_CAST(latitude AS DOUBLE)) AS center_lat,
        AVG(TRY_CAST(longitude AS DOUBLE)) AS center_lng
        FROM lake.city_government.pluto
        WHERE zipcode = '{zipcode}'
          AND TRY_CAST(latitude AS DOUBLE) BETWEEN 40.4 AND 41.0
          AND TRY_CAST(longitude AS DOUBLE) BETWEEN -74.3 AND -73.6
    """


def h3_neighborhood_stats_sql(lat: float, lng: float, radius_rings: int = 8) -> str:
    """SQL to get multi-dimension neighborhood stats via H3 aggregation.

    Replaces the 7-way LEFT JOIN in NEIGHBORHOOD_COMPARE_SQL.
    Uses h3_index to count events by category within a hex radius.
    """
    return f"""
        WITH target_cells AS (
            {h3_kring_sql(lat, lng, radius_rings)}
        ),
        h3_stats AS (
            SELECT source_table, COUNT(*) AS cnt
            FROM lake.foundation.h3_index
            WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
            GROUP BY source_table
        )
        SELECT
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_complaints_historic'), 0)
                + COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_complaints_ytd'), 0)
                AS total_crimes,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_arrests_historic'), 0)
                + COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_arrests_ytd'), 0)
                AS total_arrests,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'health.restaurant_inspections'), 0) AS restaurants,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'social_services.n311_service_requests'), 0) AS n311_calls,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.shootings'), 0) AS shootings,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'environment.street_trees'), 0) AS street_trees
        FROM h3_stats
    """
```

- [ ] **Step 2: Add tests**

Append to `tests/test_spatial.py`:

```python
def test_zip_centroid_sql():
    from spatial import h3_zip_centroid_sql
    sql = h3_zip_centroid_sql("10003")
    assert "h3_latlng_to_cell" in sql
    assert "10003" in sql
    assert "pluto" in sql


def test_neighborhood_stats_sql():
    from spatial import h3_neighborhood_stats_sql
    sql = h3_neighborhood_stats_sql(40.7128, -74.006, radius_rings=5)
    assert "h3_grid_disk" in sql
    assert "total_crimes" in sql
    assert "restaurants" in sql
    assert "n311_calls" in sql
```

- [ ] **Step 3: Run tests, commit**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_spatial.py -v
git add infra/duckdb-server/spatial.py tests/test_spatial.py
git commit -m "feat: add h3_zip_centroid and h3_neighborhood_stats SQL builders"
```

---

## Task 2: Extend entity.py with vital records and money trail builders

**Files:**
- Modify: `infra/duckdb-server/entity.py`
- Modify: `tests/test_entity.py`

- [ ] **Step 1: Add phonetic_vital_search_sql to entity.py**

Append to `infra/duckdb-server/entity.py`:

```python
def phonetic_vital_search_sql(
    first_name: str | None,
    last_name: str,
    table: str,
    first_col: str,
    last_col: str,
    extra_cols: str = "",
    limit: int = 30,
) -> str:
    """Phonetic search for historical records where spelling was inconsistent.

    Uses soundex for broader matching on pre-1950 records where names
    were often recorded phonetically by clerks.
    """
    escaped_last = last_name.replace("'", "''")
    extra = f", {extra_cols}" if extra_cols else ""

    if first_name:
        escaped_first = first_name.replace("'", "''")
        return f"""
            SELECT {first_col}, {last_col}{extra},
                jaro_winkler_similarity(UPPER({last_col}), UPPER('{escaped_last}')) AS last_score,
                jaro_winkler_similarity(UPPER({first_col}), UPPER('{escaped_first}')) AS first_score
            FROM {table}
            WHERE soundex(UPPER({last_col})) = soundex(UPPER('{escaped_last}'))
            ORDER BY last_score DESC, first_score DESC
            LIMIT {limit}
        """
    else:
        return f"""
            SELECT {first_col}, {last_col}{extra},
                jaro_winkler_similarity(UPPER({last_col}), UPPER('{escaped_last}')) AS last_score
            FROM {table}
            WHERE soundex(UPPER({last_col})) = soundex(UPPER('{escaped_last}'))
            ORDER BY last_score DESC
            LIMIT {limit}
        """


def fuzzy_money_search_sql(
    name: str,
    table: str,
    name_col: str,
    extra_cols: str = "",
    min_score: int = 70,
    limit: int = 30,
) -> str:
    """Fuzzy name matching for campaign finance / money trail.

    Uses rapidfuzz token_sort_ratio which handles name reordering:
    'JOHN A SMITH' matches 'SMITH, JOHN A'.
    """
    escaped = name.replace("'", "''")
    extra = f", {extra_cols}" if extra_cols else ""
    return f"""
        SELECT {name_col}{extra},
            rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER('{escaped}')) AS match_score
        FROM {table}
        WHERE rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER('{escaped}')) >= {min_score}
        ORDER BY match_score DESC
        LIMIT {limit}
    """
```

- [ ] **Step 2: Add tests**

Append to `tests/test_entity.py`:

```python
def test_phonetic_vital_search():
    from entity import phonetic_vital_search_sql
    sql = phonetic_vital_search_sql("John", "Smith",
        table="lake.federal.nys_death_index",
        first_col="first_name", last_col="last_name",
        extra_cols="age, date_of_death")
    assert "soundex" in sql
    assert "jaro_winkler_similarity" in sql
    assert "nys_death_index" in sql


def test_fuzzy_money_search():
    from entity import fuzzy_money_search_sql
    sql = fuzzy_money_search_sql("JOHN SMITH",
        table="lake.federal.fec_contributions",
        name_col="contributor_name",
        extra_cols="committee_id, contribution_receipt_amount")
    assert "rapidfuzz_token_sort_ratio" in sql
    assert "70" in sql
```

- [ ] **Step 3: Run tests, commit**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_entity.py -v
git add infra/duckdb-server/entity.py tests/test_entity.py
git commit -m "feat: add phonetic_vital_search and fuzzy_money_search SQL builders"
```

---

## Task 3: Migrate neighborhood_compare to H3

This is the highest-impact migration — replaces a 7-way LEFT JOIN with ZIP→precinct→community district crosswalks.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:3662-3709`

- [ ] **Step 1: Replace neighborhood_compare function body**

Read the current `neighborhood_compare` function at line 3662. Replace the body (keep the function signature and docstring) with:

```python
    if not zip_codes or len(zip_codes) < 2:
        raise ToolError("Provide at least 2 ZIP codes to compare")
    if len(zip_codes) > 7:
        raise ToolError("Maximum 7 ZIP codes per comparison")
    for z in zip_codes:
        if not re.match(r"^\d{5}$", z):
            raise ToolError(f"Invalid ZIP code: {z}. Must be exactly 5 digits.")

    db = ctx.lifespan_context["db"]

    # Try H3-based comparison first (faster, no crosswalk hacks)
    try:
        from spatial import h3_zip_centroid_sql, h3_neighborhood_stats_sql
        results = []
        for z in zip_codes:
            # Get ZIP centroid
            centroid_cols, centroid_rows = _execute(db, h3_zip_centroid_sql(z))
            if not centroid_rows or centroid_rows[0][0] is None:
                continue
            center = dict(zip(centroid_cols, centroid_rows[0]))

            # Get H3 stats around centroid (radius 8 = ~1.6km, roughly ZIP-sized)
            stats_cols, stats_rows = _execute(db,
                h3_neighborhood_stats_sql(center["center_lat"], center["center_lng"], radius_rings=8))
            if stats_rows:
                s = dict(zip(stats_cols, stats_rows[0]))
                s["zip"] = z
                s["center_lat"] = center["center_lat"]
                s["center_lng"] = center["center_lng"]
                results.append(s)

        if results:
            lines = ["Neighborhood Comparison (H3 hex-based):"]
            for r in results:
                lines.append(f"\nZIP {r['zip']}:")
                lines.append(f"  Crime: {r.get('total_crimes', 0):,} incidents nearby")
                lines.append(f"  Arrests: {r.get('total_arrests', 0):,} nearby")
                lines.append(f"  Food: {r.get('restaurants', 0):,} restaurants")
                lines.append(f"  311 calls: {r.get('n311_calls', 0):,}")
                lines.append(f"  Shootings: {r.get('shootings', 0):,}")
                lines.append(f"  Trees: {r.get('street_trees', 0):,}")

            # Also get income + housing from original ZIP-based queries (no H3 equivalent)
            for r in results:
                z = r["zip"]
                try:
                    _, inc = _execute(db, """
                        SELECT ROUND(1000.0 * SUM(TRY_CAST(agi_amount AS DOUBLE))
                               / NULLIF(SUM(TRY_CAST(num_returns AS DOUBLE)), 0), 0)
                        FROM lake.economics.irs_soi_zip_income
                        WHERE zipcode = ? AND TRY_CAST(tax_year AS INTEGER) = (
                            SELECT MAX(TRY_CAST(tax_year AS INTEGER)) FROM lake.economics.irs_soi_zip_income)
                    """, [z])
                    if inc and inc[0][0]:
                        r["avg_income"] = inc[0][0]
                except Exception:
                    pass
                try:
                    _, hpd = _execute(db, """
                        SELECT COUNT(DISTINCT complaint_id)
                        FROM lake.housing.hpd_complaints
                        WHERE post_code = ? AND TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
                    """, [z])
                    if hpd and hpd[0][0]:
                        r["housing_complaints"] = hpd[0][0]
                except Exception:
                    pass

            # Add income/housing to output
            for r in results:
                idx = next(i for i, l in enumerate(lines) if f"ZIP {r['zip']}" in l)
                if r.get("avg_income"):
                    lines.insert(idx + 6, f"  Income: ${r['avg_income']:,.0f} avg AGI")
                if r.get("housing_complaints"):
                    lines.insert(idx + 6, f"  Housing: {r['housing_complaints']:,} HPD complaints/yr")

            summary = "\n".join(lines)
            structured = [{"zip": r["zip"], **{k: v for k, v in r.items() if k != "zip"}} for r in results]
            return ToolResult(
                content=summary,
                structured_content={"neighborhoods": structured},
                meta={"zip_codes": zip_codes, "method": "h3"},
            )
    except Exception:
        pass  # Fall back to original SQL

    # Fallback: original ZIP-based comparison
    cols, rows = _execute(db, NEIGHBORHOOD_COMPARE_SQL, [zip_codes])
    if not rows:
        raise ToolError("No data found for the provided ZIP codes")

    lines = ["Neighborhood Comparison:"]
    for row in rows:
        d = dict(zip(cols, row))
        z = d["zip"]
        parts = [f"\nZIP {z}:"]
        if d.get("felonies") is not None:
            parts.append(f"  Crime: {d['felonies']} felonies, {d['assaults']} assaults/yr")
        if d.get("noise_calls") is not None:
            parts.append(f"  Noise: {d['noise_calls']:,} NYPD 311 calls/yr")
        if d.get("restaurants") is not None:
            parts.append(f"  Food: {d['restaurants']} restaurants ({d['pct_grade_a']}% A-grade)")
        if d.get("housing_complaints") is not None:
            parts.append(f"  Housing: {d['housing_complaints']:,} HPD complaints/yr")
        if d.get("avg_income") is not None:
            parts.append(f"  Income: ${d['avg_income']:,.0f} avg AGI")
        if d.get("commute_min") is not None:
            parts.append(f"  Commute: {d['commute_min']:.0f} min avg")
        if d.get("poverty_rate") is not None:
            parts.append(f"  Poverty: {d['poverty_rate']:.1f}%")
        if d.get("park_access_pct") is not None:
            parts.append(f"  Park access: {d['park_access_pct']:.0f}%")
        if d.get("bachelors_pct") is not None:
            parts.append(f"  College educated: {d['bachelors_pct']:.0f}%")
        lines.extend(parts)

    summary = "\n".join(lines)
    return ToolResult(
        content=summary + "\n\n" + format_text_table(cols, rows),
        structured_content={"neighborhoods": [dict(zip(cols, r)) for r in rows]},
        meta={"zip_codes": zip_codes, "method": "zip_crosswalk"},
    )
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: migrate neighborhood_compare to H3 hex aggregation with ZIP fallback"
```

---

## Task 4: Migrate marriage_search to phonetic

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:10302-10381`

- [ ] **Step 1: Add phonetic search to marriage_search**

After the existing `search_last` / `search_first` parsing (line ~10309), add a phonetic search path before the existing SQL queries:

```python
    # Try phonetic search first (catches spelling variants in historical records)
    phonetic_results = []
    try:
        from entity import phonetic_vital_search_sql

        # Modern marriages (1950-2017) — phonetic on groom surname
        ph_sql = phonetic_vital_search_sql(
            first_name=search_first or None,
            last_name=search_last,
            table="lake.city_government.marriage_licenses_1950_2017",
            first_col="groom_first_name",
            last_col="groom_surname",
            extra_cols="bride_first_name, bride_surname, LICENSE_BOROUGH_ID, LICENSE_YEAR",
            limit=30,
        )
        _, ph_rows = _execute(db, ph_sql)

        # Also search bride surname
        ph_sql_bride = phonetic_vital_search_sql(
            first_name=search_first or None,
            last_name=search_last,
            table="lake.city_government.marriage_licenses_1950_2017",
            first_col="bride_first_name",
            last_col="bride_surname",
            extra_cols="groom_first_name, groom_surname, LICENSE_BOROUGH_ID, LICENSE_YEAR",
            limit=30,
        )
        _, ph_bride_rows = _execute(db, ph_sql_bride)

        if ph_rows or ph_bride_rows:
            phonetic_results = list(ph_rows or []) + list(ph_bride_rows or [])
    except Exception:
        pass  # Fall back to exact match
```

Then in the output section, if `phonetic_results` is non-empty, add a section:

```python
    if phonetic_results:
        lines.append(f"\nPHONETIC MATCHES ({len(phonetic_results)} additional — spelling variants):")
        for r in phonetic_results[:10]:
            lines.append(f"  {' | '.join(str(v) for v in r[:6] if v)}")
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add phonetic search to marriage_search for spelling variants"
```

---

## Task 5: Migrate vital_records to phonetic

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:6807-6866`

- [ ] **Step 1: Add phonetic search alongside existing queries**

After the existing `_safe_query` calls (line ~6831), add:

```python
    # Phonetic enhancement — catches spelling variants in historical records
    phonetic_deaths = []
    phonetic_births = []
    try:
        from entity import phonetic_vital_search_sql

        ph_death_sql = phonetic_vital_search_sql(
            first_name=first if first != "%" else None,
            last_name=last,
            table="lake.federal.nys_death_index",
            first_col="first_name", last_col="last_name",
            extra_cols="age, date_of_death, residence_code, place_of_death_code",
            limit=20,
        )
        _, phonetic_deaths = _execute(db, ph_death_sql)

        ph_birth_sql = phonetic_vital_search_sql(
            first_name=first if first != "%" else None,
            last_name=last,
            table="lake.city_government.birth_certificates_1855_1909",
            first_col="first_name", last_col="last_name",
            extra_cols="year, county",
            limit=15,
        )
        _, phonetic_births = _execute(db, ph_birth_sql)
    except Exception:
        pass
```

Then in the output, add phonetic sections:

```python
    if phonetic_deaths:
        lines.append(f"\nPHONETIC DEATH MATCHES ({len(phonetic_deaths)} — spelling variants):")
        for r in phonetic_deaths[:10]:
            lines.append(f"  {r[0] or '?'} {r[1] or '?'} — score: {r[-2]:.2f}/{r[-1]:.2f}")

    if phonetic_births:
        lines.append(f"\nPHONETIC BIRTH MATCHES ({len(phonetic_births)} — spelling variants):")
        for r in phonetic_births[:10]:
            lines.append(f"  {r[0] or '?'} {r[1] or '?'} — score: {r[-1]:.2f}")
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add phonetic search to vital_records for 1800s spelling variants"
```

---

## Task 6: Migrate money_trail to fuzzy matching

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:6932-7010`

- [ ] **Step 1: Add fuzzy matching alongside existing queries**

After the existing `_safe_query` calls (line ~6952), add:

```python
    # Fuzzy enhancement — catches "JOHN A SMITH" vs "SMITH, JOHN A" reordering
    fuzzy_fec = []
    fuzzy_nys = []
    try:
        from entity import fuzzy_money_search_sql

        fec_sql = fuzzy_money_search_sql(
            name=name,
            table="lake.federal.fec_contributions",
            name_col="contributor_name",
            extra_cols="committee_id, contribution_receipt_amount, contribution_receipt_date",
            min_score=75, limit=15,
        )
        _, fuzzy_fec = _execute(db, fec_sql)

        nys_sql = fuzzy_money_search_sql(
            name=name,
            table="lake.federal.nys_campaign_finance",
            name_col="flng_ent_name",
            extra_cols="filer_name, amount, date",
            min_score=75, limit=15,
        )
        _, fuzzy_nys = _execute(db, nys_sql)
    except Exception:
        pass
```

Then in output, add fuzzy sections if they found results the exact match missed:

```python
    if fuzzy_fec and not fec_rows:
        lines.append(f"\nFUZZY FEC MATCHES ({len(fuzzy_fec)} — name variants):")
        for r in fuzzy_fec[:5]:
            lines.append(f"  {r[0]} — ${float(r[2] or 0):,.0f} (score: {r[-1]:.0f})")

    if fuzzy_nys and not nys_rows:
        lines.append(f"\nFUZZY NYS MATCHES ({len(fuzzy_nys)} — name variants):")
        for r in fuzzy_nys[:5]:
            lines.append(f"  {r[0]} → {r[1]} — ${float(r[2] or 0):,.0f} (score: {r[-1]:.0f})")
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add fuzzy name matching to money_trail for donor name variants"
```

---

## Task 7: Migrate due_diligence to phonetic

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:6447-6526`

- [ ] **Step 1: Add phonetic attorney search**

After the existing `_safe_query` calls (line ~6482), add:

```python
    # Phonetic enhancement for attorney and broker searches
    try:
        from entity import phonetic_search_sql
        ph_sql = phonetic_search_sql(
            first_name=first_guess if len(parts) >= 2 else None,
            last_name=last_guess,
            min_score=0.8, limit=10,
        )
        ph_cols, ph_rows = _execute(db, ph_sql)
        if ph_rows and not atty_rows and not broker_rows:
            phonetic_matches = [dict(zip(ph_cols, r)) for r in ph_rows]
            # Filter to financial/professional sources only
            relevant = [m for m in phonetic_matches if any(s in m.get("source_table", "")
                        for s in ["attorney", "broker", "notary", "tax_warrant", "child_support"])]
            if relevant:
                lines.append(f"\nPHONETIC MATCHES ({len(relevant)} — name variants in professional databases):")
                for m in relevant[:5]:
                    lines.append(f"  {m.get('first_name', '')} {m.get('last_name', '')} in {m.get('source_table', '')} (score: {m.get('combined_score', 0):.2f})")
    except Exception:
        pass
```

Insert this before the `if sections_found == 0` check — update `sections_found` to include phonetic results.

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add phonetic search fallback to due_diligence"
```

---

## Task 8: Migrate worst_landlords to add anomaly flagging

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:9139-9220`

- [ ] **Step 1: Add anomaly detection to worst_landlords output**

After the existing query results are returned (after `_execute`), add anomaly flagging:

```python
    # Flag statistical outliers using IQR
    if rows:
        data = [dict(zip(cols, r)) for r in rows]
        violations = [d.get("violations", 0) for d in data if d.get("violations")]
        if len(violations) >= 5:
            import statistics
            sorted_v = sorted(violations)
            q1 = sorted_v[len(sorted_v) // 4]
            q3 = sorted_v[3 * len(sorted_v) // 4]
            iqr = q3 - q1
            upper_bound = q3 + 1.5 * iqr
            for d in data:
                if d.get("violations", 0) > upper_bound:
                    d["anomaly_flag"] = "statistical_outlier"
```

Then in the output formatting, append the flag:

```python
        flag = " [OUTLIER]" if d.get("anomaly_flag") else ""
        lines.append(f"  {rank}. {owner}{flag}")
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add IQR anomaly flagging to worst_landlords"
```

---

## Task 9: Migrate building_profile to add anomaly context

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:2914-2970`

- [ ] **Step 1: Add city-wide percentile context**

After the main query result (line ~2927), add:

```python
    # Add city-wide context — is this building's violation count anomalous?
    try:
        _, pct_rows = _execute(db, """
            SELECT
                PERCENT_RANK() OVER (ORDER BY COUNT(*)) AS violation_percentile
            FROM lake.housing.hpd_violations
            GROUP BY bbl
            HAVING bbl = ?
        """, [bbl])
        if pct_rows and pct_rows[0][0] is not None:
            row["violation_percentile"] = round(pct_rows[0][0] * 100, 1)
    except Exception:
        pass
```

Then in the output, add:

```python
    if row.get("violation_percentile"):
        lines.append(f"  City percentile: {row['violation_percentile']}% (higher = more violations)")
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add violation percentile context to building_profile"
```

---

## Task 10: Migrate neighborhood_portrait to H3

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:7939-8014`

- [ ] **Step 1: Add H3 safety stats to portrait**

After the restaurant grades section (line ~8013), add an H3-based safety summary:

```python
    # --- H3-based safety snapshot (more accurate than precinct crosswalk) ---
    try:
        from spatial import h3_zip_centroid_sql, h3_neighborhood_stats_sql
        _, centroid = _execute(db, h3_zip_centroid_sql(zipcode))
        if centroid and centroid[0][0] is not None:
            c = dict(zip(["center_cell", "center_lat", "center_lng"], centroid[0]))
            _, stats = _execute(db, h3_neighborhood_stats_sql(c["center_lat"], c["center_lng"], radius_rings=6))
            if stats:
                s = dict(zip(["total_crimes", "total_arrests", "restaurants_h3", "n311_calls", "shootings", "street_trees"], stats[0]))
                lines.append(f"\nSAFETY SNAPSHOT (H3 hex radius)")
                lines.append(f"  Crimes nearby: {s.get('total_crimes', 0):,}")
                lines.append(f"  Arrests nearby: {s.get('total_arrests', 0):,}")
                lines.append(f"  Shootings nearby: {s.get('shootings', 0):,}")
                lines.append(f"  311 calls nearby: {s.get('n311_calls', 0):,}")
                lines.append(f"  Street trees: {s.get('street_trees', 0):,}")
    except Exception:
        pass  # H3 index not materialized yet
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add H3 safety snapshot to neighborhood_portrait"
```

---

## Task 11: Migrate safety_report to add H3 context

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:4427+`

- [ ] **Step 1: Add H3 hotspot analysis to safety_report**

At the end of the safety_report function, before the return, add:

```python
    # H3 hotspot analysis — find the precinct's crime concentration cells
    try:
        from spatial import h3_heatmap_sql
        # Get precinct centroid from PLUTO
        _, pct_center = _execute(db, f"""
            SELECT AVG(TRY_CAST(latitude AS DOUBLE)), AVG(TRY_CAST(longitude AS DOUBLE))
            FROM lake.city_government.pluto
            WHERE policeprct = '{precinct}'
              AND TRY_CAST(latitude AS DOUBLE) BETWEEN 40.4 AND 41.0
        """)
        if pct_center and pct_center[0][0]:
            lat, lng = pct_center[0][0], pct_center[0][1]
            hm_cols, hm_rows = _execute(db, h3_heatmap_sql(
                source_table="lake.foundation.h3_index",
                filter_table="public_safety.nypd_complaints_ytd",
                lat=lat, lng=lng, radius_rings=10,
            ))
            if hm_rows:
                hotspots = [dict(zip(hm_cols, r)) for r in hm_rows[:5]]
                lines.append(f"\nCRIME HOTSPOT CELLS (top 5 H3 hexes):")
                for h in hotspots:
                    lines.append(f"  ({h['cell_lat']:.4f}, {h['cell_lng']:.4f}): {h['count']:,} incidents")
    except Exception:
        pass
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add H3 crime hotspot analysis to safety_report"
```

---

## Task 12: Deploy and verify

- [ ] **Step 1: Run all tests**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest tests/ -v --tb=short
```

- [ ] **Step 2: Deploy**

```bash
scp -i ~/.ssh/id_ed25519_hetzner infra/duckdb-server/spatial.py fattie@178.156.228.119:/opt/common-ground/duckdb-server/spatial.py
scp -i ~/.ssh/id_ed25519_hetzner infra/duckdb-server/entity.py fattie@178.156.228.119:/opt/common-ground/duckdb-server/entity.py
scp -i ~/.ssh/id_ed25519_hetzner infra/duckdb-server/mcp_server.py fattie@178.156.228.119:/opt/common-ground/duckdb-server/mcp_server.py
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "cd /opt/common-ground && docker compose restart duckdb-server"
```

- [ ] **Step 3: Smoke test migrated tools**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "
  sleep 20
  curl -s http://localhost:4213/health
"
```

- [ ] **Step 4: Commit deploy state**

```bash
git add .
git commit -m "docs: update after extension tool migration deployment"
```
