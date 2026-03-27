"""Tests for percentiles.py — written first (TDD), RED before GREEN."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading
import duckdb
import pytest

from percentiles import (
    build_percentile_tables,
    lookup_percentiles,
    get_population_count,
    format_percentile,
    format_percentile_block,
    DISCLAIMER,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_db():
    """In-memory DuckDB with graph fixture tables."""
    db = duckdb.connect(":memory:")

    # graph_owners: 5 owners
    db.execute("""
        CREATE TABLE main.graph_owners AS
        SELECT * FROM (VALUES
            ('o1', 'Landlord A'),
            ('o2', 'Landlord B'),
            ('o3', 'Landlord C'),
            ('o4', 'Landlord D'),
            ('o5', 'Landlord E')
        ) t(owner_id, owner_name)
    """)

    # graph_owns: ownership of 8 buildings
    # o1 owns 3 buildings, o2 owns 2, o3 owns 1, o4 owns 1, o5 owns 1
    db.execute("""
        CREATE TABLE main.graph_owns AS
        SELECT * FROM (VALUES
            ('o1', 'bbl001'),
            ('o1', 'bbl002'),
            ('o1', 'bbl003'),
            ('o2', 'bbl004'),
            ('o2', 'bbl005'),
            ('o3', 'bbl006'),
            ('o4', 'bbl007'),
            ('o5', 'bbl008')
        ) t(owner_id, bbl)
    """)

    # graph_violations: 10 violations
    # bbl001: 4 violations (2 open, 1 class C open, 1 class C closed)
    # bbl004: 3 violations (1 open class C)
    # bbl006: 2 violations (1 open)
    # bbl008: 1 violation (closed)
    # bbl002,003,005,007: 0 violations
    db.execute("""
        CREATE TABLE main.graph_violations AS
        SELECT * FROM (VALUES
            ('v01', 'bbl001', 'C', 'open',   '2024-01-01'),
            ('v02', 'bbl001', 'C', 'closed', '2023-06-01'),
            ('v03', 'bbl001', 'B', 'open',   '2024-02-01'),
            ('v04', 'bbl001', 'A', 'closed', '2023-01-01'),
            ('v05', 'bbl004', 'C', 'open',   '2024-03-01'),
            ('v06', 'bbl004', 'B', 'closed', '2023-05-01'),
            ('v07', 'bbl004', 'A', 'closed', '2023-04-01'),
            ('v08', 'bbl006', 'B', 'open',   '2024-04-01'),
            ('v09', 'bbl006', 'A', 'closed', '2023-02-01'),
            ('v10', 'bbl008', 'A', 'closed', '2022-12-01')
        ) t(violation_id, bbl, severity, status, issued_date)
    """)

    # graph_buildings: 8 buildings
    db.execute("""
        CREATE TABLE main.graph_buildings AS
        SELECT * FROM (VALUES
            ('bbl001', 24,  6),
            ('bbl002',  4,  3),
            ('bbl003',  6,  4),
            ('bbl004', 12,  5),
            ('bbl005',  8,  4),
            ('bbl006',  2,  2),
            ('bbl007',  4,  3),
            ('bbl008',  1,  1)
        ) t(bbl, total_units, stories)
    """)

    return db


# ---------------------------------------------------------------------------
# TestBuildPercentileTables
# ---------------------------------------------------------------------------

class TestBuildPercentileTables:
    def test_creates_pctile_owners(self):
        db = make_db()
        build_percentile_tables(db)
        count = db.execute("SELECT COUNT(*) FROM main.pctile_owners").fetchone()[0]
        assert count == 5

    def test_creates_pctile_buildings(self):
        db = make_db()
        build_percentile_tables(db)
        count = db.execute("SELECT COUNT(*) FROM main.pctile_buildings").fetchone()[0]
        assert count == 8

    def test_owner_percentile_columns_present(self):
        db = make_db()
        build_percentile_tables(db)
        cols = [c[0] for c in db.execute("DESCRIBE main.pctile_owners").fetchall()]
        for expected in [
            "owner_id", "building_count", "violation_count",
            "open_violation_count", "class_c_count",
            "violation_pctile", "open_violation_pctile",
            "class_c_pctile", "portfolio_size_pctile",
        ]:
            assert expected in cols, f"Missing column: {expected}"

    def test_building_percentile_columns_present(self):
        db = make_db()
        build_percentile_tables(db)
        cols = [c[0] for c in db.execute("DESCRIBE main.pctile_buildings").fetchall()]
        for expected in [
            "bbl", "total_units", "stories",
            "violation_count", "open_violation_count",
            "violation_pctile", "open_violation_pctile",
        ]:
            assert expected in cols, f"Missing column: {expected}"

    def test_owner_percentiles_in_0_1_range(self):
        db = make_db()
        build_percentile_tables(db)
        rows = db.execute("""
            SELECT violation_pctile, open_violation_pctile,
                   class_c_pctile, portfolio_size_pctile
            FROM main.pctile_owners
        """).fetchall()
        for row in rows:
            for val in row:
                assert 0.0 <= val <= 1.0, f"Percentile out of range: {val}"

    def test_building_percentiles_in_0_1_range(self):
        db = make_db()
        build_percentile_tables(db)
        rows = db.execute(
            "SELECT violation_pctile, open_violation_pctile FROM main.pctile_buildings"
        ).fetchall()
        for row in rows:
            for val in row:
                assert 0.0 <= val <= 1.0, f"Percentile out of range: {val}"

    def test_worst_owner_has_highest_violation_pctile(self):
        # o1 has most violations (4) across its buildings
        db = make_db()
        build_percentile_tables(db)
        row = db.execute("""
            SELECT owner_id FROM main.pctile_owners
            ORDER BY violation_pctile DESC
            LIMIT 1
        """).fetchone()
        assert row[0] == "o1"

    def test_worst_owner_violation_pctile_is_1(self):
        db = make_db()
        build_percentile_tables(db)
        val = db.execute("""
            SELECT MAX(violation_pctile) FROM main.pctile_owners
        """).fetchone()[0]
        assert val == 1.0

    def test_owner_with_zero_violations_has_0_pctile(self):
        # o2, o3, o4, o5 have 0 or low violations relative to o1
        # At minimum, owner with 0 violations should not have 1.0 pctile
        db = make_db()
        build_percentile_tables(db)
        # o5 owns bbl008 which has 1 violation (closed), o4 owns bbl007 with 0
        val = db.execute("""
            SELECT violation_pctile FROM main.pctile_owners WHERE owner_id = 'o4'
        """).fetchone()[0]
        # bbl007 has no violations, so o4 should be at 0.0
        assert val == 0.0

    def test_largest_portfolio_has_highest_portfolio_pctile(self):
        # o1 owns 3 buildings (most)
        db = make_db()
        build_percentile_tables(db)
        row = db.execute("""
            SELECT owner_id FROM main.pctile_owners
            ORDER BY portfolio_size_pctile DESC
            LIMIT 1
        """).fetchone()
        assert row[0] == "o1"

    def test_building_with_most_violations_has_highest_pctile(self):
        # bbl001 has 4 violations
        db = make_db()
        build_percentile_tables(db)
        row = db.execute("""
            SELECT bbl FROM main.pctile_buildings
            ORDER BY violation_pctile DESC
            LIMIT 1
        """).fetchone()
        assert row[0] == "bbl001"

    def test_building_with_no_violations_has_0_pctile(self):
        db = make_db()
        build_percentile_tables(db)
        val = db.execute("""
            SELECT violation_pctile FROM main.pctile_buildings WHERE bbl = 'bbl002'
        """).fetchone()[0]
        assert val == 0.0

    def test_replaces_existing_tables(self):
        db = make_db()
        build_percentile_tables(db)
        # Call again — should not raise (DROP OR REPLACE)
        build_percentile_tables(db)
        count = db.execute("SELECT COUNT(*) FROM main.pctile_owners").fetchone()[0]
        assert count == 5


# ---------------------------------------------------------------------------
# TestLookupPercentiles
# ---------------------------------------------------------------------------

class TestLookupPercentiles:
    def setup_method(self):
        self.db = make_db()
        build_percentile_tables(self.db)

    def test_owner_lookup_returns_dict(self):
        result = lookup_percentiles(self.db, "owner", "o1")
        assert isinstance(result, dict)

    def test_owner_lookup_has_expected_keys(self):
        result = lookup_percentiles(self.db, "owner", "o1")
        assert "violation_pctile" in result
        assert "open_violation_pctile" in result
        assert "class_c_pctile" in result
        assert "portfolio_size_pctile" in result

    def test_owner_lookup_values_in_range(self):
        result = lookup_percentiles(self.db, "owner", "o1")
        for v in result.values():
            assert 0.0 <= v <= 1.0

    def test_building_lookup_returns_dict(self):
        result = lookup_percentiles(self.db, "building", "bbl001")
        assert isinstance(result, dict)
        assert "violation_pctile" in result

    def test_missing_entity_returns_empty_dict(self):
        result = lookup_percentiles(self.db, "owner", "nonexistent_owner_xyz")
        assert result == {}

    def test_unknown_entity_type_returns_empty_dict(self):
        result = lookup_percentiles(self.db, "flying_saucer", "anything")
        assert result == {}

    def test_lookup_never_raises(self):
        # Even with a closed DB, should not raise
        db2 = make_db()
        build_percentile_tables(db2)
        db2.close()
        result = lookup_percentiles(db2, "owner", "o1")
        assert result == {}


# ---------------------------------------------------------------------------
# TestGetPopulationCount
# ---------------------------------------------------------------------------

class TestGetPopulationCount:
    def setup_method(self):
        self.db = make_db()
        build_percentile_tables(self.db)

    def test_owner_count(self):
        count = get_population_count(self.db, "owner")
        assert count == 5

    def test_building_count(self):
        count = get_population_count(self.db, "building")
        assert count == 8

    def test_unknown_type_returns_0(self):
        count = get_population_count(self.db, "flying_saucer")
        assert count == 0

    def test_returns_int(self):
        count = get_population_count(self.db, "owner")
        assert isinstance(count, int)


# ---------------------------------------------------------------------------
# TestFormatPercentile
# ---------------------------------------------------------------------------

class TestFormatPercentile:
    def test_94th(self):
        assert format_percentile(0.94) == "94th"

    def test_1st(self):
        assert format_percentile(0.01) == "1st"

    def test_2nd(self):
        assert format_percentile(0.02) == "2nd"

    def test_3rd(self):
        assert format_percentile(0.03) == "3rd"

    def test_4th(self):
        assert format_percentile(0.04) == "4th"

    def test_11th_teen_exception(self):
        assert format_percentile(0.11) == "11th"

    def test_12th_teen_exception(self):
        assert format_percentile(0.12) == "12th"

    def test_13th_teen_exception(self):
        assert format_percentile(0.13) == "13th"

    def test_21st(self):
        assert format_percentile(0.21) == "21st"

    def test_22nd(self):
        assert format_percentile(0.22) == "22nd"

    def test_23rd(self):
        assert format_percentile(0.23) == "23rd"

    def test_0th(self):
        assert format_percentile(0.0) == "0th"

    def test_100th(self):
        assert format_percentile(1.0) == "100th"

    def test_50th(self):
        assert format_percentile(0.50) == "50th"

    def test_rounding(self):
        # 0.945 rounds to 95 (round half to even in Python, but int(round()) is fine)
        result = format_percentile(0.945)
        assert result in ("94th", "95th")  # accept either due to banker's rounding


# ---------------------------------------------------------------------------
# TestFormatPercentileBlock
# ---------------------------------------------------------------------------

class TestFormatPercentileBlock:
    def test_owner_header(self):
        pcts = {"violation_pctile": 0.94, "open_violation_pctile": 0.91}
        result = format_percentile_block(pcts, "owner", 194000)
        assert "PERCENTILE RANKING" in result
        assert "landlords" in result

    def test_population_count_formatted(self):
        pcts = {"violation_pctile": 0.50}
        result = format_percentile_block(pcts, "owner", 194000)
        assert "194,000" in result

    def test_violation_label(self):
        pcts = {"violation_pctile": 0.94}
        result = format_percentile_block(pcts, "owner", 100)
        assert "Violations" in result
        assert "94th" in result

    def test_open_violation_label(self):
        pcts = {"open_violation_pctile": 0.91}
        result = format_percentile_block(pcts, "building", 50)
        assert "Open violations" in result
        assert "91st" in result

    def test_class_c_label(self):
        pcts = {"class_c_pctile": 0.88}
        result = format_percentile_block(pcts, "owner", 50)
        assert "Class C" in result

    def test_portfolio_size_label(self):
        pcts = {"portfolio_size_pctile": 0.75}
        result = format_percentile_block(pcts, "owner", 50)
        assert "Portfolio size" in result

    def test_pipe_separator_between_metrics(self):
        pcts = {"violation_pctile": 0.94, "open_violation_pctile": 0.91}
        result = format_percentile_block(pcts, "owner", 100)
        assert "|" in result

    def test_building_entity_label(self):
        pcts = {"violation_pctile": 0.50}
        result = format_percentile_block(pcts, "building", 100)
        assert "buildings" in result

    def test_restaurant_entity_label(self):
        pcts = {"violation_pctile": 0.50}
        result = format_percentile_block(pcts, "restaurant", 100)
        assert "restaurants" in result

    def test_zip_entity_label(self):
        pcts = {"complaint_pctile": 0.50}
        result = format_percentile_block(pcts, "zip", 100)
        assert "ZIP codes" in result

    def test_precinct_entity_label(self):
        pcts = {"crime_pctile": 0.50}
        result = format_percentile_block(pcts, "precinct", 100)
        assert "precincts" in result

    def test_empty_percentiles_returns_empty_string(self):
        result = format_percentile_block({}, "owner", 100)
        assert result == ""

    def test_critical_violation_label(self):
        pcts = {"critical_pctile": 0.80}
        result = format_percentile_block(pcts, "restaurant", 100)
        assert "Critical violations" in result

    def test_complaint_pctile_label(self):
        pcts = {"complaint_pctile": 0.60}
        result = format_percentile_block(pcts, "zip", 100)
        assert "311 complaints" in result

    def test_crime_pctile_label(self):
        pcts = {"crime_pctile": 0.70}
        result = format_percentile_block(pcts, "precinct", 100)
        assert "Crime reports" in result

    def test_multiple_metrics_on_one_line(self):
        pcts = {
            "violation_pctile": 0.94,
            "open_violation_pctile": 0.91,
            "class_c_pctile": 0.88,
        }
        result = format_percentile_block(pcts, "owner", 194000)
        # All three should appear on the metrics line
        lines = result.strip().split("\n")
        metrics_line = lines[-1]
        assert "94th" in metrics_line
        assert "91st" in metrics_line
        assert "88th" in metrics_line


# ---------------------------------------------------------------------------
# TestDisclaimer
# ---------------------------------------------------------------------------

class TestDisclaimer:
    def test_disclaimer_is_string(self):
        assert isinstance(DISCLAIMER, str)

    def test_disclaimer_mentions_percentile(self):
        assert "Percentile" in DISCLAIMER or "percentile" in DISCLAIMER.lower()

    def test_disclaimer_starts_with_newline(self):
        assert DISCLAIMER.startswith("\n")
