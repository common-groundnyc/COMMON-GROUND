"""Tests for formatters.py — written first (TDD), RED before GREEN."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from formatters import (
    format_markdown_table,
    format_toon,
    format_markdown_kv,
    detect_format,
    filter_columns,
)


# ---------------------------------------------------------------------------
# TestMarkdownTable
# ---------------------------------------------------------------------------

class TestMarkdownTable:
    def test_basic(self):
        cols = ["name", "age"]
        rows = [["Alice", 30], ["Bob", 25]]
        result = format_markdown_table(cols, rows)
        assert "| name | age |" in result
        assert "| Alice | 30 |" in result
        assert "| Bob | 25 |" in result
        # separator row
        assert "|---|---|" in result or "| --- | --- |" in result

    def test_empty_cols(self):
        result = format_markdown_table([], [])
        assert result == "(no columns)"

    def test_empty_rows(self):
        result = format_markdown_table(["col1"], [])
        assert result == "(no rows)"

    def test_truncation_notice_shown(self):
        cols = ["id"]
        rows = [[i] for i in range(25)]
        result = format_markdown_table(cols, rows, max_rows=20)
        assert "showing 20 of 25" in result
        assert "sql_query()" in result
        assert "LIMIT" in result

    def test_no_truncation_notice_when_under_limit(self):
        cols = ["id"]
        rows = [[i] for i in range(10)]
        result = format_markdown_table(cols, rows, max_rows=20)
        assert "showing" not in result

    def test_cell_truncation_with_ellipsis(self):
        cols = ["desc"]
        long_val = "x" * 50
        rows = [[long_val]]
        result = format_markdown_table(cols, rows, max_cell=40)
        assert "x" * 40 in result
        assert "\u2026" in result  # ellipsis character
        # Should not contain the full value
        assert long_val not in result

    def test_cell_exactly_at_limit_not_truncated(self):
        cols = ["v"]
        val = "a" * 40
        rows = [[val]]
        result = format_markdown_table(cols, rows, max_cell=40)
        assert "\u2026" not in result
        assert val in result

    def test_pipe_escaping(self):
        cols = ["expr"]
        rows = [["a | b | c"]]
        result = format_markdown_table(cols, rows)
        # Pipes in values must be escaped
        assert r"a \| b \| c" in result

    def test_none_values_rendered_as_empty(self):
        cols = ["a", "b"]
        rows = [[None, "val"]]
        result = format_markdown_table(cols, rows)
        assert "| val |" in result
        # None should not appear literally
        assert "None" not in result

    def test_total_count_override(self):
        cols = ["id"]
        rows = [[i] for i in range(20)]
        # total_count says there are 1000 rows in the dataset
        result = format_markdown_table(cols, rows, max_rows=20, total_count=1000)
        assert "showing 20 of 1000" in result

    def test_total_count_no_notice_when_rows_under_max(self):
        cols = ["id"]
        rows = [[i] for i in range(5)]
        result = format_markdown_table(cols, rows, max_rows=20, total_count=5)
        assert "showing" not in result

    def test_pipe_escaping_happens_before_truncation(self):
        # Value is 36 chars + " | x" = 40 chars with pipe
        # After escaping " | " becomes " \| " (adds 1 char), total 41 -> truncated
        cols = ["v"]
        val = "a" * 36 + " | x"
        rows = [[val]]
        result = format_markdown_table(cols, rows, max_cell=40)
        # Escaped pipe should appear, then truncation
        assert r"\|" in result


# ---------------------------------------------------------------------------
# TestToon
# ---------------------------------------------------------------------------

class TestToon:
    def test_basic_header(self):
        cols = ["name", "city"]
        rows = [["Alice", "NYC"], ["Bob", "LA"]]
        result = format_toon(cols, rows)
        assert "results[2]{name,city}:" in result

    def test_basic_rows(self):
        cols = ["name", "city"]
        rows = [["Alice", "NYC"], ["Bob", "LA"]]
        result = format_toon(cols, rows)
        assert "Alice,NYC" in result
        assert "Bob,LA" in result

    def test_rows_are_indented(self):
        cols = ["a"]
        rows = [["v"]]
        result = format_toon(cols, rows)
        lines = result.strip().split("\n")
        # Row lines should be indented (start with whitespace)
        data_lines = [l for l in lines if "results[" not in l]
        assert all(l.startswith("  ") for l in data_lines if l.strip())

    def test_empty_cols(self):
        result = format_toon([], [])
        assert result == "(no columns)"

    def test_empty_rows(self):
        result = format_toon(["col1"], [])
        assert result == "(no rows)"

    def test_truncation_notice(self):
        cols = ["id"]
        rows = [[i] for i in range(600)]
        result = format_toon(cols, rows, max_rows=500)
        assert "showing 500 of 600" in result

    def test_comma_escaping(self):
        cols = ["addr"]
        rows = [["123 Main St, Apt 4"]]
        result = format_toon(cols, rows)
        assert r"123 Main St\, Apt 4" in result

    def test_none_values(self):
        cols = ["a", "b"]
        rows = [[None, "x"]]
        result = format_toon(cols, rows)
        assert "None" not in result

    def test_fewer_tokens_than_markdown_for_large_table(self):
        cols = ["id", "name", "city", "state", "zip"]
        rows = [[i, f"Person{i}", "New York", "NY", "10001"] for i in range(100)]
        md = format_markdown_table(cols, rows, max_rows=100)
        toon = format_toon(cols, rows, max_rows=100)
        # TOON should be significantly shorter
        assert len(toon) < len(md) * 0.85, (
            f"TOON ({len(toon)}) not meaningfully shorter than markdown ({len(md)})"
        )

    def test_total_count_override(self):
        cols = ["id"]
        rows = [[i] for i in range(500)]
        result = format_toon(cols, rows, max_rows=500, total_count=9999)
        assert "showing 500 of 9999" in result

    def test_cell_truncation_with_ellipsis(self):
        cols = ["desc"]
        long_val = "y" * 70
        rows = [[long_val]]
        result = format_toon(cols, rows, max_cell=60)
        assert "\u2026" in result
        assert long_val not in result


# ---------------------------------------------------------------------------
# TestMarkdownKV
# ---------------------------------------------------------------------------

class TestMarkdownKV:
    def test_single_record(self):
        cols = ["name", "age", "city"]
        rows = [["Alice", 30, "NYC"]]
        result = format_markdown_kv(cols, rows)
        assert result is not None
        assert "**name:** Alice" in result
        assert "**age:** 30" in result
        assert "**city:** NYC" in result

    def test_two_records(self):
        cols = ["name", "score"]
        rows = [["Alice", 95], ["Bob", 87]]
        result = format_markdown_kv(cols, rows)
        assert result is not None
        assert "---" in result
        assert "**name:** Alice" in result
        assert "**name:** Bob" in result

    def test_three_records(self):
        cols = ["x"]
        rows = [["a"], ["b"], ["c"]]
        result = format_markdown_kv(cols, rows)
        assert result is not None
        assert result.count("---") == 2

    def test_four_records_returns_none(self):
        cols = ["x"]
        rows = [["a"], ["b"], ["c"], ["d"]]
        result = format_markdown_kv(cols, rows)
        assert result is None

    def test_none_values(self):
        cols = ["a", "b"]
        rows = [[None, "val"]]
        result = format_markdown_kv(cols, rows)
        assert result is not None
        assert "**a:**" in result
        assert "None" not in result

    def test_empty_rows(self):
        result = format_markdown_kv(["col"], [])
        assert result is None or result == ""


# ---------------------------------------------------------------------------
# TestDetectFormat
# ---------------------------------------------------------------------------

class TestDetectFormat:
    def test_single_row_is_kv(self):
        assert detect_format(1, 5) == "kv"

    def test_three_rows_is_kv(self):
        assert detect_format(3, 8) == "kv"

    def test_four_rows_many_cols_is_markdown(self):
        assert detect_format(4, 5) == "markdown"

    def test_fifty_rows_many_cols_is_markdown(self):
        assert detect_format(50, 6) == "markdown"

    def test_fifty_one_rows_many_cols_is_toon(self):
        assert detect_format(51, 4) == "toon"

    def test_large_table_few_cols_stays_markdown(self):
        # 51+ rows but only 3 columns → markdown
        assert detect_format(100, 3) == "markdown"

    def test_zero_rows_is_markdown(self):
        # Edge: no rows, many cols
        result = detect_format(0, 10)
        assert result in ("kv", "markdown")  # either is acceptable


# ---------------------------------------------------------------------------
# TestFilterColumns
# ---------------------------------------------------------------------------

class TestFilterColumns:
    def test_under_max_passthrough(self):
        cols = ["a", "b", "c"]
        rows = [["1", "2", "3"], ["4", "5", "6"]]
        out_cols, out_rows = filter_columns(cols, rows, max_cols=12)
        assert out_cols == cols
        assert out_rows == rows

    def test_null_heavy_columns_removed(self):
        # col2 is >90% null, should be deprioritized
        cols = ["id", "name", "sparse"]
        rows = [
            [1, "Alice", None],
            [2, "Bob", None],
            [3, "Carol", None],
            [4, "Dave", None],
            [5, "Eve", None],
            [6, "Frank", None],
            [7, "Grace", None],
            [8, "Hank", None],
            [9, "Iris", None],
            [10, "Jake", "value"],
        ]
        out_cols, _ = filter_columns(cols, rows, max_cols=2)
        assert "id" in out_cols
        assert "name" in out_cols
        assert "sparse" not in out_cols

    def test_id_suffix_columns_deprioritized(self):
        cols = ["name", "score", "internal_id", "ref_fk"]
        rows = [["Alice", 95, 1, 100], ["Bob", 87, 2, 200]]
        out_cols, _ = filter_columns(cols, rows, max_cols=2)
        assert "name" in out_cols
        assert "score" in out_cols
        assert "internal_id" not in out_cols
        assert "ref_fk" not in out_cols

    def test_first_column_kept_even_if_id_suffix(self):
        # First column is special — always preserved even with _id suffix
        cols = ["entity_id", "score", "other_fk", "extra"]
        rows = [["a", 1, 2, 3], ["b", 4, 5, 6]]
        out_cols, _ = filter_columns(cols, rows, max_cols=2)
        assert "entity_id" in out_cols

    def test_original_order_preserved(self):
        cols = ["z", "a", "m", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"]
        rows = [[i] * 13 for i in range(5)]
        out_cols, _ = filter_columns(cols, rows, max_cols=5)
        # Output should be a subsequence of original order
        original_indices = [cols.index(c) for c in out_cols]
        assert original_indices == sorted(original_indices)

    def test_exact_max_cols_passthrough(self):
        cols = ["a", "b", "c"]
        rows = [["1", "2", "3"]]
        out_cols, out_rows = filter_columns(cols, rows, max_cols=3)
        assert out_cols == cols
