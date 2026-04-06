import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from csv_export import generate_branded_csv, write_export


def parse_csv_body(csv_text):
    """Extract non-comment lines and parse as CSV."""
    lines = [l for l in csv_text.splitlines() if not l.startswith("#")]
    return list(csv.reader(lines))


def get_comment_lines(csv_text):
    return [l for l in csv_text.splitlines() if l.startswith("#")]


# --- generate_branded_csv ---

def test_header_contains_branding():
    result = generate_branded_csv(["name"], [["Alice"]])
    assert "COMMON GROUND" in result
    assert "common-ground.nyc" in result


def test_row_id_column_present():
    rows = parse_csv_body(generate_branded_csv(["name"], [["Alice"], ["Bob"]]))
    assert rows[0][0] == "row_id"


def test_data_rows_present():
    rows = parse_csv_body(generate_branded_csv(["name"], [["Alice"], ["Bob"]]))
    assert rows[1] == ["1", "Alice"]
    assert rows[2] == ["2", "Bob"]


def test_row_id_sequential_one_indexed():
    rows = parse_csv_body(generate_branded_csv(["x"], [["a"], ["b"], ["c"]]))
    ids = [r[0] for r in rows[1:]]
    assert ids == ["1", "2", "3"]


def test_commas_in_values_quoted():
    result = generate_branded_csv(["addr"], [["123 Main St, Apt 4"]])
    assert '"123 Main St, Apt 4"' in result


def test_quotes_in_values_escaped():
    result = generate_branded_csv(["note"], [['He said "hello"']])
    assert '""hello""' in result or '\\"hello\\"' in result or '"He said ""hello"""' in result


def test_none_values_become_empty_string():
    rows = parse_csv_body(generate_branded_csv(["a", "b"], [[None, "x"]]))
    assert rows[1] == ["1", "", "x"]


def test_sql_in_header_when_provided():
    result = generate_branded_csv(["x"], [["1"]], sql="SELECT * FROM foo")
    comments = get_comment_lines(result)
    assert any("SELECT * FROM foo" in c for c in comments)


def test_sql_truncated_to_200_chars():
    long_sql = "SELECT " + "a, " * 100 + "b FROM foo"
    result = generate_branded_csv(["x"], [["1"]], sql=long_sql)
    comments = get_comment_lines(result)
    sql_line = next(c for c in comments if "SELECT" in c)
    # The SQL portion in the line should not exceed 200 chars
    sql_part = sql_line.split("# Query: ", 1)[1] if "# Query: " in sql_line else ""
    assert len(sql_part) <= 200


def test_tool_name_in_header():
    result = generate_branded_csv(["x"], [["1"]], tool_name="building_profile")
    comments = get_comment_lines(result)
    assert any("building_profile" in c for c in comments)


def test_tool_args_in_header():
    result = generate_branded_csv(["x"], [["1"]], tool_name="building_profile", tool_args={"bbl": "1000670001"})
    comments = get_comment_lines(result)
    assert any("bbl=" in c for c in comments)


def test_sources_in_header():
    result = generate_branded_csv(["x"], [["1"]], sources=["HPD violations", "DOB permits"])
    comments = get_comment_lines(result)
    assert any("HPD violations" in c for c in comments)


def test_empty_rows_rows_count_zero():
    result = generate_branded_csv(["x"], [])
    assert "Rows: 0" in result


def test_empty_rows_no_data_lines():
    rows = parse_csv_body(generate_branded_csv(["x"], []))
    # Only header row, no data
    assert len(rows) == 1
    assert rows[0][0] == "row_id"


def test_timestamp_in_header():
    result = generate_branded_csv(["x"], [["1"]])
    comments = get_comment_lines(result)
    assert any("Generated:" in c for c in comments)


def test_rows_count_in_header():
    result = generate_branded_csv(["a"], [["1"], ["2"], ["3"]])
    assert "Rows: 3" in result


def test_comment_lines_count_in_header():
    result = generate_branded_csv(["x"], [["1"]])
    comments = get_comment_lines(result)
    count_line = next((c for c in comments if "Comment lines:" in c), None)
    assert count_line is not None
    n = int(count_line.split("Comment lines:")[1].strip().split()[0])
    assert n == len(comments)


def test_footer_present():
    result = generate_branded_csv(["x"], [["1"]])
    assert "DuckDB" in result
    assert "DuckLake" in result


def test_no_sql_no_query_line():
    result = generate_branded_csv(["x"], [["1"]])
    assert "# Query:" not in result


def test_no_tool_no_tool_line():
    result = generate_branded_csv(["x"], [["1"]])
    assert "# Tool:" not in result


def test_no_sources_no_sources_line():
    result = generate_branded_csv(["x"], [["1"]])
    assert "# Sources:" not in result


# --- write_export ---

def test_write_export_creates_file():
    with tempfile.TemporaryDirectory() as d:
        csv_text = generate_branded_csv(["x"], [["1"]])
        path = write_export(csv_text, "test_export", export_dir=d)
        assert os.path.isfile(path)


def test_write_export_filename_includes_name():
    with tempfile.TemporaryDirectory() as d:
        path = write_export("data", "my_report", export_dir=d)
        assert "my_report" in os.path.basename(path)


def test_write_export_filename_includes_timestamp():
    with tempfile.TemporaryDirectory() as d:
        path = write_export("data", "rpt", export_dir=d)
        name = os.path.basename(path)
        # timestamp format YYYYMMDD_HHMMSS — 15 chars
        assert len(name) > 20
        assert name.endswith(".csv")


def test_write_export_creates_directory():
    with tempfile.TemporaryDirectory() as base:
        new_dir = os.path.join(base, "subdir", "exports")
        path = write_export("data", "x", export_dir=new_dir)
        assert os.path.isdir(new_dir)
        assert os.path.isfile(path)


def test_write_export_sanitizes_name():
    with tempfile.TemporaryDirectory() as d:
        path = write_export("data", "hello world/foo:bar", export_dir=d)
        name = os.path.basename(path)
        # Only safe chars before timestamp
        safe_part = name.split("_20")[0]  # split before timestamp
        assert all(c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in safe_part)


def test_write_export_long_name_truncated():
    with tempfile.TemporaryDirectory() as d:
        long_name = "a" * 200
        path = write_export("data", long_name, export_dir=d)
        name = os.path.basename(path)
        # filename = {safe_name}_{YYYYMMDD_HHMMSS}.csv
        # Strip .csv, then strip last 16 chars (_YYYYMMDD_HHMMSS) to get safe_name
        stem = name[:-4]  # remove .csv
        safe_part = stem[:-16]  # remove _YYYYMMDD_HHMMSS (underscore + 15 chars)
        assert len(safe_part) <= 100


def test_write_export_returns_absolute_path():
    with tempfile.TemporaryDirectory() as d:
        path = write_export("data", "rpt", export_dir=d)
        assert os.path.isabs(path)


def test_write_export_file_contents():
    with tempfile.TemporaryDirectory() as d:
        csv_text = generate_branded_csv(["name"], [["Alice"]])
        path = write_export(csv_text, "test", export_dir=d)
        with open(path) as f:
            content = f.read()
        assert content == csv_text
