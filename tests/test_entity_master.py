import duckdb
import pytest
from dagster_pipeline.defs.entity_master_asset import (
    classify_entity_type,
    generate_entity_id,
    select_canonical_name,
    aggregate_confidence,
    build_entity_master,
)


@pytest.mark.parametrize("name,expected", [
    ("JOHN SMITH", "PERSON"),
    ("SMITH REALTY LLC", "ORGANIZATION"),
    ("123 MAIN ST HOLDINGS", "ORGANIZATION"),
    ("CITY OF NEW YORK", "ORGANIZATION"),
    ("ABC CORP", "ORGANIZATION"),
    ("MARY O'BRIEN", "PERSON"),
    ("VAN DER BERG", "PERSON"),
    ("FIRST NATIONAL BANK", "ORGANIZATION"),
    ("GREENPOINT MANAGEMENT", "ORGANIZATION"),
    ("DEPARTMENT OF EDUCATION", "ORGANIZATION"),
    ("SMITH", "PERSON"),
    ("", "UNKNOWN"),
    (None, "UNKNOWN"),
])
def test_classify_entity_type(name, expected):
    assert classify_entity_type(name) == expected


def test_entity_id_is_deterministic():
    members_a = ["unique_id_1", "unique_id_2", "unique_id_3"]
    members_b = ["unique_id_3", "unique_id_1", "unique_id_2"]
    assert generate_entity_id(members_a) == generate_entity_id(members_b)


def test_entity_id_differs_for_different_members():
    members_a = ["unique_id_1", "unique_id_2"]
    members_b = ["unique_id_1", "unique_id_3"]
    assert generate_entity_id(members_a) != generate_entity_id(members_b)


def test_entity_id_single_member():
    result = generate_entity_id(["unique_id_42"])
    assert len(str(result)) == 36
    assert "-" in str(result)


def test_entity_id_is_uuid_format():
    import uuid as uuid_mod
    result = generate_entity_id(["a", "b", "c"])
    parsed = uuid_mod.UUID(str(result))
    assert str(parsed) == str(result)


def test_canonical_name_most_frequent():
    records = [
        {"last_name": "SMITH", "first_name": "JOHN"},
        {"last_name": "SMITH", "first_name": "JOHN"},
        {"last_name": "SMITH", "first_name": "J"},
        {"last_name": "SMYTH", "first_name": "JOHN"},
    ]
    last, first = select_canonical_name(records)
    assert last == "SMITH"
    assert first == "JOHN"


def test_canonical_name_tiebreak_alphabetical():
    records = [
        {"last_name": "SMITH", "first_name": "JOHN"},
        {"last_name": "SMYTH", "first_name": "JOHN"},
    ]
    last, first = select_canonical_name(records)
    assert last == "SMITH"


def test_canonical_name_single_record():
    records = [{"last_name": "JONES", "first_name": "ALICE"}]
    last, first = select_canonical_name(records)
    assert last == "JONES"
    assert first == "ALICE"


def test_aggregate_confidence_returns_mean():
    assert abs(aggregate_confidence([0.95, 0.92, 0.98]) - 0.95) < 0.001


def test_aggregate_confidence_empty():
    assert aggregate_confidence([]) == 1.0


def test_aggregate_confidence_single():
    assert aggregate_confidence([0.93]) == 0.93


@pytest.fixture
def mock_lake():
    """Create an in-memory DuckDB with mock resolved_entities, name_index, pairwise_probabilities."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS lake__federal")
    conn.execute("CREATE SCHEMA IF NOT EXISTS lake__foundation")

    # name_index: two people sharing a cluster, one org in its own cluster
    conn.execute("""
        CREATE TABLE lake__federal__name_index AS
        SELECT * FROM (VALUES
            (1, 'SMITH', 'JOHN', 'housing.hpd_complaints'),
            (2, 'SMITH', 'JOHN', 'public_safety.nypd_arrests'),
            (3, 'SMITH', 'J', 'financial.acris_parties'),
            (4, 'GREENPOINT MANAGEMENT LLC', NULL, 'financial.acris_parties')
        ) AS t(unique_id, last_name, first_name, source_table)
    """)

    # resolved_entities: cluster 100 has records 1,2,3; cluster 200 has record 4
    conn.execute("""
        CREATE TABLE lake__federal__resolved_entities AS
        SELECT * FROM (VALUES
            (100, 1), (100, 2), (100, 3), (200, 4)
        ) AS t(cluster_id, unique_id)
    """)

    # pairwise_probabilities
    conn.execute("""
        CREATE TABLE lake__federal__pairwise_probabilities AS
        SELECT * FROM (VALUES
            (1, 2, 0.95),
            (1, 3, 0.88),
            (2, 3, 0.90)
        ) AS t(unique_id_l, unique_id_r, match_probability)
    """)

    return conn


def test_build_entity_master_produces_rows(mock_lake):
    result = build_entity_master(mock_lake, table_prefix="lake__federal__", output_table="entity_master_out")
    rows = mock_lake.execute("SELECT * FROM entity_master_out ORDER BY member_count DESC").fetchall()
    assert len(rows) == 2


def test_build_entity_master_cluster_person(mock_lake):
    build_entity_master(mock_lake, table_prefix="lake__federal__", output_table="entity_master_out")
    row = mock_lake.execute(
        "SELECT * FROM entity_master_out WHERE member_count = 3"
    ).fetchone()
    # columns: entity_id, canonical_last, canonical_first, entity_type, confidence, member_count, source_count
    assert row is not None
    assert len(str(row[0])) == 36  # entity_id is UUID
    assert row[1] == "SMITH"       # canonical_last
    assert row[2] == "JOHN"        # canonical_first
    assert row[3] == "PERSON"      # entity_type
    assert row[4] > 0              # confidence
    assert row[5] == 3             # member_count
    assert row[6] == 3             # source_count (3 distinct sources)


def test_build_entity_master_cluster_org(mock_lake):
    build_entity_master(mock_lake, table_prefix="lake__federal__", output_table="entity_master_out")
    row = mock_lake.execute(
        "SELECT * FROM entity_master_out WHERE member_count = 1"
    ).fetchone()
    assert row is not None
    assert row[3] == "ORGANIZATION"  # entity_type
    assert row[4] == 1.0            # confidence = 1.0 for single member (no pairs)


def test_build_entity_master_deterministic_ids(mock_lake):
    build_entity_master(mock_lake, table_prefix="lake__federal__", output_table="em1")
    build_entity_master(mock_lake, table_prefix="lake__federal__", output_table="em2")
    ids1 = mock_lake.execute("SELECT entity_id FROM em1 ORDER BY entity_id").fetchall()
    ids2 = mock_lake.execute("SELECT entity_id FROM em2 ORDER BY entity_id").fetchall()
    assert ids1 == ids2
