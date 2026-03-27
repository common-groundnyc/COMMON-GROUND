"""Fuzzy entity search — phonetic blocking + rapidfuzz scoring.

Replaces raw UPPER() = UPPER() matching in entity_xray, person_crossref,
money_trail, marriage_search, vital_records, due_diligence.
"""

import re

_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]*$')


def _validate_identifier(name: str) -> str:
    if not name or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _validate_extra_cols(extra_cols: str) -> str:
    for col in extra_cols.split(","):
        _validate_identifier(col.strip())
    return extra_cols


def phonetic_search_sql(
    first_name: str | None,
    last_name: str,
    min_score: float = 0.75,
    limit: int = 50,
) -> tuple[str, list]:
    """SQL to find person matches using phonetic blocking + fuzzy scoring.

    Strategy:
    1. Block on double_metaphone(last_name) — reduces candidates by ~100x
    2. Score with jaro_winkler_similarity — handles typos, nicknames
    """
    if first_name:
        sql = f"""
            SELECT
                unique_id, last_name, first_name, source_table,
                jaro_winkler_similarity(UPPER(last_name), UPPER(?)) AS last_score,
                jaro_winkler_similarity(UPPER(first_name), UPPER(?)) AS first_score,
                (jaro_winkler_similarity(UPPER(last_name), UPPER(?)) * 0.6
                 + jaro_winkler_similarity(UPPER(first_name), UPPER(?)) * 0.4
                ) AS combined_score
            FROM lake.foundation.phonetic_index
            WHERE dm_last = double_metaphone(UPPER(?))
              AND (jaro_winkler_similarity(UPPER(last_name), UPPER(?)) * 0.6
                   + jaro_winkler_similarity(UPPER(first_name), UPPER(?)) * 0.4
                  ) >= {min_score}
            ORDER BY combined_score DESC
            LIMIT {limit}
        """
        params = [
            last_name, first_name,
            last_name, first_name,
            last_name,
            last_name, first_name,
        ]
    else:
        sql = f"""
            SELECT
                unique_id, last_name, first_name, source_table,
                jaro_winkler_similarity(UPPER(last_name), UPPER(?)) AS last_score,
                1.0 AS first_score,
                jaro_winkler_similarity(UPPER(last_name), UPPER(?)) AS combined_score
            FROM lake.foundation.phonetic_index
            WHERE dm_last = double_metaphone(UPPER(?))
              AND jaro_winkler_similarity(UPPER(last_name), UPPER(?)) >= {min_score}
            ORDER BY combined_score DESC
            LIMIT {limit}
        """
        params = [last_name, last_name, last_name, last_name]

    return sql, params


def fuzzy_name_sql(
    name: str,
    table: str = "lake.federal.name_index",
    name_col: str = "last_name",
    min_score: int = 70,
    limit: int = 30,
) -> tuple[str, list]:
    """SQL for fuzzy entity/company name matching using rapidfuzz token_sort_ratio."""
    _validate_identifier(table)
    _validate_identifier(name_col)

    sql = f"""
        SELECT *,
            rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) AS match_score
        FROM {table}
        WHERE rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) >= {min_score}
        ORDER BY match_score DESC
        LIMIT {limit}
    """
    params = [name, name]
    return sql, params


def phonetic_vital_search_sql(
    first_name: str | None,
    last_name: str,
    table: str,
    first_col: str,
    last_col: str,
    extra_cols: str = "",
    limit: int = 30,
) -> tuple[str, list]:
    """Phonetic search for historical records where spelling was inconsistent."""
    _validate_identifier(table)
    _validate_identifier(first_col)
    _validate_identifier(last_col)
    if extra_cols:
        _validate_extra_cols(extra_cols)

    extra = f", {extra_cols}" if extra_cols else ""

    if first_name:
        sql = f"""
            SELECT {first_col}, {last_col}{extra},
                jaro_winkler_similarity(UPPER({last_col}), UPPER(?)) AS last_score,
                jaro_winkler_similarity(UPPER({first_col}), UPPER(?)) AS first_score
            FROM {table}
            WHERE soundex(UPPER({last_col})) = soundex(UPPER(?))
            ORDER BY last_score DESC, first_score DESC
            LIMIT {limit}
        """
        params = [last_name, first_name, last_name]
    else:
        sql = f"""
            SELECT {first_col}, {last_col}{extra},
                jaro_winkler_similarity(UPPER({last_col}), UPPER(?)) AS last_score
            FROM {table}
            WHERE soundex(UPPER({last_col})) = soundex(UPPER(?))
            ORDER BY last_score DESC
            LIMIT {limit}
        """
        params = [last_name, last_name]

    return sql, params


def fuzzy_money_search_sql(
    name: str,
    table: str,
    name_col: str,
    extra_cols: str = "",
    min_score: int = 70,
    limit: int = 30,
) -> tuple[str, list]:
    """Fuzzy name matching for campaign finance / money trail."""
    _validate_identifier(table)
    _validate_identifier(name_col)
    if extra_cols:
        _validate_extra_cols(extra_cols)

    extra = f", {extra_cols}" if extra_cols else ""
    sql = f"""
        SELECT {name_col}{extra},
            rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) AS match_score
        FROM {table}
        WHERE rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) >= {min_score}
        ORDER BY match_score DESC
        LIMIT {limit}
    """
    params = [name, name]
    return sql, params
