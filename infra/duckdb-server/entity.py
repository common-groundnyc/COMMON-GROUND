"""Fuzzy entity search — phonetic blocking + rapidfuzz scoring.

Replaces raw UPPER() = UPPER() matching in entity_xray, person_crossref,
money_trail, marriage_search, vital_records, due_diligence.
"""

from sql_utils import validate_identifier as _validate_identifier


def phonetic_search_sql(
    first_name: str | None,
    last_name: str,
    min_score: float = 0.75,
    limit: int = 50,
) -> tuple[str, list]:
    """SQL + params for phonetic person search. Returns (sql, params) tuple."""
    if first_name:
        sql = """
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
                  ) >= ?
            ORDER BY combined_score DESC
            LIMIT ?
        """
        params = [last_name, first_name, last_name, first_name, last_name, last_name, first_name, min_score, limit]
        return sql, params
    else:
        sql = """
            SELECT
                unique_id, last_name, first_name, source_table,
                jaro_winkler_similarity(UPPER(last_name), UPPER(?)) AS last_score,
                1.0 AS first_score,
                jaro_winkler_similarity(UPPER(last_name), UPPER(?)) AS combined_score
            FROM lake.foundation.phonetic_index
            WHERE dm_last = double_metaphone(UPPER(?))
              AND jaro_winkler_similarity(UPPER(last_name), UPPER(?)) >= ?
            ORDER BY combined_score DESC
            LIMIT ?
        """
        params = [last_name, last_name, last_name, last_name, min_score, limit]
        return sql, params


def fuzzy_name_sql(
    name: str,
    table: str = "lake.federal.name_index",
    name_col: str = "last_name",
    min_score: int = 70,
    limit: int = 30,
) -> tuple[str, list]:
    """SQL + params for fuzzy entity/company name matching."""
    _validate_identifier(table)
    _validate_identifier(name_col)
    sql = f"""
        SELECT *,
            rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) AS match_score
        FROM {table}
        WHERE rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) >= ?
        ORDER BY match_score DESC
        LIMIT ?
    """
    return sql, [name, name, min_score, limit]


def phonetic_vital_search_sql(
    first_name: str | None,
    last_name: str,
    table: str,
    first_col: str,
    last_col: str,
    extra_cols: str = "",
    limit: int = 30,
) -> tuple[str, list]:
    """Phonetic search for historical records. Returns (sql, params) tuple."""
    _validate_identifier(table)
    _validate_identifier(first_col)
    _validate_identifier(last_col)
    if extra_cols:
        for col in extra_cols.split(","):
            _validate_identifier(col.strip())
    extra = f", {extra_cols}" if extra_cols else ""

    if first_name:
        sql = f"""
            SELECT {first_col}, {last_col}{extra},
                jaro_winkler_similarity(UPPER({last_col}), UPPER(?)) AS last_score,
                jaro_winkler_similarity(UPPER({first_col}), UPPER(?)) AS first_score
            FROM {table}
            WHERE soundex(UPPER({last_col})) = soundex(UPPER(?))
            ORDER BY last_score DESC, first_score DESC
            LIMIT ?
        """
        return sql, [last_name, first_name, last_name, limit]
    else:
        sql = f"""
            SELECT {first_col}, {last_col}{extra},
                jaro_winkler_similarity(UPPER({last_col}), UPPER(?)) AS last_score
            FROM {table}
            WHERE soundex(UPPER({last_col})) = soundex(UPPER(?))
            ORDER BY last_score DESC
            LIMIT ?
        """
        return sql, [last_name, last_name, limit]


def fuzzy_money_search_sql(
    name: str,
    table: str,
    name_col: str,
    extra_cols: str = "",
    min_score: int = 70,
    limit: int = 30,
) -> tuple[str, list]:
    """Fuzzy name matching for campaign finance. Returns (sql, params) tuple."""
    _validate_identifier(table)
    _validate_identifier(name_col)
    if extra_cols:
        for col in extra_cols.split(","):
            _validate_identifier(col.strip())
    extra = f", {extra_cols}" if extra_cols else ""
    sql = f"""
        SELECT {name_col}{extra},
            rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) AS match_score
        FROM {table}
        WHERE rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) >= ?
        ORDER BY match_score DESC
        LIMIT ?
    """
    return sql, [name, name, min_score, limit]
