"""Fuzzy entity search — phonetic blocking + rapidfuzz scoring.

Replaces raw UPPER() = UPPER() matching in entity_xray, person_crossref,
money_trail, marriage_search, vital_records, due_diligence.
"""


def phonetic_search_sql(
    first_name: str | None,
    last_name: str,
    min_score: float = 0.75,
    limit: int = 50,
) -> str:
    """SQL to find person matches using phonetic blocking + fuzzy scoring.

    Strategy:
    1. Block on double_metaphone(last_name) — reduces candidates by ~100x
    2. Score with jaro_winkler_similarity — handles typos, nicknames
    """
    escaped_last = last_name.replace("'", "''")

    if first_name:
        escaped_first = first_name.replace("'", "''")
        return f"""
            SELECT
                unique_id, last_name, first_name, source_table,
                jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) AS last_score,
                jaro_winkler_similarity(UPPER(first_name), UPPER('{escaped_first}')) AS first_score,
                (jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) * 0.6
                 + jaro_winkler_similarity(UPPER(first_name), UPPER('{escaped_first}')) * 0.4
                ) AS combined_score
            FROM lake.foundation.phonetic_index
            WHERE dm_last = double_metaphone(UPPER('{escaped_last}'))
              AND (jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) * 0.6
                   + jaro_winkler_similarity(UPPER(first_name), UPPER('{escaped_first}')) * 0.4
                  ) >= {min_score}
            ORDER BY combined_score DESC
            LIMIT {limit}
        """
    else:
        return f"""
            SELECT
                unique_id, last_name, first_name, source_table,
                jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) AS last_score,
                1.0 AS first_score,
                jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) AS combined_score
            FROM lake.foundation.phonetic_index
            WHERE dm_last = double_metaphone(UPPER('{escaped_last}'))
              AND jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) >= {min_score}
            ORDER BY combined_score DESC
            LIMIT {limit}
        """


def fuzzy_name_sql(
    name: str,
    table: str = "lake.federal.name_index",
    name_col: str = "last_name",
    min_score: int = 70,
    limit: int = 30,
) -> str:
    """SQL for fuzzy entity/company name matching using rapidfuzz token_sort_ratio."""
    escaped = name.replace("'", "''")
    return f"""
        SELECT *,
            rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER('{escaped}')) AS match_score
        FROM {table}
        WHERE rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER('{escaped}')) >= {min_score}
        ORDER BY match_score DESC
        LIMIT {limit}
    """


def phonetic_vital_search_sql(
    first_name: str | None,
    last_name: str,
    table: str,
    first_col: str,
    last_col: str,
    extra_cols: str = "",
    limit: int = 30,
) -> str:
    """Phonetic search for historical records where spelling was inconsistent."""
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
    """Fuzzy name matching for campaign finance / money trail."""
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
