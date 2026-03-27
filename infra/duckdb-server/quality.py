"""Data quality helpers — datasketches profiling + anomaly detection.

Provides SQL builders for approximate analytics (HLL distinct counts,
KLL quantiles, frequent items) and anomaly flagging (IQR).
"""

import re

_VALID_IDENTIFIERS = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]*$')


def _validate_identifier(name: str) -> str:
    if not _VALID_IDENTIFIERS.match(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


def approx_distinct_sql(table: str, column: str) -> str:
    """Approximate distinct count using HyperLogLog."""
    _validate_identifier(table)
    _validate_identifier(column)
    return f"""
        SELECT datasketch_hll_estimate(datasketch_hll({column})) AS approx_distinct
        FROM {table}
        WHERE {column} IS NOT NULL
    """


def approx_quantiles_sql(table: str, column: str) -> str:
    """Approximate quantiles (p25, p50, p75, p95, p99) using KLL sketch."""
    _validate_identifier(table)
    _validate_identifier(column)
    return f"""
        WITH sketch AS (
            SELECT datasketch_kll(TRY_CAST({column} AS FLOAT)) AS s
            FROM {table}
            WHERE TRY_CAST({column} AS FLOAT) IS NOT NULL
        )
        SELECT
            datasketch_kll_quantile(s, 0.25) AS p25,
            datasketch_kll_quantile(s, 0.50) AS median,
            datasketch_kll_quantile(s, 0.75) AS p75,
            datasketch_kll_quantile(s, 0.95) AS p95,
            datasketch_kll_quantile(s, 0.99) AS p99
        FROM sketch
    """


def frequent_items_sql(table: str, column: str, top_n: int = 20) -> str:
    """Find most frequent items using GROUP BY aggregation."""
    _validate_identifier(table)
    _validate_identifier(column)
    return f"""
        SELECT {column}, COUNT(*) AS freq
        FROM {table}
        WHERE {column} IS NOT NULL
        GROUP BY {column}
        ORDER BY freq DESC
        LIMIT {top_n}
    """


def iqr_outliers_sql(table: str, column: str, multiplier: float = 1.5) -> str:
    """Flag IQR outliers in a numeric column."""
    _validate_identifier(table)
    _validate_identifier(column)
    return f"""
        WITH stats AS (
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TRY_CAST({column} AS DOUBLE)) AS q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TRY_CAST({column} AS DOUBLE)) AS q3
            FROM {table}
            WHERE TRY_CAST({column} AS DOUBLE) IS NOT NULL
        ),
        bounds AS (
            SELECT q1, q3, (q3 - q1) AS iqr,
                   q1 - {multiplier} * (q3 - q1) AS lower_bound,
                   q3 + {multiplier} * (q3 - q1) AS upper_bound
            FROM stats
        )
        SELECT t.*, b.lower_bound, b.upper_bound,
               CASE WHEN TRY_CAST(t.{column} AS DOUBLE) < b.lower_bound THEN 'low_outlier'
                    WHEN TRY_CAST(t.{column} AS DOUBLE) > b.upper_bound THEN 'high_outlier'
                    ELSE 'normal' END AS outlier_flag
        FROM {table} t, bounds b
        WHERE TRY_CAST(t.{column} AS DOUBLE) < b.lower_bound
           OR TRY_CAST(t.{column} AS DOUBLE) > b.upper_bound
        ORDER BY TRY_CAST(t.{column} AS DOUBLE) DESC
    """
