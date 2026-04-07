"""Anomaly detection across NYC open data — surfaces unusual patterns automatically."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AnomalyMetric:
    """Definition of a metric to monitor for anomalies."""
    name: str
    description: str
    sql_current: str  # SQL returning current period value
    sql_baseline: str  # SQL returning baseline (avg, stddev) over historical window
    group_by: str  # grouping dimension (e.g., ZIP, borough, owner)
    unit: str  # display unit (e.g., "complaints", "violations", "sales")


# 10 key metrics to monitor
ANOMALY_METRICS: list[AnomalyMetric] = [
    AnomalyMetric(
        name="311_noise_complaints",
        description="311 noise complaints by ZIP code",
        sql_current="""
            SELECT incident_zip AS group_key, COUNT(*) AS current_value
            FROM lake.social_services.n311_service_requests
            WHERE complaint_type ILIKE '%noise%'
              AND created_date >= CURRENT_DATE - INTERVAL '7 days'
              AND incident_zip IS NOT NULL AND LENGTH(incident_zip) = 5
            GROUP BY incident_zip
        """,
        sql_baseline="""
            SELECT incident_zip AS group_key,
                   AVG(weekly_count) AS avg_value,
                   STDDEV(weekly_count) AS stddev_value
            FROM (
                SELECT incident_zip,
                       DATE_TRUNC('week', TRY_CAST(created_date AS DATE)) AS week,
                       COUNT(*) AS weekly_count
                FROM lake.social_services.n311_service_requests
                WHERE complaint_type ILIKE '%noise%'
                  AND created_date >= CURRENT_DATE - INTERVAL '90 days'
                  AND created_date < CURRENT_DATE - INTERVAL '7 days'
                  AND incident_zip IS NOT NULL AND LENGTH(incident_zip) = 5
                GROUP BY incident_zip, week
            )
            GROUP BY incident_zip
            HAVING COUNT(*) >= 4
        """,
        group_by="ZIP code",
        unit="noise complaints",
    ),
    AnomalyMetric(
        name="hpd_violations_opened",
        description="New HPD violations by borough",
        sql_current="""
            SELECT boro AS group_key, COUNT(*) AS current_value
            FROM lake.housing.hpd_violations
            WHERE TRY_CAST(inspectiondate AS DATE) >= CURRENT_DATE - INTERVAL '7 days'
              AND boro IS NOT NULL
            GROUP BY boro
        """,
        sql_baseline="""
            SELECT boro AS group_key,
                   AVG(weekly_count) AS avg_value,
                   STDDEV(weekly_count) AS stddev_value
            FROM (
                SELECT boro,
                       DATE_TRUNC('week', TRY_CAST(inspectiondate AS DATE)) AS week,
                       COUNT(*) AS weekly_count
                FROM lake.housing.hpd_violations
                WHERE TRY_CAST(inspectiondate AS DATE) >= CURRENT_DATE - INTERVAL '90 days'
                  AND TRY_CAST(inspectiondate AS DATE) < CURRENT_DATE - INTERVAL '7 days'
                  AND boro IS NOT NULL
                GROUP BY boro, week
            )
            GROUP BY boro
            HAVING COUNT(*) >= 4
        """,
        group_by="borough",
        unit="HPD violations",
    ),
    AnomalyMetric(
        name="restaurant_failures",
        description="Restaurant inspection failures by borough",
        sql_current="""
            SELECT boro AS group_key, approx_count_distinct(camis) AS current_value
            FROM lake.health.restaurant_inspections
            WHERE grade IN ('C', 'Z', 'P')
              AND TRY_CAST(inspection_date AS DATE) >= CURRENT_DATE - INTERVAL '30 days'
              AND boro IS NOT NULL
            GROUP BY boro
        """,
        sql_baseline="""
            SELECT boro AS group_key,
                   AVG(monthly_count) AS avg_value,
                   STDDEV(monthly_count) AS stddev_value
            FROM (
                SELECT boro,
                       DATE_TRUNC('month', TRY_CAST(inspection_date AS DATE)) AS month,
                       approx_count_distinct(camis) AS monthly_count
                FROM lake.health.restaurant_inspections
                WHERE grade IN ('C', 'Z', 'P')
                  AND TRY_CAST(inspection_date AS DATE) >= CURRENT_DATE - INTERVAL '365 days'
                  AND TRY_CAST(inspection_date AS DATE) < CURRENT_DATE - INTERVAL '30 days'
                  AND boro IS NOT NULL
                GROUP BY boro, month
            )
            GROUP BY boro
            HAVING COUNT(*) >= 3
        """,
        group_by="borough",
        unit="restaurant failures",
    ),
    AnomalyMetric(
        name="dob_permits",
        description="New DOB construction permits by borough",
        sql_current="""
            SELECT borough AS group_key, COUNT(*) AS current_value
            FROM lake.housing.dob_permit_issuance
            WHERE TRY_CAST(issuance_date AS DATE) >= CURRENT_DATE - INTERVAL '30 days'
              AND borough IS NOT NULL
            GROUP BY borough
        """,
        sql_baseline="""
            SELECT borough AS group_key,
                   AVG(monthly_count) AS avg_value,
                   STDDEV(monthly_count) AS stddev_value
            FROM (
                SELECT borough,
                       DATE_TRUNC('month', TRY_CAST(issuance_date AS DATE)) AS month,
                       COUNT(*) AS monthly_count
                FROM lake.housing.dob_permit_issuance
                WHERE TRY_CAST(issuance_date AS DATE) >= CURRENT_DATE - INTERVAL '365 days'
                  AND TRY_CAST(issuance_date AS DATE) < CURRENT_DATE - INTERVAL '30 days'
                  AND borough IS NOT NULL
                GROUP BY borough, month
            )
            GROUP BY borough
            HAVING COUNT(*) >= 3
        """,
        group_by="borough",
        unit="construction permits",
    ),
    AnomalyMetric(
        name="evictions",
        description="Evictions executed by ZIP code",
        sql_current="""
            SELECT eviction_zip AS group_key, COUNT(*) AS current_value
            FROM lake.housing.evictions
            WHERE TRY_CAST(executed_date AS DATE) >= CURRENT_DATE - INTERVAL '30 days'
              AND eviction_zip IS NOT NULL AND LENGTH(eviction_zip) = 5
            GROUP BY eviction_zip
        """,
        sql_baseline="""
            SELECT eviction_zip AS group_key,
                   AVG(monthly_count) AS avg_value,
                   STDDEV(monthly_count) AS stddev_value
            FROM (
                SELECT eviction_zip,
                       DATE_TRUNC('month', TRY_CAST(executed_date AS DATE)) AS month,
                       COUNT(*) AS monthly_count
                FROM lake.housing.evictions
                WHERE TRY_CAST(executed_date AS DATE) >= CURRENT_DATE - INTERVAL '365 days'
                  AND TRY_CAST(executed_date AS DATE) < CURRENT_DATE - INTERVAL '30 days'
                  AND eviction_zip IS NOT NULL AND LENGTH(eviction_zip) = 5
                GROUP BY eviction_zip, month
            )
            GROUP BY eviction_zip
            HAVING COUNT(*) >= 3
        """,
        group_by="ZIP code",
        unit="evictions",
    ),
    AnomalyMetric(
        name="nypd_complaints",
        description="NYPD felony complaints by precinct",
        sql_current="""
            SELECT addr_pct_cd::VARCHAR AS group_key, COUNT(*) AS current_value
            FROM lake.public_safety.nypd_complaints_ytd
            WHERE law_cat_cd = 'FELONY'
              AND TRY_CAST(cmplnt_fr_dt AS DATE) >= CURRENT_DATE - INTERVAL '30 days'
              AND addr_pct_cd IS NOT NULL
            GROUP BY addr_pct_cd
        """,
        sql_baseline="""
            SELECT addr_pct_cd::VARCHAR AS group_key,
                   AVG(monthly_count) AS avg_value,
                   STDDEV(monthly_count) AS stddev_value
            FROM (
                SELECT addr_pct_cd,
                       DATE_TRUNC('month', TRY_CAST(cmplnt_fr_dt AS DATE)) AS month,
                       COUNT(*) AS monthly_count
                FROM lake.public_safety.nypd_complaints_historic
                WHERE law_cat_cd = 'FELONY'
                  AND TRY_CAST(cmplnt_fr_dt AS DATE) >= CURRENT_DATE - INTERVAL '365 days'
                  AND TRY_CAST(cmplnt_fr_dt AS DATE) < CURRENT_DATE - INTERVAL '30 days'
                  AND addr_pct_cd IS NOT NULL
                GROUP BY addr_pct_cd, month
            )
            GROUP BY addr_pct_cd
            HAVING COUNT(*) >= 3
        """,
        group_by="precinct",
        unit="felony complaints",
    ),
    AnomalyMetric(
        name="311_heat_complaints",
        description="311 heat/hot water complaints by ZIP code",
        sql_current="""
            SELECT incident_zip AS group_key, COUNT(*) AS current_value
            FROM lake.social_services.n311_service_requests
            WHERE complaint_type ILIKE '%heat%'
              AND created_date >= CURRENT_DATE - INTERVAL '7 days'
              AND incident_zip IS NOT NULL AND LENGTH(incident_zip) = 5
            GROUP BY incident_zip
        """,
        sql_baseline="""
            SELECT incident_zip AS group_key,
                   AVG(weekly_count) AS avg_value,
                   STDDEV(weekly_count) AS stddev_value
            FROM (
                SELECT incident_zip,
                       DATE_TRUNC('week', TRY_CAST(created_date AS DATE)) AS week,
                       COUNT(*) AS weekly_count
                FROM lake.social_services.n311_service_requests
                WHERE complaint_type ILIKE '%heat%'
                  AND created_date >= CURRENT_DATE - INTERVAL '90 days'
                  AND created_date < CURRENT_DATE - INTERVAL '7 days'
                  AND incident_zip IS NOT NULL AND LENGTH(incident_zip) = 5
                GROUP BY incident_zip, week
            )
            GROUP BY incident_zip
            HAVING COUNT(*) >= 4
        """,
        group_by="ZIP code",
        unit="heat complaints",
    ),
    AnomalyMetric(
        name="motor_vehicle_collisions",
        description="Motor vehicle collisions by borough",
        sql_current="""
            SELECT borough AS group_key, COUNT(*) AS current_value
            FROM lake.public_safety.motor_vehicle_collisions_crashes
            WHERE TRY_CAST(crash_date AS DATE) >= CURRENT_DATE - INTERVAL '30 days'
              AND borough IS NOT NULL
            GROUP BY borough
        """,
        sql_baseline="""
            SELECT borough AS group_key,
                   AVG(monthly_count) AS avg_value,
                   STDDEV(monthly_count) AS stddev_value
            FROM (
                SELECT borough,
                       DATE_TRUNC('month', TRY_CAST(crash_date AS DATE)) AS month,
                       COUNT(*) AS monthly_count
                FROM lake.public_safety.motor_vehicle_collisions_crashes
                WHERE TRY_CAST(crash_date AS DATE) >= CURRENT_DATE - INTERVAL '365 days'
                  AND TRY_CAST(crash_date AS DATE) < CURRENT_DATE - INTERVAL '30 days'
                  AND borough IS NOT NULL
                GROUP BY borough, month
            )
            GROUP BY borough
            HAVING COUNT(*) >= 3
        """,
        group_by="borough",
        unit="collisions",
    ),
    AnomalyMetric(
        name="hpd_complaints_opened",
        description="New HPD housing complaints by borough",
        sql_current="""
            SELECT borough AS group_key, COUNT(*) AS current_value
            FROM lake.housing.hpd_complaints
            WHERE TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL '7 days'
              AND borough IS NOT NULL
            GROUP BY borough
        """,
        sql_baseline="""
            SELECT borough AS group_key,
                   AVG(weekly_count) AS avg_value,
                   STDDEV(weekly_count) AS stddev_value
            FROM (
                SELECT borough,
                       DATE_TRUNC('week', TRY_CAST(received_date AS DATE)) AS week,
                       COUNT(*) AS weekly_count
                FROM lake.housing.hpd_complaints
                WHERE TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL '90 days'
                  AND TRY_CAST(received_date AS DATE) < CURRENT_DATE - INTERVAL '7 days'
                  AND borough IS NOT NULL
                GROUP BY borough, week
            )
            GROUP BY borough
            HAVING COUNT(*) >= 4
        """,
        group_by="borough",
        unit="housing complaints",
    ),
    AnomalyMetric(
        name="dob_ecb_violations",
        description="DOB/ECB violations issued by borough",
        sql_current="""
            SELECT boro AS group_key, COUNT(*) AS current_value
            FROM lake.housing.dob_ecb_violations
            WHERE TRY_CAST(issue_date AS DATE) >= CURRENT_DATE - INTERVAL '30 days'
              AND boro IS NOT NULL
            GROUP BY boro
        """,
        sql_baseline="""
            SELECT boro AS group_key,
                   AVG(monthly_count) AS avg_value,
                   STDDEV(monthly_count) AS stddev_value
            FROM (
                SELECT boro,
                       DATE_TRUNC('month', TRY_CAST(issue_date AS DATE)) AS month,
                       COUNT(*) AS monthly_count
                FROM lake.housing.dob_ecb_violations
                WHERE TRY_CAST(issue_date AS DATE) >= CURRENT_DATE - INTERVAL '365 days'
                  AND TRY_CAST(issue_date AS DATE) < CURRENT_DATE - INTERVAL '30 days'
                  AND boro IS NOT NULL
                GROUP BY boro, month
            )
            GROUP BY boro
            HAVING COUNT(*) >= 3
        """,
        group_by="borough",
        unit="ECB violations",
    ),
]


def detect_anomalies(pool, z_threshold: float = 2.0, max_results: int = 20) -> list[dict]:
    """Run anomaly detection across all metrics.

    Returns list of anomalies sorted by severity (highest Z-score first).
    Each anomaly: {metric, group_key, current, baseline_avg, z_score, pct_change, description}
    """
    from shared.db import safe_query

    anomalies = []

    for metric in ANOMALY_METRICS:
        try:
            _, current_rows = safe_query(pool, metric.sql_current)
            if not current_rows:
                continue
            current_map = {str(r[0]): r[1] for r in current_rows if r[0] is not None}

            _, baseline_rows = safe_query(pool, metric.sql_baseline)
            if not baseline_rows:
                continue
            baseline_map = {str(r[0]): (r[1], r[2]) for r in baseline_rows if r[0] is not None}

            for group_key, current_val in current_map.items():
                if group_key not in baseline_map:
                    continue
                avg, stddev = baseline_map[group_key]
                if avg is None or avg == 0 or stddev is None or stddev == 0:
                    continue

                z_score = (current_val - avg) / stddev
                pct_change = ((current_val - avg) / avg) * 100

                if abs(z_score) >= z_threshold:
                    direction = "up" if z_score > 0 else "down"
                    anomalies.append({
                        "metric": metric.name,
                        "metric_description": metric.description,
                        "group_by": metric.group_by,
                        "group_key": group_key,
                        "current": round(current_val),
                        "baseline_avg": round(avg, 1),
                        "z_score": round(z_score, 2),
                        "pct_change": round(pct_change, 1),
                        "direction": direction,
                        "unit": metric.unit,
                        "headline": (
                            f"{metric.description.split(' by ')[0]} {direction} "
                            f"{abs(round(pct_change))}% in {group_key}"
                        ),
                    })
        except Exception as e:
            print(f"  Anomaly detection failed for {metric.name}: {e}", flush=True)
            continue

    anomalies.sort(key=lambda a: abs(a["z_score"]), reverse=True)
    return anomalies[:max_results]


def format_anomalies(anomalies: list[dict]) -> str:
    """Format anomalies as a readable markdown report."""
    if not anomalies:
        return "No significant anomalies detected in the current data."

    lines = [f"# NYC Data Anomalies ({len(anomalies)} detected)\n"]

    for i, a in enumerate(anomalies, 1):
        emoji = "\U0001f4c8" if a["direction"] == "up" else "\U0001f4c9"
        lines.append(
            f"{i}. {emoji} **{a['headline']}** "
            f"(Z={a['z_score']}, current: {a['current']:,}, "
            f"baseline: {a['baseline_avg']:,.0f} {a['unit']})"
        )

    return "\n".join(lines)
