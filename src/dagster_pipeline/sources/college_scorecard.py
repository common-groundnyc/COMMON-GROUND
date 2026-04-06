"""College Scorecard — API field definitions.

API docs: https://collegescorecard.ed.gov/data/documentation/
"""

SCORECARD_FIELDS = [
    "id",
    "school.name",
    "school.city",
    "school.state",
    "school.zip",
    "school.school_url",
    "school.ownership",
    "school.locale",
    "school.carnegie_basic",
    "school.degrees_awarded.predominant",
    "school.degrees_awarded.highest",
    "latest.student.size",
    "latest.student.enrollment.all",
    "latest.admissions.admission_rate.overall",
    "latest.cost.tuition.in_state",
    "latest.cost.tuition.out_of_state",
    "latest.cost.avg_net_price.overall",
    "latest.cost.avg_net_price.income.0-30000",
    "latest.cost.avg_net_price.income.30001-48000",
    "latest.cost.avg_net_price.income.48001-75000",
    "latest.cost.avg_net_price.income.75001-110000",
    "latest.cost.avg_net_price.income.110001-plus",
    "latest.aid.median_debt.completers.overall",
    "latest.aid.median_debt.completers.monthly_payments",
    "latest.earnings.10_yrs_after_entry.median",
    "latest.earnings.6_yrs_after_entry.median",
    "latest.completion.rate_suppressed.overall",
    "latest.student.retention_rate.four_year.full_time",
    "latest.student.demographics.avg_family_income",
    "latest.student.share_25_plus",
    "latest.student.avg_age_of_entry",
]
