"""CourtListener — federal court bulk data config.

Bulk data: https://www.courtlistener.com/help/api/bulk-data/
API docs: https://www.courtlistener.com/help/api/rest/
"""

S3_BASE = "https://com-courtlistener-storage.s3.us-west-2.amazonaws.com/bulk-data"
API_BASE = "https://www.courtlistener.com/api/rest/v4"

# Bulk CSV files on S3 — latest quarterly dump (2025-12-31)
BULK_DATASETS = {
    "cl_courts": "courts-2025-12-31.csv.bz2",
    "cl_judges": "people-db-people-2025-12-31.csv.bz2",
    "cl_positions": "people-db-positions-2025-12-31.csv.bz2",
    "cl_financial_disclosures": "financial-disclosures-2025-12-31.csv.bz2",
    # "cl_investments": disabled — CSV has malformed rows that shift columns
    # causing type inference failures. Need custom CSV parser or wait for
    # CourtListener to fix their bulk export. 1.9M rows, 699 malformed.
    "cl_debts": "financial-disclosures-debts-2025-12-31.csv.bz2",
    "cl_gifts": "financial-disclosures-gifts-2025-12-31.csv.bz2",
    "cl_fjc_cases": "fjc-integrated-database-2025-12-31.csv.bz2",
    "cl_educations": "people-db-educations-2025-12-31.csv.bz2",
    "cl_schools": "people-db-schools-2025-12-31.csv.bz2",
    "cl_agreements": "financial-disclosures-agreements-2025-12-31.csv.bz2",
    "cl_reimbursements": "financial-disclosures-reimbursements-2025-12-31.csv.bz2",
    "cl_spousal_income": "financial-disclosures-spousal-income-2025-12-31.csv.bz2",
    "cl_non_investment_income": "financial-disclosures-non-investment-income-2025-12-31.csv.bz2",
}
