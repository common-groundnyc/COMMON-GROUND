"""NYS Board of Elections campaign finance — column definitions.

Source: https://publicreporting.elections.ny.gov/DownloadCampaignFinanceData/
"""

# Column names from NYS BOE FileFormatReference.pdf (no header in CSV)
NYS_BOE_COLUMNS = [
    "filer_id", "filer_prev_id", "filer_name", "election_year", "election_type",
    "county_desc", "filing_abbrev", "filing_desc", "r_amend", "filing_cat_desc",
    "filing_sched_abbrev", "filing_sched_desc", "loan_lib_num", "trans_number",
    "trans_mapping", "sched_date", "org_date", "cntrbr_type_desc", "cntrbn_type_desc",
    "transfer_type_desc", "receipt_type_desc", "receipt_code_desc", "purpose_code_desc",
    "r_subcontract", "flng_ent_name", "flng_ent_first_name", "flng_ent_middle_name",
    "flng_ent_last_name", "flng_ent_add1", "flng_ent_city", "flng_ent_state",
    "flng_ent_zip", "flng_ent_country", "payment_type_desc", "pay_number",
    "owed_amt", "org_amt", "loan_other_desc", "trans_explntn",
    "r_itemized", "r_liability", "election_year_str", "office_desc",
    "district", "dist_off_cand_bal_prop",
    "r_amend_2", "filing_cat_desc_2", "filing_sched_abbrev_2", "filing_sched_desc_2",
    "cand_last_name", "cand_first_name", "cand_middle_name", "cand_office_desc",
    "cand_district", "cand_party",
]
