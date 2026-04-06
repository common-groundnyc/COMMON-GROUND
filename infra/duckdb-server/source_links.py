DOMAINS = {
    "nyc": "data.cityofnewyork.us",
    "nys": "data.ny.gov",
    "health": "health.data.ny.gov",
    "cdc": "data.cdc.gov",
}

DATASET_URLS = {
    "hpd_violations": {"id": "wvxf-dwi5", "domain": "nyc", "key_col": "violationid"},
    "hpd_complaints": {"id": "ygpa-z7cr", "domain": "nyc", "key_col": "complaintid"},
    "hpd_jurisdiction": {"id": "kj4p-ruqc", "domain": "nyc", "key_col": "buildingid"},
    "hpd_registration_contacts": {"id": "feu5-w2e2", "domain": "nyc", "key_col": "registrationid"},
    "hpd_litigations": {"id": "59kj-x8nc", "domain": "nyc"},
    "evictions": {"id": "6z8x-wfk4", "domain": "nyc"},
    "dob_violations": {"id": "3h2n-5cm9", "domain": "nyc"},
    "dob_ecb_violations": {"id": "6bgk-3dad", "domain": "nyc"},
    "dob_permit_issuance": {"id": "ipu4-2q9a", "domain": "nyc"},
    "acris_master": {"id": "bnx9-e6tj", "domain": "nyc", "key_col": "document_id"},
    "acris_parties": {"id": "636b-3b5g", "domain": "nyc", "key_col": "document_id"},
    "acris_legals": {"id": "8h5j-fqxa", "domain": "nyc", "key_col": "document_id"},
    "property_valuation": {"id": "8y4t-faws", "domain": "nyc"},
    "pluto": {"id": "64uk-42ks", "domain": "nyc"},
    "restaurant_inspections": {"id": "43nn-pn8j", "domain": "nyc", "key_col": "camis"},
    "rodent_inspections": {"id": "p937-wjvj", "domain": "nyc"},
    "nypd_complaints_ytd": {"id": "5uac-w243", "domain": "nyc", "key_col": "cmplnt_num"},
    "nypd_complaints_historic": {"id": "qgea-i56i", "domain": "nyc", "key_col": "cmplnt_num"},
    "nypd_arrests_ytd": {"id": "uip8-fykc", "domain": "nyc"},
    "nypd_arrests_historic": {"id": "8h9b-rp9u", "domain": "nyc"},
    "shootings": {"id": "5ucz-vwe8", "domain": "nyc"},
    "motor_vehicle_collisions": {"id": "h9gi-nx95", "domain": "nyc"},
    "n311_service_requests": {"id": "erm2-nwe9", "domain": "nyc", "key_col": "unique_key"},
    "campaign_contributions": {"id": "rjkp-yttg", "domain": "nyc"},
    "campaign_expenditures": {"id": "qxzj-vkn2", "domain": "nyc"},
    "oath_hearings": {"id": "jz4z-kudi", "domain": "nyc"},
    "citywide_payroll": {"id": "k397-673e", "domain": "nyc"},
    "contract_awards": {"id": "qyyg-4tf5", "domain": "nyc"},
    "nys_corporations": {"id": "n9v6-gdp6", "domain": "nys", "key_col": "dos_id"},
    "bic_violations": {"id": "upii-frjc", "domain": "nyc"},
    "ll84_energy_2023": {"id": "5zyy-y8am", "domain": "nyc", "key_col": "bbl"},
    "parking_violations": {"id": "pvqr-7yc4", "domain": "nyc", "key_col": "summons_number"},
}


def build_source_url(table_name: str, row: dict) -> str | None:
    entry = DATASET_URLS.get(table_name)
    if entry is None:
        return None

    host = DOMAINS[entry["domain"]]
    dataset_id = entry["id"]
    key_col = entry.get("key_col")

    # Always link to the browseable Socrata data page (not API JSON)
    # The /d/ short URL shows the dataset in the NYC Open Data web UI
    return f"https://{host}/d/{dataset_id}"


def parse_bbl(bbl: str) -> tuple[str, str, str] | None:
    if not bbl or len(bbl) != 10 or not bbl.isdigit():
        return None
    borough = bbl[0]
    block = bbl[1:6]
    lot = bbl[6:10]
    return (borough, block, lot)


def build_bbl_urls(bbl: str) -> list[dict]:
    parsed = parse_bbl(bbl)
    if parsed is None:
        return []

    boro, block_5, lot_4 = parsed
    block_int = int(block_5)
    lot_int = int(lot_4)

    return [
        {
            "name": "HPD (violations)",
            "url": f"https://data.cityofnewyork.us/d/wvxf-dwi5",
        },
        {
            "name": "DOB BIS",
            "url": f"https://a810-bisweb.nyc.gov/bisweb/PropertyProfileOverviewServlet?boro={boro}&block={block_5}&lot={lot_4}",
        },
        {
            "name": "ACRIS (transactions)",
            "url": f"https://a836-acris.nyc.gov/bblsearch/bblsearch.asp?borough={boro}&block={block_5}&lot={lot_4}",
        },
    ]
