"""ACS 5-Year ZCTA demographics for NYC ZIP codes."""

import dlt
import requests

ACS_YEAR = 2023
ACS_BASE = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"

NYC_ZIP_PREFIXES = {"100", "101", "102", "103", "104", "110", "111", "112", "113", "114", "116"}

VARIABLES = {
    "B01003_001E": "total_population",
    "B01002_001E": "median_age",
    "B19013_001E": "median_household_income",
    "B25064_001E": "median_gross_rent",
    "B25003_001E": "total_occupied_units",
    "B25003_002E": "owner_occupied_units",
    "B17001_001E": "poverty_universe",
    "B17001_002E": "below_poverty",
    "B02001_001E": "race_universe",
    "B02001_002E": "white_alone",
    "B02001_003E": "black_alone",
    "B02001_005E": "asian_alone",
    "B03003_001E": "ethnicity_universe",
    "B03003_003E": "hispanic_latino",
    "B25001_001E": "total_housing_units",
    "B25002_003E": "vacant_units",
    "B08303_001E": "commute_universe",
    "B08303_013E": "commute_60_plus_min",
    "B25077_001E": "median_home_value",
}


@dlt.source(name="acs_zcta_demographics")
def acs_zcta_source():
    @dlt.resource(
        name="acs_zcta_demographics",
        write_disposition="replace",
        columns={"zcta": {"data_type": "text", "nullable": False}},
    )
    def acs_zcta_demographics():
        var_codes = ",".join(VARIABLES.keys())
        url = f"{ACS_BASE}?get=NAME,{var_codes}&for=zip%20code%20tabulation%20area:*"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        headers = data[0]

        for row in data[1:]:
            record = dict(zip(headers, row))
            zcta = record.get("zip code tabulation area", "")
            if zcta[:3] not in NYC_ZIP_PREFIXES:
                continue

            out = {"zcta": zcta, "acs_year": ACS_YEAR, "name": record.get("NAME", "")}
            for code, col_name in VARIABLES.items():
                val = record.get(code)
                if val and val not in ("-666666666", "-999999999", "null", "None"):
                    try:
                        out[col_name] = float(val)
                    except (ValueError, TypeError):
                        out[col_name] = None
                else:
                    out[col_name] = None
            yield out

    return acs_zcta_demographics


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="acs_zcta",
        destination="duckdb",
        dataset_name="federal",
    )
    load_info = pipeline.run(acs_zcta_source())
    print(load_info)
