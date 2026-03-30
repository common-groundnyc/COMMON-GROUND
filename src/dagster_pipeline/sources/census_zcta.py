"""ACS 5-Year ZCTA demographics — variable definitions for NYC ZIP codes."""

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
