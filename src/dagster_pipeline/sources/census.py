"""Census Bureau ACS 5-Year — NYC demographics config and helper functions."""

ACS_YEAR = 2024
NYC_STATE = "36"
NYC_COUNTIES = {
    "005": "bronx",
    "047": "brooklyn",
    "061": "manhattan",
    "081": "queens",
    "085": "staten_island",
}
VARIABLES = {
    "B01001_001E": "total_population",
    "B01002_001E": "median_age",
    "B01001_002E": "male_population",
    "B01001_026E": "female_population",
}

# Additional ACS tables — each becomes its own resource
ACS_TABLES = {
    # --- Demographics ---
    "acs_hispanic_origin": {
        "table": "B03001",
        "variables": {
            "B03001_001E": "total",
            "B03001_002E": "not_hispanic",
            "B03001_003E": "hispanic",
            "B03001_004E": "mexican",
            "B03001_005E": "puerto_rican",
            "B03001_006E": "cuban",
            "B03001_007E": "dominican",
            "B03001_008E": "central_american",
            "B03001_015E": "south_american",
            "B03001_028E": "other_hispanic",
        },
    },
    "acs_race": {
        "table": "B02001",
        "variables": {
            "B02001_001E": "total",
            "B02001_002E": "white_alone",
            "B02001_003E": "black_alone",
            "B02001_004E": "american_indian_alone",
            "B02001_005E": "asian_alone",
            "B02001_006E": "pacific_islander_alone",
            "B02001_007E": "other_alone",
            "B02001_008E": "two_or_more",
        },
    },
    "acs_age_sex": {
        "table": "B01001",
        "variables": {
            "B01001_001E": "total",
            "B01001_002E": "male_total",
            "B01001_003E": "male_under_5",
            "B01001_006E": "male_15_17",
            "B01001_007E": "male_18_19",
            "B01001_010E": "male_25_29",
            "B01001_011E": "male_30_34",
            "B01001_012E": "male_35_39",
            "B01001_013E": "male_40_44",
            "B01001_020E": "male_60_61",
            "B01001_021E": "male_62_64",
            "B01001_022E": "male_65_66",
            "B01001_024E": "male_70_74",
            "B01001_025E": "male_75_79",
            "B01001_026E": "female_total",
            "B01001_044E": "female_60_61",
            "B01001_046E": "female_65_66",
            "B01001_048E": "female_70_74",
            "B01001_049E": "female_75_79",
        },
    },
    "acs_nativity": {
        "table": "B05002",
        "variables": {
            "B05002_001E": "total",
            "B05002_002E": "native_born",
            "B05002_013E": "foreign_born",
            "B05002_014E": "foreign_born_naturalized",
            "B05002_021E": "foreign_born_not_citizen",
        },
    },
    "acs_place_of_birth_foreign": {
        "table": "B05006",
        "variables": {
            "B05006_001E": "total_foreign_born",
            "B05006_002E": "europe",
            "B05006_047E": "asia",
            "B05006_087E": "africa",
            "B05006_124E": "caribbean",
            "B05006_138E": "central_america",
            "B05006_143E": "south_america",
        },
    },
    # --- Income & Poverty ---
    "acs_median_income": {
        "table": "B19013",
        "variables": {
            "B19013_001E": "median_household_income",
        },
    },
    "acs_income_distribution": {
        "table": "B19001",
        "variables": {
            "B19001_001E": "total_households",
            "B19001_002E": "income_under_10k",
            "B19001_003E": "income_10k_15k",
            "B19001_004E": "income_15k_20k",
            "B19001_005E": "income_20k_25k",
            "B19001_006E": "income_25k_30k",
            "B19001_007E": "income_30k_35k",
            "B19001_008E": "income_35k_40k",
            "B19001_009E": "income_40k_45k",
            "B19001_010E": "income_45k_50k",
            "B19001_011E": "income_50k_60k",
            "B19001_012E": "income_60k_75k",
            "B19001_013E": "income_75k_100k",
            "B19001_014E": "income_100k_125k",
            "B19001_015E": "income_125k_150k",
            "B19001_016E": "income_150k_200k",
            "B19001_017E": "income_200k_plus",
        },
    },
    "acs_poverty": {
        "table": "B17001",
        "variables": {
            "B17001_001E": "total_poverty_determined",
            "B17001_002E": "below_poverty",
        },
    },
    "acs_public_assistance": {
        "table": "B19058",
        "variables": {
            "B19058_001E": "total_households",
            "B19058_002E": "with_public_assistance",
            "B19058_003E": "without_public_assistance",
        },
    },
    "acs_gini_index": {
        "table": "B19083",
        "variables": {
            "B19083_001E": "gini_index",
        },
    },
    # --- Housing ---
    "acs_housing_tenure": {
        "table": "B25003",
        "variables": {
            "B25003_001E": "total_occupied",
            "B25003_002E": "owner_occupied",
            "B25003_003E": "renter_occupied",
        },
    },
    "acs_median_rent": {
        "table": "B25064",
        "variables": {
            "B25064_001E": "median_gross_rent",
        },
    },
    "acs_median_home_value": {
        "table": "B25077",
        "variables": {
            "B25077_001E": "median_home_value",
        },
    },
    "acs_rent_burden": {
        "table": "B25070",
        "variables": {
            "B25070_001E": "total_renters",
            "B25070_007E": "rent_30_35_pct",
            "B25070_008E": "rent_35_40_pct",
            "B25070_009E": "rent_40_50_pct",
            "B25070_010E": "rent_50_plus_pct",
            "B25070_011E": "rent_not_computed",
        },
    },
    "acs_housing_units": {
        "table": "B25001",
        "variables": {
            "B25001_001E": "total_housing_units",
        },
    },
    "acs_vacancy": {
        "table": "B25002",
        "variables": {
            "B25002_001E": "total",
            "B25002_002E": "occupied",
            "B25002_003E": "vacant",
        },
    },
    "acs_year_built": {
        "table": "B25034",
        "variables": {
            "B25034_001E": "total",
            "B25034_002E": "built_2020_later",
            "B25034_003E": "built_2010_2019",
            "B25034_004E": "built_2000_2009",
            "B25034_005E": "built_1990_1999",
            "B25034_006E": "built_1980_1989",
            "B25034_007E": "built_1970_1979",
            "B25034_008E": "built_1960_1969",
            "B25034_009E": "built_1950_1959",
            "B25034_010E": "built_1940_1949",
            "B25034_011E": "built_1939_earlier",
        },
    },
    "acs_rooms": {
        "table": "B25021",
        "variables": {
            "B25021_001E": "median_rooms_total",
            "B25021_002E": "median_rooms_owner",
            "B25021_003E": "median_rooms_renter",
        },
    },
    "acs_overcrowding": {
        "table": "B25014",
        "variables": {
            "B25014_001E": "total_occupied",
            "B25014_005E": "owner_1_to_1_5_per_room",
            "B25014_006E": "owner_1_5_to_2_per_room",
            "B25014_007E": "owner_2_plus_per_room",
            "B25014_011E": "renter_1_to_1_5_per_room",
            "B25014_012E": "renter_1_5_to_2_per_room",
            "B25014_013E": "renter_2_plus_per_room",
        },
    },
    # --- Transportation ---
    "acs_commute_mode": {
        "table": "B08301",
        "variables": {
            "B08301_001E": "total_workers",
            "B08301_002E": "drove_alone",
            "B08301_004E": "carpooled",
            "B08301_010E": "public_transit",
            "B08301_011E": "bus",
            "B08301_012E": "subway",
            "B08301_013E": "railroad",
            "B08301_016E": "taxi_motorcycle_other",
            "B08301_018E": "bicycle",
            "B08301_019E": "walked",
            "B08301_021E": "worked_from_home",
        },
    },
    "acs_commute_time": {
        "table": "B08303",
        "variables": {
            "B08303_001E": "total_workers",
            "B08303_002E": "less_5_min",
            "B08303_003E": "5_9_min",
            "B08303_004E": "10_14_min",
            "B08303_005E": "15_19_min",
            "B08303_006E": "20_24_min",
            "B08303_007E": "25_29_min",
            "B08303_008E": "30_34_min",
            "B08303_009E": "35_39_min",
            "B08303_010E": "40_44_min",
            "B08303_011E": "45_59_min",
            "B08303_012E": "60_89_min",
            "B08303_013E": "90_plus_min",
        },
    },
    "acs_vehicles_available": {
        "table": "B25044",
        "variables": {
            "B25044_001E": "total_occupied",
            "B25044_003E": "owner_no_vehicle",
            "B25044_010E": "renter_no_vehicle",
        },
    },
    # --- Education ---
    "acs_education": {
        "table": "B15003",
        "variables": {
            "B15003_001E": "total_25plus",
            "B15003_002E": "no_schooling",
            "B15003_017E": "high_school_diploma",
            "B15003_018E": "ged",
            "B15003_021E": "associates",
            "B15003_022E": "bachelors",
            "B15003_023E": "masters",
            "B15003_024E": "professional",
            "B15003_025E": "doctorate",
        },
    },
    "acs_school_enrollment": {
        "table": "B14001",
        "variables": {
            "B14001_001E": "total_3plus",
            "B14001_002E": "enrolled",
            "B14001_003E": "enrolled_nursery_preschool",
            "B14001_004E": "enrolled_kindergarten",
            "B14001_005E": "enrolled_1_4",
            "B14001_006E": "enrolled_5_8",
            "B14001_007E": "enrolled_9_12",
            "B14001_008E": "enrolled_college_undergrad",
            "B14001_009E": "enrolled_graduate",
            "B14001_010E": "not_enrolled",
        },
    },
    # --- Employment ---
    "acs_employment": {
        "table": "B23025",
        "variables": {
            "B23025_001E": "total_16plus",
            "B23025_002E": "in_labor_force",
            "B23025_003E": "civilian_labor_force",
            "B23025_004E": "employed",
            "B23025_005E": "unemployed",
            "B23025_006E": "armed_forces",
            "B23025_007E": "not_in_labor_force",
        },
    },
    "acs_occupation": {
        "table": "C24010",
        "variables": {
            "C24010_001E": "total_employed",
            "C24010_003E": "management_business_science_arts",
            "C24010_019E": "service",
            "C24010_027E": "sales_office",
            "C24010_034E": "natural_resources_construction_maintenance",
            "C24010_038E": "production_transportation",
        },
    },
    # --- Language ---
    "acs_language_at_home": {
        "table": "B16001",
        "variables": {
            "B16001_001E": "total_population_5plus",
            "B16001_002E": "english_only",
            "B16001_003E": "spanish",
            "B16001_004E": "spanish_speak_english_very_well",
            "B16001_005E": "spanish_speak_english_less_than_very_well",
        },
    },
    # --- Health & Insurance ---
    "acs_health_insurance": {
        "table": "B27001",
        "variables": {
            "B27001_001E": "total",
            "B27001_004E": "male_under6_with_insurance",
            "B27001_005E": "male_under6_no_insurance",
            "B27001_008E": "male_18_25_with_insurance",
            "B27001_009E": "male_18_25_no_insurance",
            "B27001_012E": "male_26_34_with_insurance",
            "B27001_013E": "male_26_34_no_insurance",
            "B27001_016E": "male_35_44_with_insurance",
            "B27001_017E": "male_35_44_no_insurance",
            "B27001_020E": "male_45_54_with_insurance",
            "B27001_021E": "male_45_54_no_insurance",
            "B27001_024E": "male_55_64_with_insurance",
            "B27001_025E": "male_55_64_no_insurance",
        },
    },
    "acs_disability": {
        "table": "B18101",
        "variables": {
            "B18101_001E": "total",
            "B18101_004E": "male_under5_with_disability",
            "B18101_007E": "male_5_17_with_disability",
            "B18101_010E": "male_18_34_with_disability",
            "B18101_013E": "male_35_64_with_disability",
            "B18101_016E": "male_65_74_with_disability",
            "B18101_019E": "male_75plus_with_disability",
        },
    },
    # --- Household Structure ---
    "acs_household_type": {
        "table": "B11001",
        "variables": {
            "B11001_001E": "total_households",
            "B11001_002E": "family",
            "B11001_003E": "married_couple",
            "B11001_004E": "male_no_spouse",
            "B11001_005E": "female_no_spouse",
            "B11001_007E": "nonfamily",
            "B11001_008E": "living_alone",
        },
    },
    "acs_household_size": {
        "table": "B25009",
        "variables": {
            "B25009_001E": "total_occupied",
            "B25009_003E": "owner_1_person",
            "B25009_004E": "owner_2_person",
            "B25009_005E": "owner_3_person",
            "B25009_006E": "owner_4_person",
            "B25009_011E": "renter_1_person",
            "B25009_012E": "renter_2_person",
            "B25009_013E": "renter_3_person",
            "B25009_014E": "renter_4_person",
        },
    },
    # --- Internet & Technology ---
    "acs_internet_access": {
        "table": "B28002",
        "variables": {
            "B28002_001E": "total_households",
            "B28002_002E": "with_internet",
            "B28002_004E": "broadband",
            "B28002_012E": "without_internet",
            "B28002_013E": "no_internet_subscription",
        },
    },
    "acs_computer_access": {
        "table": "B28001",
        "variables": {
            "B28001_001E": "total_households",
            "B28001_002E": "has_computer",
            "B28001_003E": "has_desktop_laptop",
            "B28001_005E": "has_smartphone_only",
            "B28001_011E": "no_computer",
        },
    },
}


def _parse_census_value(val: str | None) -> float | None:
    if val is None:
        return None
    try:
        num = float(val)
        return None if num < -999999 else num  # Census uses -666666666 for missing
    except (ValueError, TypeError):
        return None


