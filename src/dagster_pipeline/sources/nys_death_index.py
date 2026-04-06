"""NYS Death Index (1880-1971) — archive.org file config.

Source: https://archive.org/details/reclaim-the-records-new-york-state-death-index-1880-1971
"""

ARCHIVE_BASE = "https://archive.org/download/reclaim-the-records-new-york-state-death-index-1880-1971"

CSV_FILES = [
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1880-1885.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1886-1890.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1891-1895.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1896-1899.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1900-1905.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1906-1910.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1911-1915.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1916-1920.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1921-1925.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1926-1930.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1931-1935.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1936-1940.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1941-1945.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1946-1950.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1951-1956.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1957.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1958.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1959.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1960.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1961.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1962.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1963.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1964.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1965.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1966.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1967.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1968.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1969.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1970.csv",
    "Reclaim_The_Records_-_New_York_State_Death_Index_-_1971.csv",
]

COLUMN_RENAME = {
    "Year": "year",
    "Last Name": "last_name",
    "First Name": "first_name",
    "MI": "middle_initial",
    "Residence": "residence_code",
    "Place of Death": "place_of_death_code",
    "Age": "age",
    "Age Units": "age_units",
    "Date of Death": "date_of_death",
    "State File Number": "state_file_number",
}
