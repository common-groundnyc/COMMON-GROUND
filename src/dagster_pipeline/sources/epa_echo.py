"""EPA ECHO — facility compliance config for NYC.

Docs: https://echo.epa.gov/tools/web-services
"""

ECHO_BASE = "https://echodata.epa.gov/echo"

# NYC county FIPS codes
NYC_COUNTIES = {
    "005": "bronx",
    "047": "brooklyn",
    "061": "manhattan",
    "081": "queens",
    "085": "staten_island",
}
