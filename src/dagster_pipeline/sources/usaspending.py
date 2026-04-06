"""USAspending — federal contracts and grants config for NYC.

No auth required. POST-based API.
"""

USA_BASE = "https://api.usaspending.gov/api/v2"

# Award type codes: A-D = contracts, 06-10 = grants
CONTRACT_CODES = ["A", "B", "C", "D"]
GRANT_CODES = ["02", "03", "04", "05"]

NYC_LOCATION = {"country": "USA", "state": "NY", "city": "New York"}

FIELDS = [
    "Award ID", "Recipient Name", "Award Amount",
    "Total Outlays", "Start Date", "End Date",
    "Awarding Agency", "Awarding Sub Agency",
    "Award Type", "recipient_id",
    "Place of Performance City Code",
    "Place of Performance State Code",
]
