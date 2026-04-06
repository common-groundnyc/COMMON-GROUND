"""NYC Marriage Index (1950-2017) — file path config.

Source: https://archive.org/details/NewYorkCityMarriageIndex1950-1995
"""
from pathlib import Path

PARQUET_PATH = Path.home() / "Desktop" / "data-pipeline" / "data" / "marriage-index" / "nyc_marriage_index.parquet"
