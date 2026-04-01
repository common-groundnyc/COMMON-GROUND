"""Dagster asset producing lake.foundation.address_lookup from NYC PAD."""

import csv
import logging
import os
import tempfile
import time
import zipfile
from pathlib import Path

import requests
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)

PAD_SOCRATA_ID = "bc8t-ecyu"
PAD_DOWNLOAD_URL = (
    f"https://data.cityofnewyork.us/api/views/{PAD_SOCRATA_ID}/rows.csv"
    "?accessType=DOWNLOAD"
)

_INCLUDED_ADDR_TYPES = {"", "V"}

_OUTPUT_COLUMNS = [
    "bbl", "bin", "house_number", "house_number_high",
    "street_std", "street_code", "boro_code", "zipcode",
    "addr_type", "address_std",
]


def parse_adr_row(row: dict) -> dict | None:
    """Parse a single PAD ADR row into our lookup schema.

    Returns None for rows that should be excluded (pseudo, NAP, no house number).
    """
    addr_type = row.get("addrtype", "").strip()
    if addr_type not in _INCLUDED_ADDR_TYPES:
        return None

    lhns = row.get("lhns", "").strip()
    if not lhns or not lhns.isdigit():
        return None

    boro = row.get("boro", "").strip()
    block_raw = row.get("block", "").strip()
    lot_raw = row.get("lot", "").strip()
    if not boro or not block_raw or not lot_raw:
        return None

    hhns = row.get("hhns", "").strip()
    house_number = int(lhns)
    house_number_high = int(hhns) if hhns and hhns.isdigit() else house_number

    block = block_raw.zfill(5)
    lot = lot_raw.zfill(4)
    bbl = f"{boro}{block}{lot}"

    street_std = row.get("stname", "").strip()
    if not street_std:
        return None

    address_std = f"{house_number} {street_std}"

    return {
        "bbl": bbl,
        "bin": row.get("bin", "").strip(),
        "house_number": house_number,
        "house_number_high": house_number_high,
        "street_std": street_std,
        "street_code": row.get("b10sc", "").strip(),
        "boro_code": boro,
        "zipcode": row.get("zipcode", "").strip(),
        "addr_type": addr_type,
        "address_std": address_std,
    }


def parse_adr_csv(csv_path: str, output_path: str) -> int:
    """Parse PAD ADR CSV, write filtered rows to output_path, return row count."""
    row_count = 0
    with open(csv_path, "r", encoding="latin-1") as src, \
         open(output_path, "w", newline="", encoding="utf-8") as dst:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(dst, fieldnames=_OUTPUT_COLUMNS)
        writer.writeheader()
        for raw in reader:
            parsed = parse_adr_row(raw)
            if parsed is not None:
                writer.writerow(parsed)
                row_count += 1
    return row_count


def download_pad_zip(dest_dir: str) -> str:
    """Download PAD zip from Socrata and return path to extracted ADR CSV."""
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    zip_path = dest / "pad.zip"

    meta_url = f"https://data.cityofnewyork.us/api/views/{PAD_SOCRATA_ID}.json"
    resp = requests.get(meta_url, timeout=30)
    resp.raise_for_status()
    meta = resp.json()

    download_url = None
    if "blobId" in meta:
        download_url = (
            f"https://data.cityofnewyork.us/api/views/{PAD_SOCRATA_ID}"
            f"/files/{meta['blobId']}?download=true&filename=pad.zip"
        )
    if not download_url:
        access = meta.get("metadata", {}).get("accessPoints", {})
        download_url = access.get("blob", PAD_DOWNLOAD_URL)

    logger.info("Downloading PAD from %s", download_url)
    resp = requests.get(download_url, timeout=300, stream=True)
    resp.raise_for_status()

    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

    logger.info("Downloaded PAD zip: %.1f MB", zip_path.stat().st_size / 1e6)

    with zipfile.ZipFile(zip_path) as zf:
        adr_names = [n for n in zf.namelist() if "adr" in n.lower()]
        if not adr_names:
            adr_names = [
                n for n in zf.namelist()
                if n.endswith(".txt") and "bbl" not in n.lower()
            ]
        if not adr_names:
            raise RuntimeError(
                f"No ADR file found in PAD zip. Contents: {zf.namelist()}"
            )

        adr_name = adr_names[0]

        # Zip path traversal guard
        resolved = os.path.realpath(os.path.join(str(dest), adr_name))
        if not resolved.startswith(str(dest.resolve())):
            raise RuntimeError(f"Zip slip detected: {adr_name}")

        zf.extract(adr_name, dest)
        logger.info("Extracted %s", adr_name)
        return str(dest / adr_name)


@asset(
    key=AssetKey(["foundation", "address_lookup"]),
    group_name="foundation",
    description=(
        "PAD-based address→BBL lookup table. DCP-standardized street names, "
        "~1M addresses, quarterly updates from NYC Property Address Directory."
    ),
    compute_kind="duckdb",
)
def address_lookup(context) -> MaterializeResult:
    """Download PAD, parse ADR CSV, load into lake.foundation.address_lookup."""
    t0 = time.time()

    with tempfile.TemporaryDirectory() as tmp_dir:
        context.log.info("Downloading PAD from Socrata...")
        adr_path = download_pad_zip(tmp_dir)

        context.log.info("Parsing ADR CSV (streaming to temp file)...")
        clean_csv_path = os.path.join(tmp_dir, "adr_clean.csv")
        row_count_parsed = parse_adr_csv(adr_path, clean_csv_path)
        context.log.info("Parsed %d address rows", row_count_parsed)

        conn = _connect_ducklake()
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")

            # Write to staging, then atomic rename — avoids partial-table window
            conn.execute("DROP TABLE IF EXISTS lake.foundation.address_lookup_staging")
            conn.execute("""
                CREATE TABLE lake.foundation.address_lookup_staging (
                    bbl              VARCHAR(10),
                    bin              VARCHAR(7),
                    house_number     INT,
                    house_number_high INT,
                    street_std       VARCHAR,
                    street_code      VARCHAR(10),
                    boro_code        VARCHAR(1),
                    zipcode          VARCHAR(5),
                    addr_type        VARCHAR(1),
                    address_std      VARCHAR
                )
            """)

            conn.execute(
                """
                INSERT INTO lake.foundation.address_lookup_staging
                SELECT
                    bbl, bin,
                    house_number::INT,
                    house_number_high::INT,
                    street_std, street_code, boro_code,
                    zipcode, addr_type, address_std
                FROM read_csv(?, header=true)
                """,
                [clean_csv_path],
            )

            conn.execute("DROP TABLE IF EXISTS lake.foundation.address_lookup")
            conn.execute(
                "ALTER TABLE lake.foundation.address_lookup_staging "
                "RENAME TO address_lookup"
            )

            row_count = conn.execute(
                "SELECT COUNT(*) FROM lake.foundation.address_lookup"
            ).fetchone()[0]
            distinct_bbls = conn.execute(
                "SELECT COUNT(DISTINCT bbl) FROM lake.foundation.address_lookup"
            ).fetchone()[0]
            distinct_streets = conn.execute(
                "SELECT COUNT(DISTINCT street_std) FROM lake.foundation.address_lookup"
            ).fetchone()[0]

            elapsed = time.time() - t0
            context.log.info(
                "address_lookup: %s rows, %s BBLs, %s streets in %.1fs",
                f"{row_count:,}", f"{distinct_bbls:,}", f"{distinct_streets:,}", elapsed,
            )

            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(row_count),
                    "distinct_bbls": MetadataValue.int(distinct_bbls),
                    "distinct_streets": MetadataValue.int(distinct_streets),
                    "duration_seconds": MetadataValue.float(elapsed),
                }
            )
        finally:
            conn.close()
