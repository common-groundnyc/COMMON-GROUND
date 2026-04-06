"""FEC (Federal Election Commission) — NYC individual contributions config and helpers.

Bulk files: https://www.fec.gov/files/bulk-downloads/{year}/indiv{yy}.zip
Each contains a pipe-delimited CSV with ~40-60M rows per 2-year cycle.
"""
import logging
import os
import zipfile

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

FEC_BULK_URL = "https://www.fec.gov/files/bulk-downloads/{year}/indiv{yy}.zip"

FEC_COLUMNS = [
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state", "zip_code",
    "employer", "occupation", "transaction_dt", "transaction_amt",
    "other_id", "tran_id", "file_num", "memo_cd", "memo_text", "sub_id",
]

NYC_ZIPS = {"100", "101", "102", "103", "104", "110", "111", "112", "113", "114", "116"}

CYCLES = [2020, 2022, 2024]

DOWNLOAD_TIMEOUT = httpx.Timeout(connect=30.0, read=600.0, write=60.0, pool=30.0)
DOWNLOAD_CHUNK = 1024 * 1024
BATCH_SIZE = 500_000  # rows per Arrow batch — keeps memory ~200 MB per batch


@retry(
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=10, max=120),
)
def _download_bulk(year: int, dest_dir: str) -> str:
    """Download and extract FEC bulk individual contributions ZIP."""
    yy = str(year)[-2:]
    url = FEC_BULK_URL.format(year=year, yy=yy)
    zip_path = os.path.join(dest_dir, f"indiv{yy}.zip")
    txt_path = os.path.join(dest_dir, f"indiv{yy}.txt")

    if os.path.exists(txt_path):
        logger.info("FEC %d: already extracted at %s", year, txt_path)
        return txt_path

    logger.info("FEC %d: downloading %s", year, url)

    with httpx.stream("GET", url, timeout=DOWNLOAD_TIMEOUT, follow_redirects=True) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0

        with open(zip_path, "wb") as f:
            for chunk in resp.iter_bytes(DOWNLOAD_CHUNK):
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0 and downloaded % (50 * DOWNLOAD_CHUNK) == 0:
                    pct = downloaded / total * 100
                    logger.info("FEC %d: %.1f%% (%d MB)", year, pct, downloaded // (1024 * 1024))

    logger.info("FEC %d: extracting ZIP (%d MB)", year, os.path.getsize(zip_path) // (1024 * 1024))

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        txt_name = [n for n in names if n.endswith(".txt")][0]
        with zf.open(txt_name) as src, open(txt_path, "wb") as dst:
            while True:
                chunk = src.read(DOWNLOAD_CHUNK)
                if not chunk:
                    break
                dst.write(chunk)

    os.remove(zip_path)
    size_mb = os.path.getsize(txt_path) // (1024 * 1024)
    logger.info("FEC %d: extracted %s (%d MB)", year, txt_path, size_mb)
    return txt_path


def _filter_nyc_batch(batch: pa.RecordBatch, year: int) -> pa.Table | None:
    """Filter a single Arrow batch to NYC ZIPs."""
    zip_col = batch.column("zip_code")
    zip_prefix = pc.utf8_slice_codeunits(zip_col, 0, 3)
    mask = pc.is_in(zip_prefix, pa.array(list(NYC_ZIPS)))
    filtered = batch.filter(mask)

    if len(filtered) == 0:
        return None

    table = pa.Table.from_batches([filtered])
    cycle_col = pa.array([year] * len(table), type=pa.int32())
    return table.append_column("election_cycle", cycle_col)


