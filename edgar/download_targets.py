#!/usr/bin/env python3
"""
Targeted downloader for selected CIK/GVKEY pairs (pilot for MSFT & NVDA).
"""
from __future__ import annotations

import csv
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List

from src.edgar_downloader import EdgarDownloader
from config import (
    START_DATE,
    END_DATE,
    RAW_FILINGS_DIR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/download_targets.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


TARGET_COMPANIES = {
    "MICROSOFT": {"ticker": "MSFT"},
    "NVIDIA": {"ticker": "NVDA"},
}

FORM_TYPES = ["10-K", "10-K/A", "10-Q", "10-Q/A", "8-K"]
PILOT_START = date(2023, 1, 1)
PILOT_END = date(2024, 12, 31)


def load_cik_mapping() -> Dict[str, Dict[str, str]]:
    mapping_path = Path("cik_to_gvkey_mapping.csv")
    data: Dict[str, Dict[str, str]] = {}
    with mapping_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["company_name"].upper()
            data[key] = row
    return data


def filter_target_companies(mapping: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    targets: Dict[str, Dict[str, str]] = {}
    for name in TARGET_COMPANIES.keys():
        row = mapping.get(f"{name} CORP") or mapping.get(name)
        if not row:
            logger.error("Company %s not found in mapping file", name)
            continue
        targets[row["CIK"].lstrip("0")] = {
            "gvkey": row["GVKEY"],
            "cik": row["CIK"],
            "company_name": row["company_name"],
        }
    return targets


def quarters_in_range(start: date, end: date) -> List[tuple[int, int]]:
    quarters: List[tuple[int, int]] = []
    current = date(start.year, start.month, 1)
    while current <= end:
        year = current.year
        quarter = (current.month - 1) // 3 + 1
        quarters.append((year, quarter))
        if quarter == 4:
            current = date(year + 1, 1, 1)
        else:
            current = date(year, quarter * 3 + 1, 1)
    return quarters


def main() -> None:
    mapping = load_cik_mapping()
    target_companies = filter_target_companies(mapping)
    if not target_companies:
        logger.error("No target companies found. Aborting.")
        return

    logger.info("Targets: %s", target_companies)
    downloader = EdgarDownloader()

    manifest_path = Path("logs/manifest_msft_nvda.csv")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    with manifest_path.open("w", newline="") as manifest_file:
        fieldnames = ["company_name", "gvkey", "cik", "form_type", "date_filed", "accession", "local_path"]
        writer = csv.DictWriter(manifest_file, fieldnames=fieldnames)
        writer.writeheader()

        total_downloaded = 0
        for year, quarter in quarters_in_range(PILOT_START, PILOT_END):
            logger.info("Processing %d Q%d", year, quarter)
            filings = downloader.download_full_index(year, quarter) or []
            logger.info("Total filings in index: %d", len(filings))

            relevant = [
                f
                for f in filings
                if f["cik"] in target_companies
                and PILOT_START <= f["date_filed"] <= PILOT_END
                and f["form_type"] in FORM_TYPES
            ]
            logger.info("Relevant filings: %d", len(relevant))

            for filing in relevant:
                company = target_companies[filing["cik"]]
                output = downloader.download_filing(
                    filing["cik"],
                    filing["accession_number"],
                    filing["form_type"],
                    filing["filename"],
                    filing["date_filed"],
                )
                if not output:
                    continue
                writer.writerow(
                    {
                        "company_name": company["company_name"],
                        "gvkey": company["gvkey"],
                        "cik": company["cik"],
                        "form_type": filing["form_type"],
                        "date_filed": filing["date_filed"],
                        "accession": filing["accession_number"],
                        "local_path": str(output.relative_to(RAW_FILINGS_DIR)),
                    }
                )
                total_downloaded += 1

        logger.info("Downloaded %d filings for pilot companies", total_downloaded)


if __name__ == "__main__":
    main()

