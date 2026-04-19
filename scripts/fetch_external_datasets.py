#!/usr/bin/env python3
"""
Fetch and normalize external datasets used by fancy_serial_analyzer.py.

Sources:
- Nager.Date public holidays API (US): https://date.nager.at/api
- GeoNames postal code export (US.zip): https://download.geonames.org/export/zip/

Optional:
- Merge ZIP population/city/state from a local Kaggle ZIP CSV.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import urllib.request
import zipfile
from datetime import datetime
from pathlib import Path


NAGER_API_TEMPLATE = "https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"
GEONAMES_US_ZIP_URL = "https://download.geonames.org/export/zip/US.zip"


def http_get_bytes(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "serialanalyzer-dataset-fetcher/1.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as response:
        return response.read()


def load_kaggle_zip_enrichment(path: Path) -> dict[str, dict[str, str]]:
    enrichment = {}
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return enrichment

        fields = {name.lower().strip(): name for name in reader.fieldnames}
        zip_col = fields.get("zip") or fields.get("zipcode") or fields.get("postal_code")
        city_col = fields.get("city")
        state_col = fields.get("state") or fields.get("state_id")
        pop_col = fields.get("population") or fields.get("zcta_population") or fields.get("pop")

        if not zip_col:
            return enrichment

        for row in reader:
            zip_code = str(row.get(zip_col, "")).strip().zfill(5)
            if len(zip_code) != 5 or not zip_code.isdigit():
                continue
            enrichment[zip_code] = {
                "city": str(row.get(city_col, "")).strip() if city_col else "",
                "state": str(row.get(state_col, "")).strip() if state_col else "",
                "population": str(row.get(pop_col, "")).strip() if pop_col else "",
            }
    return enrichment


def fetch_us_holidays(output_path: Path, start_year: int, end_year: int, country_code: str = "US") -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []

    for year in range(start_year, end_year + 1):
        url = NAGER_API_TEMPLATE.format(year=year, country=country_code.upper())
        payload = http_get_bytes(url)
        data = json.loads(payload.decode("utf-8"))
        if not isinstance(data, list):
            continue
        for item in data:
            date_str = str(item.get("date", "")).strip()
            if not date_str:
                continue
            rows.append(
                {
                    "date": date_str,
                    "name": str(item.get("name", "")).strip(),
                    "localName": str(item.get("localName", "")).strip(),
                    "countryCode": str(item.get("countryCode", "")).strip(),
                    "types": "|".join(item.get("types", []) or []),
                    "global": str(bool(item.get("global", False))),
                }
            )

    rows.sort(key=lambda r: (r["date"], r["name"]))
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["date", "name", "localName", "countryCode", "types", "global"],
        )
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def fetch_zip_reference(
    output_path: Path,
    geonames_url: str = GEONAMES_US_ZIP_URL,
    kaggle_enrichment: dict[str, dict[str, str]] | None = None,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    kaggle_enrichment = kaggle_enrichment or {}

    payload = http_get_bytes(geonames_url)
    archive = zipfile.ZipFile(io.BytesIO(payload))
    txt_members = [n for n in archive.namelist() if n.lower().endswith(".txt")]
    if not txt_members:
        raise RuntimeError("GeoNames zip archive did not contain a .txt file")

    member = next((n for n in txt_members if Path(n).name.upper() == "US.TXT"), txt_members[0])
    by_zip = {}
    with archive.open(member) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
        reader = csv.reader(text, delimiter="\t")
        for row in reader:
            if len(row) < 11:
                continue
            zip_code = row[1].strip()
            if len(zip_code) != 5 or not zip_code.isdigit():
                continue
            if zip_code in by_zip:
                continue

            enrich = kaggle_enrichment.get(zip_code, {})
            city = enrich.get("city") or row[2].strip()
            state = enrich.get("state") or row[3].strip()
            by_zip[zip_code] = {
                "zip": zip_code,
                "city": city,
                "state": state,
                "latitude": row[9].strip(),
                "longitude": row[10].strip(),
                "accuracy": row[11].strip() if len(row) > 11 else "",
                "population": enrich.get("population", ""),
                "source": "GeoNames+Kaggle" if enrich else "GeoNames",
            }

    rows = [by_zip[z] for z in sorted(by_zip)]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "zip",
                "city",
                "state",
                "latitude",
                "longitude",
                "accuracy",
                "population",
                "source",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def parse_args() -> argparse.Namespace:
    current_year = datetime.now().year
    parser = argparse.ArgumentParser(description="Fetch external datasets for serial analyzer.")
    parser.add_argument("--output-dir", default=".", help="Directory for generated CSV files.")
    parser.add_argument("--holiday-start-year", type=int, default=current_year - 20)
    parser.add_argument("--holiday-end-year", type=int, default=current_year + 5)
    parser.add_argument("--country-code", default="US")
    parser.add_argument(
        "--kaggle-zip-csv",
        help="Optional local path to a Kaggle ZIP CSV (used to enrich city/state/population).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    kaggle_enrichment = {}
    if args.kaggle_zip_csv:
        kaggle_path = Path(args.kaggle_zip_csv).resolve()
        if not kaggle_path.exists():
            print(f"Kaggle ZIP CSV not found: {kaggle_path}", file=sys.stderr)
            return 1
        kaggle_enrichment = load_kaggle_zip_enrichment(kaggle_path)
        print(f"Loaded Kaggle ZIP enrichment rows: {len(kaggle_enrichment)}")

    holidays_path = output_dir / "us_public_holidays.csv"
    holiday_rows = fetch_us_holidays(
        output_path=holidays_path,
        start_year=args.holiday_start_year,
        end_year=args.holiday_end_year,
        country_code=args.country_code,
    )
    print(f"Saved {holiday_rows} holiday rows -> {holidays_path}")

    zip_path = output_dir / "us_zip_reference.csv"
    zip_rows = fetch_zip_reference(
        output_path=zip_path,
        kaggle_enrichment=kaggle_enrichment,
    )
    print(f"Saved {zip_rows} ZIP reference rows -> {zip_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
