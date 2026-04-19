# Dataset Sources

This project uses a mix of automatically fetched and manually downloadable datasets.

## Auto-fetched (no account required)

1. Nager.Date Public Holidays API (US)
   - API docs: https://date.nager.at/api
   - Example endpoint used: https://date.nager.at/api/v3/PublicHolidays/2026/US
   - Output file: `us_public_holidays.csv`

2. GeoNames Postal Codes Export
   - Index: https://download.geonames.org/export/zip/
   - US archive used: https://download.geonames.org/export/zip/US.zip
   - Output file: `us_zip_reference.csv`

## Optional manual datasets (Kaggle / account-based)

You can download these and pass them into `scripts/fetch_external_datasets.py` for enrichment.

1. US Zip Codes (Kaggle search)
   - https://www.kaggle.com/datasets?search=zip+codes
   - Example candidate:
     - https://www.kaggle.com/datasets/flynn28/united-states-zipcodes
   - Use for: city/state/population enrichment of ZIP matches.

2. US Federal Holidays (Kaggle)
   - https://www.kaggle.com/datasets/joebeachcapital/us-federal-holidays
   - Use for: historical holiday metadata cross-checking.

## Refresh command

```bash
python scripts/fetch_external_datasets.py --output-dir .
```

With optional Kaggle ZIP CSV:

```bash
python scripts/fetch_external_datasets.py --output-dir . --kaggle-zip-csv /path/to/your/kaggle_zip.csv
```
