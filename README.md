# Fancy Serial Analyzer Web App

This repository now supports a Netlify-ready web app UI for the existing Python serial analyzer.

## What was added

- `web/` static web client with a cleaner UI
- In-browser Python execution via Pyodide (no backend server required)
- `analyze_serials_for_web(...)` helper in `fancy_serial_analyzer.py` for JSON-friendly output
- Input normalization for pasted text (commas/dashes/OCR repairs) with issue reporting
- Star/error note detection for serials containing `*`
- Value-based pattern ordering (most rare/valuable to least) per serial
- Sellability summary table of top resale candidates
- Web export to `XLSX` and `CSV` with sortable ranking views
- `scripts/build-web.mjs` to produce deployable assets in `dist/`
- `netlify.toml` + `package.json` scripts for local and Netlify deployment
- `mcp.json` with practical MCP servers for this workflow (Netlify, Playwright, Filesystem)

## Run locally

1. Install Node.js 22+.
2. Run:

```bash
npm run preview
```

Open `http://localhost:4173`.

## Netlify deploy

1. Authenticate CLI if needed:

```bash
npx netlify login
```

2. Deploy preview:

```bash
npm run netlify:deploy
```

3. Deploy production:

```bash
npm run netlify:deploy:prod
```

Netlify build settings are already configured in `netlify.toml`:
- Build command: `npm run build`
- Publish directory: `dist`

## MCP integration

`mcp.json` includes:
- `netlify` MCP server for deploy/project actions
- `playwright` MCP server for browser UI testing
- `filesystem` MCP server scoped to this repository

If your MCP client supports local project config, point it to this `mcp.json`.

## Notes on dataset matching in browser

- Core fancy-pattern analysis works immediately.
- Date datasets are bundled into the deploy by default and loaded automatically.
- You do not need to upload files each run.
- If needed, you can override bundled datasets in the UI by uploading CSV files:
  - `birthdays.csv`
  - `World Important Dates.csv`
  - `disorder_events_sample.csv`

## External dataset refresh (online sources)

You can refresh additional datasets used by the analyzer:
- `us_public_holidays.csv` from Nager.Date API (`date.nager.at`)
- `us_zip_reference.csv` from GeoNames postal codes (`download.geonames.org`)

If `us_zip_reference.csv` is missing, the analyzer now also supports GeoNames extracted format at:
- `US/US.txt`

Run:

```bash
python scripts/fetch_external_datasets.py --output-dir .
```

Optional Kaggle enrichment:

```bash
python scripts/fetch_external_datasets.py --output-dir . --kaggle-zip-csv /path/to/kaggle_zip.csv
```

See dataset references and manual-download links in [`DATASET_SOURCES.md`](./DATASET_SOURCES.md).

## CLI JSON output

You can generate JSON (same schema used by the web UI):

```bash
python fancy_serial_analyzer.py --serial 01012026 --json-output analysis.json
```

JSON-only mode (skip Excel/CSV generation):

```bash
python fancy_serial_analyzer.py --serial 01012026 --json-only --json-output analysis.json
```
