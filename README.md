# S&P 500 Hourly Market Tracker

A Flask web app that:

- loads the current S&P 500 constituents,
- captures hourly price snapshots for every symbol,
- stores snapshots in SQLite,
- exposes API endpoints for latest and historical views,
- shows a browser UI for table + chart exploration.

## Database structure

The schema is defined in `schema.sql` with two tables:

- `symbols`: one row per S&P 500 ticker + metadata.
- `hourly_prices`: one row per symbol per hour (`UNIQUE(symbol_id, captured_at)`).

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Then open <http://localhost:5000>.

## Deploy on Railway

This repo is ready for Railway with a `Procfile` that starts Gunicorn.

1. Create a new Railway project and connect this repo.
2. Railway will install dependencies from `requirements.txt`.
3. Start command is read from `Procfile`:
   `gunicorn app:app --bind 0.0.0.0:${PORT:-5000}`.
4. (Optional) add `SP500_DB_PATH` if you want a custom SQLite file location.

Notes:
- By default SQLite data is ephemeral unless you mount persistent storage.
- On startup the app creates the schema, attempts symbol bootstrap, and schedules hourly snapshots.

## API

- `POST /api/bootstrap` — load/refresh S&P 500 constituents.
- `POST /api/snapshot` — capture a full hourly price snapshot now.
- `GET /api/prices/latest` — latest stored price for each symbol.
- `GET /api/prices/history?ticker=AAPL&hours=168` — historical rows for a ticker.
