from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request

DB_PATH = os.getenv("SP500_DB_PATH", "sp500.db")
WIKI_SYMBOLS_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

app = Flask(__name__)


@dataclass
class SymbolInfo:
    ticker: str
    company_name: str | None
    sector: str | None


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(get_conn()) as conn, open("schema.sql", "r", encoding="utf-8") as schema_file:
        conn.executescript(schema_file.read())
        conn.commit()


def normalize_ticker(raw_ticker: str) -> str:
    return raw_ticker.replace(".", "-").strip().upper()


def load_sp500_symbols() -> list[SymbolInfo]:
    df = pd.read_html(WIKI_SYMBOLS_URL)[0]
    symbols = []
    for _, row in df.iterrows():
        symbols.append(
            SymbolInfo(
                ticker=normalize_ticker(str(row["Symbol"])),
                company_name=str(row["Security"]),
                sector=str(row["GICS Sector"]),
            )
        )
    return symbols


def upsert_symbols(symbols: Iterable[SymbolInfo]) -> None:
    with closing(get_conn()) as conn:
        conn.executemany(
            """
            INSERT INTO symbols (ticker, company_name, sector)
            VALUES (?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
              company_name = excluded.company_name,
              sector = excluded.sector
            """,
            [(s.ticker, s.company_name, s.sector) for s in symbols],
        )
        conn.commit()


def get_all_tickers() -> list[str]:
    with closing(get_conn()) as conn:
        rows = conn.execute("SELECT ticker FROM symbols ORDER BY ticker").fetchall()
        return [row["ticker"] for row in rows]


def fetch_latest_prices(tickers: list[str]) -> dict[str, float]:
    if not tickers:
        return {}

    joined = " ".join(tickers)
    data = yf.download(
        tickers=joined,
        period="1d",
        interval="1m",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )

    prices: dict[str, float] = {}
    if isinstance(data.columns, pd.MultiIndex):
        for ticker in tickers:
            try:
                close_series = data[ticker]["Close"].dropna()
                if not close_series.empty:
                    prices[ticker] = float(close_series.iloc[-1])
            except Exception:
                continue
    else:
        # Handles the case when only one ticker is present.
        close_series = data["Close"].dropna()
        if not close_series.empty and len(tickers) == 1:
            prices[tickers[0]] = float(close_series.iloc[-1])
    return prices


def store_hourly_snapshot(captured_at: datetime | None = None) -> int:
    tickers = get_all_tickers()
    prices = fetch_latest_prices(tickers)
    if not prices:
        return 0

    if captured_at is None:
        captured_at = datetime.now(timezone.utc)
    captured_hour = captured_at.replace(minute=0, second=0, microsecond=0).isoformat()

    with closing(get_conn()) as conn:
        symbol_lookup = {
            row["ticker"]: row["id"]
            for row in conn.execute("SELECT id, ticker FROM symbols").fetchall()
        }
        insert_rows = [
            (symbol_lookup[ticker], captured_hour, price)
            for ticker, price in prices.items()
            if ticker in symbol_lookup
        ]

        conn.executemany(
            """
            INSERT INTO hourly_prices (symbol_id, captured_at, price)
            VALUES (?, ?, ?)
            ON CONFLICT(symbol_id, captured_at) DO UPDATE SET
              price = excluded.price
            """,
            insert_rows,
        )
        conn.commit()
        return len(insert_rows)


def bootstrap_symbols() -> int:
    symbols = load_sp500_symbols()
    upsert_symbols(symbols)
    return len(symbols)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/bootstrap", methods=["POST"])
def api_bootstrap():
    total = bootstrap_symbols()
    return jsonify({"symbols_loaded": total})


@app.route("/api/snapshot", methods=["POST"])
def api_snapshot():
    inserted = store_hourly_snapshot()
    return jsonify({"rows_upserted": inserted})


@app.route("/api/prices/latest")
def api_latest_prices():
    query = """
        SELECT s.ticker, s.company_name, s.sector, hp.price, hp.captured_at
        FROM hourly_prices hp
        JOIN symbols s ON s.id = hp.symbol_id
        JOIN (
          SELECT symbol_id, MAX(captured_at) AS max_time
          FROM hourly_prices
          GROUP BY symbol_id
        ) latest ON latest.symbol_id = hp.symbol_id AND latest.max_time = hp.captured_at
        ORDER BY s.ticker
    """
    with closing(get_conn()) as conn:
        rows = [dict(row) for row in conn.execute(query).fetchall()]
    return jsonify(rows)


@app.route("/api/prices/history")
def api_prices_history():
    ticker = normalize_ticker(request.args.get("ticker", ""))
    hours = int(request.args.get("hours", "168"))
    if not ticker:
        return jsonify({"error": "ticker query parameter is required"}), 400

    query = """
      SELECT s.ticker, s.company_name, hp.price, hp.captured_at
      FROM hourly_prices hp
      JOIN symbols s ON s.id = hp.symbol_id
      WHERE s.ticker = ?
        AND hp.captured_at >= datetime('now', ?)
      ORDER BY hp.captured_at ASC
    """
    with closing(get_conn()) as conn:
        rows = [dict(row) for row in conn.execute(query, (ticker, f"-{hours} hours"))]

    return jsonify(rows)


scheduler = BackgroundScheduler(timezone="UTC")


def start_scheduler() -> None:
    if not scheduler.running:
        scheduler.add_job(store_hourly_snapshot, "interval", hours=1, id="hourly_snapshot", replace_existing=True)
        scheduler.start()


if __name__ == "__main__":
    init_db()
    try:
        bootstrap_symbols()
        store_hourly_snapshot()
    except (requests.RequestException, ValueError, KeyError, IndexError):
        # Allow the server to run even if network/bootstrap fails.
        pass
    start_scheduler()
    app.run(host="0.0.0.0", port=5000, debug=True)
