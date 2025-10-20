# kolmo_core/data/ingestion.py
import os
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

from kolmo_core.config.config import CONFIG
from kolmo_core.data.sources.eia import fetch_eia_series
from kolmo_core.data.sources.news import fetch_news


# ---------- Load ENV ----------
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")


# ---------- DB PATH HANDLER ----------
def _normalize_db_url(raw: str) -> str:
    """
    Normalize DB_URL (handles duckdb:/// and relative paths)
    """
    if not raw:
        return str(ROOT / "kolmo_core" / "data" / "kolmo.duckdb")
    if "://" in raw:
        u = urlparse(raw)
        if u.scheme == "duckdb":
            if u.netloc:
                return f"/{u.netloc}{u.path}"
            return u.path.lstrip("/")
    return raw


DB_URL = _normalize_db_url(CONFIG["db"]["url"])
EIA_API_KEY = os.getenv("EIA_API_KEY", "")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")


# ---------- DATABASE ----------
def connect_db():
    path = Path(DB_URL)
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[duckdb] connecting -> {path}")
    return duckdb.connect(str(path))


def ensure_schema():
    with connect_db() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS market_prices (
                symbol TEXT,
                name   TEXT,
                ts     TIMESTAMP,
                open   DOUBLE,
                high   DOUBLE,
                low    DOUBLE,
                close  DOUBLE,
                volume DOUBLE,
                source TEXT,
                PRIMARY KEY (symbol, ts)
            );
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS market_news (
                id            TEXT PRIMARY KEY,
                headline      TEXT,
                description   TEXT,
                url           TEXT,
                published_at  TIMESTAMP,
                source        TEXT,
                tickers       TEXT,
                keywords      TEXT
            );
        """)


def upsert_prices(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    with connect_db() as con:
        con.register("df_prices", df)
        con.execute("""
            INSERT OR REPLACE INTO market_prices
            SELECT symbol, name, ts, open, high, low, close, volume, source
            FROM df_prices
        """)
    return len(df)


def upsert_news(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    with connect_db() as con:
        con.register("df_news", df)
        con.execute("""
            INSERT OR REPLACE INTO market_news
            SELECT id, headline, description, url, published_at, source, tickers, keywords
            FROM df_news
        """)
    return len(df)


# ---------- HELPERS ----------
def _last_ts_for_symbol(con, symbol: str):
    try:
        row = con.execute(
            "SELECT max(ts) FROM market_prices WHERE symbol = ?", [symbol]
        ).fetchone()
        return row[0]
    except Exception:
        return None


def _next_day(ts):
    return None if ts is None else ts + timedelta(days=1)


# ---------- EIA FETCHER ----------
def _fetch_eia(sym: str, name: str, series_id: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Download historical series from EIA and prepare for DuckDB.
    """
    if not EIA_API_KEY:
        print(f"[{sym}] Missing EIA_API_KEY — skipping.")
        return pd.DataFrame()

    df = fetch_eia_series(series_id, EIA_API_KEY)
    if df.empty:
        print(f"[{sym}] No data returned from EIA.")
        return pd.DataFrame()

    # Filter by requested window
    df = df[(df["ts"] >= pd.Timestamp(start.date())) & (df["ts"] <= pd.Timestamp(end.date()))]
    if df.empty:
        print(f"[{sym}] No rows in window {start.date()} → {end.date()}.")
        return pd.DataFrame()

    df = df.rename(columns={"value": "close"})
    df["open"] = df["high"] = df["low"] = df["close"]
    df["volume"] = None
    df["symbol"] = sym
    df["name"] = name
    df["source"] = "eia"
    return df[["symbol", "name", "ts", "open", "high", "low", "close", "volume", "source"]]


# ---------- INGESTION ----------
def ingest_prices(days_back: int = 365 * 3) -> int:
    ensure_schema()
    end = datetime.now(timezone.utc)
    frames = []

    with connect_db() as con:
        for sym, meta in CONFIG["market"]["symbols"].items():
            provider = meta.get("provider")
            series_id = meta.get("id")
            name = meta.get("name")

            last = _last_ts_for_symbol(con, sym)
            start = _next_day(last) or (end - timedelta(days=days_back))
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)

            print(f"[{sym}] {name} | provider={provider} | window {start.date()} → {end.date()}")

            df = pd.DataFrame()
            try:
                if provider == "eia":
                    df = _fetch_eia(sym, name, series_id, start, end)
                    time.sleep(0.3)  # respectful pause
                else:
                    print(f"[{sym}] Unknown provider {provider} — skipping.")
                    continue
            except Exception as e:
                print(f"[WARN] {sym} failed: {e}")
                continue

            if df.empty:
                print(f"[{sym}] no new rows")
                continue

            print(f"[{sym}] fetched {len(df)} rows")
            frames.append(df)

    if not frames:
        print("No price frames to insert.")
        return 0

    all_df = pd.concat(frames, ignore_index=True)
    inserted = upsert_prices(all_df)
    print(f"[OK] inserted {inserted} price rows")
    return inserted


def ingest_news() -> int:
    ensure_schema()
    if not NEWS_API_KEY:
        print("[news] NEWS_API_KEY missing — skipping.")
        return 0

    frames = []
    for q in CONFIG["news"]["default_queries"]:
        try:
            df = fetch_news(q, NEWS_API_KEY, page_size=CONFIG["news"]["max_per_query"])
        except Exception as e:
            print(f"[news] '{q}' failed: {e}")
            continue
        if df.empty:
            continue
        df["tickers"] = None
        frames.append(df)
        time.sleep(0.6)

    if not frames:
        print("[news] no rows")
        return 0

    all_df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["id"])
    inserted = upsert_news(all_df)
    print(f"[OK] inserted {inserted} news rows")
    return inserted


if __name__ == "__main__":
    n_prices = ingest_prices(days_back=120)
    n_news = ingest_news()
    print(f"Ingested prices: {n_prices}; news: {n_news}")




