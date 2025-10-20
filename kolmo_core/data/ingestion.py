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
from kolmo_core.data.sources.oilprice import fetch_oilprice_series
from kolmo_core.data.sources.nasdaq import fetch_nasdaq_series
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
OILPRICE_API_KEY = os.getenv("OILPRICE_API_KEY", "")
NASDAQ_API_KEY = os.getenv("NASDAQ_DATA_LINK_API_KEY", "")  # Updated for nasdaqdatalink
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
        print("[DEBUG] No data to upsert into market_prices.")
        return 0
    with connect_db() as con:
        con.register("df_prices", df)
        try:
            con.execute("""
                INSERT OR REPLACE INTO market_prices
                SELECT symbol, name, ts, open, high, low, close, volume, source
                FROM df_prices
            """)
            print(f"[DEBUG] Upsert attempt for {len(df)} rows completed.")
        except Exception as e:
            print(f"[WARN] Upsert failed: {e}")
            return 0
    return len(df)


def upsert_news(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    with connect_db() as con:
        con.register("df_news", df)
        con.execute("""
            INSERT OR REPLACE INTO market_prices
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

    print(f"[DEBUG] Fetching EIA data for {sym} with series_id={series_id}, window {start.date()} → {end.date()}")
    df = fetch_eia_series(series_id, EIA_API_KEY)
    if df.empty:
        print(f"[DEBUG] {sym} No data returned from EIA for series_id={series_id}. Raw response check needed.")
        return pd.DataFrame()

    print(f"[DEBUG] {sym} Raw EIA data shape: {df.shape}, columns: {df.columns.tolist()}")
    print(f"[DEBUG] {sym} Raw ts range: {df['ts'].min().date()} to {df['ts'].max().date()}")
    # Filter by requested window, but only enforce lower bound initially
    df = df[df["ts"] >= pd.Timestamp(start.date())]
    if df.empty:
        print(f"[DEBUG] {sym} No rows after lower bound filter {start.date()} → {end.date()}.")
        return pd.DataFrame()

    df = df.rename(columns={"value": "close"})
    df["open"] = df["high"] = df["low"] = df["close"]
    df["volume"] = None
    df["symbol"] = sym
    df["name"] = name
    df["source"] = "eia"
    print(f"[DEBUG] {sym} Processed EIA data shape: {df.shape}")
    return df[["symbol", "name", "ts", "open", "high", "low", "close", "volume", "source"]]


# ---------- OILPRICEAPI FETCHER ----------
def _fetch_oilprice(sym: str, name: str, commodity: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Download recent price data from OilPriceAPI and prepare for DuckDB.
    Note: OilPriceAPI free tier provides recent/real-time data (limited history).
    """
    if not OILPRICE_API_KEY:
        print(f"[{sym}] Missing OILPRICE_API_KEY — skipping.")
        return pd.DataFrame()

    df = fetch_oilprice_series(commodity)
    if df.empty:
        print(f"[DEBUG] {sym} No data returned from OilPriceAPI for {commodity}.")
        return pd.DataFrame()

    print(f"[DEBUG] {sym} Raw OilPrice data shape: {df.shape}, columns: {df.columns.tolist()}")
    # Relax filtering to capture all data
    df = df[df["ts"] >= pd.Timestamp(start.date())]
    if df.empty:
        print(f"[DEBUG] {sym} No rows in window {start.date()} → {end.date()} (OilPriceAPI limited to recent data).")
        return pd.DataFrame()

    df = df.rename(columns={"value": "close"})
    df["open"] = df["high"] = df["low"] = df["close"]
    df["volume"] = None
    df["symbol"] = sym
    df["name"] = name
    df["source"] = "oilprice"
    print(f"[DEBUG] {sym} Processed OilPrice data shape: {df.shape}")
    return df[["symbol", "name", "ts", "open", "high", "low", "close", "volume", "source"]]


# ---------- NASDAQ FETCHER ----------
def _fetch_nasdaq(sym: str, name: str, dataset: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Download historical series from Nasdaq Data Link and prepare for DuckDB.
    Includes caching to avoid repeated API calls.
    """
    if not NASDAQ_API_KEY:
        print(f"[{sym}] Missing NASDAQ_DATA_LINK_API_KEY — skipping.")
        return pd.DataFrame()

    cache_path = ROOT / "cache" / f"{sym}_nasdaq_cache.csv"
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    # Check cache first
    if cache_path.exists():
        df = pd.read_csv(cache_path)
        df["ts"] = pd.to_datetime(df["ts"])
        if not df.empty and (df["ts"].max() >= pd.Timestamp(end.date()) or start is None):
            print(f"[{sym}] Using cached data up to {df['ts'].max().date()}")
            return df[(df["ts"] >= pd.Timestamp(start.date())) & (df["ts"] <= pd.Timestamp(end.date()))]

    # Fetch new data
    df = fetch_nasdaq_series(dataset, start=start, end=end)
    if df.empty:
        print(f"[DEBUG] {sym} No data returned from Nasdaq for {dataset}.")
        return pd.DataFrame()

    print(f"[DEBUG] {sym} Raw Nasdaq data shape: {df.shape}, columns: {df.columns.tolist()}")
    # Hybrid: If fewer than 5 rows, fetch EIA as fallback for deeper history
    if len(df) < 5 and EIA_API_KEY:
        print(f"[{sym}] Nasdaq returned <5 rows, falling back to EIA.")
        eia_df = _fetch_eia(sym, name, dataset, start, end)  # Use dataset as series_id proxy
        if not eia_df.empty:
            df = pd.concat([df, eia_df]).drop_duplicates("ts").sort_values("ts")

    df["symbol"] = sym
    df["name"] = name
    df["source"] = "nasdaq"
    df.to_csv(cache_path, index=False)  # Cache for next run
    print(f"[DEBUG] {sym} Processed Nasdaq data shape: {df.shape}")
    return df[["symbol", "name", "ts", "open", "high", "low", "close", "volume", "source"]]


# ---------- INGESTION ----------
def ingest_prices(days_back: int = 365) -> int:
    ensure_schema()
    end = datetime.now(timezone.utc)
    frames = []

    with connect_db() as con:
        for sym, meta in CONFIG["market"]["symbols"].items():
            provider = meta.get("provider")
            name = meta.get("name")

            last = _last_ts_for_symbol(con, sym)
            start = _next_day(last) or (end - timedelta(days=days_back))
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)

            print(f"[{sym}] {name} | provider={provider} | window {start.date()} → {end.date()}")

            df = pd.DataFrame()
            try:
                if provider == "eia":
                    series_id = meta.get("id")
                    df = _fetch_eia(sym, name, series_id, start, end)
                    time.sleep(0.3)  # Respectful pause
                elif provider == "oilprice":
                    commodity = meta.get("commodity")
                    if not commodity:
                        raise ValueError(f"Missing 'commodity' in config for {sym}")
                    df = _fetch_oilprice(sym, name, commodity, start, end)
                    time.sleep(0.3)  # Respectful pause
                elif provider == "nasdaq":
                    dataset = meta.get("dataset")
                    if not dataset:
                        raise ValueError(f"Missing 'dataset' in config for {sym}")
                    df = _fetch_nasdaq(sym, name, dataset, start, end)
                    time.sleep(0.3)  # Respectful pause
                else:
                    print(f"[{sym}] Unknown provider {provider} — skipping.")
                    continue
            except Exception as e:
                print(f"[WARN] {sym} failed: {e}")
                continue

            if df.empty:
                print(f"[DEBUG] {sym} no new rows after processing.")
                continue

            print(f"[{sym}] fetched {len(df)} rows")
            frames.append(df)

    if not frames:
        print("[DEBUG] No price frames to insert.")
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
    n_prices = ingest_prices(days_back=365)
    n_news = ingest_news()
    print(f"Ingested prices: {n_prices}; news: {n_news}")

    # Suggestion: Schedule daily at off-peak (e.g., 2 AM UTC) via cron: 0 2 * * * python /path/to/ingestion.py