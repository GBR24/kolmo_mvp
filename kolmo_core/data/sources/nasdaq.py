import quandl
import pandas as pd
from dotenv import load_dotenv
import os
import datetime

# Load environment variables
load_dotenv()

# Retrieve Nasdaq API key
NASDAQ_API_KEY = os.getenv('NASDAQ_API_KEY')
if not NASDAQ_API_KEY:
    raise ValueError("NASDAQ_API_KEY not found in .env file")

class NasdaqError(Exception):
    pass

def fetch_nasdaq_series(dataset: str, start: datetime.datetime = None, end: datetime.datetime = None) -> pd.DataFrame:
    """
    Fetch historical time series from Nasdaq Data Link.
    Returns columns: ['ts', 'open', 'high', 'low', 'close', 'volume'] (volume may be None).
    """
    if not NASDAQ_API_KEY:
        raise NasdaqError("Missing NASDAQ_API_KEY")

    quandl.ApiConfig.api_key = NASDAQ_API_KEY

    try:
        # Fetch with date range (end +1 day to include end date)
        data = quandl.get(
            dataset,
            start_date=start.date() if start else None,
            end_date=(end + datetime.timedelta(days=1)).date() if end else None
        )
        print(f"[DEBUG] Raw data shape: {data.shape} | Index name: {data.index.name}")
    except Exception as e:
        raise NasdaqError(f"Failed to fetch Nasdaq data for {dataset}: {e}")

    if data.empty or not isinstance(data, pd.DataFrame):
        print(f"[DEBUG] Empty or invalid response for {dataset}. Response type: {type(data)}")
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

    # Reset index to ensure 'Date' column exists, handle if index name differs
    df = data.reset_index()
    if "Date" not in df.columns:
        print(f"[DEBUG] 'Date' not found in columns. Available columns: {df.columns.tolist()}")
        # Attempt to use index name if it exists and is a date-like column
        if data.index.name and "Date" not in df.columns:
            df = df.rename(columns={data.index.name: "Date"})
        else:
            raise NasdaqError(f"Expected 'Date' column not found in {dataset} data.")

    # Rename columns
    df = df.rename(columns={
        "Date": "ts",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Last": "close",  # 'Last' is the settle/close for futures
        "Volume": "volume"
    })

    # Standardize
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    required_cols = ["ts", "open", "high", "low", "close"]
    if not all(col in df for col in required_cols):
        print(f"[DEBUG] Missing columns in {dataset}. Available: {df.columns.tolist()}")
        raise NasdaqError(f"Missing expected columns in {dataset} data.")

    df["volume"] = df.get("volume", None)  # Volume may not always be present
    df = df[required_cols + ["volume"]].dropna(subset=["ts"]).sort_values("ts")

    # Filter strictly to window
    if start:
        df = df[df["ts"] >= pd.Timestamp(start)]
    if end:
        df = df[df["ts"] <= pd.Timestamp(end)]

    print(f"[DEBUG] Fetched {len(df)} rows for {dataset} | Range: {df['ts'].min()} to {df['ts'].max()}")
    return df