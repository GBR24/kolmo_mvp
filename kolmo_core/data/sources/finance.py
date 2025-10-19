from datetime import datetime, timedelta, timezone
import pandas as pd
import yfinance as yf

def fetch_yahoo_history(ticker: str, start=None, end=None, interval="1d") -> pd.DataFrame:
    """
    Returns a DataFrame with columns: ['ts','open','high','low','close','volume','source'].
    """
    if end is None:
        end = datetime.now(timezone.utc)
    if start is None:
        start = end - timedelta(days=365*5)  # 5y back by default

    df = yf.download(ticker, start=start, end=end, interval=interval, progress=False)
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts","open","high","low","close","volume","source"])

    df = df.rename(columns=str.lower).reset_index()
    df = df.rename(columns={"date":"ts"})
    keep = ["ts","open","high","low","close","volume"]
    df = df[keep]
    df["source"] = "yahoo"
    return df
