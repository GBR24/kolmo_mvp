import requests, pandas as pd
from datetime import datetime

def fetch_eia_series(series_id: str, api_key: str) -> pd.DataFrame:
    """
    Generic EIA fetcher for a single series_id.
    Normalizes to columns: ['ts','value','source'].
    You can map 'value' to 'close' later in ingestion.
    """
    url = "https://api.eia.gov/series/"
    params = {"api_key": api_key, "series_id": series_id}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    series = data["series"][0]["data"]  # list of [period, value]
    rows = []
    for period, val in series:
        # EIA period formats vary (YYYYMM, YYYY, YYYYMMDD). Parse conservatively:
        ts = _parse_eia_period(period)
        rows.append({"ts": ts, "value": float(val), "source": "eia"})
    df = pd.DataFrame(rows).sort_values("ts")
    return df

def _parse_eia_period(s: str):
    # Very simple parser covering common daily/monthly/yearly patterns.
    if len(s) == 8:  # YYYYMMDD
        return datetime.strptime(s, "%Y%m%d")
    if len(s) == 6:  # YYYYMM
        return datetime.strptime(s, "%Y%m")
    if len(s) == 4:  # YYYY
        return datetime.strptime(s, "%Y")
    # Fallback: try date
    return pd.to_datetime(s, errors="coerce")
