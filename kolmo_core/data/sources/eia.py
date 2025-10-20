import requests
import pandas as pd

class EIAError(Exception):
    pass

def fetch_eia_series(series_id: str, api_key: str) -> pd.DataFrame:
    """
    Fetch a single time series using EIA API v2 "seriesid" route.
    Accepts legacy APIv1 series IDs (e.g., PET.RBRTE.D, PET.RWTC.D, NG.RNGWHHD.D).
    Returns columns: ['ts','value'] sorted by ts asc.
    """
    url = f"https://api.eia.gov/v2/seriesid/{series_id}"
    params = {"api_key": api_key}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise EIAError(f"{r.status_code} {r.reason}: {r.url}")

    js = r.json()
    # API v2 returns under ["response"]["data"]
    data = (js.get("response") or {}).get("data", [])
    if not data:
        # Some series put warnings here; still return empty df gracefully
        return pd.DataFrame(columns=["ts", "value"])

    df = pd.DataFrame(data)
    # Standardize
    if "period" not in df or "value" not in df:
        return pd.DataFrame(columns=["ts", "value"])

    df["ts"] = pd.to_datetime(df["period"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[["ts", "value"]].dropna().sort_values("ts")
    return df
