import requests
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve OilPriceAPI key
OILPRICE_API_KEY = os.getenv('OILPRICE_API_KEY')
if not OILPRICE_API_KEY:
    raise ValueError("OILPRICE_API_KEY not found in .env file")

class OilPriceAPIError(Exception):
    pass

def fetch_oilprice_series(commodity: str = "wti") -> pd.DataFrame:
    """
    Fetch recent price data from OilPriceAPI for a given commodity.
    Commodity options: 'wti' (WTI Crude), 'brent' (Brent Crude), 'natural_gas' (Henry Hub).
    Returns columns: ['ts', 'value'] sorted by ts asc.
    """
    url = "https://api.oilpriceapi.com/v1/prices"
    headers = {"Authorization": f"Bearer {OILPRICE_API_KEY}"}
    # Map commodity to OilPriceAPI's expected format
    commodity_map = {
        "wti": "WTI",
        "brent": "Brent",
        "natural_gas": "Natural Gas"
    }
    if commodity.lower() not in commodity_map:
        raise OilPriceAPIError(f"Unsupported commodity: {commodity}. Choose from {list(commodity_map.keys())}")

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()  # Raise for non-200 status codes
    except requests.exceptions.RequestException as e:
        raise OilPriceAPIError(f"Failed to fetch OilPriceAPI data: {e}")

    js = r.json()
    # OilPriceAPI typically returns a list of price objects under 'data.prices'
    prices = (js.get("data") or {}).get("prices", [])
    if not prices:
        return pd.DataFrame(columns=["ts", "value"])

    # Filter for the requested commodity
    target_commodity = commodity_map[commodity.lower()]
    filtered_prices = [p for p in prices if p.get("commodity") == target_commodity]

    if not filtered_prices:
        return pd.DataFrame(columns=["ts", "value"])

    # Standardize to ts, value
    df = pd.DataFrame(filtered_prices)
    if "timestamp" not in df or "price" not in df:
        return pd.DataFrame(columns=["ts", "value"])

    df["ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["value"] = pd.to_numeric(df["price"], errors="coerce")
    df = df[["ts", "value"]].dropna().sort_values("ts")
    return df

# Example usage (uncomment to test)
# if __name__ == "__main__":
#     try:
#         wti_data = fetch_oilprice_series("wti")
#         print("OilPriceAPI WTI Data:")
#         print(wti_data.head())
#     except OilPriceAPIError as e:
#         print(f"OilPriceAPI Error: {e}")