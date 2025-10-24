from __future__ import annotations
import pandas as pd
from typing import Dict, Any
from .db_tools import query_db

def run_forecast(symbol: str, horizon: int = 5, model_hint: str | None = None) -> Dict[str, Any]:
    hist = query_db([symbol], "prices", None, None, ["dt","symbol","close"])
    if hist.empty or "close" not in hist.columns:
        return {"preds": [], "meta": {"symbol": symbol, "horizon": horizon, "model": model_hint or "naive_hold", "note": "no close column"}}
    last = float(hist["close"].iloc[-1])
    last_dt = pd.to_datetime(hist["dt"].iloc[-1])
    future = pd.date_range(last_dt, periods=horizon, inclusive="right", freq="D")
    preds = [{"target_dt": d.strftime("%Y-%m-%d"), "yhat": last} for d in future]
    return {"preds": preds, "meta": {"symbol": symbol, "horizon": horizon, "model": model_hint or "naive_hold"}}
