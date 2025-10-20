from __future__ import annotations
import duckdb, pandas as pd, numpy as np
from datetime import datetime, timedelta

def _load_wide(db_path: str, symbols=None) -> pd.DataFrame:
    con = duckdb.connect(db_path)
    if symbols:
        sql = f"""
          SELECT date, symbol, price FROM prices
          WHERE symbol IN ({",".join(repr(s) for s in symbols)})
          ORDER BY date, symbol
        """
    else:
        sql = "SELECT date, symbol, price FROM prices ORDER BY date, symbol"
    df = con.execute(sql).fetchdf()
    wide = df.pivot(index="date", columns="symbol", values="price").sort_index()
    wide.index = pd.to_datetime(wide.index)
    return wide

def _next_bday(d: pd.Timestamp) -> pd.Timestamp:
    # simple next business day: +1 day; if weekend, roll forward
    n = d + pd.tseries.offsets.BDay(1)
    return pd.Timestamp(n.date())

def ewma_next(db_path: str, span: int = 20, symbols=None) -> pd.DataFrame:
    X = _load_wide(db_path, symbols).dropna(how="all")
    latest_date = X.index.max()
    target_date = _next_bday(latest_date)
    y_last = X.loc[latest_date]
    y_hat = X.ewm(span=span, adjust=False).mean().iloc[-1]
    out = pd.DataFrame({
        "symbol": y_hat.index,
        "y_hat": y_hat.values,
        "y_last": y_last.values,
        "method": f"EWMA_{span}",
        "horizon": "1d",
        "date": target_date.date(),
        "run_ts": datetime.utcnow(),
    })
    return out.dropna()

def ar1_next(db_path: str, lookback: int = 60, symbols=None) -> pd.DataFrame:
    X = _load_wide(db_path, symbols).last(f"{lookback}D").dropna(how="any")
    latest_date = X.index.max()
    target_date = _next_bday(latest_date)
    preds = []
    for sym in X.columns:
        s = np.log(X[sym]).dropna()
        if len(s) < 10: 
            continue
        r = s.diff().dropna()
        r_lag = r.shift(1).dropna()
        # OLS for AR(1) on returns: r_t = a + b r_{t-1}
        a, b = np.polyfit(r_lag.values, r.loc[r_lag.index].values, 1)
        r_next = a + b * r.iloc[-1]
        y_last = X[sym].iloc[-1]
        y_hat = float(np.exp(np.log(y_last) + r_next))
        preds.append((sym, y_hat, y_last))
    out = pd.DataFrame(preds, columns=["symbol","y_hat","y_last"])
    out["method"] = f"AR1_ret"
    out["horizon"] = "1d"
    out["date"] = target_date.date()
    out["run_ts"] = datetime.utcnow()
    return out
