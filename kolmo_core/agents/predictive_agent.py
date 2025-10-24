# kolmo_core/agents/predictive_agent.py
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
import numpy as np
import pandas as pd

# ---------- Optional ARIMA dependency ----------
try:
    from statsmodels.tsa.arima.model import ARIMA  # type: ignore
    _HAS_ARIMA = True
except Exception:
    _HAS_ARIMA = False


# ===================== DB helpers =====================
def _resolve_db_path() -> str:
    """
    Accept DB_URL as:
      - 'duckdb:///relative/or/absolute/path.duckdb'  OR
      - plain filesystem path like 'kolmo_core/data/kolmo.duckdb'
    Return a filesystem path suitable for duckdb.connect().
    """
    db_url = os.getenv("DB_URL", "duckdb:///kolmo_core/data/kolmo.duckdb")
    if db_url.startswith("duckdb:///"):
        db_path = db_url.replace("duckdb:///", "")
    else:
        db_path = db_url
    return str(Path(db_path).resolve())


def _connect():
    return duckdb.connect(_resolve_db_path())


def _ensure_tables(con: duckdb.DuckDBPyConnection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS predictions (
        symbol VARCHAR,
        ts TIMESTAMP,
        horizon INTEGER,
        method VARCHAR,
        yhat DOUBLE,
        yhat_lower DOUBLE,
        yhat_upper DOUBLE,
        asof_ts TIMESTAMP
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS metrics (
        symbol VARCHAR,
        asof_ts TIMESTAMP,
        method VARCHAR,
        metric VARCHAR,
        value DOUBLE
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS model_selection (
        symbol VARCHAR,
        asof_ts TIMESTAMP,
        best_method VARCHAR,
        metric VARCHAR,
        value DOUBLE
    );
    """)


def ensure_predictions_canonical(con: duckdb.DuckDBPyConnection):
    """
    Keep only the latest asof_ts per (symbol, ts, horizon, method).
    """
    con.execute("""
    DELETE FROM predictions
    WHERE (symbol, ts, horizon, method, asof_ts) NOT IN (
        SELECT symbol, ts, horizon, method, max_asof
        FROM (
            SELECT
                symbol, ts, horizon, method,
                max(asof_ts) OVER (PARTITION BY symbol, ts, horizon, method) AS max_asof
            FROM predictions
        )
        GROUP BY 1,2,3,4,5
    );
    """)


# ===================== Core predictive utilities =====================
@dataclass
class ForecastResult:
    future_ts: List[pd.Timestamp]
    yhat: np.ndarray
    lower: np.ndarray
    upper: np.ndarray


def _last_60(con: duckdb.DuckDBPyConnection, symbol: str) -> pd.DataFrame:
    """
    Pull the last 60 observations ascending. Assumes 'prices(symbol, ts, price)'.
    """
    df = con.execute("""
        SELECT symbol, ts, price
        FROM prices
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT 60
    """, [symbol]).df()
    if df.empty:
        return df
    return df.sort_values("ts").reset_index(drop=True)


def _infer_freq(ts: pd.Series) -> str:
    """
    Naive frequency inference; defaults to daily.
    """
    if len(ts) < 2:
        return "D"
    diffs = ts.diff().dropna()
    median = diffs.median()
    if pd.isna(median):
        return "D"
    seconds = median.total_seconds()
    if 23 * 3600 <= seconds <= 25 * 3600:
        return "D"
    if 6.5 * 3600 <= seconds <= 7.5 * 3600:
        return "H"
    return "D"


def _compute_resid_scale(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    resid = y_true - y_pred
    if len(resid) < 2:
        return float(np.nan)
    return float(np.nanstd(resid, ddof=1))


# ===================== Methods =====================
def _future_index(last_ts: pd.Timestamp, horizon: int, freq: str) -> List[pd.Timestamp]:
    return pd.date_range(
        last_ts + pd.tseries.frequencies.to_offset(freq),
        periods=horizon,
        freq=freq
    ).to_list()


def forecast_naive_last(
    y: np.ndarray, horizon: int, freq: str, last_ts: pd.Timestamp
) -> ForecastResult:
    yhat = np.repeat(float(y[-1]), horizon).astype(float)
    if len(y) > 1:
        pred_in = y[:-1]
        true_in = y[1:]
        s = _compute_resid_scale(true_in, pred_in)
    else:
        s = np.nan
    lower = yhat - 1.96 * s if s == s else yhat
    upper = yhat + 1.96 * s if s == s else yhat
    return ForecastResult(_future_index(last_ts, horizon, freq), yhat, lower, upper)


def forecast_sma(
    y: np.ndarray, horizon: int, freq: str, last_ts: pd.Timestamp, window: int = 7
) -> ForecastResult:
    sma = pd.Series(y).rolling(window=window, min_periods=1).mean().values
    yhat_last = float(sma[-1])
    yhat = np.repeat(yhat_last, horizon).astype(float)

    if len(y) > 1:
        sma_bt = pd.Series(y).shift(1).rolling(window=window, min_periods=1).mean().values
        true_in = y[1:]
        pred_in = sma_bt[1:]
        s = _compute_resid_scale(true_in, pred_in)
    else:
        s = np.nan

    lower = yhat - 1.96 * s if s == s else yhat
    upper = yhat + 1.96 * s if s == s else yhat
    return ForecastResult(_future_index(last_ts, horizon, freq), yhat, lower, upper)


def forecast_gbm_mc(
    y: np.ndarray, horizon: int, freq: str, last_ts: pd.Timestamp, n_sims: int = 2000, seed: int = 42
) -> ForecastResult:
    rng = np.random.default_rng(seed)
    last = float(y[-1])
    # estimate log-returns from positive values only
    pos = y[y > 0]
    r = np.diff(np.log(pos)) if len(pos) >= 2 else np.array([])
    if len(r) < 2:
        mu, sigma = 0.0, 0.0
    else:
        mu, sigma = float(np.nanmean(r)), float(np.nanstd(r, ddof=1))

    shocks = rng.normal(loc=(mu - 0.5 * sigma**2), scale=sigma, size=(n_sims, horizon))
    steps = np.exp(shocks).cumprod(axis=1)
    sims = last * steps

    yhat = np.nanmean(sims, axis=0)
    lower = np.nanpercentile(sims, 5, axis=0)
    upper = np.nanpercentile(sims, 95, axis=0)
    return ForecastResult(_future_index(last_ts, horizon, freq), yhat, lower, upper)


def forecast_arima(
    y: np.ndarray, horizon: int, freq: str, last_ts: pd.Timestamp, order: Tuple[int, int, int] = (1, 1, 1)
) -> ForecastResult:
    if not _HAS_ARIMA or len(y) < 5:
        # fallback if ARIMA unavailable or too few points
        return forecast_naive_last(y, horizon, freq, last_ts)

    model = ARIMA(y, order=order)
    fit = model.fit()

    fc = fit.get_forecast(steps=horizon)

    # predicted_mean can be ndarray or pandas Series depending on statsmodels version
    pm = getattr(fc, "predicted_mean", None)
    yhat = np.asarray(pm, dtype=float)

    # conf_int can return DataFrame or ndarray; normalize robustly
    ci = fc.conf_int(alpha=0.10)  # ~5/95
    if hasattr(ci, "values"):  # DataFrame
        ci_arr = np.asarray(ci.values, dtype=float)
    else:
        ci_arr = np.asarray(ci, dtype=float)

    if ci_arr.ndim == 2 and ci_arr.shape[1] >= 2:
        lower = ci_arr[:, 0].astype(float)
        upper = ci_arr[:, 1].astype(float)
    else:
        # Fallback: no CI available -> use point forecast as band
        lower = yhat.copy()
        upper = yhat.copy()

    return ForecastResult(_future_index(last_ts, horizon, freq), yhat, lower, upper)


# ===================== Backtest & metrics =====================
def _one_step_predictions(y: np.ndarray, method: str) -> np.ndarray:
    """
    Produce 1-step-ahead predictions for y[1:], using only info up to t-1.
    """
    y = y.astype(float)
    n = len(y)
    preds = np.full(n, np.nan)

    if method == "naive_last":
        preds[1:] = y[:-1]

    elif method == "sma_7":
        s = pd.Series(y)
        preds[1:] = s.shift(1).rolling(7, min_periods=1).mean().values[1:]

    elif method == "gbm_mc":
        pos = y[y > 0]
        r = np.diff(np.log(pos)) if len(pos) >= 2 else np.array([])
        mu = float(np.nanmean(r)) if len(r) else 0.0
        exp_growth = np.exp(mu)
        preds[1:] = y[:-1] * exp_growth

    elif method == "arima":
        if _HAS_ARIMA and n >= 6:
            try:
                fit = ARIMA(y, order=(1, 1, 1)).fit()
                in_pred = fit.get_prediction(start=1, end=n - 1)
                pm = getattr(in_pred, "predicted_mean", None)
                preds[1:] = np.asarray(pm, dtype=float)
            except Exception:
                preds[1:] = y[:-1]
        else:
            preds[1:] = y[:-1]

    else:
        raise ValueError(f"Unknown method: {method}")

    return preds


def _metrics(y: np.ndarray, preds: np.ndarray) -> Dict[str, float]:
    mask = ~np.isnan(preds[1:])
    if mask.sum() == 0:
        return {"RMSE": float("nan"), "MAE": float("nan")}
    e = y[1:][mask] - preds[1:][mask]
    rmse = float(np.sqrt(np.nanmean(e**2)))
    mae = float(np.nanmean(np.abs(e)))
    return {"RMSE": rmse, "MAE": mae}


# ===================== Public API =====================
def predict_for_symbol(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    horizon: int = 5,
    methods: List[str] = None
) -> None:
    """
    Run forecasts for a single symbol, write predictions + metrics + model_selection.
    """
    _ensure_tables(con)

    df = _last_60(con, symbol)
    if df.empty:
        raise ValueError(f"No price history for symbol={symbol}")

    y = df["price"].astype(float).values
    last_ts = pd.to_datetime(df["ts"].iloc[-1])
    freq = _infer_freq(df["ts"])

    if methods is None:
        methods = ["naive_last", "sma_7", "gbm_mc", "arima"]

    asof_ts = pd.Timestamp.utcnow()

    rows_pred = []
    rows_metrics = []
    metric_for_selection: List[Tuple[str, float]] = []

    for m in methods:
        if m == "naive_last":
            fr = forecast_naive_last(y, horizon, freq, last_ts)
        elif m == "sma_7":
            fr = forecast_sma(y, horizon, freq, last_ts, window=7)
        elif m == "gbm_mc":
            fr = forecast_gbm_mc(y, horizon, freq, last_ts)
        elif m == "arima":
            fr = forecast_arima(y, horizon, freq, last_ts, order=(1, 1, 1))
        else:
            continue

        for h, (ts_f, yh, lo, up) in enumerate(zip(fr.future_ts, fr.yhat, fr.lower, fr.upper), start=1):
            rows_pred.append((
                symbol,
                pd.Timestamp(ts_f).to_pydatetime(),
                h,
                m,
                float(yh),
                float(lo),
                float(up),
                asof_ts.to_pydatetime()
            ))

        preds_1 = _one_step_predictions(y, m)
        mm = _metrics(y, preds_1)
        for k, v in mm.items():
            rows_metrics.append((symbol, asof_ts.to_pydatetime(), m, k, float(v)))
        metric_for_selection.append((m, mm.get("RMSE", float("nan"))))

    # Write predictions
    if rows_pred:
        con.executemany("""INSERT INTO predictions (symbol, ts, horizon, method, yhat, yhat_lower, yhat_upper, asof_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", rows_pred)

    # Canonicalize predictions
    ensure_predictions_canonical(con)

    # Write metrics
    if rows_metrics:
        con.executemany("INSERT INTO metrics (symbol, asof_ts, method, metric, value) VALUES (?, ?, ?, ?, ?)""", rows_metrics)

    # Model selection (best by RMSE lowest)
    best_method, best_value = None, float("inf")
    for m, v in metric_for_selection:
        if v == v and v < best_value:
            best_method, best_value = m, v
    if best_method is not None:
        con.execute("""
            INSERT INTO model_selection (symbol, asof_ts, best_method, metric, value)
            VALUES (?, ?, ?, 'RMSE', ?)
        """, [symbol, asof_ts.to_pydatetime(), best_method, float(best_value)])


def predict_all(con: duckdb.DuckDBPyConnection, symbols: List[str], horizon: int = 5, methods: List[str] = None):
    for s in symbols:
        predict_for_symbol(con, s, horizon=horizon, methods=methods)
