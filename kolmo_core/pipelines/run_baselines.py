# kolmo_core/pipelines/run_baselines.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
import duckdb
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
DBF  = ROOT / "kolmo_core" / "data" / "kolmo.duckdb"

METHODS = [
    ("naive_last", 1),
    ("sma_7",      1),
    ("sma_14",     1),
]

# ----------------- helpers -----------------
def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()[0])

def _has_col(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    return bool(con.execute(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name=? AND column_name=?",
        [table, col]
    ).fetchone()[0])

def _prices_ts_col(con: duckdb.DuckDBPyConnection) -> str:
    if _has_col(con, "prices", "ts"):   return "ts"
    if _has_col(con, "prices", "date"): return "date"
    raise RuntimeError("prices needs a 'ts' or 'date' column")

def _ensure_predictions_canonical(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ensure 'predictions' exists with canonical schema:
      ts TIMESTAMP, symbol TEXT, method TEXT, horizon INT, yhat DOUBLE
    Migrate legacy schemas (date/y_hat/y_last/run_ts/horizon as text) if present.
    """
    if not _table_exists(con, "predictions"):
        con.execute("""
            CREATE TABLE predictions (
                ts TIMESTAMP,
                symbol TEXT,
                method TEXT,
                horizon INTEGER,
                yhat DOUBLE
            );
        """)
        return

    # Already exists -> check if canonical enough
    has_ts    = _has_col(con, "predictions", "ts")
    has_yhat  = _has_col(con, "predictions", "yhat")
    if has_ts and has_yhat:
        return  # good

    # Legacy columns we might map
    has_date   = _has_col(con, "predictions", "date")
    has_y_hat  = _has_col(con, "predictions", "y_hat")
    has_y_last = _has_col(con, "predictions", "y_last")
    has_method = _has_col(con, "predictions", "method")
    has_hor    = _has_col(con, "predictions", "horizon")  # may be TEXT like '1d'

    # Choose best y source
    y_expr = None
    if has_yhat:
        y_expr = "yhat"
    elif has_y_hat:
        y_expr = "y_hat"
    elif has_y_last:
        y_expr = "y_last"

    # TS source
    if has_ts:
        ts_expr = "ts"
    elif has_date:
        ts_expr = "CAST(date AS TIMESTAMP)"
    else:
        ts_expr = "NULL::TIMESTAMP"

    # Method source
    method_expr = "method" if has_method else "CAST('legacy' AS TEXT)"

    # Horizon source: strip non-digits then try_cast, default 1
    if has_hor:
        horizon_expr = "COALESCE(TRY_CAST(regexp_replace(horizon, '[^0-9]', '') AS INTEGER), 1)"
    else:
        horizon_expr = "1"

    con.execute(f"""
        CREATE TABLE predictions_new AS
        SELECT
            {ts_expr}      AS ts,
            symbol         AS symbol,
            {method_expr}  AS method,
            {horizon_expr} AS horizon,
            { (y_expr if y_expr else 'NULL') } AS yhat
        FROM predictions
    """)
    con.execute("DROP TABLE predictions")
    con.execute("ALTER TABLE predictions_new RENAME TO predictions")

def _load_prices(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    ts_col = _prices_ts_col(con)
    df = con.execute(f"""
        SELECT symbol, {ts_col} AS ts, price
        FROM prices
        ORDER BY symbol, {ts_col}
    """).fetchdf()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["symbol", "ts", "price"])
    return df

def _predict_for_symbol(df_sym: pd.DataFrame) -> list[dict]:
    if df_sym.empty:
        return []
    df_sym = df_sym.sort_values("ts")
    next_ts = df_sym["ts"].iloc[-1] + pd.Timedelta(days=1)

    s = df_sym["price"].astype(float)
    naive = s.iloc[-1]
    sma7  = s.rolling(7,  min_periods=1).mean().iloc[-1]
    sma14 = s.rolling(14, min_periods=1).mean().iloc[-1]

    out = []
    for method, horizon in METHODS:
        if method == "naive_last":
            yhat = float(naive)
        elif method == "sma_7":
            yhat = float(sma7)
        elif method == "sma_14":
            yhat = float(sma14)
        else:
            continue
        out.append({
            "ts": next_ts,
            "symbol": df_sym["symbol"].iloc[0],
            "method": method,
            "horizon": int(horizon),
            "yhat": float(yhat),
        })
    return out

# ----------------- main -----------------
def main():
    con = duckdb.connect(str(DBF))
    try:
        # Ensure canonical predictions table (migrate legacy if needed)
        _ensure_predictions_canonical(con)

        prices = _load_prices(con)
        if prices.empty:
            print("[run_baselines] No prices found; skipping predictions.")
            return

        preds_rows: list[dict] = []
        for sym, df_sym in prices.groupby("symbol", sort=False):
            preds_rows.extend(_predict_for_symbol(df_sym))

        if not preds_rows:
            print("[run_baselines] Nothing to write.")
            return

        df_preds = pd.DataFrame(preds_rows, columns=["ts","symbol","method","horizon","yhat"])

        con.register("df_src", df_preds)
        con.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                ts TIMESTAMP,
                symbol TEXT,
                method TEXT,
                horizon INTEGER,
                yhat DOUBLE
            );
        """)
        con.execute("""
            DELETE FROM predictions
            USING df_src s
            WHERE predictions.ts = s.ts
              AND predictions.symbol = s.symbol
              AND predictions.method = s.method
              AND predictions.horizon = s.horizon;
        """)
        con.execute("""
            INSERT INTO predictions (ts, symbol, method, horizon, yhat)
            SELECT ts, symbol, method, horizon, yhat FROM df_src;
        """)

        total = con.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        print(f"[run_baselines] Wrote {len(df_preds)} rows. predictions total={total}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
