from __future__ import annotations
import duckdb, pandas as pd
from kolmo_core.config.config import CONFIG
from kolmo_core.models.baselines import ewma_next, ar1_next

DB = CONFIG["storage"]["db_url"].replace("duckdb:///", "")

def upsert(df: pd.DataFrame):
    con = duckdb.connect(DB)
    con.register("dfp", df)
    con.execute("""
    CREATE TABLE IF NOT EXISTS predictions (
      run_ts TIMESTAMP, date DATE, symbol TEXT, y_hat DOUBLE, y_last DOUBLE,
      method TEXT, horizon TEXT
    );
    """)
    con.execute("""
    DELETE FROM predictions
    USING dfp s
    WHERE predictions.date = s.date
      AND predictions.symbol = s.symbol
      AND predictions.method = s.method
      AND predictions.horizon = s.horizon;
    """)
    con.execute("""
    INSERT INTO predictions
    SELECT run_ts, date, symbol, y_hat, y_last, method, horizon
    FROM dfp;
    """)
    n = con.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
    print("predictions rowcount:", n)

if __name__ == "__main__":
    dfs = []
    dfs.append(ewma_next(DB, span=20))  # fast trend
    dfs.append(ar1_next(DB, lookback=120))
    final = pd.concat(dfs, ignore_index=True)
    upsert(final)
    print(final.sort_values(["symbol","method"]))