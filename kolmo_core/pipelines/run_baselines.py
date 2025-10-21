# kolmo_core/pipelines/run_baselines.py
from __future__ import annotations
import duckdb, pandas as pd, subprocess, os, datetime as dt
from kolmo_core.config.config import CONFIG
from kolmo_core.models.baselines import ewma_next, ar1_next

DB = CONFIG["storage"]["db_url"].replace("duckdb:///", "")

def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode().strip()
    except Exception:
        return "unknown"

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
    run_ts = dt.datetime.utcnow()
    commit = _git_commit()
    print(f"[run_baselines] ts={run_ts.isoformat()}Z commit={commit}")

    dfs = [
        ewma_next(DB, span=20),
        ar1_next(DB, lookback=120),
    ]
    final = pd.concat(dfs, ignore_index=True)

    # add consistent run_ts (already set in helpers, but enforce)
    final["run_ts"] = run_ts

    # Snapshot to runs/
    outdir = "kolmo_core/runs"
    os.makedirs(outdir, exist_ok=True)
    snap = os.path.join(outdir, f"{run_ts.strftime('%Y%m%d_%H%M%S')}_predictions.csv")
    final.to_csv(snap, index=False)
    print(f"[run_baselines] snapshot -> {snap} ({len(final)} rows)")

    # Upsert
    upsert(final)

    # Short stdout summary
    print(final.sort_values(["symbol","method"])[["date","symbol","method","y_last","y_hat"]])
    print("[run_baselines] DONE ✔")
