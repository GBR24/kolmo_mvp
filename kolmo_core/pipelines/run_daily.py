# kolmo_core/pipelines/run_daily.py
from __future__ import annotations
import os, sys, io, traceback, subprocess, datetime as dt
from pathlib import Path
import duckdb
import pandas as pd

# --- Paths & DB ---
ROOT = Path(__file__).resolve().parents[2]
DBF  = ROOT / "kolmo_core" / "data" / "kolmo.duckdb"
RUNS = ROOT / "kolmo_core" / "runs"
REPORTS = ROOT / "kolmo_core" / "reports"
RUNS.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)

def git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode().strip()
    except Exception:
        return "unknown"

def _run_module(mod: str, args: list[str] | None = None) -> str:
    """Run `python -m <mod> [args...]` and return captured stdout/stderr."""
    cmd = [sys.executable, "-m", mod]
    if args:
        cmd += args
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=str(ROOT))
    out, _ = proc.communicate()
    text = out.decode(errors="ignore")
    if proc.returncode != 0:
        raise RuntimeError(f"Module {mod} failed with code {proc.returncode}\n{text}")
    return text

def ensure_tables():
    con = duckdb.connect(str(DBF))
    con.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_runs (
      run_ts TIMESTAMP,
      status TEXT,               -- 'OK' or 'ERROR'
      stage  TEXT,               -- last successful stage or failing stage
      git_commit TEXT,
      prices_rows BIGINT,
      preds_rows  BIGINT,
      error TEXT
    );
    """)
    con.close()

def db_scalar(query: str):
    con = duckdb.connect(str(DBF))
    try:
        return con.execute(query).fetchone()[0]
    finally:
        con.close()

def _safe_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return 0

def rowcounts() -> tuple[int, int]:
    con = duckdb.connect(str(DBF))
    try:
        prices = _safe_count(con, "prices")
        preds  = _safe_count(con, "predictions")
        return prices, preds
    finally:
        con.close()

def _has_column(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    try:
        return bool(con.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name=? AND column_name=?",
            [table, col]
        ).fetchone()[0])
    except Exception:
        return False

def write_report_md(run_ts: dt.datetime, status: str, stage: str, prices_rows: int, preds_rows: int, logs: str, err: str|None):
    name = f"{run_ts.strftime('%Y%m%d_%H%M%S')}_{status.lower()}.md"
    path = REPORTS / name
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# Kolmo Daily Run — {run_ts.isoformat()}Z\n\n")
        f.write(f"- **Status:** {status}\n")
        f.write(f"- **Stage:** `{stage}`\n")
        f.write(f"- **Git:** `{git_commit()}`\n")
        f.write(f"- **Rows:** prices={prices_rows}, predictions={preds_rows}\n\n")
        f.write("## Logs\n\n```\n")
        f.write(logs.strip())
        f.write("\n```\n")
        if err:
            f.write("\n## Error\n\n```\n")
            f.write(err.strip())
            f.write("\n```\n")
        # Dashboard contract preview (robust to date/ts and yhat/y_hat)
        try:
            con = duckdb.connect(str(DBF))
            # Detect column names
            prices_ts_col = "ts" if _has_column(con, "prices", "ts") else ("date" if _has_column(con, "prices", "date") else None)
            preds_ts_col  = "ts" if _has_column(con, "predictions", "ts") else ("date" if _has_column(con, "predictions", "date") else None)
            yhat_col      = "yhat" if _has_column(con, "predictions", "yhat") else ("y_hat" if _has_column(con, "predictions", "y_hat") else None)
            method_col    = "method" if _has_column(con, "predictions", "method") else None

            if prices_ts_col is None:
                raise RuntimeError("prices table has neither 'ts' nor 'date'")
            if preds_rows > 0 and (preds_ts_col is None or yhat_col is None):
                # If predictions exist but schema unknown, we'll still preview latest prices only
                pass

            # Build a normalized query
            # Latest price per symbol
            latest_prices_sql = f"""
            WITH latest AS (
              SELECT symbol, MAX({prices_ts_col}) AS max_ts
              FROM prices
              GROUP BY symbol
            )
            SELECT p.symbol, p.price AS last_price
            FROM prices p
            JOIN latest l
              ON p.symbol = l.symbol AND p.{prices_ts_col} = l.max_ts
            """

            # Latest prediction per (symbol, method)
            if preds_rows > 0 and preds_ts_col and yhat_col:
                latest_preds_sql = f"""
                WITH latest AS (
                  SELECT symbol{', ' + method_col if method_col else ''}, MAX({preds_ts_col}) AS max_ts
                  FROM predictions
                  GROUP BY symbol{', ' + method_col if method_col else ''}
                )
                SELECT pr.symbol{', pr.' + method_col if method_col else ''}, pr.{yhat_col} AS pred_1d
                FROM predictions pr
                JOIN latest l
                  ON pr.symbol = l.symbol
                 AND pr.{preds_ts_col} = l.max_ts
                 {f"AND pr.{method_col} = l.{method_col}" if method_col else ""}
                """
                df = con.execute(f"""
                WITH lp AS ({latest_prices_sql}),
                     pr AS ({latest_preds_sql})
                SELECT s.symbol,
                       lp.last_price,
                       pr.pred_1d,
                       {f"pr.{method_col}" if method_col else "CAST(NULL AS VARCHAR) AS method"}
                FROM (SELECT DISTINCT symbol FROM prices) s
                LEFT JOIN lp ON s.symbol = lp.symbol
                LEFT JOIN pr ON s.symbol = pr.symbol
                ORDER BY s.symbol, method
                """).fetchdf()
            else:
                # No predictions or unknown schema -> just prices
                df = con.execute(f"""
                WITH lp AS ({latest_prices_sql})
                SELECT s.symbol,
                       lp.last_price,
                       CAST(NULL AS DOUBLE) AS pred_1d,
                       CAST(NULL AS VARCHAR) AS method
                FROM (SELECT DISTINCT symbol FROM prices) s
                LEFT JOIN lp ON s.symbol = lp.symbol
                ORDER BY s.symbol
                """).fetchdf()

            f.write("\n## Dashboard Contract (preview)\n\n")
            if not df.empty:
                f.write(df.to_markdown(index=False))
            else:
                f.write("_No data to preview yet._")
        except Exception as e:
            f.write("\n_Contract preview failed:_ " + str(e))
        finally:
            try: con.close()
            except: pass
    return path

def insert_run(run_ts, status, stage, prices_rows, preds_rows, err):
    con = duckdb.connect(str(DBF))
    try:
        con.execute("""
        INSERT INTO pipeline_runs(run_ts,status,stage,git_commit,prices_rows,preds_rows,error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [run_ts, status, stage, git_commit(), prices_rows, preds_rows, err])
    finally:
        con.close()

if __name__ == "__main__":
    ensure_tables()
    run_ts = dt.datetime.utcnow()
    logs = io.StringIO()
    status, stage, err = "OK", "start", None

    try:
        # 1) (Optional) regenerate mocks if you want fresh noise each day
        # logs.write(_run_module("scripts.make_mock_prices"))

        # 2) Load prices -> DuckDB (creates/updates 'prices')
        stage = "ingestion.mock"
        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        # Use the NEW module path that works:
        logs.write(_run_module("kolmo_core.data.sources.mock_ingestion"))
        # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        # 3) Baselines -> predictions (non-fatal if not ready)
        stage = "pipelines.run_baselines"
        try:
            logs.write(_run_module("kolmo_core.pipelines.run_baselines"))
        except Exception as e:
            logs.write(f"\n[WARN] run_baselines skipped: {e}\n")

        # 4) Create/refresh helpful views (optional)
        stage = "pipelines.create_views"
        try:
            logs.write(_run_module("kolmo_core.pipelines.create_views"))
        except Exception as e:
            logs.write(f"\n[WARN] create_views skipped: {e}\n")

        # 5) Validate data (optional strict checks)
        stage = "pipelines.validate_data"
        try:
            logs.write(_run_module("kolmo_core.pipelines.validate_data"))
        except Exception as e:
            logs.write(f"\n[WARN] validate_data skipped: {e}\n")

    except Exception as e:
        status = "ERROR"
        err = traceback.format_exc()[:10000]
        logs.write("\n\n[EXCEPTION]\n" + err)

    # Snapshot counts & persist artifacts
    try:
        prices_rows, preds_rows = rowcounts()
    except Exception:
        prices_rows, preds_rows = -1, -1

    insert_run(run_ts, status, stage, prices_rows, preds_rows, err)
    rep = write_report_md(run_ts, status, stage, prices_rows, preds_rows, logs.getvalue(), err)

    print(f"[run_daily] {status} stage={stage} prices={prices_rows} preds={preds_rows}")
    print(f"[run_daily] report -> {rep}")
