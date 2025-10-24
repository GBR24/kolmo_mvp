# kolmo_core/pipelines/run_daily.py
from __future__ import annotations

import os
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, date, time
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List

import duckdb
import pandas as pd

# --- Project config ---------------------------------------------------------

try:
    from kolmo_core.config import CONFIG
except Exception:
    CONFIG = {
        "db_url": os.getenv("DB_URL", "duckdb:///kolmo_core/data/kolmo.duckdb"),
        "market": {"symbols": ["CL", "HO", "RB", "NG", "JET"]},
        "paths": {"data_dir": "kolmo_core/data", "reports_dir": "kolmo_core/reports"},
    }

# --- Utilities --------------------------------------------------------------

def _resolve_db_path(db_url: str) -> str:
    if db_url.startswith("duckdb:///"):
        db_path = db_url.replace("duckdb:///", "")
    else:
        db_path = db_url
    return str(Path(db_path).resolve())

def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _reports_dir() -> Path:
    p = Path(CONFIG["paths"]["reports_dir"]).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p

def log(msg: str, **kv):
    if kv:
        kv_str = " ".join(f"{k}={v}" for k, v in kv.items())
        print(f"[run_daily] {msg} {kv_str}")
    else:
        print(f"[run_daily] {msg}")

@dataclass
class StageResult:
    ok: bool
    info: Dict[str, Any]
    error: Optional[str] = None

# --- Database bootstrap -----------------------------------------------------

DDL_PRICES = """
CREATE TABLE IF NOT EXISTS prices (
    symbol TEXT,
    dt DATE,
    close DOUBLE,
    source TEXT,
    ingested_at TIMESTAMP
);
"""

DDL_NEWS = """
CREATE TABLE IF NOT EXISTS news (
    published_at TIMESTAMP,
    source TEXT,
    title TEXT,
    url TEXT,
    tickers TEXT,
    ingested_at TIMESTAMP
);
"""

# 'ASOF' is reserved in DuckDB; use 'as_of'
DDL_PREDICTIONS = """
CREATE TABLE IF NOT EXISTS predictions (
    symbol TEXT,
    as_of DATE,
    horizon INTEGER,
    target_dt DATE,
    yhat DOUBLE,
    model TEXT,
    created_at TIMESTAMP
);
"""

def ensure_tables(con: duckdb.DuckDBPyConnection):
    con.execute(DDL_PRICES)
    con.execute(DDL_NEWS)
    con.execute(DDL_PREDICTIONS)

# --- Introspection helpers (handle dt vs ts in prices) ---------------------

def _table_cols(con: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    try:
        df = con.execute(f"PRAGMA table_info('{table}')").fetchdf()
        return [c.lower() for c in df["name"].tolist()]
    except Exception:
        return []

def _prices_has_dt(con) -> bool:
    return "dt" in _table_cols(con, "prices")

def _prices_has_ts(con) -> bool:
    return "ts" in _table_cols(con, "prices")

def _prices_date_delete_predicate(con) -> str:
    """
    Returns a WHERE predicate that matches the given date parameter (?) against the table's date/timestamp.
    - If dt exists: 'dt = ?'
    - If ts exists: 'DATE(ts) = ?'
    """
    if _prices_has_dt(con):
        return "dt = ?"
    if _prices_has_ts(con):
        return "DATE(ts) = ?"
    # Fall back to dt to surface a clear error elsewhere
    return "dt = ?"

def _prices_select_fields(con) -> str:
    """
    Returns a select field list ensuring we get a 'dt' alias in the result.
    """
    if _prices_has_dt(con):
        return "symbol, dt, close"
    if _prices_has_ts(con):
        return "symbol, DATE(ts) AS dt, close"
    return "symbol, dt, close"  # default; will error if neither exists (as it should)

def _make_prices_ingest_df(as_of: date, symbols: List[str], use_ts: bool) -> pd.DataFrame:
    rows = []
    if use_ts:
        ts_val = datetime.combine(as_of, time.min)  # 00:00:00 of that day
        for s in symbols:
            rows.append((s, ts_val, 100.0, "mock", datetime.now()))
        return pd.DataFrame(rows, columns=["symbol", "ts", "close", "source", "ingested_at"])
    else:
        for s in symbols:
            rows.append((s, as_of, 100.0, "mock", datetime.now()))
        return pd.DataFrame(rows, columns=["symbol", "dt", "close", "source", "ingested_at"])

# --- Stage 1: Ingestion (prices) -------------------------------------------

def ingest_prices(con: duckdb.DuckDBPyConnection, as_of: date, limit: Optional[int] = None) -> StageResult:
    try:
        symbols = CONFIG["market"]["symbols"]
        if limit is not None:
            symbols = symbols[:limit]

        use_ts = _prices_has_ts(con) and not _prices_has_dt(con)
        df = _make_prices_ingest_df(as_of, symbols, use_ts=use_ts)

        con.execute("BEGIN")
        con.execute(f"DELETE FROM prices WHERE {_prices_date_delete_predicate(con)}", [as_of])
        con.register("prices_df", df)

        if use_ts:
            con.execute("""
                INSERT INTO prices (symbol, ts, close, source, ingested_at)
                SELECT symbol, ts, close, source, ingested_at FROM prices_df
            """)
        else:
            con.execute("""
                INSERT INTO prices (symbol, dt, close, source, ingested_at)
                SELECT symbol, dt, close, source, ingested_at FROM prices_df
            """)
        con.execute("COMMIT")

        cnt = con.execute(
            f"SELECT COUNT(*) FROM prices WHERE {_prices_date_delete_predicate(con)}", [as_of]
        ).fetchone()[0]
        return StageResult(ok=True, info={"prices": int(cnt), "symbols": len(symbols), "schema": "ts" if use_ts else "dt"})
    except Exception as e:
        con.execute("ROLLBACK")
        return StageResult(ok=False, info={}, error=f"{type(e).__name__}: {e}")

# --- Stage 2: Ingestion (news) ---------------------------------------------

def ingest_news(con: duckdb.DuckDBPyConnection, as_of: date, limit: int = 50) -> StageResult:
    try:
        rows = [
            (datetime.combine(as_of, time.min), "mock-news", "Energy markets stable", "https://example.com/a", "CL,NG", datetime.now()),
            (datetime.combine(as_of, time.min), "mock-news", "Refinery outages hit HO", "https://example.com/b", "HO", datetime.now()),
        ]
        df = pd.DataFrame(rows, columns=["published_at", "source", "title", "url", "tickers", "ingested_at"])

        con.execute("BEGIN")
        con.execute("DELETE FROM news WHERE DATE(published_at) = ?", [as_of])
        con.register("news_df", df)
        con.execute("INSERT INTO news SELECT * FROM news_df")
        con.execute("COMMIT")

        cnt = con.execute("SELECT COUNT(*) FROM news WHERE DATE(published_at) = ?", [as_of]).fetchone()[0]
        return StageResult(ok=True, info={"news": int(cnt)})
    except Exception as e:
        con.execute("ROLLBACK")
        return StageResult(ok=False, info={}, error=f"{type(e).__name__}: {e}")

# --- Stage 3: Baseline models ----------------------------------------------

def run_baseline_models(con: duckdb.DuckDBPyConnection, as_of: date, horizon_days: int = 5) -> StageResult:
    try:
        prices_df = con.execute(
            f"SELECT {_prices_select_fields(con)} FROM prices WHERE {_prices_date_delete_predicate(con)} ORDER BY symbol",
            [as_of],
        ).fetchdf()

        if prices_df.empty:
            return StageResult(ok=False, info={}, error="No prices for as_of date")

        preds: List[Tuple[str, date, int, date, float, str, datetime]] = []
        for _, row in prices_df.iterrows():
            sym = row["symbol"]
            close = float(row["close"])
            for h in range(1, horizon_days + 1):
                target = pd.Timestamp(as_of) + pd.Timedelta(days=h)
                preds.append((sym, as_of, h, target.date(), close, "naive-close-hold", datetime.now()))

        pred_df = pd.DataFrame(
            preds, columns=["symbol", "as_of", "horizon", "target_dt", "yhat", "model", "created_at"]
        )

        con.execute("BEGIN")
        con.execute("DELETE FROM predictions WHERE as_of = ?", [as_of])
        con.register("pred_df", pred_df)
        con.execute("INSERT INTO predictions SELECT * FROM pred_df")
        con.execute("COMMIT")

        cnt = con.execute("SELECT COUNT(*) FROM predictions WHERE as_of = ?", [as_of]).fetchone()[0]
        return StageResult(ok=True, info={"predictions": int(cnt), "model": "naive-close-hold"})
    except Exception as e:
        con.execute("ROLLBACK")
        return StageResult(ok=False, info={}, error=f"{type(e).__name__}: {e}")

# --- Report writer ----------------------------------------------------------

def write_report(success: bool, as_of: date, stage_infos: Dict[str, Dict[str, Any]], err: Optional[str]) -> Path:
    ts = _now_ts()
    kind = "ok" if success else "error"
    path = _reports_dir() / f"{ts}_{kind}.md"

    lines = []
    lines.append(f"# Kolmo Daily Run — {as_of.isoformat()}")
    lines.append("")
    lines.append(f"- Run timestamp: `{ts}`")
    lines.append(f"- Status: **{'SUCCESS' if success else 'ERROR'}**")
    lines.append("")
    lines.append("## Stages")
    for stage, info in stage_infos.items():
        lines.append(f"### {stage}")
        for k, v in info.items():
            lines.append(f"- {k}: {v}")
        lines.append("")

    if err:
        lines.append("## Error")
        lines.append("")
        lines.append("```\n" + err + "\n```")
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return path

# --- Orchestrator -----------------------------------------------------------

def run(as_of: date, force: bool = False, limit_symbols: Optional[int] = None) -> Path:
    db_path = _resolve_db_path(CONFIG.get("db_url", "duckdb:///kolmo_core/data/kolmo.duckdb"))
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(db_path)
    ensure_tables(con)

    stage_infos: Dict[str, Dict[str, Any]] = {}
    try:
        log("START", as_of=as_of)

        s1 = ingest_prices(con, as_of=as_of, limit=limit_symbols)
        stage_infos["ingestion.prices"] = ({"ok": s1.ok} | s1.info | ({"error": s1.error} if s1.error else {}))
        if not s1.ok and not force:
            raise RuntimeError(f"Ingestion(prices) failed: {s1.error}")

        s2 = ingest_news(con, as_of=as_of)
        stage_infos["ingestion.news"] = ({"ok": s2.ok} | s2.info | ({"error": s2.error} if s2.error else {}))
        if not s2.ok and not force:
            raise RuntimeError(f"Ingestion(news) failed: {s2.error}")

        s3 = run_baseline_models(con, as_of=as_of, horizon_days=5)
        stage_infos["models.baseline"] = ({"ok": s3.ok} | s3.info | ({"error": s3.error} if s3.error else {}))
        if not s3.ok and not force:
            raise RuntimeError(f"Models(baseline) failed: {s3.error}")

        report = write_report(True, as_of, stage_infos, err=None)
        log("DONE", report=str(report))
        return report

    except Exception as e:
        tb = traceback.format_exc(limit=6)
        msg = f"{type(e).__name__}: {e}\n{tb}"
        log("ERROR", stage="pipeline", message=str(e))
        report = write_report(False, as_of, stage_infos, err=msg)
        return report
    finally:
        con.close()

# --- CLI --------------------------------------------------------------------

def _parse_args(argv: List[str]) -> Tuple[date, bool, Optional[int]]:
    import argparse
    parser = argparse.ArgumentParser(description="Run the Kolmo daily pipeline.")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (defaults to today)")
    parser.add_argument("--force", action="store_true", help="Continue even if a stage fails")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of symbols for quick runs")
    args = parser.parse_args(argv)
    as_of = date.fromisoformat(args.date) if args.date else date.today()
    return as_of, args.force, args.limit

def main():
    as_of, force, limit = _parse_args(sys.argv[1:])
    report_path = run(as_of=as_of, force=force, limit_symbols=limit)
    print(f"[run_daily] report -> {report_path}")

if __name__ == "__main__":
    main()
