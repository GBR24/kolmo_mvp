from __future__ import annotations

import os, sys, traceback
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
    return (
        str(Path(db_url.replace("duckdb:///", "")).resolve())
        if db_url.startswith("duckdb:///")
        else str(Path(db_url).resolve())
    )

def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _reports_dir() -> Path:
    p = Path(CONFIG["paths"]["reports_dir"]).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p

def log(msg: str, **kv):
    print(f"[run_daily] {msg}" + ("" if not kv else " " + " ".join(f"{k}={v}" for k, v in kv.items())))

@dataclass
class StageResult:
    ok: bool
    info: Dict[str, Any]
    error: Optional[str] = None

# --- Bootstrap DDLs (used only if tables don't exist) ----------------------
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

# canonical; we won't override an existing incompatible table
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

# --- Introspection helpers --------------------------------------------------
def _table_cols(con: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    try:
        df = con.execute(f"PRAGMA table_info('{table}')").fetchdf()
        return [str(x).lower() for x in df["name"].tolist()]
    except Exception:
        return []

def _existing_cols(con, table: str) -> set[str]:
    return set(_table_cols(con, table))

def _df_trim_to_table(df: pd.DataFrame, table_cols: set[str]) -> pd.DataFrame:
    keep = [c for c in df.columns if c.lower() in table_cols]
    return df[keep]

# --- prices schema adapters -------------------------------------------------
def _prices_has_dt(con) -> bool: return "dt" in _table_cols(con, "prices")
def _prices_has_ts(con) -> bool: return "ts" in _table_cols(con, "prices")

def _prices_date_predicate(con) -> str:
    if _prices_has_dt(con): return "dt = ?"
    if _prices_has_ts(con): return "DATE(ts) = ?"
    return "dt = ?"

def _prices_date_expr(con) -> str:
    if _prices_has_dt(con): return "dt"
    if _prices_has_ts(con): return "DATE(ts)"
    return "dt"

def _prices_price_col(con) -> str:
    cols = _table_cols(con, "prices")
    for name in ["close", "price", "settle", "last", "value", "px", "c"]:
        if name in cols: return name
    return "close"

# --- predictions schema adapters -------------------------------------------
def _preds_cols(con: duckdb.DuckDBPyConnection) -> List[str]:
    return _table_cols(con, "predictions")

def _preds_delete_predicate(con: duckdb.DuckDBPyConnection) -> str:
    cols = set(_preds_cols(con))
    if "as_of" in cols: return "as_of = ?"
    if "asof" in cols: return "asof = ?"
    if "asof_ts" in cols: return "DATE(asof_ts) = ?"
    if "ts" in cols: return "DATE(ts) = ?"
    if "train_end" in cols: return "DATE(train_end) = ?"
    return "as_of = ?"

def _preds_apply_compat_columns(pred_df: pd.DataFrame, con: duckdb.DuckDBPyConnection, as_of: date) -> pd.DataFrame:
    """
    Map our canonical pred_df columns onto whatever the table actually has:
      - as_of -> asof or asof_ts or ts or train_end
      - target_dt -> ts or target_date
      - yhat -> yhat_lower/yhat_upper (copy yhat if bands exist)
      - fill created_at/model if present on table but missing on df
    Then trim to intersection of table columns.
    """
    tbl_cols = set(_preds_cols(con))
    df = pred_df.copy()

    # as_of variants
    if "as_of" not in tbl_cols and "asof" in tbl_cols and "asof" not in df.columns:
        df["asof"] = df["as_of"]
    if "asof_ts" in tbl_cols and "asof_ts" not in df.columns:
        df["asof_ts"] = pd.Timestamp(as_of)
    if "train_end" in tbl_cols and "train_end" not in df.columns:
        df["train_end"] = pd.Timestamp(as_of).date()
    if "ts" in tbl_cols and "ts" not in df.columns:
        # 'ts' sometimes represents the target timestamp; we'll also set this below for target_dt
        pass

    # target date variants
    if "ts" in tbl_cols and "ts" not in df.columns:
        df["ts"] = pd.to_datetime(df["target_dt"])
    if "target_date" in tbl_cols and "target_date" not in df.columns and "target_dt" in df.columns:
        df["target_date"] = df["target_dt"]

    # bands vs point forecast
    if "yhat" not in tbl_cols:
        # Copy yhat into bands if those exist
        if "yhat_lower" in tbl_cols and "yhat_lower" not in df.columns:
            df["yhat_lower"] = df["yhat"]
        if "yhat_upper" in tbl_cols and "yhat_upper" not in df.columns:
            df["yhat_upper"] = df["yhat"]

    # created_at/model defaults
    if "created_at" in tbl_cols and "created_at" not in df.columns:
        df["created_at"] = pd.Timestamp.now()
    if "model" in tbl_cols and "model" not in df.columns:
        df["model"] = "naive-close-hold"

    # Final trim
    keep = [c for c in df.columns if c.lower() in tbl_cols]
    return df[keep]

# --- Stage 1: Ingestion (prices) -------------------------------------------
def _make_prices_ingest_df(as_of: date, symbols: List[str], use_ts: bool, price_col: str) -> pd.DataFrame:
    rows = []
    if use_ts:
        ts_val = datetime.combine(as_of, time.min)
        for s in symbols:
            rows.append((s, ts_val, 100.0, "mock", datetime.now()))
        df = pd.DataFrame(rows, columns=["symbol", "ts", price_col, "source", "ingested_at"])
    else:
        for s in symbols:
            rows.append((s, as_of, 100.0, "mock", datetime.now()))
        df = pd.DataFrame(rows, columns=["symbol", "dt", price_col, "source", "ingested_at"])
    return df

def ingest_prices(con: duckdb.DuckDBPyConnection, as_of: date, limit: Optional[int] = None) -> StageResult:
    try:
        symbols = CONFIG["market"]["symbols"]
        if limit is not None: symbols = symbols[:limit]

        use_ts = _prices_has_ts(con) and not _prices_has_dt(con)
        price_col = _prices_price_col(con)
        df = _make_prices_ingest_df(as_of, symbols, use_ts=use_ts, price_col=price_col)

        con.execute("BEGIN")
        con.execute(f"DELETE FROM prices WHERE {_prices_date_predicate(con)}", [as_of])
        con.register("prices_df_raw", df)

        tbl_cols = _existing_cols(con, "prices")
        df_trim = _df_trim_to_table(df, tbl_cols)
        con.register("prices_df", df_trim)

        cols_csv = ", ".join(df_trim.columns)
        con.execute(f"INSERT INTO prices ({cols_csv}) SELECT {cols_csv} FROM prices_df")
        con.execute("COMMIT")

        cnt = con.execute(f"SELECT COUNT(*) FROM prices WHERE {_prices_date_predicate(con)}", [as_of]).fetchone()[0]
        return StageResult(ok=True, info={"prices": int(cnt), "symbols": len(symbols), "schema_cols": list(tbl_cols)})
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

# --- Stage 3: Baseline models (schema-aware) --------------------------------
def run_baseline_models(con: duckdb.DuckDBPyConnection, as_of: date, horizon_days: int = 5) -> StageResult:
    try:
        price_col = _prices_price_col(con)
        date_expr = _prices_date_expr(con)
        where_pred = _prices_date_predicate(con)

        prices_df = con.execute(
            f"SELECT symbol, {date_expr} AS dt, {price_col} AS close FROM prices WHERE {where_pred} ORDER BY symbol",
            [as_of],
        ).fetchdf()

        if prices_df.empty:
            return StageResult(ok=False, info={}, error="No prices for as_of date")

        preds = []
        for _, row in prices_df.iterrows():
            sym = row["symbol"]; last_close = float(row["close"])
            for h in range(1, horizon_days + 1):
                target = pd.Timestamp(as_of) + pd.Timedelta(days=h)
                preds.append((sym, as_of, h, target.date(), last_close, "naive-close-hold", datetime.now()))

        pred_df = pd.DataFrame(
            preds, columns=["symbol", "as_of", "horizon", "target_dt", "yhat", "model", "created_at"]
        )

        # Map to existing predictions schema & trim
        pred_df2 = _preds_apply_compat_columns(pred_df, con, as_of)

        con.execute("BEGIN")
        con.execute(f"DELETE FROM predictions WHERE {_preds_delete_predicate(con)}", [as_of])
        con.register("pred_df2", pred_df2)
        cols_csv = ", ".join(pred_df2.columns)
        con.execute(f"INSERT INTO predictions ({cols_csv}) SELECT {cols_csv} FROM pred_df2")
        con.execute("COMMIT")

        cnt = con.execute(f"SELECT COUNT(*) FROM predictions WHERE {_preds_delete_predicate(con)}", [as_of]).fetchone()[0]
        return StageResult(ok=True, info={
            "predictions": int(cnt),
            "model": "naive-close-hold",
            "pred_cols": list(pred_df2.columns)
        })
    except Exception as e:
        con.execute("ROLLBACK")
        return StageResult(ok=False, info={}, error=f"{type(e).__name__}: {e}")

# --- Report writer ----------------------------------------------------------
def write_report(success: bool, as_of: date, stage_infos: Dict[str, Dict[str, Any]], err: Optional[str]) -> Path:
    ts = _now_ts()
    kind = "ok" if success else "error"
    path = _reports_dir() / f"{ts}_{kind}.md"
    lines = [
        f"# Kolmo Daily Run — {as_of.isoformat()}",
        "",
        f"- Run timestamp: `{ts}`",
        f"- Status: **{'SUCCESS' if success else 'ERROR'}**",
        "",
        "## Stages",
    ]
    for stage, info in stage_infos.items():
        lines.append(f"### {stage}")
        for k, v in info.items():
            lines.append(f"- {k}: {v}")
        lines.append("")
    if err:
        lines += ["## Error", "", "```\n" + err + "\n```", ""]
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
        if not s1.ok and not force: raise RuntimeError(f"Ingestion(prices) failed: {s1.error}")

        s2 = ingest_news(con, as_of=as_of)
        stage_infos["ingestion.news"] = ({"ok": s2.ok} | s2.info | ({"error": s2.error} if s2.error else {}))
        if not s2.ok and not force: raise RuntimeError(f"Ingestion(news) failed: {s2.error}")

        s3 = run_baseline_models(con, as_of=as_of, horizon_days=5)
        stage_infos["models.baseline"] = ({"ok": s3.ok} | s3.info | ({"error": s3.error} if s3.error else {}))
        if not s3.ok and not force: raise RuntimeError(f"Models(baseline) failed: {s3.error}")

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
    p = argparse.ArgumentParser(description="Run the Kolmo daily pipeline.")
    p.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (defaults to today)")
    p.add_argument("--force", action="store_true", help="Continue even if a stage fails")
    p.add_argument("--limit", type=int, default=None, help="Limit number of symbols for quick runs")
    a = p.parse_args(argv)
    return (date.fromisoformat(a.date) if a.date else date.today(), a.force, a.limit)

def main():
    as_of, force, limit = _parse_args(sys.argv[1:])
    report_path = run(as_of=as_of, force=force, limit_symbols=limit)
    print(f"[run_daily] report -> {report_path}")

if __name__ == "__main__":
    main()
