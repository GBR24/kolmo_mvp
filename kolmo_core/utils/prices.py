# kolmo_core/utils/prices.py
from __future__ import annotations
import duckdb
import pandas as pd
from typing import Sequence, Optional
from kolmo_core.config.config import CONFIG
import logging

def _db_path() -> str:
    """Return filesystem path from CONFIG['storage']['db_url']."""
    return CONFIG["storage"]["db_url"].replace("duckdb:///", "")

# ---------- Prices ----------
def load_prices_long(db_path: Optional[str] = None,
                     symbols: Optional[Sequence[str]] = None) -> pd.DataFrame:
    db = db_path or _db_path()
    con = duckdb.connect(db)
    if symbols:
        q = f"""
        SELECT date, symbol, price
        FROM prices
        WHERE symbol IN ({",".join(repr(s) for s in symbols)})
        ORDER BY date, symbol
        """
    else:
        q = "SELECT date, symbol, price FROM prices ORDER BY date, symbol"
    df = con.execute(q).fetchdf()
    return df

def load_prices_wide(db_path: Optional[str] = None,
                     symbols: Optional[Sequence[str]] = None) -> pd.DataFrame:
    df = load_prices_long(db_path, symbols)
    wide = df.pivot(index="date", columns="symbol", values="price").sort_index()
    wide.index = pd.to_datetime(wide.index)
    return wide

def latest_snapshot(db_path: Optional[str] = None,
                    symbols: Optional[Sequence[str]] = None) -> pd.DataFrame:
    db = db_path or _db_path()
    con = duckdb.connect(db)
    filt = ""
    if symbols:
        filt = "WHERE symbol IN ({})".format(",".join(repr(s) for s in symbols))
    q = f"""
    WITH latest AS (
      SELECT symbol, MAX(date) AS max_date
      FROM prices
      {filt}
      GROUP BY symbol
    )
    SELECT p.symbol, p.date, p.price
    FROM prices p
    JOIN latest l ON p.symbol = l.symbol AND p.date = l.max_date
    ORDER BY p.symbol
    """
    return con.execute(q).fetchdf()

# ---------- Predictions ----------
def get_predictions(db_path: Optional[str] = None,
                    symbols: Optional[Sequence[str]] = None,
                    method: Optional[str] = None,
                    latest_only: bool = False) -> pd.DataFrame:
    """
    Read predictions from DuckDB.
    - latest_only=True returns only the latest date per symbol/method.
    - method can be 'EWMA_20', 'AR1_ret', etc.
    """
    db = db_path or _db_path()
    con = duckdb.connect(db)

    conds = []
    if symbols:
        conds.append("symbol IN ({})".format(",".join(repr(s) for s in symbols)))
    if method:
        conds.append(f"method = {repr(method)}")
    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    if latest_only:
        q = f"""
        WITH latest AS (
          SELECT symbol, method, MAX(date) AS max_date
          FROM predictions
          {where.replace('WHERE','WHERE' if where else '')}
          GROUP BY 1,2
        )
        SELECT p.*
        FROM predictions p
        JOIN latest l
          ON p.symbol = l.symbol
         AND p.method = l.method
         AND p.date = l.max_date
        {where if not where else ''}
        ORDER BY p.symbol, p.method
        """
    else:
        q = f"""
        SELECT *
        FROM predictions
        {where}
        ORDER BY date, symbol, method
        """
    return con.execute(q).fetchdf()

# ---------- Views & Joined convenience ----------
def ensure_views(db_path: Optional[str] = None) -> None:
    """Create/replace helpful views for dashboards."""
    db = db_path or _db_path()
    con = duckdb.connect(db)

    con.execute("""
    CREATE OR REPLACE VIEW prices_latest AS
    WITH latest AS (
      SELECT symbol, MAX(date) AS max_date
      FROM prices
      GROUP BY symbol
    )
    SELECT p.*
    FROM prices p
    JOIN latest l ON p.symbol = l.symbol AND p.date = l.max_date;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW predictions_latest AS
    WITH latest AS (
      SELECT symbol, method, MAX(date) AS max_date
      FROM predictions
      GROUP BY symbol, method
    )
    SELECT p.*
    FROM predictions p
    JOIN latest l
      ON p.symbol = l.symbol
     AND p.method = l.method
     AND p.date = l.max_date;
    """)

def latest_price_vs_prediction(db_path: Optional[str] = None) -> pd.DataFrame:
    """
    Returns one row per (symbol, method) with the latest price and latest prediction.
    Columns: symbol, method, last_date, last_price, pred_date, y_hat
    """
    db = db_path or _db_path()
    con = duckdb.connect(db)
    ensure_views(db)  # make sure views exist
    q = """
    SELECT
      s.symbol,
      pr.method,
      p.date   AS last_date,
      p.price  AS last_price,
      pr.date  AS pred_date,
      pr.y_hat
    FROM (SELECT DISTINCT symbol FROM prices) s
    LEFT JOIN prices_latest       p  USING(symbol)
    LEFT JOIN predictions_latest  pr USING(symbol)
    ORDER BY s.symbol, pr.method;
    """
    return con.execute(q).fetchdf()
