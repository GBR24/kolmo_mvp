# kolmo_core/utils/quality.py
from __future__ import annotations
import duckdb
import pandas as pd
from typing import Sequence, Optional
from kolmo_core.config.config import CONFIG

def _db() -> str:
    return CONFIG["storage"]["db_url"].replace("duckdb:///", "")

def _fail_if(cond: bool, msg: str):
    if cond: 
        raise AssertionError(msg)

def check_prices(db_path: Optional[str] = None, min_days: int = 60, symbols: Optional[Sequence[str]] = None) -> pd.DataFrame:
    db = db_path or _db()
    con = duckdb.connect(db)

    sym_filter = f"WHERE symbol IN ({','.join(repr(s) for s in symbols)})" if symbols else ""

    # Basic stats per symbol
    stats = con.execute(f"""
      SELECT symbol, COUNT(*) AS n_rows, MIN(date) AS min_d, MAX(date) AS max_d
      FROM prices {sym_filter}
      GROUP BY 1 ORDER BY 1
    """).fetchdf()

    # Nulls?
    nulls = con.execute(f"""
      SELECT COUNT(*) AS nulls
      FROM prices {sym_filter}
      WHERE date IS NULL OR symbol IS NULL OR price IS NULL
    """).fetchone()[0]
    _fail_if(nulls > 0, f"[prices] Found {nulls} NULLs in (date/symbol/price)")

    # Duplicates (date, symbol)
    dups = con.execute(f"""
      SELECT COUNT(*) FROM (
        SELECT date, symbol, COUNT(*) c
        FROM prices {sym_filter}
        GROUP BY 1,2 HAVING COUNT(*) > 1
      )
    """).fetchone()[0]
    _fail_if(dups > 0, f"[prices] Found {dups} duplicate (date,symbol) keys")

    # Gaps or not enough days
    too_short = stats[stats["n_rows"] < min_days]
    _fail_if(len(too_short) > 0, f"[prices] Too few rows for: {too_short['symbol'].tolist()} (<{min_days} days)")

    return stats

def check_predictions(db_path: Optional[str] = None, symbols: Optional[Sequence[str]] = None) -> pd.DataFrame:
    db = db_path or _db()
    con = duckdb.connect(db)

    # optional symbol filter
    where_clause = ""
    if symbols:
        sym_list = ",".join(repr(s) for s in symbols)
        where_clause = f"WHERE symbol IN ({sym_list})"

    # Nulls?
    nulls = con.execute(f"""
      SELECT COUNT(*) AS nulls
      FROM predictions {where_clause}
      WHERE date IS NULL OR symbol IS NULL OR y_hat IS NULL OR method IS NULL
    """).fetchone()[0]
    _fail_if(nulls > 0, f"[predictions] Found {nulls} NULLs in required columns")

    # Duplicates (date, symbol, method)
    dups = con.execute(f"""
      SELECT COUNT(*) FROM (
        SELECT date, symbol, method, COUNT(*) c
        FROM predictions {where_clause}
        GROUP BY 1,2,3 HAVING COUNT(*) > 1
      )
    """).fetchone()[0]
    _fail_if(dups > 0, f"[predictions] Found {dups} duplicate (date,symbol,method) keys")

    # Prediction date should be >= latest price date
    sym_filter = f"WHERE pr.symbol IN ({sym_list})" if symbols else ""
    misaligned = con.execute(f"""
      WITH last_price AS (
        SELECT symbol, MAX(date) AS maxp FROM prices GROUP BY symbol
      )
      SELECT COUNT(*) FROM predictions pr
      JOIN last_price lp USING(symbol)
      {sym_filter}
      AND pr.date < lp.maxp
    """).fetchone()[0]
    _fail_if(misaligned > 0, f"[predictions] {misaligned} rows predict before latest price date")

    # Summary table
    summary = con.execute(f"""
      SELECT symbol, method, COUNT(*) AS n_rows, MIN(date) AS min_d, MAX(date) AS max_d
      FROM predictions {where_clause}
      GROUP BY 1,2 ORDER BY 1,2
    """).fetchdf()
    return summary
