from __future__ import annotations
import os
from pathlib import Path
import duckdb
import pandas as pd
from typing import List, Dict, Any, Optional

def _resolve_db_path() -> str:
    db_url = os.getenv("DB_URL", "duckdb:///kolmo_core/data/kolmo.duckdb")
    return str(Path(db_url.replace("duckdb:///", "") if db_url.startswith("duckdb:///") else db_url).resolve())

def _table_cols(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    return con.execute(f"PRAGMA table_info({table})").df()  # columns: cid, name, type, notnull, dflt_value, pk

def _resolve_time_col(cols_df: pd.DataFrame) -> str:
    candidates = ["ts", "dt", "date", "timestamp"]
    names = set(cols_df["name"].tolist())
    for c in candidates:
        if c in names:
            return c
    return cols_df["name"].iloc[0]

def _resolve_price_col(cols_df: pd.DataFrame) -> Optional[str]:
    # Prefer common price/settlement names; fallback to first numeric column that isn't symbol/time/source
    pref = ["close", "settle", "price", "last", "px_close", "adj_close", "value"]
    names = [c for c in cols_df["name"].tolist()]
    for p in pref:
        if p in names:
            return p
    # Fallback: choose first numeric-ish column
    for _, row in cols_df.iterrows():
        n = row["name"].lower()
        t = (row["type"] or "").upper()
        if n in {"symbol", "source"}:
            continue
        if any(x in n for x in ["ts", "dt", "date", "time"]):
            continue
        if any(k in t for k in ["INT", "DEC", "DOUB", "REAL", "NUM"]):
            return row["name"]
    return None

def query_db(symbols: List[str], table: str, start: Optional[str], end: Optional[str], fields: List[str]) -> pd.DataFrame:
    con = duckdb.connect(_resolve_db_path(), read_only=True)
    cols_df = _table_cols(con, table)
    tcol = _resolve_time_col(cols_df)
    pcol = _resolve_price_col(cols_df)

    # Map requested fields; if user asked for 'dt' or 'close', alias them
    mapped_fields: List[str] = []
    for f in (fields or ["*"]):
        if f == "dt":
            mapped_fields.append(f"{tcol} AS dt")
        elif f == "close":
            if pcol:
                mapped_fields.append(f"{pcol} AS close")
            else:
                # If no numeric price-like col, just skip; forecast layer will see no 'close'
                continue
        else:
            mapped_fields.append(f)
    fcols = ", ".join(mapped_fields) if mapped_fields else "*"

    where = []
    params: Dict[str, Any] = {}
    if symbols:
        where.append("symbol = ANY($symbols)")
        params["symbols"] = symbols
    if start:
        where.append(f"{tcol} >= $start")
        params["start"] = start
    if end:
        where.append(f"{tcol} <= $end")
        params["end"] = end

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = f"SELECT {fcols} FROM {table}{where_sql} ORDER BY {tcol}"
    df = con.execute(sql, params).df()
    con.close()
    return df

def list_symbols(table: str = "prices") -> List[str]:
    con = duckdb.connect(_resolve_db_path(), read_only=True)
    df = con.execute(f"SELECT DISTINCT symbol FROM {table} ORDER BY symbol").df()
    con.close()
    return df["symbol"].tolist()
