from __future__ import annotations
import duckdb
from pathlib import Path
import os
from typing import List, Dict, Any

def _resolve_db_path() -> str:
    db_url = os.getenv("DB_URL", "duckdb:///kolmo_core/data/kolmo.duckdb")
    return str(Path(db_url.replace("duckdb:///", "") if db_url.startswith("duckdb:///") else db_url).resolve())

def _table_cols(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return con.execute(f"PRAGMA table_info({table})").df()["name"].str.lower().tolist()

def _pick(cols: list[str], candidates: list[str], default: str | None = None) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return default

def summarize_news(symbols: List[str], days: int = 3) -> List[Dict[str, Any]]:
    """
    Returns list of dicts with keys:
      published_at, symbol(optional), title, source, link, driver(summary), sentiment
    Adapts to varying schemas (symbol/ticker/asset; ts/dt/date; url/link; etc).
    """
    con = duckdb.connect(_resolve_db_path(), read_only=True)

    cols = _table_cols(con, "news")
    if not cols:
        con.close()
        return []

    # Resolve column names (lowercased for matching)
    tcol = _pick(cols, ["ts", "dt", "published_at", "timestamp", "date"])
    symcol = _pick(cols, ["symbol", "ticker", "sym", "asset", "underlying", "code"])
    titlecol = _pick(cols, ["title", "headline"])
    sourcecol = _pick(cols, ["source", "publisher", "outlet"])
    linkcol = _pick(cols, ["link", "url"])
    summarycol = _pick(cols, ["summary", "description", "snippet"])
    sentcol = _pick(cols, ["sentiment", "polarity", "sentiment_score", "score"])

    # Build SELECT list with aliases; use NULLs if missing
    select_parts = []
    select_parts.append(f"{tcol} AS published_at" if tcol else "NULL AS published_at")
    select_parts.append(f"{symcol} AS symbol" if symcol else "NULL AS symbol")
    select_parts.append(f"{titlecol} AS title" if titlecol else "NULL AS title")
    select_parts.append(f"{sourcecol} AS source" if sourcecol else "NULL AS source")
    select_parts.append(f"{linkcol} AS link" if linkcol else "NULL AS link")
    select_parts.append(f"{summarycol} AS summary" if summarycol else "NULL AS summary")
    select_parts.append(f"{sentcol} AS sentiment" if sentcol else "NULL AS sentiment")
    select_sql = ", ".join(select_parts)

    # WHERE clause: always time filter (if we have a time column).
    where = []
    params: Dict[str, Any] = {"days": int(days)}
    if tcol:
        where.append(f"{tcol} >= now() - (CAST($days AS INTEGER) * INTERVAL 1 DAY)")
    # Only add symbol filter if a symbol-like column exists
    if symcol and symbols:
        where.append(f"{symcol} = ANY($symbols)")
        params["symbols"] = symbols

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order_sql = f"ORDER BY {tcol} DESC" if tcol else ""

    sql = f"""
      SELECT {select_sql}
      FROM news
      {where_sql}
      {order_sql}
    """

    rows = con.execute(sql, params).fetchall()
    con.close()

    result = []
    for published_at, sym, title, source, link, summary, sentiment in rows:
        result.append({
            "published_at": str(published_at) if published_at is not None else "",
            "symbol": sym if sym is not None else "",
            "title": title if title is not None else "",
            "source": source if source is not None else "",
            "link": link if link is not None else "",
            "driver": summary if summary is not None else "",
            "sentiment": sentiment if sentiment is not None else "neutral",
        })
    return result
