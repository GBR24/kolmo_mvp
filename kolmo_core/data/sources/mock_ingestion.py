# kolmo_core/data/sources/mock_ingestion.py
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import duckdb

# Try to import CONFIG, but don't crash if it isn't available
try:
    from kolmo_core.config.config import CONFIG
except Exception:
    CONFIG = {}

TABLE = "prices"

def get_project_root() -> Path:
    # .../database/kolmo_core/data/sources/mock_ingestion.py -> parents[3] == repo root (database/)
    return Path(__file__).resolve().parents[3]

def resolve_csv_path(cli_csv: str | None) -> Path:
    root = get_project_root()
    default_csv = root / "kolmo_core" / "data" / "mock" / "kolmo_mock_prices.csv"

    # 1) CLI takes precedence
    candidate = Path(cli_csv) if cli_csv else None

    # 2) Then CONFIG
    if candidate is None:
        cfg_csv = (CONFIG.get("ingestion", {}) or {}).get("mock_csv")
        candidate = Path(cfg_csv) if cfg_csv else None

    # 3) Else default
    if candidate is None:
        candidate = default_csv

    # If relative, make it project-relative
    if not candidate.is_absolute():
        candidate = (root / candidate).resolve()

    # If it doesn't exist and looks like someone left /mnt, fall back to default
    if not candidate.exists():
        print(f"[mock_ingestion] WARN: CSV not found at '{candidate}'. Falling back to default '{default_csv}'.")
        candidate = default_csv

    if not candidate.exists():
        raise FileNotFoundError(f"[mock_ingestion] ERROR: Mock CSV not found at '{candidate}'. "
                                f"Expected a file at {default_csv} or pass --csv <path>.")

    return candidate

def resolve_db_path(cli_db: str | None) -> Path:
    root = get_project_root()
    default_db = root / "kolmo_core" / "data" / "kolmo.duckdb"
    # CONFIG takes precedence, then CLI, then default
    cfg_url = (CONFIG.get("storage", {}) or {}).get("db_url")
    if cfg_url and cfg_url.startswith("duckdb:///"):
        candidate = Path(cfg_url.replace("duckdb:///", ""))
    elif cli_db:
        candidate = Path(cli_db)
    else:
        candidate = default_db

    if not candidate.is_absolute():
        candidate = (root / candidate).resolve()

    candidate.parent.mkdir(parents=True, exist_ok=True)
    return candidate

def load_and_normalize(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # Flexible column mapping
    rename_map = {"ticker": "symbol", "date": "ts", "value": "price"}
    for k, v in rename_map.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    required = {"symbol", "ts", "price"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[mock_ingestion] Missing required columns {missing} in {csv_path}. "
                         f"Expected columns: {sorted(required)} (extras allowed).")

    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["symbol", "ts", "price"])

    # Add optional metadata if absent
    for col, val in {"unit": "USD/bbl", "source": "mock", "frequency": "daily"}.items():
        if col not in df.columns:
            df[col] = val

    return df

def main():
    ap = argparse.ArgumentParser(description="Kolmo mock ingestion")
    ap.add_argument("--csv", type=str, default=None, help="CSV path (columns: symbol, ts, price) - CLI overrides CONFIG")
    ap.add_argument("--db", type=str, default=None, help="DuckDB file path (default: kolmo_core/data/kolmo.duckdb)")
    ap.add_argument("--table", type=str, default=TABLE, help="Target table name (default: prices)")
    args = ap.parse_args()

    csv_path = resolve_csv_path(args.csv)
    db_path = resolve_db_path(args.db)
    table = args.table

    print(f"[mock_ingestion] INFO: Using CSV: {csv_path}")
    print(f"[mock_ingestion] INFO: Using DB : {db_path} :: {table}")

    df = load_and_normalize(csv_path)

    con = duckdb.connect(str(db_path))
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
          ts TIMESTAMP,
          symbol TEXT,
          price DOUBLE,
          unit TEXT,
          source TEXT,
          frequency TEXT
        );
    """)
    con.register("df_src", df)
    con.execute(f"""
        DELETE FROM {table}
        USING df_src s
        WHERE {table}.ts = s.ts AND {table}.symbol = s.symbol;
    """)
    con.execute(f"INSERT INTO {table} SELECT ts, symbol, price, unit, source, frequency FROM df_src;")
    con.close()

    print(f"[mock_ingestion] ✅ Ingested {len(df)} rows -> {db_path}::{table}")

if __name__ == "__main__":
    main()
