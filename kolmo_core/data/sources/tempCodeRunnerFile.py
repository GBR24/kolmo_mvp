# kolmo_core/ingestion/mock_ingestion.py
import os
from pathlib import Path
import pandas as pd
import duckdb
from kolmo_core.config import CONFIG

TABLE = "prices"

def main():
    csv_path = CONFIG["ingestion"]["mock_csv"]  # e.g. kolmo_core/data/mock/kolmo_mock_prices.csv
    db_url = CONFIG["storage"]["db_url"]        # e.g. duckdb:///kolmo_core/data/kolmo.duckdb
    db_file = db_url.replace("duckdb:///", "")  # -> kolmo_core/data/kolmo.duckdb

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Mock CSV not found: {csv_path}")

    Path(os.path.dirname(db_file)).mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path, parse_dates=["date"])
    df["date"] = df["date"].dt.date

    con = duckdb.connect(db_file)

    # Ensure table schema
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      date DATE,
      symbol TEXT,
      price DOUBLE,
      unit TEXT,
      source TEXT,
      frequency TEXT
    );
    """)

    # Idempotent upsert (delete matching keys then insert)
    con.register("df_src", df)
    con.execute(f"""
    DELETE FROM {TABLE}
    USING df_src s
    WHERE {TABLE}.date = s.date AND {TABLE}.symbol = s.symbol;
    """)
    con.execute(f"""
    INSERT INTO {TABLE}
    SELECT date, symbol, price, unit, source, frequency FROM df_src;
    """)

    print(f"Ingested {len(df)} rows from {csv_path} into {db_file}::{TABLE}")

if __name__ == "__main__":
    main()
