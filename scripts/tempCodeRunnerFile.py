# scripts/load_mock_to_duckdb.py
import os
from pathlib import Path
import pandas as pd
import duckdb

# Resolve repo root no matter where you run this from
ROOT = Path(__file__).resolve().parents[1]  # repo root if this file is under scripts/
CSV  = ROOT / "kolmo_core" / "data" / "mock" / "kolmo_mock_prices.csv"
DBF  = ROOT / "kolmo_core" / "data" / "kolmo.duckdb"

print("CSV:", CSV)
print("DB: ", DBF)

if not CSV.exists():
    raise SystemExit(f"CSV not found: {CSV}")

DBF.parent.mkdir(parents=True, exist_ok=True)

# Load CSV
df = pd.read_csv(CSV, parse_dates=["date"])
df["date"] = df["date"].dt.date

con = duckdb.connect(str(DBF))

# Create table if missing
con.execute("""
CREATE TABLE IF NOT EXISTS prices (
  date DATE,
  symbol TEXT,
  price DOUBLE,
  unit TEXT,
  source TEXT,
  frequency TEXT
);
""")

# Idempotent upsert
con.register("df_src", df)
con.execute("""
DELETE FROM prices
USING df_src s
WHERE prices.date = s.date AND prices.symbol = s.symbol;
""")
con.execute("""
INSERT INTO prices
SELECT date, symbol, price, unit, source, frequency FROM df_src;
""")

# Show proof
total = con.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
syms  = con.execute("SELECT DISTINCT symbol FROM prices ORDER BY 1").fetchdf()

print("Rows now in prices:", total)
print(syms)
