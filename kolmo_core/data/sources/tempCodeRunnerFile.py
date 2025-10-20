# scripts/make_mock_prices.py
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ---- Paths (repo-relative) ----
CSV_PATH = "kolmo_core/data/mock/kolmo_mock_prices.csv"
Path(os.path.dirname(CSV_PATH)).mkdir(parents=True, exist_ok=True)

# ---- Calendar ----
end_date = datetime.today().date()
start_date = end_date - timedelta(days=180)   # ~6 months of biz days
dates = pd.bdate_range(start=start_date, end=end_date, freq="C")

# ---- Symbols (unit, rough drift, noise) ----
SYMS = {
    # refined products
    "RBOB":  {"base": 2.40, "trend": 0.0008, "vol": 0.020, "unit": "USD/gal"},
    "HO":    {"base": 2.70, "trend": 0.0006, "vol": 0.022, "unit": "USD/gal"},
    "JET":   {"base": 2.55, "trend": 0.0007, "vol": 0.018, "unit": "USD/gal"},
    # crude benchmarks
    "BRENT": {"base": 84.0, "trend": 0.0005, "vol": 0.90,  "unit": "USD/bbl"},
    "WTI":   {"base": 80.0, "trend": 0.0005, "vol": 0.95,  "unit": "USD/bbl"},
    # gas
    "NG":    {"base": 2.80, "trend": 0.0004, "vol": 0.10,  "unit": "USD/MMBtu"},
}

rng = np.random.default_rng(42)
rows = []

for sym, p in SYMS.items():
    price = p["base"]
    for d in dates:
        # simple AR(1)-ish drift + noise; keep > 0
        price = max(0.01, price * (1 + p["trend"]) + rng.normal(0, p["vol"]))
        rows.append({
            "date": d.date().isoformat(),
            "symbol": sym,
            "price": round(float(price), 4),
            "unit": p["unit"],
            "source": "MOCK",
            "frequency": "daily",
        })

df = pd.DataFrame(rows).sort_values(["date","symbol"]).reset_index(drop=True)
df.to_csv(CSV_PATH, index=False)
print(f"Saved mock prices -> {CSV_PATH} ({len(df)} rows)")