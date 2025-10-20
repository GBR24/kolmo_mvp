import os
from pathlib import Path
from dotenv import load_dotenv

# Resolve repo root no matter where code is run from
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

CONFIG = {
    # -------------------------------------------------------------------------
    # DATABASE
    # -------------------------------------------------------------------------
    "storage": {
        "db_url": "duckdb:///kolmo_core/data/kolmo.duckdb",
    },
    "db": {
        # Use a plain filesystem path (NOT "duckdb:///...") if needed elsewhere
        "url": os.getenv("DB_URL", str(ROOT / "kolmo_core" / "data" / "kolmo.duckdb")),
    },

    # -------------------------------------------------------------------------
    # INGESTION
    # -------------------------------------------------------------------------
    "ingestion": {
        "use_mock": True,  # switch off later when EIA is stable
        "mock_csv": "kolmo_core/data/mock/kolmo_mock_prices.csv",
    },

    # -------------------------------------------------------------------------
    # MARKET DATA
    # -------------------------------------------------------------------------
    "market": {
        "symbols": {
            "BRENT": {"name": "Brent Spot Price",      "provider": "mock", "asset": "crude"},
            "WTI":   {"name": "WTI Spot Price",        "provider": "mock", "asset": "crude"},
            "NG":    {"name": "Henry Hub Natural Gas", "provider": "mock", "asset": "natgas"},
            "RBOB":  {"name": "Gasoline RBOB",         "provider": "mock", "asset": "gasoline"},
            "HO":    {"name": "Heating Oil",           "provider": "mock", "asset": "gasoil"},
            "JET":   {"name": "Jet Fuel",              "provider": "mock", "asset": "jet"},
        }
    },

    # -------------------------------------------------------------------------
    # NEWS (still functional with API)
    # -------------------------------------------------------------------------
    "news": {
        "default_queries": [
            "oil", "brent", "wti", "gasoline", "diesel", "gasoil",
            "natural gas", "opec", "refinery", "jet fuel"
        ],
        "max_per_query": 25,
    }
}
