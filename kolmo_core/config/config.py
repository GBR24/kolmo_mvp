# kolmo_core/config/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Resolve repo root no matter where code is run from
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

CONFIG = {
    "db": {
        # Use a plain filesystem path (NOT "duckdb:///...").
        "url": os.getenv("DB_URL", str(ROOT / "kolmo_core" / "data" / "kolmo.duckdb")),
    },
    "market": {
        # EIA series IDs (daily) for crude, products, and gas.
        # Sources: EIA API (free). You must have EIA_API_KEY in .env
        "symbols": {
            "BRN":  {"name": "Brent Spot Price",       "provider": "eia", "id": "PET.RBRTE.D",                                   "asset": "crude"},
            "WTI":  {"name": "WTI Spot Price",         "provider": "eia", "id": "PET.RWTC.D",                                    "asset": "crude"},
            "RBOB": {"name": "Gasoline (RBOB proxy)",  "provider": "eia", "id": "PET.EER_EPMRR_PF4_Y35NY_DPG.D",                 "asset": "gasoline"},
            "HO":   {"name": "Heating Oil (No.2)",     "provider": "eia", "id": "PET.EER_EPD2D_PF4_Y35NY_DPG.D",                 "asset": "gasoil"},
            "NG":   {"name": "Henry Hub Natural Gas",  "provider": "eia", "id": "NG.RNGWHHD.D",                                  "asset": "natgas"},
            "JET":  {"name": "Kerosene-Type Jet Fuel", "provider": "eia", "id": "PET.EER_EPDJ_PF4_Y35NY_DPG.D",                  "asset": "jet"},
        }
    },
    "news": {
        "default_queries": [
            "oil", "brent", "wti", "gasoline", "diesel", "gasoil",
            "natural gas", "opec", "refinery", "jet fuel"
        ],
        "max_per_query": 25,
    }
}
