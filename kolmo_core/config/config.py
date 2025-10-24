# kolmo_core/config/config.py
import os
from pathlib import Path
from dotenv import load_dotenv
from kolmo_core.config.path_utils import as_project_relative

# ---------------------------------------------------------------------
# Resolve repo root no matter where code is run from
# ---------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

# ---------------------------------------------------------------------
# Base configuration dictionary
# ---------------------------------------------------------------------
CONFIG = {
    "storage": {
        # Use relative path (GitHub-friendly)
        "db_url": "duckdb:///kolmo_core/data/kolmo.duckdb",
    },
    "ingestion": {
        "use_mock": True,  # switch off later when EIA is stable
        "mock_csv": "kolmo_core/data/mock/kolmo_mock_prices.csv",
    },
    "market": {
        # EIA series IDs (daily) for crude, products, and gas.
        "symbols": {
            "BRN": {"name": "Brent Spot Price", "provider": "eia", "id": "PET.RBRTE.D", "asset": "crude"},
            "WTI": {"name": "WTI Spot Price", "provider": "eia", "id": "PET.RWTC.D", "asset": "crude"},
            "NG":  {"name": "Henry Hub Natural Gas", "provider": "eia", "id": "NG.RNGWHHD.D", "asset": "natgas"},
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

# ---------------------------------------------------------------------
# Normalize paths to relative (never absolute)
# ---------------------------------------------------------------------
CONFIG["ingestion"]["mock_csv"] = as_project_relative(
    CONFIG["ingestion"].get("mock_csv"),
    "kolmo_core/data/mock/kolmo_mock_prices.csv"
)

_db_rel = CONFIG["storage"].get("db_url", "").replace("duckdb:///","")
_db_rel = as_project_relative(_db_rel, "kolmo_core/data/kolmo.duckdb")
CONFIG["storage"]["db_url"] = f"duckdb:///{_db_rel}"

# ---------------------------------------------------------------------
# Optional: log config on import for sanity
# ---------------------------------------------------------------------
if __name__ == "__main__":
    print("mock_csv =", CONFIG["ingestion"]["mock_csv"])
    print("db_url   =", CONFIG["storage"]["db_url"])
