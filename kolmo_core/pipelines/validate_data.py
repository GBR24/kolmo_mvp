# kolmo_core/pipelines/validate_data.py
from __future__ import annotations
from kolmo_core.utils.quality import check_prices, check_predictions
from kolmo_core.config.config import CONFIG

if __name__ == "__main__":
    db = CONFIG["storage"]["db_url"].replace("duckdb:///", "")
    print("Validating DuckDB:", db)
    stats_prices = check_prices(db, min_days=60)
    print("\n[OK] prices summary:\n", stats_prices)

    stats_preds = check_predictions(db)
    print("\n[OK] predictions summary:\n", stats_preds)
    print("\nValidation PASSED ✔")