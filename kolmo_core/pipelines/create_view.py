# kolmo_core/pipelines/create_views.py
from kolmo_core.utils.prices import ensure_views
from kolmo_core.config.config import CONFIG

if __name__ == "__main__":
    ensure_views(CONFIG["storage"]["db_url"].replace("duckdb:///", ""))
    print("Views created: prices_latest, predictions_latest ✔")
