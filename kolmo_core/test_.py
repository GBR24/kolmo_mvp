from kolmo_core.utils.prices import (
    load_prices_wide, latest_snapshot, get_predictions,
    ensure_views, latest_price_vs_prediction
)

# Wide matrix for models
print(load_prices_wide().tail())

# Latest actual prices
print(latest_snapshot())

# All predictions or latest only
print(get_predictions(latest_only=True))          # latest per symbol/method
print(get_predictions(method="EWMA_20"))          # all EWMA_20 rows

# Create views and join latest actual vs prediction (for dashboard)
ensure_views()
print(latest_price_vs_prediction())