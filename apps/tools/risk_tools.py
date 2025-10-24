from __future__ import annotations
import math
from typing import Dict, Any, List

def compute_var(positions: List[Dict[str, float]], horizon_days: int = 1, method: str = "norm") -> Dict[str, Any]:
    # Super-simple placeholder: sum positions and apply toy volatility.
    # Replace with your GBM/historical VaR later.
    gross = sum(abs(p.get("position", 0.0)) for p in positions)
    toy_vol = 0.02 * math.sqrt(horizon_days)  # 2% daily vol placeholder
    var_95 = 1.65 * toy_vol * gross
    return {"var": var_95, "assumptions": {"vol": toy_vol, "cl": 0.95, "method": method}}
