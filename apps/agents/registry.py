from __future__ import annotations
from typing import Dict, Callable, Any

# Tool imports
from apps.tools.db_tools import query_db, list_symbols
from apps.tools.forecast_tools import run_forecast
from apps.tools.news_tools import summarize_news
from apps.tools.risk_tools import compute_var
from apps.tools.report_tools import render_report

TOOL_REGISTRY: Dict[str, Callable[..., Any]] = {
    "query_db": query_db,
    "list_symbols": list_symbols,
    "run_forecast": run_forecast,
    "summarize_news": summarize_news,
    "compute_var": compute_var,
    "render_report": render_report,
}
