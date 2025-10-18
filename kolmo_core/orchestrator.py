from datetime import datetime
from typing import Any, Dict

def run_pipeline(user_query: str) -> Dict[str, Any]:
    # Day-3/4: wire to mcp_server tools
    return {
        "as_of": datetime.utcnow().isoformat()+"Z",
        "echo": user_query,
        "prices": [],
        "forecast": {},
        "insights": {}
    }