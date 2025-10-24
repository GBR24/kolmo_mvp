from __future__ import annotations
from typing import Any, Dict
from .types import AgentResult
from .blackboard import Blackboard

def run(args: Dict[str, Any], tools: Dict[str, Any], bb: Blackboard) -> AgentResult:
    try:
        symbol: str = args["symbol"]
        horizon: int = int(args.get("horizon", 5))
        res = tools["run_forecast"](symbol, horizon, args.get("model_hint"))
        bb.put("forecast", res)
        return AgentResult(ok=True, payload=res, confidence=0.7)
    except Exception as e:
        return AgentResult(ok=False, error=str(e), confidence=0.2)
