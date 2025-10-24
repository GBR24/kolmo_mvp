from __future__ import annotations
from typing import Any, Dict, List
from .types import AgentResult
from .blackboard import Blackboard

def run(args: Dict[str, Any], tools: Dict[str, Any], bb: Blackboard) -> AgentResult:
    try:
        positions: List[Dict[str, float]] = args.get("positions", [])
        horizon: int = int(args.get("horizon", 1))
        out = tools["compute_var"](positions, horizon_days=horizon)
        bb.put("risk", out)
        return AgentResult(ok=True, payload=out, confidence=0.65)
    except Exception as e:
        return AgentResult(ok=False, error=str(e), confidence=0.2)
