from __future__ import annotations
from typing import Any, Dict, List
from .types import AgentResult
from .blackboard import Blackboard

def run(args: Dict[str, Any], tools: Dict[str, Any], bb: Blackboard) -> AgentResult:
    try:
        symbols: List[str] = args.get("symbols", [])
        days: int = int(args.get("days", 3))
        items = tools["summarize_news"](symbols, days)
        for it in items:
            bb.add_citation(it.get("title",""), it.get("link",""), it.get("source",""))
        bb.put("news", items)
        conf = 0.6 if not items else 0.8
        return AgentResult(ok=True, payload={"count": len(items)}, confidence=conf, used_tables=["news"], time_window=f"last_{days}_days")
    except Exception as e:
        return AgentResult(ok=False, error=str(e), confidence=0.2)
