from __future__ import annotations
from typing import Any, Dict, List
from .types import AgentResult
from .blackboard import Blackboard

def run(args: Dict[str, Any], tools: Dict[str, Any], bb: Blackboard) -> AgentResult:
    try:
        symbols: List[str] = args.get("symbols", [])
        table: str = args.get("table", "prices")
        start: str | None = args.get("start")
        end: str | None = args.get("end")
        fields: List[str] = args.get("fields", ["dt","symbol","close"])

        df = tools["query_db"](symbols, table, start, end, fields)
        bb.put("history", df)
        return AgentResult(ok=True, payload={"rows": len(df)}, confidence=0.85, used_tables=[table], time_window=f"{start}..{end}")
    except Exception as e:
        return AgentResult(ok=False, error=str(e), confidence=0.2, used_tables=[args.get("table","?")])
