from __future__ import annotations
from typing import Any, Dict, List
from .blackboard import Blackboard
from .types import AgentResult
from . import data_retriever as DR
from . import forecaster as FM
from . import news_interpreter as NI
from . import risk_engine as RK
from . import reporter as RP

SUPERVISOR_PROMPT = """You are Kolmo's Supervisor. Given a user request, produce a short ordered plan using the available agents:
- DataRetriever (history from DuckDB)
- Forecaster (predict horizons)
- NewsInterpreter (recent cited bullets)
- RiskEngine (optional)
- Reporter (compile markdown)

Keep plans minimal, deterministic, and only include agents required to answer the question. Always end with Reporter.
"""

def _make_default_plan(user_prompt: str, tools) -> List[Dict[str, Any]]:
    # Simple heuristic planner
    plan: List[Dict[str, Any]] = []
    symbols = []
    try:
        symbols = tools["list_symbols"]()
    except Exception:
        pass

    # Pick first symbol if none specified (MVP)
    chosen = None
    for s in symbols:
        if s in user_prompt:
            chosen = s
            break
    if not chosen and symbols:
        chosen = symbols[0]

    if chosen:
        plan.append({"agent":"DataRetriever", "args":{"symbols":[chosen], "table":"prices", "start":None, "end":None, "fields":["dt","symbol","close"]}})
        plan.append({"agent":"Forecaster", "args":{"symbol":chosen, "horizon":5}})
        plan.append({"agent":"NewsInterpreter", "args":{"symbols":[chosen], "days":3}})
        plan.append({"agent":"Reporter", "args":{}})
    else:
        # Fallback: just try Reporter to say no data
        plan.append({"agent":"Reporter", "args":{}})
    return plan

def handle(user_prompt: str, tools, blackboard: Blackboard | None = None) -> Dict[str, Any]:
    bb = blackboard or Blackboard()
    plan = _make_default_plan(user_prompt, tools)
    bb.plan = plan

    # Execute turn-by-turn with simple confidence checks
    def _exec(step: Dict[str, Any]) -> AgentResult:
        agent = step["agent"]
        args = step.get("args", {})
        if agent == "DataRetriever":
            return DR.run(args, tools, bb)
        if agent == "Forecaster":
            return FM.run(args, tools, bb)
        if agent == "NewsInterpreter":
            return NI.run(args, tools, bb)
        if agent == "RiskEngine":
            return RK.run(args, tools, bb)
        if agent == "Reporter":
            return RP.run(args, tools, bb)
        return AgentResult(ok=False, error=f"Unknown agent {agent}")

    last_result: AgentResult | None = None
    for step in plan:
        res = _exec(step)
        bb.add_confidence(step["agent"], res.confidence, (res.error or "ok"))
        # Simple retry policy for low confidence (except Reporter)
        if not res.ok or res.confidence < 0.6:
            if step["agent"] != "Reporter":
                # One retry with narrower window / shorter horizon
                retry_args = dict(step.get("args", {}))
                if "days" in retry_args: retry_args["days"] = max(1, int(retry_args["days"]) // 2 or 1)
                if "horizon" in retry_args: retry_args["horizon"] = max(1, int(retry_args["horizon"]) // 2 or 1)
                res = _exec({"agent": step["agent"], "args": retry_args})
                bb.add_confidence(step["agent"], res.confidence, f"retry: {res.error or 'ok'}")
        last_result = res

    # Return final report pointer + audit
    return {
        "ok": bool(last_result and last_result.ok),
        "report": last_result.payload if last_result else None,
        "plan": plan,
        "citations": bb.citations,
        "confidence": bb.confidence_notes,
    }
