from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class AgentResult:
    ok: bool
    payload: Any = None
    confidence: float = 0.5
    citations: List[Dict[str, str]] = field(default_factory=list)
    used_tables: List[str] = field(default_factory=list)
    time_window: Optional[str] = None
    error: Optional[str] = None
