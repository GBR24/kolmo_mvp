from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class Blackboard:
    plan: List[Dict[str, Any]] = field(default_factory=list)
    artifacts: Dict[str, Any] = field(default_factory=dict)
    citations: List[Dict[str, str]] = field(default_factory=list)
    confidence_notes: List[Dict[str, Any]] = field(default_factory=list)
    audit: Dict[str, Any] = field(default_factory=dict)

    def put(self, key: str, value: Any) -> None:
        self.artifacts[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self.artifacts.get(key, default)

    def add_citation(self, title: str, link: str, source: str) -> None:
        self.citations.append({"title": title, "link": link, "source": source})

    def add_confidence(self, agent: str, confidence: float, note: str = "") -> None:
        self.confidence_notes.append({"agent": agent, "confidence": confidence, "note": note})
