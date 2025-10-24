from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

def render_report(blocks: List[str]) -> Dict[str, str]:
    reports_dir = Path("kolmo_core/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    fname = datetime.now().strftime("%Y%m%d_%H%M%S_multi_agent.md")
    path = reports_dir / fname
    md = "\n\n".join(blocks)
    path.write_text(md, encoding="utf-8")
    return {"md_path": str(path)}
