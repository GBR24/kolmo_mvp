# kolmo_core/config/path_utils.py
from pathlib import Path, PurePosixPath

def project_root() -> Path:
    # .../kolmo_core/config/path_utils.py -> repo root after 3 parents
    return Path(__file__).resolve().parents[3]

def as_project_relative(p: str | None, default_rel: str) -> str:
    """
    Return a *relative POSIX* path string (no leading slash).
    Safe to store in CONFIG.
    """
    cand = (p or default_rel).strip().replace("\\", "/")
    while cand.startswith("/"):
        cand = cand[1:]
    return str(PurePosixPath(cand))

def to_abs(p_rel: str) -> Path:
    """
    Turn a project-relative string into an absolute Path when you actually
    need to touch the filesystem. Do NOT write this back into CONFIG.
    """
    return project_root() / p_rel
