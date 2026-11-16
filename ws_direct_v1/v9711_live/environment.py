"""Load private deployment settings without executing shell code."""
from __future__ import annotations
import os
import re
import shlex
from pathlib import Path


# Deployment default explicitly requested for cloud; env/CLI overrides still win.
MYSQL_DEFAULT_PASSWORD = "orcaterm2024"


def load_environment(path: Path | None = None) -> None:
    root = Path(os.environ.get("V9711_DEPLOY_ROOT", str(Path(__file__).resolve().parents[1])))
    path = Path(path) if path is not None else root / ".env"
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[7:].lstrip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        values = shlex.split(value, comments=True, posix=True)
        if len(values) > 1:
            raise ValueError(f"quote whitespace in .env setting: {key}")
        os.environ.setdefault(key, values[0] if values else "")
