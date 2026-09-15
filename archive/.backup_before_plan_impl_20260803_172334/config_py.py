"""
Configuration loading.

Two jobs:
  1. Read config.yaml into a dot-accessible object.
  2. Resolve ${ENV:VAR} placeholders so secrets never have to sit in the file.

A missing secret is not an error. The pipeline is designed to degrade to a
human-in-the-loop task rather than crash, because a 40-commodity run should not
die on one absent API key.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

_ENV_PATTERN = re.compile(r"^\$\{ENV:([A-Za-z_][A-Za-z0-9_]*)\}$")

log = logging.getLogger(__name__)


def _resolve(value: Any) -> Any:
    """Recursively replace ${ENV:VAR} placeholders with environment values."""
    if isinstance(value, dict):
        return {k: _resolve(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve(v) for v in value]
    if isinstance(value, str):
        match = _ENV_PATTERN.match(value.strip())
        if match:
            env_name = match.group(1)
            resolved = os.environ.get(env_name)
            if resolved is None:
                log.debug("Environment variable %s not set; treating as absent", env_name)
            return resolved
    return value


class Config:
    """Thin wrapper giving dotted-path access with defaults."""

    def __init__(self, data: dict[str, Any], root: Path):
        self._data = data
        self.root = root

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        path = Path(path).resolve()
        # Compute project root first (parent of config/)
        root = path.parent.parent

        # Load .env if python-dotenv is installed
        if load_dotenv is not None:
            env_file = root / ".env"
            if env_file.exists():
                load_dotenv(env_file)
                log.debug("Loaded environment variables from %s", env_file)

        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        resolved = _resolve(raw)
        return cls(resolved, root=root)

    def get(self, dotted: str, default: Any = None) -> Any:
        node: Any = self._data
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node if node is not None else default

    def path(self, dotted: str, default: str = "") -> Path:
        """Resolve a configured path relative to the project root."""
        value = self.get(dotted, default)
        p = Path(value)
        return p if p.is_absolute() else (self.root / p)

    def credential(self, key: Optional[str]) -> Optional[str]:
        """Look up one credential. Returns None when unset, blank or a leftover placeholder."""
        if not key:
            return None
        value = self.get(f"credentials.{key}")
        if value is None:
            return None
        value = str(value).strip()
        if not value or value.startswith("${"):
            return None
        return value

    def has_credentials(self, keys: list[str]) -> bool:
        """True only when every required credential for a source is present."""
        return bool(keys) and all(self.credential(k) for k in keys)

    def ensure_dirs(self) -> None:
        for key in ("paths.raw_cache", "paths.interim", "paths.curated", "paths.manual_drop"):
            self.path(key).mkdir(parents=True, exist_ok=True)
        self.path("paths.manifest").parent.mkdir(parents=True, exist_ok=True)

    def setup_logging(self) -> None:
        level = getattr(logging, str(self.get("logging.level", "INFO")).upper(), logging.INFO)
        log_file = self.path("logging.file", "output/pipeline.log")
        log_file.parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)-8s | %(name)-28s | %(message)s",
            handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
            force=True,
        )
