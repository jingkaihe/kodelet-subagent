"""Durable background-agent support for Kodelet."""

import tomllib
from importlib.metadata import version
from pathlib import Path

from .persistence import AgentStore
from .runtime import RuntimeState

_source_metadata = Path(__file__).resolve().parents[2] / "pyproject.toml"
if _source_metadata.is_file():
    with _source_metadata.open("rb") as pyproject:
        __version__ = str(tomllib.load(pyproject)["project"]["version"])
else:  # pragma: no cover - installed wheel metadata
    __version__ = version("kodelet-subagent")

__all__ = ["AgentStore", "RuntimeState", "__version__"]
