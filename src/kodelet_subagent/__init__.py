"""Durable background-agent support for Kodelet."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("kodelet-subagent")
except PackageNotFoundError:  # pragma: no cover - source tree without installation
    __version__ = "0.4.0"

from .persistence import AgentStore
from .runtime import RuntimeState

__all__ = ["AgentStore", "RuntimeState", "__version__"]
