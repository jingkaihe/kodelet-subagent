"""Install the package as a globally discovered Kodelet plugin extension."""

from __future__ import annotations

import os
from pathlib import Path

from . import __version__

PLUGIN_NAME = "jingkaihe@kodelet-subagent"
EXTENSION_NAME = "subagent"
EXECUTABLE_NAME = "kodelet-extension-subagent"


def plugin_executable(home: Path | None = None) -> Path:
    resolved_home = home or Path.home()
    return (
        resolved_home
        / ".kodelet"
        / "plugins"
        / PLUGIN_NAME
        / "extensions"
        / EXTENSION_NAME
        / EXECUTABLE_NAME
    )


def extension_wrapper(version: str = __version__) -> str:
    package = f"kodelet-subagent=={version}"
    return (
        "#!/usr/bin/env sh\n"
        "set -eu\n"
        f"exec uvx --from '{package}' kodelet-extension-subagent \"$@\"\n"
    )


def install_plugin(*, home: Path | None = None) -> Path:
    executable = plugin_executable(home)
    executable.parent.mkdir(parents=True, exist_ok=True)
    temporary = executable.with_name(f".{executable.name}.tmp-{os.getpid()}")
    try:
        temporary.write_text(extension_wrapper(), encoding="utf-8")
        temporary.chmod(0o755)
        temporary.replace(executable)
    finally:
        temporary.unlink(missing_ok=True)
    return executable
