"""Install the package as a globally discovered Kodelet plugin extension."""

from __future__ import annotations

import json
import os
import shlex
from importlib import metadata
from pathlib import Path
from typing import Any

from . import __version__

PACKAGE_NAME = "kodelet-subagent"
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


def installed_package_source(version: str = __version__) -> str:
    """Return a reproducible package source for the generated wrapper."""

    fallback = f"{PACKAGE_NAME}=={version}"
    try:
        direct_url_text = metadata.distribution(PACKAGE_NAME).read_text("direct_url.json")
        direct_url: Any = json.loads(direct_url_text) if direct_url_text else None
    except (metadata.PackageNotFoundError, json.JSONDecodeError, TypeError):
        return fallback
    if not isinstance(direct_url, dict):
        return fallback

    url = direct_url.get("url")
    vcs_info = direct_url.get("vcs_info")
    if not isinstance(url, str) or not isinstance(vcs_info, dict):
        return fallback
    commit_id = vcs_info.get("commit_id")
    if vcs_info.get("vcs") != "git" or not isinstance(commit_id, str) or not commit_id:
        return fallback
    return f"git+{url}@{commit_id}"


def extension_wrapper(version: str = __version__) -> str:
    package = installed_package_source(version)
    return (
        "#!/usr/bin/env sh\n"
        "set -eu\n"
        f'exec uvx --from {shlex.quote(package)} kodelet-extension-subagent "$@"\n'
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
