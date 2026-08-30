from __future__ import annotations

import json
import tempfile
import tomllib
from pathlib import Path
from unittest import mock

from kodelet_subagent import __version__
from kodelet_subagent.install import (
    extension_wrapper,
    install_plugin,
    installed_package_source,
    plugin_executable,
)

ROOT = Path(__file__).resolve().parents[1]


def test_install_plugin_writes_executable_pinned_wrapper() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        home = Path(temporary)
        installed = install_plugin(home=home)

        assert installed == plugin_executable(home)
        assert installed.read_text(encoding="utf-8") == extension_wrapper()
        assert installed.stat().st_mode & 0o111
        assert f"kodelet-subagent=={__version__}" in installed.read_text(encoding="utf-8")

        installed_again = install_plugin(home=home)
        assert installed_again == installed
        assert installed_again.read_text(encoding="utf-8") == extension_wrapper()


def test_repository_plugin_wrapper_pins_the_package_version() -> None:
    wrapper = ROOT / "extensions" / "subagent" / "kodelet-extension-subagent"

    assert wrapper.stat().st_mode & 0o111
    assert f"kodelet-subagent=={__version__}" in wrapper.read_text(encoding="utf-8")


def test_git_install_pins_the_resolved_commit_in_the_extension_wrapper() -> None:
    distribution = mock.Mock()
    distribution.read_text.return_value = json.dumps(
        {
            "url": "https://github.com/jingkaihe/kodelet-subagent",
            "vcs_info": {
                "vcs": "git",
                "commit_id": "7a57e47a40497919823ec1237af97571f2cf3af2",
            },
        }
    )

    with mock.patch(
        "kodelet_subagent.install.metadata.distribution",
        return_value=distribution,
    ):
        source = installed_package_source()
        wrapper = extension_wrapper()

    assert source == (
        "git+https://github.com/jingkaihe/kodelet-subagent@7a57e47a40497919823ec1237af97571f2cf3af2"
    )
    assert source in wrapper


def test_non_vcs_install_uses_the_published_package_version() -> None:
    distribution = mock.Mock()
    distribution.read_text.return_value = json.dumps(
        {
            "url": "file:///workspace/kodelet-subagent",
            "dir_info": {"editable": True},
        }
    )

    with mock.patch(
        "kodelet_subagent.install.metadata.distribution",
        return_value=distribution,
    ):
        assert installed_package_source() == f"kodelet-subagent=={__version__}"


def test_project_declares_and_ships_the_mit_license() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    license_text = (ROOT / "LICENSE").read_text(encoding="utf-8")

    assert project["license"] == "MIT"
    assert project["license-files"] == ["LICENSE"]
    assert license_text.startswith("MIT License\n")
    assert "Permission is hereby granted, free of charge" in license_text
