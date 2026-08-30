from __future__ import annotations

from pathlib import Path

import pytest

import kodelet_subagent.__main__ as extension_main
import kodelet_subagent.cli as cli
import kodelet_subagent.extension as extension
import kodelet_subagent.persistence as persistence


def test_install_command_reports_installed_extension(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    expected = tmp_path / ".kodelet" / "plugins" / "subagent"
    observed_home: Path | None = None

    def fake_install_plugin(*, home: Path | None = None) -> Path:
        nonlocal observed_home
        observed_home = home
        return expected

    monkeypatch.setattr(cli, "install_plugin", fake_install_plugin)

    assert cli.main(["install", "--home", str(tmp_path)]) == 0
    assert observed_home == tmp_path
    assert capsys.readouterr().out == f"Installed Kodelet subagent extension at {expected}\n"


def test_migrate_command_resolves_path_and_reports_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "data" / "subagents.sqlite"
    migrated: list[Path] = []
    monkeypatch.setattr(persistence, "migrate_database", migrated.append)

    assert cli.main(["migrate", str(database)]) == 0
    assert migrated == [database.resolve()]
    assert capsys.readouterr().out == f"Migrated {database.resolve()}\n"


def test_extension_entrypoint_runs_registered_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fake_run_sync() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(extension.ext, "run_sync", fake_run_sync)

    extension_main.main()

    assert calls == 1
