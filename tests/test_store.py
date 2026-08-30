from __future__ import annotations

from pathlib import Path

import pytest

from kodelet_subagent.persistence import AgentStore


@pytest.mark.asyncio
async def test_store_resolves_relative_database_path_at_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_cwd = tmp_path / "original"
    later_cwd = tmp_path / "later"
    original_cwd.mkdir()
    later_cwd.mkdir()
    monkeypatch.chdir(original_cwd)

    store = AgentStore(Path("subagents.sqlite"), "runtime-relative-path")
    await store.initialize()
    expected_path = original_cwd / "subagents.sqlite"
    assert store.path == expected_path

    monkeypatch.chdir(later_cwd)
    claim = await store.create(
        "owner",
        "stable-path",
        "verify stable database ownership",
        str(later_cwd),
        "fresh",
    )
    persisted = await store.get("owner", claim.agent.id)

    assert persisted.name == "stable-path"
    assert expected_path.exists()
    assert not (later_cwd / "subagents.sqlite").exists()
