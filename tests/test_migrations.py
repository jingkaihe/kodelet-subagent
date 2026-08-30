from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kodelet_subagent.persistence import (
    SQLITE_BUSY_TIMEOUT_MS,
    UnsupportedDatabaseError,
    copy_legacy_database,
    find_legacy_database,
    migrate_database,
)
from kodelet_subagent.persistence._schema import (
    BASELINE_REVISION,
    LEGACY_SCHEMA_SQL,
)
from kodelet_subagent.persistence.database import open_database
from kodelet_subagent.runtime import RuntimeState


def create_legacy_database(path: Path, *, agent_name: str = "legacy-worker") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.executescript(LEGACY_SCHEMA_SQL)
        connection.execute(
            """
            INSERT INTO agents (
                id, name, owner_conversation_id, child_conversation_id,
                context_mode, cwd, status, active_run_id, generation,
                lease_runtime_id, lease_token, lease_expires_at, created_at,
                updated_at
            ) VALUES (
                'agt_legacy', ?, 'owner-legacy', 'child-legacy',
                'fresh', '/tmp', 'idle', 'run_legacy', 1,
                NULL, NULL, NULL, 1000.0, 1001.0
            )
            """,
            (agent_name,),
        )
        connection.execute(
            """
            INSERT INTO runs (
                id, agent_id, generation, lease_token, task, status,
                result, error, created_at, started_at, completed_at, updated_at
            ) VALUES (
                'run_legacy', 'agt_legacy', 1, 'legacy-token', 'legacy task',
                'completed', 'legacy result', NULL, 1000.0, 1000.5, 1001.0,
                1001.0
            )
            """
        )
        connection.execute(
            """
            INSERT INTO steering_messages (
                agent_id, run_id, generation, message, created_at
            ) VALUES (
                'agt_legacy', 'run_legacy', 1, 'pending legacy guidance', 1000.75
            )
            """
        )


def read_revision(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    assert row is not None
    return str(row[0])


def read_legacy_data(path: Path) -> tuple[tuple[object, ...], tuple[object, ...]]:
    with sqlite3.connect(path) as connection:
        agent = connection.execute(
            """
            SELECT id, name, owner_conversation_id, child_conversation_id,
                   status, active_run_id, generation
            FROM agents WHERE id = 'agt_legacy'
            """
        ).fetchone()
        run = connection.execute(
            """
            SELECT id, agent_id, generation, task, status, result
            FROM runs WHERE id = 'run_legacy'
            """
        ).fetchone()
    assert agent is not None
    assert run is not None
    return tuple(agent), tuple(run)


def test_fresh_database_upgrades_to_head_with_required_pragmas(tmp_path: Path) -> None:
    path = tmp_path / "fresh" / "subagents.sqlite"

    assert migrate_database(path) is None

    with sqlite3.connect(path) as connection:
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
    assert {"agents", "runs", "steering_messages", "alembic_version"} <= tables
    assert user_version == 1
    assert journal_mode == "wal"
    assert read_revision(path) == BASELINE_REVISION

    connection = open_database(path)
    try:
        assert int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) == 1
        assert (
            int(connection.execute("PRAGMA busy_timeout").fetchone()[0]) == SQLITE_BUSY_TIMEOUT_MS
        )
        assert int(connection.execute("PRAGMA synchronous").fetchone()[0]) == 1
    finally:
        connection.close()


def test_legacy_database_is_validated_stamped_and_preserved(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite"
    create_legacy_database(path)
    before = read_legacy_data(path)

    assert migrate_database(path) is None

    assert read_revision(path) == BASELINE_REVISION
    assert read_legacy_data(path) == before
    with sqlite3.connect(path) as connection:
        message = connection.execute(
            "SELECT run_id, generation, message FROM steering_messages"
        ).fetchone()
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    assert message == ("run_legacy", 1, "pending legacy guidance")
    assert user_version == 1


def test_migration_is_idempotent_and_preserves_adopted_data(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite"
    create_legacy_database(path)
    expected = read_legacy_data(path)

    migrate_database(path)
    migrate_database(path)

    assert read_revision(path) == BASELINE_REVISION
    assert read_legacy_data(path) == expected


def test_concurrent_fresh_migrations_share_the_database_lock(tmp_path: Path) -> None:
    path = tmp_path / "concurrent" / "subagents.sqlite"

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _index: migrate_database(path), range(4)))

    assert results == [None, None, None, None]
    assert read_revision(path) == BASELINE_REVISION
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA quick_check").fetchone() == ("ok",)


def test_malformed_legacy_schema_is_rejected_without_stamping(tmp_path: Path) -> None:
    path = tmp_path / "malformed.sqlite"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE agents (id TEXT PRIMARY KEY);
            PRAGMA user_version = 1;
            """
        )

    with pytest.raises(
        UnsupportedDatabaseError,
        match="schema does not match version 1",
    ):
        migrate_database(path)

    with sqlite3.connect(path) as connection:
        columns = [str(row[1]) for row in connection.execute("PRAGMA table_info(agents)")]
        alembic_table = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'alembic_version'
            """
        ).fetchone()
    assert columns == ["id"]
    assert alembic_table is None


def test_missing_legacy_source_is_rejected_without_creating_files(tmp_path: Path) -> None:
    source = tmp_path / "missing" / "subagents.sqlite"
    destination = tmp_path / "destination" / "subagents.sqlite"

    with pytest.raises(FileNotFoundError, match="does not exist"):
        copy_legacy_database(source, destination)

    assert not source.exists()
    assert not source.parent.exists()
    assert not destination.exists()
    assert not destination.parent.exists()


def test_legacy_source_must_be_a_regular_file(tmp_path: Path) -> None:
    source = tmp_path / "legacy-directory"
    source.mkdir()
    destination = tmp_path / "destination" / "subagents.sqlite"

    with pytest.raises(UnsupportedDatabaseError, match="source is not a file"):
        copy_legacy_database(source, destination)

    assert source.is_dir()
    assert not destination.parent.exists()


def test_legacy_candidate_is_copied_atomically_then_migrated(tmp_path: Path) -> None:
    source = tmp_path / "legacy-location" / "subagents.sqlite"
    destination = tmp_path / "new-location" / "subagents.sqlite"
    missing = tmp_path / "missing.sqlite"
    create_legacy_database(source, agent_name="copied-worker")
    source_data = read_legacy_data(source)

    assert find_legacy_database([missing, source], destination=destination) == source.resolve()
    copied_from = migrate_database(
        destination,
        legacy_candidates=[missing, source],
    )

    assert copied_from == source.resolve()
    assert read_revision(destination) == BASELINE_REVISION
    assert read_legacy_data(destination) == source_data
    assert read_legacy_data(source) == source_data
    with sqlite3.connect(source) as connection:
        source_alembic_table = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'alembic_version'
            """
        ).fetchone()
    assert source_alembic_table is None


async def test_runtime_imports_the_former_skills_plugin_database(tmp_path: Path) -> None:
    data_root = tmp_path / "extensions" / "data"
    source = data_root / "jingkaihe@skills_subagent" / "subagents.sqlite"
    target_data_dir = data_root / "jingkaihe@kodelet-subagent_subagent"
    create_legacy_database(source, agent_name="moved-worker")
    context = SimpleNamespace(
        storage=SimpleNamespace(data_dir=str(target_data_dir)),
    )
    runtime = RuntimeState(runtime_id="runtime-migration-test")

    store = await runtime.store_for_context(cast(Any, context))
    imported = await store.get("owner-legacy", "agt_legacy")

    assert store.path == target_data_dir / "subagents.sqlite"
    assert imported.name == "moved-worker"
    assert imported.run.result == "legacy result"
    assert read_revision(store.path) == BASELINE_REVISION
    assert read_legacy_data(source)[0][1] == "moved-worker"
