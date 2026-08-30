from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from kodelet_subagent.persistence import (
    SQLITE_BUSY_TIMEOUT_MS,
    DatabaseMigrationError,
    UnsupportedDatabaseError,
    migrate_database,
)
from kodelet_subagent.persistence.database import open_database

INITIAL_REVISION = "0001_initial"


def read_revision(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    assert row is not None
    return str(row[0])


def test_fresh_database_upgrades_to_head_with_required_pragmas(tmp_path: Path) -> None:
    path = tmp_path / "fresh" / "subagents.sqlite"

    assert migrate_database(path) is None

    with sqlite3.connect(path) as connection:
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        indexes = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
        }
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
    assert {"agents", "runs", "steering_messages", "alembic_version"} <= tables
    assert {
        "idx_agents_owner_updated",
        "idx_agents_owner_name",
        "idx_agents_status_lease",
        "idx_agents_runtime_status",
        "idx_runs_agent_created",
        "idx_steering_run",
    } <= indexes
    assert journal_mode == "wal"
    assert read_revision(path) == INITIAL_REVISION

    connection = open_database(path)
    try:
        assert int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) == 1
        assert (
            int(connection.execute("PRAGMA busy_timeout").fetchone()[0]) == SQLITE_BUSY_TIMEOUT_MS
        )
        assert int(connection.execute("PRAGMA synchronous").fetchone()[0]) == 1
    finally:
        connection.close()


def test_migration_is_idempotent_and_preserves_managed_data(tmp_path: Path) -> None:
    path = tmp_path / "managed.sqlite"
    migrate_database(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO agents (
                id, name, owner_conversation_id, child_conversation_id,
                context_mode, cwd, status, active_run_id, generation,
                lease_runtime_id, lease_token, lease_expires_at, created_at,
                updated_at
            ) VALUES (
                'agt_managed', 'managed-worker', 'owner', 'child',
                'fresh', '/tmp', 'idle', 'run_managed', 1,
                NULL, NULL, NULL, 1000.0, 1001.0
            )
            """
        )
        connection.execute(
            """
            INSERT INTO runs (
                id, agent_id, generation, lease_token, task, status,
                result, error, created_at, started_at, completed_at, updated_at
            ) VALUES (
                'run_managed', 'agt_managed', 1, 'token', 'managed task',
                'completed', 'managed result', NULL, 1000.0, 1000.5,
                1001.0, 1001.0
            )
            """
        )

    migrate_database(path)
    migrate_database(path)

    with sqlite3.connect(path) as connection:
        row = connection.execute(
            """
            SELECT agents.name, runs.task, runs.result
            FROM agents JOIN runs ON runs.agent_id = agents.id
            WHERE agents.id = 'agt_managed'
            """
        ).fetchone()
    assert row == ("managed-worker", "managed task", "managed result")
    assert read_revision(path) == INITIAL_REVISION


def test_existing_empty_database_is_initialized(tmp_path: Path) -> None:
    path = tmp_path / "empty.sqlite"
    path.touch()

    migrate_database(path)

    assert read_revision(path) == INITIAL_REVISION


def test_concurrent_fresh_migrations_share_the_database_lock(tmp_path: Path) -> None:
    path = tmp_path / "concurrent" / "subagents.sqlite"

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _index: migrate_database(path), range(4)))

    assert results == [None, None, None, None]
    assert read_revision(path) == INITIAL_REVISION
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA quick_check").fetchone() == ("ok",)


def test_nonempty_unversioned_database_is_rejected_without_adoption(
    tmp_path: Path,
) -> None:
    path = tmp_path / "unversioned.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE agents (id TEXT PRIMARY KEY)")

    with pytest.raises(
        UnsupportedDatabaseError,
        match="without Alembic metadata",
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


def test_database_path_must_be_a_regular_file(tmp_path: Path) -> None:
    path = tmp_path / "subagents.sqlite"
    path.mkdir()

    with pytest.raises(UnsupportedDatabaseError, match="is not a file"):
        migrate_database(path)


def test_corrupt_database_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "corrupt.sqlite"
    path.write_bytes(b"not a sqlite database")

    with pytest.raises(UnsupportedDatabaseError, match="failed to inspect"):
        migrate_database(path)


def test_unknown_alembic_revision_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "unknown-revision.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        connection.execute("INSERT INTO alembic_version (version_num) VALUES ('unknown_revision')")

    with pytest.raises(DatabaseMigrationError, match="failed to migrate"):
        migrate_database(path)


def test_managed_database_with_foreign_key_violations_is_rejected(
    tmp_path: Path,
) -> None:
    path = tmp_path / "invalid-foreign-key.sqlite"
    migrate_database(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO runs (
                id, agent_id, generation, lease_token, task, status,
                result, error, created_at, started_at, completed_at, updated_at
            ) VALUES (
                'run_orphan', 'agt_missing', 1, 'token', 'orphan task',
                'completed', 'result', NULL, 1000.0, 1000.0, 1001.0, 1001.0
            )
            """
        )

    with pytest.raises(DatabaseMigrationError, match="foreign-key violations"):
        migrate_database(path)
