from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from .models import DatabaseMigrationError, UnsupportedDatabaseError

SQLITE_BUSY_TIMEOUT_MS = 5_000

_initialization_locks_guard = threading.Lock()
_initialization_locks: dict[Path, threading.Lock] = {}


@dataclass(frozen=True, slots=True)
class DatabaseState:
    has_alembic_version: bool
    has_application_schema: bool


def database_initialization_lock(path: Path) -> threading.Lock:
    resolved = path.expanduser().resolve()
    with _initialization_locks_guard:
        return _initialization_locks.setdefault(resolved, threading.Lock())


def open_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        path,
        timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
        isolation_level=None,
    )
    try:
        _apply_connection_pragmas(connection, ensure_wal=True)
    except BaseException:
        connection.close()
        raise
    connection.row_factory = sqlite3.Row
    return connection


def migrate_database(path: Path) -> None:
    """Create or upgrade an Alembic-managed subagent database."""

    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with database_initialization_lock(resolved):
        with _cross_process_lock(resolved):
            _bootstrap_locked(resolved)


def _bootstrap_locked(path: Path) -> None:
    if path.exists() and not path.is_file():
        raise UnsupportedDatabaseError(f"subagent database is not a file: {path}")

    if path.exists():
        state = _inspect_database(path)
        if state.has_application_schema and not state.has_alembic_version:
            raise UnsupportedDatabaseError(
                "unsupported subagent database without Alembic metadata: "
                f"{path}; remove it to initialize a new pre-release database"
            )

    _configure_database_file(path)
    _run_alembic(path)
    _verify_migrated_database(path)


def _configure_database_file(path: Path) -> None:
    connection = sqlite3.connect(
        path,
        timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
        isolation_level=None,
    )
    try:
        _apply_connection_pragmas(connection, ensure_wal=True)
    finally:
        connection.close()


def _apply_connection_pragmas(
    connection: sqlite3.Connection,
    *,
    ensure_wal: bool,
) -> None:
    connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    if ensure_wal:
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        if journal_mode.lower() != "wal":
            selected = str(connection.execute("PRAGMA journal_mode = WAL").fetchone()[0])
            if selected.lower() != "wal":
                raise DatabaseMigrationError(
                    f"failed to enable WAL journal mode; SQLite selected {selected!r}"
                )
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA synchronous = NORMAL")


def _inspect_database(path: Path) -> DatabaseState:
    resolved = path.expanduser().resolve()
    try:
        connection = sqlite3.connect(
            f"{resolved.as_uri()}?mode=ro",
            uri=True,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
        )
        try:
            quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            if quick_check.lower() != "ok":
                raise UnsupportedDatabaseError(
                    f"subagent database failed SQLite quick_check: {path}: {quick_check}"
                )
            has_alembic_version = (
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'alembic_version'
                    """
                ).fetchone()
                is not None
            )
            has_application_schema = (
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE name NOT LIKE 'sqlite_%'
                      AND name != 'alembic_version'
                    LIMIT 1
                    """
                ).fetchone()
                is not None
            )
        finally:
            connection.close()
    except UnsupportedDatabaseError:
        raise
    except sqlite3.DatabaseError as exc:
        raise UnsupportedDatabaseError(
            f"failed to inspect subagent database {path}: {exc}"
        ) from exc
    return DatabaseState(
        has_alembic_version=has_alembic_version,
        has_application_schema=has_application_schema,
    )


def _alembic_config():
    from alembic.config import Config

    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(__file__).with_name("migrations")),
    )
    return config


def _run_alembic(path: Path) -> None:
    from alembic import command
    from sqlalchemy import create_engine, event
    from sqlalchemy.engine import URL
    from sqlalchemy.pool import NullPool

    engine = create_engine(
        URL.create("sqlite+pysqlite", database=str(path)),
        connect_args={"timeout": SQLITE_BUSY_TIMEOUT_MS / 1000},
        poolclass=NullPool,
    )

    @event.listens_for(engine, "connect")
    def set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        _apply_connection_pragmas(dbapi_connection, ensure_wal=False)

    config = _alembic_config()
    try:
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    except Exception as exc:
        raise DatabaseMigrationError(f"failed to migrate subagent database {path}: {exc}") from exc
    finally:
        engine.dispose()


def _verify_migrated_database(path: Path) -> None:
    from alembic.script import ScriptDirectory

    config = _alembic_config()
    try:
        head = ScriptDirectory.from_config(config).get_current_head()
    except Exception as exc:
        raise DatabaseMigrationError(
            f"failed to resolve packaged subagent migration head: {exc}"
        ) from exc
    if head is None:
        raise DatabaseMigrationError("packaged subagent migrations have no head")

    try:
        with sqlite3.connect(path) as connection:
            quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            rows = connection.execute(
                "SELECT version_num FROM alembic_version ORDER BY version_num"
            ).fetchall()
            foreign_key_violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    except sqlite3.DatabaseError as exc:
        raise DatabaseMigrationError(
            f"failed to verify migrated subagent database {path}: {exc}"
        ) from exc

    if quick_check.lower() != "ok":
        raise DatabaseMigrationError(
            f"subagent database failed SQLite quick_check after migration: {path}: {quick_check}"
        )
    revisions = tuple(str(row[0]) for row in rows)
    if revisions != (head,):
        raise DatabaseMigrationError(
            f"subagent database {path} is at {revisions!r}, expected {(head,)!r}"
        )
    if foreign_key_violations:
        raise DatabaseMigrationError(f"subagent database {path} contains foreign-key violations")


@contextlib.contextmanager
def _cross_process_lock(path: Path) -> Iterator[None]:
    lock_path = path.with_name(f".{path.name}.migration.lock")
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        if os.name == "nt":
            import msvcrt

            if os.fstat(descriptor).st_size == 0:
                os.write(descriptor, b"\0")
                os.fsync(descriptor)
            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        if os.name == "nt":
            import msvcrt

            os.lseek(descriptor, 0, os.SEEK_SET)
            with contextlib.suppress(OSError):
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            with contextlib.suppress(OSError):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
