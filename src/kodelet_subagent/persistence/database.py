from __future__ import annotations

import contextlib
import functools
import os
import sqlite3
import tempfile
import threading
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

from ._schema import (
    BASELINE_REVISION,
    LEGACY_SCHEMA_SQL,
    LEGACY_USER_VERSION,
)
from .models import DatabaseMigrationError, UnsupportedDatabaseError

SQLITE_BUSY_TIMEOUT_MS = 5_000

_initialization_locks_guard = threading.Lock()
_initialization_locks: dict[Path, threading.Lock] = {}


@dataclass(frozen=True, slots=True)
class DatabaseState:
    user_version: int
    has_alembic_version: bool
    schema: Mapping[tuple[str, str], str]

    @property
    def has_application_schema(self) -> bool:
        return bool(self.schema)


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


def migrate_database(
    path: Path,
    *,
    legacy_candidates: Iterable[Path] = (),
) -> Path | None:
    """Create or migrate a subagent database and return a copied legacy source."""

    resolved = path.expanduser().resolve()
    candidates = tuple(candidate.expanduser().resolve() for candidate in legacy_candidates)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with database_initialization_lock(resolved):
        with _cross_process_lock(resolved):
            copied_from = _copy_first_legacy_candidate(resolved, candidates)
            _bootstrap_locked(resolved)
            return copied_from


def find_legacy_database(
    candidates: Iterable[Path],
    *,
    destination: Path | None = None,
) -> Path | None:
    excluded = destination.expanduser().resolve() if destination is not None else None
    for raw_candidate in candidates:
        candidate = raw_candidate.expanduser().resolve()
        if candidate != excluded and candidate.exists():
            return candidate
    return None


def copy_legacy_database(source: Path, destination: Path) -> None:
    """Validate and atomically copy an unmigrated version 1 database."""

    resolved_source = source.expanduser().resolve()
    resolved_destination = destination.expanduser().resolve()
    if resolved_source == resolved_destination:
        raise ValueError("legacy source and migration destination must differ")
    if not resolved_source.exists():
        raise FileNotFoundError(f"legacy subagent database does not exist: {resolved_source}")
    if not resolved_source.is_file():
        raise UnsupportedDatabaseError(
            f"legacy subagent database source is not a file: {resolved_source}"
        )
    resolved_destination.parent.mkdir(parents=True, exist_ok=True)
    with database_initialization_lock(resolved_destination):
        with _cross_process_lock(resolved_destination):
            if resolved_destination.exists():
                raise FileExistsError(
                    f"subagent database destination already exists: {resolved_destination}"
                )
            state = _inspect_database(resolved_source)
            if state.has_alembic_version or state.user_version != LEGACY_USER_VERSION:
                raise UnsupportedDatabaseError(
                    "legacy subagent database source is not an unmigrated version 1 "
                    f"database: {resolved_source}"
                )
            _validate_legacy_state(resolved_source, state)
            _atomic_sqlite_backup(resolved_source, resolved_destination)


def _bootstrap_locked(path: Path) -> None:
    if not path.exists():
        _configure_database_file(path)
        _run_alembic(path, stamp_legacy=False)
        _verify_migrated_database(path)
        return
    if not path.is_file():
        raise UnsupportedDatabaseError(f"subagent database is not a file: {path}")

    state = _inspect_database(path)
    if state.has_alembic_version:
        _configure_database_file(path)
        _run_alembic(path, stamp_legacy=False)
        _verify_migrated_database(path)
        return

    if not state.has_application_schema and state.user_version == 0:
        _configure_database_file(path)
        _run_alembic(path, stamp_legacy=False)
        _verify_migrated_database(path)
        return

    if state.user_version == LEGACY_USER_VERSION:
        _validate_legacy_state(path, state)
        _configure_database_file(path)
        _run_alembic(path, stamp_legacy=True)
        _verify_migrated_database(path)
        return

    raise UnsupportedDatabaseError(
        "unsupported subagent database without Alembic metadata: "
        f"{path} has PRAGMA user_version={state.user_version}"
    )


def _copy_first_legacy_candidate(path: Path, candidates: tuple[Path, ...]) -> Path | None:
    if path.exists():
        return None
    candidate = find_legacy_database(candidates, destination=path)
    if candidate is None:
        return None
    if not candidate.is_file():
        raise UnsupportedDatabaseError(
            f"legacy subagent database candidate is not a file: {candidate}"
        )
    state = _inspect_database(candidate)
    if state.has_alembic_version or state.user_version != LEGACY_USER_VERSION:
        raise UnsupportedDatabaseError(
            "legacy subagent database candidate is not an unmigrated version 1 "
            f"database: {candidate}"
        )
    _validate_legacy_state(candidate, state)
    _atomic_sqlite_backup(candidate, path)
    return candidate


def _atomic_sqlite_backup(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        source_uri = f"{source.as_uri()}?mode=ro"
        with (
            sqlite3.connect(
                source_uri,
                uri=True,
                timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
            ) as source_connection,
            sqlite3.connect(
                temporary,
                timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
            ) as destination_connection,
        ):
            source_connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
            source_connection.backup(destination_connection)
            destination_connection.commit()
        _fsync_file(temporary)
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


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
            user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            has_alembic_version = (
                connection.execute(
                    """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'alembic_version'
                """
                ).fetchone()
                is not None
            )
            schema = _schema_fingerprint(connection)
        finally:
            connection.close()
    except UnsupportedDatabaseError:
        raise
    except sqlite3.DatabaseError as exc:
        raise UnsupportedDatabaseError(
            f"failed to inspect subagent database {path}: {exc}"
        ) from exc
    return DatabaseState(
        user_version=user_version,
        has_alembic_version=has_alembic_version,
        schema=schema,
    )


def _schema_fingerprint(
    connection: sqlite3.Connection,
) -> dict[tuple[str, str], str]:
    rows = connection.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%'
          AND name != 'alembic_version'
        ORDER BY type, name
        """
    ).fetchall()
    return {
        (str(row[0]), str(row[1])): _normalize_schema_sql(str(row[2]))
        for row in rows
        if row[2] is not None
    }


def _normalize_schema_sql(sql: str) -> str:
    return " ".join(sql.split()).casefold()


@functools.lru_cache(maxsize=1)
def _expected_legacy_schema() -> Mapping[tuple[str, str], str]:
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(LEGACY_SCHEMA_SQL)
        return _schema_fingerprint(connection)


def _validate_legacy_state(path: Path, state: DatabaseState) -> None:
    expected = _expected_legacy_schema()
    actual = state.schema
    if actual == expected:
        return

    expected_keys = set(expected)
    actual_keys = set(actual)
    missing = sorted(name for _kind, name in expected_keys - actual_keys)
    extra = sorted(name for _kind, name in actual_keys - expected_keys)
    changed = sorted(
        name
        for kind, name in expected_keys & actual_keys
        if expected[(kind, name)] != actual[(kind, name)]
    )
    details: list[str] = []
    if missing:
        details.append(f"missing objects: {', '.join(missing)}")
    if extra:
        details.append(f"unexpected objects: {', '.join(extra)}")
    if changed:
        details.append(f"mismatched objects: {', '.join(changed)}")
    raise UnsupportedDatabaseError(
        f"legacy subagent database schema does not match version 1 at {path}: " + "; ".join(details)
    )


def _alembic_config():
    from alembic.config import Config

    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(__file__).with_name("migrations")),
    )
    return config


def _run_alembic(path: Path, *, stamp_legacy: bool) -> None:
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
            if stamp_legacy:
                command.stamp(config, BASELINE_REVISION)
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

    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            "SELECT version_num FROM alembic_version ORDER BY version_num"
        ).fetchall()
    revisions = tuple(str(row[0]) for row in rows)
    if revisions != (head,):
        raise DatabaseMigrationError(
            f"subagent database {path} is at {revisions!r}, expected {(head,)!r}"
        )

    state = _inspect_database(path)
    if head == BASELINE_REVISION:
        if state.user_version != LEGACY_USER_VERSION:
            raise DatabaseMigrationError(
                f"subagent database {path} has PRAGMA user_version="
                f"{state.user_version}, expected {LEGACY_USER_VERSION}"
            )
        _validate_legacy_state(path, state)


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


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
