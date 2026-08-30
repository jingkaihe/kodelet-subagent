from __future__ import annotations

import asyncio
import builtins
import contextlib
import hmac
import re
import secrets
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import cast

from .database import migrate_database, open_database
from .models import (
    ACTIVE_AGENT_STATUSES,
    ACTIVE_RUN_STATUSES,
    CLAIMABLE_AGENT_STATUSES,
    AgentConflictError,
    AgentLimitError,
    AgentNotFoundError,
    AgentRecord,
    AgentStatus,
    Claim,
    EnqueueSteeringResult,
    Lease,
    LeaseLostError,
    RunRecord,
    RunStatus,
    SpawnContextMode,
    SteeringMessage,
    WorkerTerminalStatus,
)

DEFAULT_LIST_LIMIT = 64
MAX_LIST_LIMIT = 256
MAX_ACTIVE_AGENTS_PER_CONVERSATION = 3
MAX_ACTIVE_AGENTS_TOTAL = 8
LEASE_DURATION_SECONDS = 60.0
MAX_STEERING_MESSAGE_LENGTH = 10_000
AGENT_NAME_MAX_LENGTH = 48
AGENT_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+){0,2}$")


def validate_agent_name(name: str) -> str:
    if not name:
        raise ValueError("name is required")
    if len(name) > AGENT_NAME_MAX_LENGTH:
        raise ValueError(f"name must be at most {AGENT_NAME_MAX_LENGTH} characters")
    if AGENT_NAME_PATTERN.fullmatch(name) is None:
        raise ValueError(
            "name must contain one to three lowercase words separated by single "
            "hyphens, start with a letter, and contain only letters and digits"
        )
    return name


class AgentStore:
    def __init__(
        self,
        path: Path,
        runtime_id: str,
        *,
        clock: Callable[[], float] = time.time,
        legacy_candidates: Iterable[Path] = (),
    ) -> None:
        self.path = path.expanduser().resolve()
        self.runtime_id = runtime_id
        self._clock = clock
        self._legacy_candidates = tuple(
            candidate.expanduser().resolve() for candidate in legacy_candidates
        )
        self._initialized = False

    def current_time(self) -> float:
        return self._clock()

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_and_reconcile_sync)

    async def create(
        self,
        owner_id: str,
        name: str,
        task: str,
        cwd: str,
        context_mode: SpawnContextMode,
    ) -> Claim:
        return await asyncio.to_thread(
            self._create_sync,
            owner_id,
            name,
            task,
            cwd,
            context_mode,
        )

    async def get(
        self,
        owner_id: str,
        agent_id: str,
        run_id: str | None = None,
    ) -> AgentRecord:
        return await asyncio.to_thread(self._get_sync, owner_id, agent_id, run_id)

    async def list(
        self, owner_id: str, limit: int = DEFAULT_LIST_LIMIT
    ) -> builtins.list[AgentRecord]:
        return await asyncio.to_thread(self._list_sync, owner_id, limit)

    async def claim(self, owner_id: str, agent_id: str, task: str) -> Claim:
        return await asyncio.to_thread(self._claim_sync, owner_id, agent_id, task)

    async def attach_conversation(
        self,
        lease: Lease,
        conversation_id: str,
    ) -> AgentRecord:
        return await asyncio.to_thread(
            self._attach_conversation_sync,
            lease,
            conversation_id,
        )

    async def mark_running(
        self,
        lease: Lease,
        conversation_id: str,
    ) -> AgentRecord:
        return await asyncio.to_thread(
            self._mark_running_sync,
            lease,
            conversation_id,
        )

    async def terminal(
        self,
        lease: Lease,
        status: WorkerTerminalStatus,
        *,
        conversation_id: str | None = None,
        result: str | None = None,
        error: str | None = None,
    ) -> AgentRecord:
        return await asyncio.to_thread(
            self._terminal_sync,
            lease,
            status,
            conversation_id,
            result,
            error,
        )

    async def heartbeat(self, lease: Lease) -> float:
        expires_at = await asyncio.to_thread(self._heartbeat_sync, lease)
        lease.expires_at = expires_at
        return expires_at

    async def abort(self, lease: Lease) -> bool:
        return await asyncio.to_thread(self._abort_sync, lease)

    async def cancel(self, owner_id: str, agent_id: str) -> AgentRecord:
        return await asyncio.to_thread(self._cancel_sync, owner_id, agent_id)

    async def enqueue_steering(
        self,
        owner_id: str,
        agent_id: str,
        message: str,
    ) -> EnqueueSteeringResult:
        return await asyncio.to_thread(
            self._enqueue_steering_sync,
            owner_id,
            agent_id,
            message,
        )

    async def next_steering(self, lease: Lease) -> SteeringMessage | None:
        return await asyncio.to_thread(self._next_steering_sync, lease)

    async def acknowledge_steering(self, lease: Lease, message_id: int) -> bool:
        return await asyncio.to_thread(
            self._acknowledge_steering_sync,
            lease,
            message_id,
        )

    async def interrupt_runtime(self, error: str) -> int:
        return await asyncio.to_thread(self._interrupt_runtime_sync, error)

    async def reconcile_expired(self) -> int:
        return await asyncio.to_thread(self._reconcile_expired_sync)

    def _open_connection(self) -> sqlite3.Connection:
        return open_database(self.path)

    def _ensure_initialized_sync(self) -> None:
        if self._initialized:
            return
        migrate_database(
            self.path,
            legacy_candidates=self._legacy_candidates,
        )
        self._initialized = True

    def _initialize_and_reconcile_sync(self) -> None:
        self._ensure_initialized_sync()
        self._reconcile_expired_sync()

    @contextlib.contextmanager
    def _immediate_transaction(self) -> Iterator[sqlite3.Connection]:
        self._ensure_initialized_sync()
        connection = self._open_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _reconcile_expired_tx(
        self,
        connection: sqlite3.Connection,
        now: float,
    ) -> int:
        rows = connection.execute(
            """
            SELECT id, active_run_id, generation
            FROM agents
            WHERE status IN ('starting', 'running')
              AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
            """,
            (now,),
        ).fetchall()
        for row in rows:
            self._interrupt_row_tx(
                connection,
                str(row["id"]),
                str(row["active_run_id"]),
                int(row["generation"]),
                "agent lease expired",
                now,
            )
        return len(rows)

    def _interrupt_row_tx(
        self,
        connection: sqlite3.Connection,
        agent_id: str,
        run_id: str,
        generation: int,
        error: str,
        now: float,
    ) -> None:
        connection.execute(
            """
            UPDATE runs
            SET status = 'interrupted', error = ?, completed_at = ?, updated_at = ?
            WHERE id = ? AND status IN ('starting', 'running')
            """,
            (error, now, now, run_id),
        )
        connection.execute(
            """
            UPDATE agents
            SET status = 'interrupted', lease_runtime_id = NULL,
                lease_token = NULL, lease_expires_at = NULL, updated_at = ?
            WHERE id = ? AND active_run_id = ? AND generation = ?
              AND status IN ('starting', 'running')
            """,
            (now, agent_id, run_id, generation),
        )

    def _check_limits_tx(
        self,
        connection: sqlite3.Connection,
        owner_id: str,
    ) -> None:
        owner_count = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM agents
                WHERE owner_conversation_id = ? AND status IN ('starting', 'running')
                """,
                (owner_id,),
            ).fetchone()[0]
        )
        if owner_count >= MAX_ACTIVE_AGENTS_PER_CONVERSATION:
            raise AgentLimitError(
                "this conversation already has the maximum of "
                f"{MAX_ACTIVE_AGENTS_PER_CONVERSATION} active agents"
            )
        total_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM agents WHERE status IN ('starting', 'running')"
            ).fetchone()[0]
        )
        if total_count >= MAX_ACTIVE_AGENTS_TOTAL:
            raise AgentLimitError(
                f"this extension already has the maximum of {MAX_ACTIVE_AGENTS_TOTAL} active agents"
            )

    def _create_sync(
        self,
        owner_id: str,
        name: str,
        task: str,
        cwd: str,
        context_mode: SpawnContextMode,
    ) -> Claim:
        owner_id = owner_id.strip()
        name = validate_agent_name(name)
        task = task.strip()
        cwd = cwd.strip()
        if not owner_id:
            raise ValueError("async agents require an active conversation ID")
        if not task:
            raise ValueError("task is required")
        if not cwd:
            raise ValueError("cwd is required")
        if context_mode not in {"fork", "fresh"}:
            raise ValueError(f"unsupported context mode: {context_mode}")

        agent_id = f"agt_{uuid.uuid4().hex}"
        run_id = f"run_{uuid.uuid4().hex}"
        token = secrets.token_hex(32)
        now = self.current_time()
        expires_at = now + LEASE_DURATION_SECONDS
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            self._check_limits_tx(connection, owner_id)
            existing = connection.execute(
                """
                SELECT 1 FROM agents
                WHERE owner_conversation_id = ? AND name = ?
                """,
                (owner_id, name),
            ).fetchone()
            if existing is not None:
                raise AgentConflictError(f"agent name already exists in this conversation: {name}")
            connection.execute(
                """
                INSERT INTO agents (
                    id, name, owner_conversation_id, child_conversation_id,
                    context_mode, cwd, status, active_run_id, generation,
                    lease_runtime_id, lease_token, lease_expires_at, created_at,
                    updated_at
                ) VALUES (?, ?, ?, NULL, ?, ?, 'starting', ?, 1, ?, ?, ?, ?, ?)
                """,
                (
                    agent_id,
                    name,
                    owner_id,
                    context_mode,
                    cwd,
                    run_id,
                    self.runtime_id,
                    token,
                    expires_at,
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO runs (
                    id, agent_id, generation, lease_token, task, status,
                    result, error, created_at, started_at, completed_at, updated_at
                ) VALUES (?, ?, 1, ?, ?, 'starting', NULL, NULL, ?, NULL, NULL, ?)
                """,
                (run_id, agent_id, token, task, now, now),
            )
            agent = self._fetch_agent_tx(connection, agent_id)
        return Claim(
            agent=agent,
            lease=Lease(
                agent_id=agent_id,
                run_id=run_id,
                generation=1,
                token=token,
                runtime_id=self.runtime_id,
                expires_at=expires_at,
            ),
        )

    def _get_sync(
        self,
        owner_id: str,
        agent_id: str,
        run_id: str | None,
    ) -> AgentRecord:
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            return self._fetch_agent_tx(
                connection,
                agent_id.strip(),
                run_id.strip() if run_id is not None else None,
                owner_id.strip(),
            )

    def _list_sync(
        self,
        owner_id: str,
        limit: int,
    ) -> builtins.list[AgentRecord]:
        if limit < 1 or limit > MAX_LIST_LIMIT:
            raise ValueError(f"limit must be between 1 and {MAX_LIST_LIMIT}")
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            rows = connection.execute(
                """
                SELECT id FROM agents
                WHERE owner_conversation_id = ?
                ORDER BY
                    CASE WHEN status IN ('starting', 'running') THEN 0 ELSE 1 END,
                    updated_at DESC
                LIMIT ?
                """,
                (owner_id.strip(), limit),
            ).fetchall()
            return [self._fetch_agent_tx(connection, str(row["id"])) for row in rows]

    def _claim_sync(self, owner_id: str, agent_id: str, task: str) -> Claim:
        owner_id = owner_id.strip()
        agent_id = agent_id.strip()
        task = task.strip()
        if not task:
            raise ValueError("task is required")
        run_id = f"run_{uuid.uuid4().hex}"
        token = secrets.token_hex(32)
        now = self.current_time()
        expires_at = now + LEASE_DURATION_SECONDS
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            current = self._fetch_agent_tx(connection, agent_id, owner_id=owner_id)
            if current.status not in CLAIMABLE_AGENT_STATUSES:
                raise AgentConflictError(f"agent is {current.status}")
            if current.conversation_id is None and current.status == "idle":
                raise AgentConflictError("agent has no child conversation to resume")
            self._check_limits_tx(connection, owner_id)
            generation = current.generation + 1
            connection.execute(
                """
                INSERT INTO runs (
                    id, agent_id, generation, lease_token, task, status,
                    result, error, created_at, started_at, completed_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'starting', NULL, NULL, ?, NULL, NULL, ?)
                """,
                (run_id, agent_id, generation, token, task, now, now),
            )
            cursor = connection.execute(
                """
                UPDATE agents
                SET status = 'starting', active_run_id = ?, generation = ?,
                    lease_runtime_id = ?, lease_token = ?, lease_expires_at = ?,
                    updated_at = ?
                WHERE id = ? AND owner_conversation_id = ? AND generation = ?
                  AND status IN ('idle', 'failed', 'interrupted')
                """,
                (
                    run_id,
                    generation,
                    self.runtime_id,
                    token,
                    expires_at,
                    now,
                    agent_id,
                    owner_id,
                    current.generation,
                ),
            )
            if cursor.rowcount != 1:
                raise AgentConflictError("agent changed while claiming follow-up run")
            connection.execute(
                """
                UPDATE steering_messages
                SET run_id = ?, generation = ?
                WHERE agent_id = ?
                """,
                (run_id, generation, agent_id),
            )
            agent = self._fetch_agent_tx(connection, agent_id)
        return Claim(
            agent=agent,
            lease=Lease(
                agent_id=agent_id,
                run_id=run_id,
                generation=generation,
                token=token,
                runtime_id=self.runtime_id,
                expires_at=expires_at,
            ),
        )

    def _attach_conversation_sync(
        self,
        lease: Lease,
        conversation_id: str,
    ) -> AgentRecord:
        conversation_id = conversation_id.strip()
        if not conversation_id:
            raise ValueError("conversation id is required")
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            agent_row, _ = self._validate_active_lease_tx(connection, lease, now)
            existing = agent_row["child_conversation_id"]
            if existing is not None and str(existing) != conversation_id:
                raise AgentConflictError("agent child conversation cannot change")
            try:
                connection.execute(
                    """
                    UPDATE agents
                    SET child_conversation_id = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (conversation_id, now, lease.agent_id),
                )
            except sqlite3.IntegrityError as exc:
                raise AgentConflictError(
                    "child conversation is already attached to another agent"
                ) from exc
            return self._fetch_agent_tx(connection, lease.agent_id)

    def _mark_running_sync(
        self,
        lease: Lease,
        conversation_id: str,
    ) -> AgentRecord:
        conversation_id = conversation_id.strip()
        if not conversation_id:
            raise ValueError("conversation id is required")
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            agent_row, run_row = self._validate_active_lease_tx(
                connection,
                lease,
                now,
            )
            if str(agent_row["status"]) not in ACTIVE_AGENT_STATUSES:
                raise AgentConflictError(f"agent is {agent_row['status']}")
            existing = agent_row["child_conversation_id"]
            if existing is not None and str(existing) != conversation_id:
                raise AgentConflictError("agent child conversation cannot change")
            try:
                connection.execute(
                    """
                    UPDATE agents
                    SET child_conversation_id = ?, status = 'running', updated_at = ?
                    WHERE id = ?
                    """,
                    (conversation_id, now, lease.agent_id),
                )
            except sqlite3.IntegrityError as exc:
                raise AgentConflictError(
                    "child conversation is already attached to another agent"
                ) from exc
            if str(run_row["status"]) not in ACTIVE_RUN_STATUSES:
                raise AgentConflictError(f"run is {run_row['status']}")
            connection.execute(
                """
                UPDATE runs
                SET status = 'running', started_at = COALESCE(started_at, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (now, now, lease.run_id),
            )
            return self._fetch_agent_tx(connection, lease.agent_id)

    def _terminal_sync(
        self,
        lease: Lease,
        status: WorkerTerminalStatus,
        conversation_id: str | None,
        result: str | None,
        error: str | None,
    ) -> AgentRecord:
        desired_run_status: RunStatus
        if status == "idle":
            desired_run_status = "completed"
        elif status == "failed":
            desired_run_status = "failed"
        elif status == "interrupted":
            desired_run_status = "interrupted"
        else:
            raise ValueError(f"unsupported terminal status: {status}")

        normalized_conversation = conversation_id.strip() if conversation_id is not None else None
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            agent_row = self._agent_row_tx(connection, lease.agent_id)
            run_row = self._run_row_tx(connection, lease.agent_id, lease.run_id)
            self._validate_run_token(run_row, lease)

            if str(run_row["status"]) not in ACTIVE_RUN_STATUSES:
                if (
                    str(run_row["status"]) == desired_run_status
                    and run_row["result"] == result
                    and run_row["error"] == error
                ):
                    return self._fetch_agent_tx(
                        connection,
                        lease.agent_id,
                        lease.run_id,
                    )
                raise LeaseLostError("async-agent run is already terminal")

            self._validate_active_lease_rows(agent_row, run_row, lease, now)
            current_agent_status = str(agent_row["status"])
            if status == "idle" and current_agent_status != "running":
                raise AgentConflictError(
                    f"agent must be running before becoming idle, currently {current_agent_status}"
                )
            if current_agent_status not in ACTIVE_AGENT_STATUSES:
                raise AgentConflictError(f"agent is {current_agent_status}")

            existing_conversation = agent_row["child_conversation_id"]
            if (
                existing_conversation is not None
                and normalized_conversation is not None
                and str(existing_conversation) != normalized_conversation
            ):
                raise AgentConflictError("agent child conversation cannot change")
            resolved_conversation = (
                str(existing_conversation)
                if existing_conversation is not None
                else normalized_conversation
            )
            if status == "idle" and not resolved_conversation:
                raise AgentConflictError("completed agent must have a child conversation")

            connection.execute(
                """
                UPDATE runs
                SET status = ?, result = ?, error = ?, completed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (desired_run_status, result, error, now, now, lease.run_id),
            )
            connection.execute(
                """
                UPDATE agents
                SET child_conversation_id = ?, status = ?, lease_runtime_id = NULL,
                    lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                (resolved_conversation, status, now, lease.agent_id),
            )
            return self._fetch_agent_tx(connection, lease.agent_id, lease.run_id)

    def _heartbeat_sync(self, lease: Lease) -> float:
        now = self.current_time()
        expires_at = now + LEASE_DURATION_SECONDS
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            self._validate_active_lease_tx(connection, lease, now)
            connection.execute(
                """
                UPDATE agents
                SET lease_expires_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (expires_at, now, lease.agent_id),
            )
        return expires_at

    def _abort_sync(self, lease: Lease) -> bool:
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            try:
                agent_row, run_row = self._validate_active_lease_tx(
                    connection,
                    lease,
                    now,
                )
            except AgentNotFoundError:
                return False
            if (
                lease.generation != 1
                or str(agent_row["status"]) != "starting"
                or str(run_row["status"]) != "starting"
                or agent_row["child_conversation_id"] is not None
            ):
                raise AgentConflictError("only an unstarted initial reservation can be aborted")
            cursor = connection.execute(
                "DELETE FROM agents WHERE id = ?",
                (lease.agent_id,),
            )
            return cursor.rowcount == 1

    def _cancel_sync(self, owner_id: str, agent_id: str) -> AgentRecord:
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            current = self._fetch_agent_tx(
                connection,
                agent_id.strip(),
                owner_id=owner_id.strip(),
            )
            if current.status != "canceled":
                self._delete_run_messages_tx(
                    connection,
                    current.id,
                    current.run.id,
                    current.run.generation,
                )
                if current.status in ACTIVE_AGENT_STATUSES:
                    connection.execute(
                        """
                        UPDATE runs
                        SET status = 'canceled', error = 'agent canceled by parent',
                            completed_at = ?, updated_at = ?
                        WHERE id = ? AND status IN ('starting', 'running')
                        """,
                        (now, now, current.run.id),
                    )
                connection.execute(
                    """
                    UPDATE agents
                    SET status = 'canceled', lease_runtime_id = NULL,
                        lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, current.id),
                )
            return self._fetch_agent_tx(connection, current.id)

    def _enqueue_steering_sync(
        self,
        owner_id: str,
        agent_id: str,
        message: str,
    ) -> EnqueueSteeringResult:
        message = message.strip()
        if not message:
            raise ValueError("steering message is required")
        if len(message) > MAX_STEERING_MESSAGE_LENGTH:
            raise ValueError(f"steering message exceeds {MAX_STEERING_MESSAGE_LENGTH} characters")
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            current = self._fetch_agent_tx(
                connection,
                agent_id.strip(),
                owner_id=owner_id.strip(),
            )
            agent_row = self._agent_row_tx(connection, current.id)
            if (
                current.status != "running"
                or current.run.status != "running"
                or current.conversation_id is None
                or agent_row["lease_expires_at"] is None
                or float(agent_row["lease_expires_at"]) <= now
            ):
                raise AgentConflictError("agent is not currently running")
            already_pending = bool(
                connection.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM steering_messages
                        WHERE agent_id = ? AND run_id = ? AND generation = ?
                    )
                    """,
                    (current.id, current.run.id, current.run.generation),
                ).fetchone()[0]
            )
            connection.execute(
                """
                INSERT INTO steering_messages (
                    agent_id, run_id, generation, message, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    current.id,
                    current.run.id,
                    current.run.generation,
                    message,
                    now,
                ),
            )
            return {"accepted": True, "alreadyPending": already_pending}

    def _next_steering_sync(self, lease: Lease) -> SteeringMessage | None:
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            agent_row, run_row = self._validate_active_lease_tx(
                connection,
                lease,
                now,
            )
            if str(agent_row["status"]) != "running" or str(run_row["status"]) != "running":
                raise LeaseLostError("async-agent run is no longer running")
            row = connection.execute(
                """
                SELECT id, message FROM steering_messages
                WHERE agent_id = ? AND run_id = ? AND generation = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (lease.agent_id, lease.run_id, lease.generation),
            ).fetchone()
            if row is None:
                return None
            return SteeringMessage(id=int(row["id"]), message=str(row["message"]))

    def _acknowledge_steering_sync(self, lease: Lease, message_id: int) -> bool:
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            self._validate_active_lease_tx(connection, lease, now)
            cursor = connection.execute(
                """
                DELETE FROM steering_messages
                WHERE id = ? AND agent_id = ? AND run_id = ? AND generation = ?
                """,
                (message_id, lease.agent_id, lease.run_id, lease.generation),
            )
            return cursor.rowcount == 1

    def _interrupt_runtime_sync(self, error: str) -> int:
        now = self.current_time()
        with self._immediate_transaction() as connection:
            self._reconcile_expired_tx(connection, now)
            rows = connection.execute(
                """
                SELECT id, active_run_id, generation
                FROM agents
                WHERE lease_runtime_id = ? AND status IN ('starting', 'running')
                """,
                (self.runtime_id,),
            ).fetchall()
            for row in rows:
                self._interrupt_row_tx(
                    connection,
                    str(row["id"]),
                    str(row["active_run_id"]),
                    int(row["generation"]),
                    error,
                    now,
                )
            return len(rows)

    def _reconcile_expired_sync(self) -> int:
        now = self.current_time()
        with self._immediate_transaction() as connection:
            return self._reconcile_expired_tx(connection, now)

    def _delete_run_messages_tx(
        self,
        connection: sqlite3.Connection,
        agent_id: str,
        run_id: str,
        generation: int,
    ) -> int:
        cursor = connection.execute(
            """
            DELETE FROM steering_messages
            WHERE agent_id = ? AND run_id = ? AND generation = ?
            """,
            (agent_id, run_id, generation),
        )
        return cursor.rowcount

    def _agent_row_tx(
        self,
        connection: sqlite3.Connection,
        agent_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM agents WHERE id = ?",
            (agent_id,),
        ).fetchone()
        if row is None:
            raise AgentNotFoundError(f"agent not found: {agent_id}")
        return row

    def _run_row_tx(
        self,
        connection: sqlite3.Connection,
        agent_id: str,
        run_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM runs WHERE id = ? AND agent_id = ?",
            (run_id, agent_id),
        ).fetchone()
        if row is None:
            raise AgentNotFoundError(f"agent run not found: {run_id}")
        return row

    def _fetch_agent_tx(
        self,
        connection: sqlite3.Connection,
        agent_id: str,
        run_id: str | None = None,
        owner_id: str | None = None,
    ) -> AgentRecord:
        agent_row = self._agent_row_tx(connection, agent_id)
        if owner_id is not None and str(agent_row["owner_conversation_id"]) != owner_id:
            raise AgentNotFoundError(f"agent not found: {agent_id}")
        selected_run_id = run_id or str(agent_row["active_run_id"])
        run_row = self._run_row_tx(connection, agent_id, selected_run_id)
        return AgentRecord(
            id=str(agent_row["id"]),
            name=str(agent_row["name"]),
            owner_conversation_id=str(agent_row["owner_conversation_id"]),
            conversation_id=(
                str(agent_row["child_conversation_id"])
                if agent_row["child_conversation_id"] is not None
                else None
            ),
            context_mode=cast(SpawnContextMode, str(agent_row["context_mode"])),
            cwd=str(agent_row["cwd"]),
            status=cast(AgentStatus, str(agent_row["status"])),
            generation=int(agent_row["generation"]),
            created_at=float(agent_row["created_at"]),
            updated_at=float(agent_row["updated_at"]),
            run=RunRecord(
                id=str(run_row["id"]),
                generation=int(run_row["generation"]),
                task=str(run_row["task"]),
                status=cast(RunStatus, str(run_row["status"])),
                result=(str(run_row["result"]) if run_row["result"] is not None else None),
                error=str(run_row["error"]) if run_row["error"] is not None else None,
                created_at=float(run_row["created_at"]),
                started_at=(
                    float(run_row["started_at"]) if run_row["started_at"] is not None else None
                ),
                completed_at=(
                    float(run_row["completed_at"]) if run_row["completed_at"] is not None else None
                ),
                updated_at=float(run_row["updated_at"]),
            ),
        )

    def _validate_run_token(self, run_row: sqlite3.Row, lease: Lease) -> None:
        if int(run_row["generation"]) != lease.generation:
            raise LeaseLostError("async-agent generation changed")
        if not hmac.compare_digest(str(run_row["lease_token"]), lease.token):
            raise LeaseLostError("async-agent lease token changed")

    def _validate_active_lease_rows(
        self,
        agent_row: sqlite3.Row,
        run_row: sqlite3.Row,
        lease: Lease,
        now: float,
    ) -> None:
        self._validate_run_token(run_row, lease)
        if lease.runtime_id != self.runtime_id:
            raise LeaseLostError("async-agent worker runtime changed")
        if str(agent_row["active_run_id"]) != lease.run_id:
            raise LeaseLostError("async-agent active run changed")
        if int(agent_row["generation"]) != lease.generation:
            raise LeaseLostError("async-agent generation changed")
        runtime_id = agent_row["lease_runtime_id"]
        if runtime_id is None or str(runtime_id) != self.runtime_id:
            raise LeaseLostError("async-agent worker runtime changed")
        token = agent_row["lease_token"]
        if token is None or not hmac.compare_digest(str(token), lease.token):
            raise LeaseLostError("async-agent lease token changed")
        expires_at = agent_row["lease_expires_at"]
        if expires_at is None or float(expires_at) <= now:
            raise LeaseLostError("async-agent lease expired")
        if str(agent_row["status"]) not in ACTIVE_AGENT_STATUSES:
            raise LeaseLostError(f"async-agent is {agent_row['status']}")
        if str(run_row["status"]) not in ACTIVE_RUN_STATUSES:
            raise LeaseLostError(f"async-agent run is {run_row['status']}")

    def _validate_active_lease_tx(
        self,
        connection: sqlite3.Connection,
        lease: Lease,
        now: float,
    ) -> tuple[sqlite3.Row, sqlite3.Row]:
        agent_row = self._agent_row_tx(connection, lease.agent_id)
        run_row = self._run_row_tx(connection, lease.agent_id, lease.run_id)
        self._validate_active_lease_rows(agent_row, run_row, lease, now)
        return agent_row, run_row
