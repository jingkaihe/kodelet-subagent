from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypedDict

DATABASE_FILENAME = "subagents.sqlite"

SpawnContextMode = Literal["fork", "fresh"]
AgentStatus = Literal[
    "starting",
    "running",
    "idle",
    "failed",
    "interrupted",
    "canceled",
]
RunStatus = Literal[
    "starting",
    "running",
    "completed",
    "failed",
    "interrupted",
    "canceled",
]
WorkerTerminalStatus = Literal["idle", "failed", "interrupted"]

ACTIVE_AGENT_STATUSES = {"starting", "running"}
ACTIVE_RUN_STATUSES = {"starting", "running"}
CLAIMABLE_AGENT_STATUSES = {"idle", "failed", "interrupted"}


class EnqueueSteeringResult(TypedDict):
    accepted: bool
    alreadyPending: bool


class StoreError(RuntimeError):
    pass


class AgentNotFoundError(StoreError):
    pass


class AgentConflictError(StoreError):
    pass


class AgentLimitError(StoreError):
    pass


class LeaseLostError(StoreError):
    pass


class DatabaseBootstrapError(StoreError):
    pass


class UnsupportedDatabaseError(DatabaseBootstrapError):
    pass


class DatabaseMigrationError(DatabaseBootstrapError):
    pass


@dataclass(frozen=True, slots=True)
class RunRecord:
    id: str
    generation: int
    task: str
    status: RunStatus
    result: str | None
    error: str | None
    created_at: float
    started_at: float | None
    completed_at: float | None
    updated_at: float


@dataclass(frozen=True, slots=True)
class AgentRecord:
    id: str
    name: str
    owner_conversation_id: str
    conversation_id: str | None
    context_mode: SpawnContextMode
    cwd: str
    status: AgentStatus
    generation: int
    created_at: float
    updated_at: float
    run: RunRecord


@dataclass(slots=True)
class Lease:
    agent_id: str
    run_id: str
    generation: int
    token: str = field(repr=False)
    runtime_id: str = field(repr=False)
    expires_at: float


@dataclass(frozen=True, slots=True)
class Claim:
    agent: AgentRecord
    lease: Lease


@dataclass(frozen=True, slots=True)
class SteeringMessage:
    id: int
    message: str
