"""Runtime orchestration for durable Kodelet background agents."""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, NotRequired, Protocol, TypedDict, cast

from kodelet_sdk import (
    BackgroundTaskLease,
    Client,
    ConversationForkUnavailableError,
    EventContext,
    ToolContext,
)

from .persistence import (
    DATABASE_FILENAME,
    AgentConflictError,
    AgentNotFoundError,
    AgentStore,
    Claim,
    Lease,
    LeaseLostError,
    SpawnContextMode,
    SteeringMessage,
    WorkerTerminalStatus,
)
from .ui import WIDGET_ID, agent_widget_lines

AGENT_TIMEOUT_SECONDS = 60 * 60 - 10
AGENT_START_TIMEOUT_SECONDS = 60
CANCEL_CLEANUP_TIMEOUT_SECONDS = 10
HEARTBEAT_INTERVAL_SECONDS = 20.0
MIN_HEARTBEAT_INTERVAL_SECONDS = 1.0
HEARTBEAT_RETRY_MAX_SECONDS = 2.0
WORKER_UPDATE_RETRY_INITIAL_SECONDS = 0.1
WORKER_UPDATE_RETRY_MAX_SECONDS = 1.0
BACKGROUND_LEASE_RELEASE_RETRY_INITIAL_SECONDS = 0.1
BACKGROUND_LEASE_RELEASE_RETRY_MAX_SECONDS = 2.0
STEERING_POLL_SECONDS = 0.1
STEERING_RETRY_SECONDS = 0.25
RECURSION_GUARD_ENV = "KODELET_SUBAGENT_EXTENSION_CHILD"


class SessionSteerResult(TypedDict):
    outcome: Literal["injected", "startedNewTurn", "promptRequired", "failed"]
    reason: NotRequired[str]


class SteeringSession(Protocol):
    @property
    def id(self) -> str: ...

    async def run_and_wait(self, task: str) -> Mapping[str, Any]: ...

    async def steer(self, message: str) -> SessionSteerResult: ...

    def on(self, event_name: str, listener: Callable[[Any], Any]) -> Any: ...

    def off(self, event_name: str, listener: Callable[[Any], Any]) -> Any: ...


class AgentClient(Protocol):
    async def create_session(self, **kwargs: Any) -> SteeringSession: ...

    async def close(self) -> None: ...


class ClientFactory(Protocol):
    def __call__(
        self,
        *,
        command: str,
        cwd: str,
        env: Mapping[str, str],
    ) -> AgentClient: ...


def default_client_factory(
    *,
    command: str,
    cwd: str,
    env: Mapping[str, str],
) -> AgentClient:
    """Construct the production Kodelet SDK client."""

    return cast(AgentClient, Client(command=command, cwd=cwd, env=env))


@dataclass(slots=True)
class LiveRun:
    """In-memory resources associated with one persisted run claim."""

    store: AgentStore
    agent_id: str
    owner_conversation_id: str
    run_id: str
    generation: int
    task: str
    cwd: Path
    context_mode: SpawnContextMode
    lease: Lease = field(repr=False)
    conversation_id: str | None = None
    runner_task: asyncio.Task[None] | None = field(default=None, repr=False)
    cleanup_task: asyncio.Task[None] | None = field(default=None, repr=False)
    client: AgentClient | None = field(default=None, repr=False)
    session: SteeringSession | None = field(default=None, repr=False)
    parent_canceled: bool = False
    heartbeat_error: str | None = None
    terminalizing: bool = False
    ui: Any | None = field(default=None, repr=False)
    background_lease: BackgroundTaskLease | None = field(default=None, repr=False)


class RuntimeState:
    """Own mutable extension-process state and background-agent orchestration."""

    def __init__(
        self,
        *,
        runtime_id: str | None = None,
        client_factory: ClientFactory = default_client_factory,
    ) -> None:
        self.runtime_id = runtime_id or f"runtime_{uuid.uuid4().hex}"
        self.client_factory = client_factory
        self.stores: dict[Path, AgentStore] = {}
        self.live_runs: dict[tuple[Path, str], LiveRun] = {}
        self.owned_runs: dict[tuple[Path, str], LiveRun] = {}
        self.setup_tasks: set[asyncio.Task[Any]] = set()
        self.cleanup_tasks: set[asyncio.Task[None]] = set()
        self.reservation_completions: set[asyncio.Future[None]] = set()
        self.widget_locks: dict[tuple[Path, str], asyncio.Lock] = {}
        self.shutting_down = False

    def reset_for_initialize(self) -> None:
        """Allow a newly initialized extension session to accept work."""

        self.shutting_down = False

    @staticmethod
    def database_path(ctx: ToolContext | EventContext) -> Path:
        return Path(ctx.storage.data_dir).resolve() / DATABASE_FILENAME

    async def store_for_context(self, ctx: ToolContext | EventContext) -> AgentStore:
        path = self.database_path(ctx)
        store = self.stores.get(path)
        if store is None:
            store = AgentStore(path, self.runtime_id)
            self.stores[path] = store
        await store.initialize()
        return store

    @staticmethod
    def live_run_key(store: AgentStore, agent_id: str) -> tuple[Path, str]:
        return store.path, agent_id

    @staticmethod
    def owned_run_key(store: AgentStore, run_id: str) -> tuple[Path, str]:
        return store.path, run_id

    def get_live_run(self, store: AgentStore, agent_id: str) -> LiveRun | None:
        return self.live_runs.get(self.live_run_key(store, agent_id))

    def agent_widget_lock(self, store: AgentStore, owner_id: str) -> asyncio.Lock:
        return self.widget_locks.setdefault((store.path, owner_id), asyncio.Lock())

    async def sync_agent_widget(self, ui: Any, store: AgentStore, owner_id: str) -> None:
        if ui is None:
            return
        async with self.agent_widget_lock(store, owner_id):
            agents = await store.list(owner_id)
            if not agents:
                await ui.set_widget(WIDGET_ID, None)
                return
            await ui.set_widget(
                WIDGET_ID,
                agent_widget_lines(agents),
                {"placement": "aboveComposer"},
            )

    async def safe_sync_agent_widget(
        self,
        ui: Any,
        store: AgentStore,
        owner_id: str,
    ) -> None:
        with contextlib.suppress(Exception):
            await self.sync_agent_widget(ui, store, owner_id)

    @staticmethod
    def live_run_from_claim(claim: Claim, task: str, store: AgentStore) -> LiveRun:
        return LiveRun(
            store=store,
            agent_id=claim.agent.id,
            owner_conversation_id=claim.agent.owner_conversation_id,
            run_id=claim.lease.run_id,
            generation=claim.lease.generation,
            task=task,
            cwd=Path(claim.agent.cwd),
            context_mode=claim.agent.context_mode,
            lease=claim.lease,
            conversation_id=claim.agent.conversation_id,
        )

    def ensure_accepting_agents(self) -> None:
        if self.shutting_down:
            raise RuntimeError("the extension session is shutting down")

    async def abandon_unlaunched_claim(
        self,
        claim: Claim,
        task: str,
        store: AgentStore,
        *,
        initial: bool,
    ) -> None:
        live = self.live_run_from_claim(claim, task, store)
        live.terminalizing = True
        heartbeat_task = self.start_agent_heartbeat(live)
        try:
            if initial and live.conversation_id is None:
                committed = await self.safe_worker_abort(live)
            else:
                committed = await self.safe_worker_terminal(
                    live,
                    "interrupted",
                    error="agent setup stopped before the worker started",
                )
            if not committed:
                raise RuntimeError("failed to persist unlaunched agent cleanup")
        finally:
            await self.stop_task(heartbeat_task)

    async def reserve_claim(
        self,
        operation: Awaitable[Claim],
        task: str,
        store: AgentStore,
        *,
        initial: bool,
    ) -> Claim:
        self.ensure_accepting_agents()
        completion = asyncio.get_running_loop().create_future()
        self.reservation_completions.add(completion)
        reservation = asyncio.ensure_future(operation)
        try:
            try:
                claim = await asyncio.shield(reservation)
            except asyncio.CancelledError:
                claim = await reservation
                await self.abandon_unlaunched_claim(
                    claim,
                    task,
                    store,
                    initial=initial,
                )
                raise
            if self.shutting_down:
                await self.abandon_unlaunched_claim(
                    claim,
                    task,
                    store,
                    initial=initial,
                )
                raise RuntimeError("the extension session is shutting down")
            return claim
        finally:
            if not completion.done():
                completion.set_result(None)
            self.reservation_completions.discard(completion)

    @staticmethod
    def is_definitive_worker_error(exc: Exception) -> bool:
        return isinstance(
            exc,
            (AgentNotFoundError, AgentConflictError, LeaseLostError),
        )

    @staticmethod
    def is_retryable_store_error(exc: Exception) -> bool:
        if not isinstance(exc, sqlite3.OperationalError):
            return False
        message = str(exc).casefold()
        return "locked" in message or "busy" in message

    @staticmethod
    def stop_live_run_for_error(live: LiveRun, error: str) -> None:
        live.heartbeat_error = error
        runner_task = live.runner_task
        if runner_task is not None and runner_task is not asyncio.current_task():
            runner_task.cancel()

    async def safe_worker_terminal(
        self,
        live: LiveRun,
        status: WorkerTerminalStatus,
        *,
        result: str | None = None,
        error: str | None = None,
    ) -> bool:
        retry_delay = min(
            WORKER_UPDATE_RETRY_INITIAL_SECONDS,
            WORKER_UPDATE_RETRY_MAX_SECONDS,
        )
        while True:
            try:
                await live.store.terminal(
                    live.lease,
                    status,
                    conversation_id=live.conversation_id,
                    result=result,
                    error=error,
                )
                await self.safe_sync_agent_widget(
                    live.ui,
                    live.store,
                    live.owner_conversation_id,
                )
                return True
            except Exception as exc:
                if self.is_definitive_worker_error(exc):
                    return False
                if not self.is_retryable_store_error(exc):
                    return False
                remaining = live.lease.expires_at - live.store.current_time()
                if remaining <= 0:
                    return False
                await asyncio.sleep(min(retry_delay, remaining))
                retry_delay = min(WORKER_UPDATE_RETRY_MAX_SECONDS, retry_delay * 2)

    async def safe_worker_abort(self, live: LiveRun) -> bool:
        retry_delay = min(
            WORKER_UPDATE_RETRY_INITIAL_SECONDS,
            WORKER_UPDATE_RETRY_MAX_SECONDS,
        )
        while True:
            try:
                await live.store.abort(live.lease)
                await self.safe_sync_agent_widget(
                    live.ui,
                    live.store,
                    live.owner_conversation_id,
                )
                return True
            except Exception as exc:
                if self.is_definitive_worker_error(exc):
                    return False
                if not self.is_retryable_store_error(exc):
                    return False
                remaining = live.lease.expires_at - live.store.current_time()
                if remaining <= 0:
                    return False
                await asyncio.sleep(min(retry_delay, remaining))
                retry_delay = min(WORKER_UPDATE_RETRY_MAX_SECONDS, retry_delay * 2)

    async def heartbeat_agent(self, live: LiveRun) -> None:
        interval = max(MIN_HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS)
        while True:
            await asyncio.sleep(interval)
            while True:
                try:
                    await live.store.heartbeat(live.lease)
                    interval = max(
                        MIN_HEARTBEAT_INTERVAL_SECONDS,
                        HEARTBEAT_INTERVAL_SECONDS,
                    )
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self.is_definitive_worker_error(exc):
                        if live.terminalizing:
                            return
                        self.stop_live_run_for_error(live, f"agent lease was lost: {exc}")
                        return
                    if not self.is_retryable_store_error(exc):
                        self.stop_live_run_for_error(live, f"agent heartbeat failed: {exc}")
                        return
                    remaining = live.lease.expires_at - live.store.current_time()
                    if remaining <= 0:
                        self.stop_live_run_for_error(
                            live,
                            f"agent lease heartbeat expired after error: {exc}",
                        )
                        return
                    await asyncio.sleep(
                        min(
                            HEARTBEAT_RETRY_MAX_SECONDS,
                            max(MIN_HEARTBEAT_INTERVAL_SECONDS, remaining / 2),
                        )
                    )

    def start_agent_heartbeat(self, live: LiveRun) -> asyncio.Task[None]:
        return asyncio.create_task(
            self.heartbeat_agent(live),
            name=f"kodelet-{live.agent_id}-heartbeat",
        )

    @staticmethod
    async def stop_task(task: asyncio.Task[None] | None) -> None:
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _wait_for_steering_message(self, live: LiveRun) -> SteeringMessage | None:
        while True:
            try:
                queued = await live.store.next_steering(live.lease)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.is_definitive_worker_error(exc):
                    self.stop_live_run_for_error(
                        live,
                        f"agent steering lease was lost: {exc}",
                    )
                    return
                if not self.is_retryable_store_error(exc):
                    self.stop_live_run_for_error(live, f"agent steering failed: {exc}")
                    return
                await asyncio.sleep(STEERING_RETRY_SECONDS)
                continue
            if queued is not None:
                return queued
            await asyncio.sleep(STEERING_POLL_SECONDS)

    @staticmethod
    async def _deliver_steering_message(
        session: SteeringSession,
        message: str,
    ) -> None:
        while True:
            try:
                result = await session.steer(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(STEERING_RETRY_SECONDS)
                continue
            if result.get("outcome") in {"injected", "startedNewTurn"}:
                return
            await asyncio.sleep(STEERING_RETRY_SECONDS)

    async def _acknowledge_steering_message(
        self,
        live: LiveRun,
        message_id: int,
    ) -> bool:
        while True:
            try:
                await live.store.acknowledge_steering(live.lease, message_id)
                return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.is_definitive_worker_error(exc):
                    self.stop_live_run_for_error(
                        live,
                        f"agent steering lease was lost: {exc}",
                    )
                    return False
                if not self.is_retryable_store_error(exc):
                    self.stop_live_run_for_error(live, f"agent steering failed: {exc}")
                    return False
                await asyncio.sleep(STEERING_RETRY_SECONDS)

    async def steering_pump(self, live: LiveRun, session: SteeringSession) -> None:
        while True:
            queued = await self._wait_for_steering_message(live)
            if queued is None:
                return
            await self._deliver_steering_message(session, queued.message)
            if not await self._acknowledge_steering_message(live, queued.id):
                return

    async def _create_agent_session(
        self,
        live: LiveRun,
        client: AgentClient,
    ) -> SteeringSession:
        conversation_id = live.conversation_id
        was_unattached = conversation_id is None
        options: dict[str, Any] = {
            "cwd": str(live.cwd),
            "streaming": True,
        }
        if conversation_id is not None:
            options["resume"] = conversation_id
        session = await asyncio.wait_for(
            client.create_session(**options),
            timeout=AGENT_START_TIMEOUT_SECONDS,
        )
        live.session = session
        live.conversation_id = session.id
        if was_unattached:
            await live.store.attach_conversation(live.lease, session.id)
        await live.store.mark_running(live.lease, session.id)
        await self.safe_sync_agent_widget(
            live.ui,
            live.store,
            live.owner_conversation_id,
        )
        return session

    async def _complete_agent_run(
        self,
        live: LiveRun,
        response: Mapping[str, Any],
    ) -> None:
        content = response.get("content")
        result = content.strip() if isinstance(content, str) else ""
        live.terminalizing = True
        if result:
            await self.safe_worker_terminal(live, "idle", result=result)
            return
        await self.safe_worker_terminal(
            live,
            "failed",
            error="agent returned an empty response",
        )

    @staticmethod
    def _agent_failure_message(phase: str, exc: Exception) -> str:
        if isinstance(exc, TimeoutError):
            if phase == "starting":
                return "agent timed out while starting the kodelet session"
            return "agent timed out while waiting for kodelet to finish"
        if isinstance(exc, OSError):
            return f"Failed to execute kodelet: {exc}"
        return f"agent failed: {exc}"

    async def run_agent_job(
        self,
        live: LiveRun,
        heartbeat_task: asyncio.Task[None] | None = None,
    ) -> None:
        client: AgentClient | None = None
        steering_task: asyncio.Task[None] | None = None
        if heartbeat_task is None:
            heartbeat_task = self.start_agent_heartbeat(live)
        heartbeat_stopped = False

        async def stop_heartbeat() -> None:
            nonlocal heartbeat_stopped
            if heartbeat_stopped:
                return
            heartbeat_stopped = True
            await self.stop_task(heartbeat_task)

        phase = "starting"
        try:
            if live.heartbeat_error is not None:
                raise RuntimeError(live.heartbeat_error)
            client = self.client_factory(
                command="kodelet",
                cwd=str(live.cwd),
                env={RECURSION_GUARD_ENV: "1"},
            )
            live.client = client
            session = await self._create_agent_session(live, client)
            phase = "running"
            steering_task = asyncio.create_task(
                self.steering_pump(live, session),
                name=f"kodelet-{live.agent_id}-steering",
            )

            response = await asyncio.wait_for(
                session.run_and_wait(live.task),
                timeout=AGENT_TIMEOUT_SECONDS,
            )
            await self.stop_task(steering_task)
            steering_task = None
            await self._complete_agent_run(live, response)
            await stop_heartbeat()
        except asyncio.CancelledError:
            await self.stop_task(steering_task)
            if not live.parent_canceled:
                live.terminalizing = True
                await self.safe_worker_terminal(
                    live,
                    "interrupted",
                    error=live.heartbeat_error
                    or "agent interrupted because the extension session stopped",
                )
            await stop_heartbeat()
            raise
        except Exception as exc:
            await self.stop_task(steering_task)
            steering_task = None
            live.terminalizing = True
            await self.safe_worker_terminal(
                live,
                "failed",
                error=self._agent_failure_message(phase, exc),
            )
            await stop_heartbeat()
        finally:
            cleanup_task = self._start_live_run_cleanup(
                live,
                client,
                steering_task,
                heartbeat_task,
            )
            await self._await_live_run_cleanup(cleanup_task)

    def _start_live_run_cleanup(
        self,
        live: LiveRun,
        client: AgentClient | None,
        steering_task: asyncio.Task[None] | None,
        heartbeat_task: asyncio.Task[None] | None,
    ) -> asyncio.Task[None]:
        existing = live.cleanup_task
        if existing is not None:
            return existing
        cleanup_task = asyncio.create_task(
            self._cleanup_live_run(
                live,
                client,
                steering_task,
                heartbeat_task,
            ),
            name=f"kodelet-{live.agent_id}-{live.generation}-cleanup",
        )
        live.cleanup_task = cleanup_task
        self.cleanup_tasks.add(cleanup_task)
        cleanup_task.add_done_callback(self.cleanup_tasks.discard)
        return cleanup_task

    @staticmethod
    async def _await_live_run_cleanup(cleanup_task: asyncio.Task[None]) -> None:
        canceled = False
        while True:
            try:
                await asyncio.shield(cleanup_task)
                break
            except asyncio.CancelledError:
                if cleanup_task.cancelled():
                    raise
                canceled = True
        if canceled:
            raise asyncio.CancelledError()

    async def _cleanup_live_run(
        self,
        live: LiveRun,
        client: AgentClient | None,
        steering_task: asyncio.Task[None] | None,
        heartbeat_task: asyncio.Task[None] | None,
    ) -> None:
        try:
            await self.stop_task(steering_task)
            await self.stop_task(heartbeat_task)
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.close()
            live.client = None
            live.session = None
            await self.close_background_lease(live)
        finally:
            key = self.live_run_key(live.store, live.agent_id)
            if self.live_runs.get(key) is live:
                self.live_runs.pop(key, None)
            owned_key = self.owned_run_key(live.store, live.run_id)
            if self.owned_runs.get(owned_key) is live:
                self.owned_runs.pop(owned_key, None)
            live.cleanup_task = None

    async def close_background_lease(self, live: LiveRun) -> None:
        lease = live.background_lease
        if lease is None:
            return
        retry_delay = BACKGROUND_LEASE_RELEASE_RETRY_INITIAL_SECONDS
        while True:
            try:
                await lease.close()
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(retry_delay)
                retry_delay = min(
                    BACKGROUND_LEASE_RELEASE_RETRY_MAX_SECONDS,
                    retry_delay * 2,
                )
                continue
            live.background_lease = None
            return

    def launch_live_run(
        self,
        live: LiveRun,
        heartbeat_task: asyncio.Task[None] | None = None,
    ) -> None:
        runner_task = asyncio.create_task(
            self.run_agent_job(live, heartbeat_task),
            name=f"kodelet-{live.agent_id}-{live.generation}",
        )
        live.runner_task = runner_task
        owned_key = self.owned_run_key(live.store, live.run_id)
        self.owned_runs[owned_key] = live
        key = self.live_run_key(live.store, live.agent_id)
        self.live_runs[key] = live

    @staticmethod
    async def fork_parent_context(parent_context: ToolContext, name: str) -> str:
        try:
            return await parent_context.fork_conversation(name=name)
        except ConversationForkUnavailableError as exc:
            raise RuntimeError(
                "context_mode='fork' requires live conversation forking; use "
                "context_mode='fresh' to start without parent conversation memory"
            ) from exc

    async def prepare_claim(
        self,
        claim: Claim,
        task: str,
        store: AgentStore,
        ctx: ToolContext,
        *,
        initial: bool,
    ) -> tuple[Claim, LiveRun]:
        self.ensure_accepting_agents()
        setup_task = asyncio.current_task()
        if setup_task is None:
            raise RuntimeError("agent setup requires an active asyncio task")
        self.setup_tasks.add(setup_task)
        try:
            live = self.live_run_from_claim(claim, task, store)
            live.ui = getattr(ctx, "ui", None)
            setup_heartbeat = self.start_agent_heartbeat(live)
            try:
                live.background_lease = await ctx.acquire_background_task(
                    f"subagent {claim.agent.name} ({live.agent_id}): {' '.join(task.split())[:160]}"
                )
                if live.conversation_id is None and live.context_mode == "fork":
                    live.conversation_id = await self.fork_parent_context(ctx, claim.agent.name)
                    if live.heartbeat_error is not None:
                        raise RuntimeError(live.heartbeat_error)
                    attached = await store.attach_conversation(
                        live.lease,
                        live.conversation_id,
                    )
                    claim = Claim(agent=attached, lease=claim.lease)
                self.ensure_accepting_agents()
                self.launch_live_run(live, setup_heartbeat)
                setup_heartbeat = None
                return claim, live
            except asyncio.CancelledError:
                live.terminalizing = True
                try:
                    if initial and live.conversation_id is None:
                        await self.safe_worker_abort(live)
                    else:
                        await self.safe_worker_terminal(
                            live,
                            "interrupted",
                            error="agent setup was canceled before the worker started",
                        )
                finally:
                    await self.stop_task(setup_heartbeat)
                    await self.close_background_lease(live)
                raise
            except Exception:
                live.terminalizing = True
                try:
                    if initial and live.conversation_id is None:
                        await self.safe_worker_abort(live)
                    else:
                        await self.safe_worker_terminal(
                            live,
                            "failed",
                            error="agent setup failed before the worker started",
                        )
                finally:
                    await self.stop_task(setup_heartbeat)
                    await self.close_background_lease(live)
                raise
        finally:
            self.setup_tasks.discard(setup_task)

    async def cancel_live_run(
        self,
        store: AgentStore,
        agent_id: str,
        *,
        cleanup_timeout: float | None = None,
    ) -> bool:
        """Cancel an in-process worker after its cancellation was persisted.

        Returns ``False`` when cleanup remains in progress after the configured
        timeout, matching the distinction made by the original tool response.
        """

        live = self.get_live_run(store, agent_id)
        if live is None:
            return True
        live.parent_canceled = True
        runner_task = live.runner_task
        if runner_task is None or runner_task.done():
            return True
        runner_task.cancel()
        cleanup = asyncio.gather(runner_task, return_exceptions=True)
        try:
            await asyncio.wait_for(
                asyncio.shield(cleanup),
                timeout=(
                    CANCEL_CLEANUP_TIMEOUT_SECONDS if cleanup_timeout is None else cleanup_timeout
                ),
            )
        except TimeoutError:
            return False
        return True

    async def shutdown(
        self,
        error: str = "agent interrupted because the extension session stopped",
    ) -> None:
        """Stop this runtime's workers and reconcile its persisted active rows."""

        self.shutting_down = True
        await self._cancel_owned_tasks()
        reservations = list(self.reservation_completions)
        if reservations:
            await asyncio.gather(*reservations, return_exceptions=True)
        # A reservation can finish concurrently with the first snapshot. It will
        # observe ``shutting_down`` and compensate, but drain again so shutdown
        # also owns any setup or worker that crossed a lifecycle boundary.
        await self._cancel_owned_tasks()
        await self._await_cleanup_tasks()
        for store in list(self.stores.values()):
            if store.runtime_id == self.runtime_id:
                with contextlib.suppress(Exception):
                    await store.interrupt_runtime(error)

    async def _cancel_owned_tasks(self) -> None:
        current = asyncio.current_task()
        tasks: set[asyncio.Task[Any]] = {
            task for task in self.setup_tasks if task is not current and not task.done()
        }
        tasks.update(
            live.runner_task
            for live in list(self.owned_runs.values())
            if live.store.runtime_id == self.runtime_id
            and live.runner_task is not None
            and live.runner_task is not current
            and not live.runner_task.done()
        )
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _await_cleanup_tasks(self) -> None:
        while True:
            tasks = [task for task in self.cleanup_tasks if not task.done()]
            if not tasks:
                return
            await asyncio.gather(*tasks, return_exceptions=True)


__all__ = [
    "AGENT_START_TIMEOUT_SECONDS",
    "AGENT_TIMEOUT_SECONDS",
    "BACKGROUND_LEASE_RELEASE_RETRY_INITIAL_SECONDS",
    "BACKGROUND_LEASE_RELEASE_RETRY_MAX_SECONDS",
    "CANCEL_CLEANUP_TIMEOUT_SECONDS",
    "DATABASE_FILENAME",
    "HEARTBEAT_INTERVAL_SECONDS",
    "HEARTBEAT_RETRY_MAX_SECONDS",
    "MIN_HEARTBEAT_INTERVAL_SECONDS",
    "RECURSION_GUARD_ENV",
    "STEERING_POLL_SECONDS",
    "STEERING_RETRY_SECONDS",
    "WORKER_UPDATE_RETRY_INITIAL_SECONDS",
    "WORKER_UPDATE_RETRY_MAX_SECONDS",
    "ClientFactory",
    "LiveRun",
    "RuntimeState",
    "SteeringSession",
    "default_client_factory",
]
