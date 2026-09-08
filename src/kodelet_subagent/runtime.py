"""Runtime orchestration for durable Kodelet background agents."""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from kodelet_sdk import (
    BackgroundTaskLease,
    ChildExecution,
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
CHILD_CANCEL_TIMEOUT_SECONDS = 10
HEARTBEAT_INTERVAL_SECONDS = 20.0
MIN_HEARTBEAT_INTERVAL_SECONDS = 1.0
HEARTBEAT_RETRY_MAX_SECONDS = 2.0
WORKER_UPDATE_RETRY_INITIAL_SECONDS = 0.1
WORKER_UPDATE_RETRY_MAX_SECONDS = 1.0
BACKGROUND_LEASE_RELEASE_RETRY_INITIAL_SECONDS = 0.1
BACKGROUND_LEASE_RELEASE_RETRY_MAX_SECONDS = 2.0
CHILD_CANCEL_RETRY_INITIAL_SECONDS = 0.1
CHILD_CANCEL_RETRY_MAX_SECONDS = 2.0
STEERING_POLL_SECONDS = 0.1
STEERING_RETRY_SECONDS = 0.25
CHILD_EVENT_HISTORY_LIMIT = 256
RECURSION_GUARD_ENV = "KODELET_SUBAGENT_EXTENSION_CHILD"


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
    setup_task: asyncio.Task[Any] | None = field(default=None, repr=False)
    runner_task: asyncio.Task[None] | None = field(default=None, repr=False)
    cleanup_task: asyncio.Task[None] | None = field(default=None, repr=False)
    child: ChildExecution | None = field(default=None, repr=False)
    child_done: bool = False
    events: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=CHILD_EVENT_HISTORY_LIMIT), repr=False
    )
    event_listeners: set[Callable[[dict[str, Any]], None]] = field(default_factory=set, repr=False)
    parent_canceled: bool = False
    heartbeat_error: str | None = None
    terminalizing: bool = False
    ui: Any | None = field(default=None, repr=False)
    background_lease: BackgroundTaskLease | None = field(default=None, repr=False)

    def record_event(self, event: dict[str, Any]) -> None:
        """Retain recent history and deliver every event to active waiters."""

        event = dict(event)
        self.events.append(event)
        for listener in tuple(self.event_listeners):
            # Presentation failures must not stop the background child.
            with contextlib.suppress(Exception):
                listener(event)

    def subscribe_events(self, listener: Callable[[dict[str, Any]], None]) -> None:
        """Replay retained history and subscribe without yielding between them."""

        for event in self.events:
            listener(event)
        self.event_listeners.add(listener)


class RuntimeState:
    """Own mutable extension-process state and background-agent orchestration."""

    def __init__(
        self,
        *,
        runtime_id: str | None = None,
    ) -> None:
        self.runtime_id = runtime_id or f"runtime_{uuid.uuid4().hex}"
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
        owned_task = live.runner_task or live.setup_task
        if owned_task is not None and owned_task is not asyncio.current_task():
            owned_task.cancel()

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

    async def safe_complete_cancel(self, live: LiveRun) -> bool:
        retry_delay = min(
            WORKER_UPDATE_RETRY_INITIAL_SECONDS,
            WORKER_UPDATE_RETRY_MAX_SECONDS,
        )
        while True:
            try:
                completed = await live.store.complete_cancel(live.lease)
                if completed:
                    await self.safe_sync_agent_widget(
                        live.ui,
                        live.store,
                        live.owner_conversation_id,
                    )
                return completed
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

    async def safe_attach_canceling_conversation(self, live: LiveRun) -> bool:
        conversation_id = live.conversation_id
        if conversation_id is None:
            return True
        retry_delay = min(
            WORKER_UPDATE_RETRY_INITIAL_SECONDS,
            WORKER_UPDATE_RETRY_MAX_SECONDS,
        )
        while True:
            try:
                await live.store.attach_canceling_conversation(
                    live.lease,
                    conversation_id,
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

    async def heartbeat_cleanup(self, live: LiveRun) -> None:
        interval = max(MIN_HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS)
        while True:
            try:
                try:
                    await live.store.heartbeat(live.lease)
                except LeaseLostError:
                    # Cancellation can arrive after failure cleanup starts,
                    # including from another extension process. Both updates
                    # preserve the exact token/generation/runtime/expiry fence.
                    await live.store.heartbeat_canceling(live.lease)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.is_definitive_worker_error(exc):
                    return
                if not self.is_retryable_store_error(exc):
                    return
                remaining = live.lease.expires_at - live.store.current_time()
                if remaining <= 0:
                    return
                await asyncio.sleep(
                    min(
                        HEARTBEAT_RETRY_MAX_SECONDS,
                        max(MIN_HEARTBEAT_INTERVAL_SECONDS, remaining / 2),
                    )
                )
                continue
            await asyncio.sleep(interval)

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
        live: LiveRun,
        child: ChildExecution,
        message: SteeringMessage,
    ) -> bool:
        while True:
            try:
                result = await child.steer(
                    message.message,
                    request_id=f"{live.agent_id}:steering:{message.id}",
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(STEERING_RETRY_SECONDS)
                continue
            if result.get("outcome") == "injected":
                return True
            if result.get("outcome") == "promptRequired":
                # Keep the durable queue entry for the next follow-up; an exact
                # completed run must never be turned into a new provider turn.
                return False
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

    async def steering_pump(self, live: LiveRun, child: ChildExecution) -> None:
        while True:
            queued = await self._wait_for_steering_message(live)
            if queued is None:
                return
            if not await self._deliver_steering_message(live, child, queued):
                return
            if not await self._acknowledge_steering_message(live, queued.id):
                return

    async def _start_child(
        self,
        live: LiveRun,
        ctx: ToolContext,
    ) -> Claim:
        conversation_id = live.conversation_id
        was_unattached = conversation_id is None
        options: dict[str, Any] = {
            "profile": "subagent",
            "message": live.task,
            "request_id": live.run_id,
            "cwd": str(live.cwd),
            "lease": live.background_lease,
        }
        if conversation_id is not None:
            options["resume"] = conversation_id
        else:
            options["context_mode"] = live.context_mode
        try:
            child = await asyncio.wait_for(
                ctx.children.start(**options),
                timeout=AGENT_START_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            raise TimeoutError("agent timed out while starting the delegated child") from None
        live.child = child
        live.conversation_id = child.conversation_id
        if was_unattached:
            await live.store.attach_conversation(live.lease, child.conversation_id)
        agent = await live.store.mark_running(live.lease, child.conversation_id)
        await self.safe_sync_agent_widget(
            live.ui,
            live.store,
            live.owner_conversation_id,
        )
        return Claim(agent=agent, lease=live.lease)

    async def _complete_agent_run(
        self,
        live: LiveRun,
        response: Mapping[str, Any],
    ) -> None:
        content = response.get("output")
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
                return "agent timed out while starting the delegated child"
            return "agent timed out while waiting for kodelet to finish"
        return f"agent failed: {exc}"

    async def run_agent_job(
        self,
        live: LiveRun,
        heartbeat_task: asyncio.Task[None] | None = None,
    ) -> None:
        steering_task: asyncio.Task[None] | None = None
        failure: tuple[bool, WorkerTerminalStatus, str] | None = None
        if heartbeat_task is None:
            heartbeat_task = self.start_agent_heartbeat(live)

        try:
            if live.heartbeat_error is not None:
                raise RuntimeError(live.heartbeat_error)
            child = live.child
            if child is None:
                raise RuntimeError("child admission must complete before launching the worker")
            steering_task = asyncio.create_task(
                self.steering_pump(live, child),
                name=f"kodelet-{live.agent_id}-steering",
            )

            response = await asyncio.wait_for(
                child.wait(on_event=live.record_event),
                timeout=AGENT_TIMEOUT_SECONDS,
            )
            live.child_done = True
            await self.stop_task(steering_task)
            steering_task = None
            await self._complete_agent_run(live, response)
        except asyncio.CancelledError:
            await self.stop_task(steering_task)
            if not live.parent_canceled:
                live.terminalizing = True
                failure = (
                    False,
                    "interrupted",
                    live.heartbeat_error
                    or "agent interrupted because the extension session stopped",
                )
            raise
        except Exception as exc:
            await self.stop_task(steering_task)
            steering_task = None
            live.terminalizing = True
            failure = (
                False,
                "failed",
                self._agent_failure_message("running", exc),
            )
        finally:
            cleanup_task = self._start_live_run_cleanup(
                live,
                steering_task,
                heartbeat_task,
                failure=failure,
            )
            await self._await_live_run_cleanup(cleanup_task)

    def _start_live_run_cleanup(
        self,
        live: LiveRun,
        steering_task: asyncio.Task[None] | None,
        heartbeat_task: asyncio.Task[None] | None,
        *,
        failure: tuple[bool, WorkerTerminalStatus, str] | None = None,
    ) -> asyncio.Task[None]:
        existing = live.cleanup_task
        if existing is not None:
            return existing
        cleanup_task = asyncio.create_task(
            self._cleanup_live_run(
                live,
                steering_task,
                heartbeat_task,
                failure=failure,
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
        steering_task: asyncio.Task[None] | None,
        heartbeat_task: asyncio.Task[None] | None,
        *,
        failure: tuple[bool, WorkerTerminalStatus, str] | None = None,
    ) -> None:
        try:
            await self.stop_task(steering_task)
            await self.stop_task(heartbeat_task)
            cleanup_heartbeat = asyncio.create_task(
                self.heartbeat_cleanup(live),
                name=f"kodelet-{live.agent_id}-{live.generation}-cleanup-heartbeat",
            )
            try:
                if live.child is not None and not live.child_done:
                    await self.cancel_child(live)
                elif live.child is None:
                    # A canceled start can have an unknown admitted identity.
                    # Revoke and drain its capability before touching accounting.
                    await self.close_background_lease(live)
                live.child = None
                if failure is not None and not live.parent_canceled:
                    initial, status, error = failure
                    if initial and live.conversation_id is None:
                        await self.safe_worker_abort(live)
                    else:
                        await self.safe_worker_terminal(live, status, error=error)
                if await self.safe_attach_canceling_conversation(live):
                    await self.safe_complete_cancel(live)
            finally:
                await self.stop_task(cleanup_heartbeat)
            await self.close_background_lease(live)
        finally:
            key = self.live_run_key(live.store, live.agent_id)
            if self.live_runs.get(key) is live:
                self.live_runs.pop(key, None)
            owned_key = self.owned_run_key(live.store, live.run_id)
            if self.owned_runs.get(owned_key) is live:
                self.owned_runs.pop(owned_key, None)
            live.setup_task = None
            live.cleanup_task = None

    async def cancel_child(self, live: LiveRun) -> None:
        child = live.child
        assert child is not None
        retry_delay = CHILD_CANCEL_RETRY_INITIAL_SECONDS
        try:
            async with asyncio.timeout(CHILD_CANCEL_TIMEOUT_SECONDS):
                while True:
                    try:
                        await child.cancel()
                        # Only a terminal response, not cancellation submission,
                        # proves this exact child has stopped.
                        while not (await child.read())["done"]:  # noqa: ASYNC110 - remote status
                            await asyncio.sleep(STEERING_POLL_SECONDS)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(CHILD_CANCEL_RETRY_MAX_SECONDS, retry_delay * 2)
                        continue
                    return
        except TimeoutError:
            # A lost result/grant need not strand local accounting indefinitely.
            # This ACK revokes and drains every child on this run's exclusive
            # lease. Transport failure keeps cleanup owned and canceling.
            await self.close_background_lease(live)

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
        live.setup_task = None
        owned_key = self.owned_run_key(live.store, live.run_id)
        self.owned_runs[owned_key] = live
        key = self.live_run_key(live.store, live.agent_id)
        self.live_runs[key] = live

    def own_live_setup(self, live: LiveRun, setup_task: asyncio.Task[Any]) -> None:
        live.setup_task = setup_task
        self.owned_runs[self.owned_run_key(live.store, live.run_id)] = live
        self.live_runs[self.live_run_key(live.store, live.agent_id)] = live

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
            self.own_live_setup(live, setup_task)
            setup_heartbeat = self.start_agent_heartbeat(live)
            try:
                live.background_lease = await ctx.acquire_background_task(
                    f"subagent {claim.agent.name} ({live.agent_id}): {' '.join(task.split())[:160]}"
                )
                # Bind retained child authority while this tool invocation is
                # still active. A background lease alone cannot submit a child.
                claim = await self._start_child(live, ctx)
                self.ensure_accepting_agents()
                await store.heartbeat(live.lease)
                self.launch_live_run(live, setup_heartbeat)
                setup_heartbeat = None
                return claim, live
            except asyncio.CancelledError:
                live.terminalizing = True
                cleanup_task = self._start_live_run_cleanup(
                    live,
                    None,
                    setup_heartbeat,
                    failure=(
                        initial,
                        "interrupted",
                        "agent setup was canceled before the worker started",
                    ),
                )
                await self._await_live_run_cleanup(cleanup_task)
                raise
            except Exception as exc:
                live.terminalizing = True
                cleanup_task = self._start_live_run_cleanup(
                    live,
                    None,
                    setup_heartbeat,
                    failure=(initial, "failed", self._agent_failure_message("starting", exc)),
                )
                await self._await_live_run_cleanup(cleanup_task)
                raise
        finally:
            self.setup_tasks.discard(setup_task)

    async def cancel_live_run(
        self,
        store: AgentStore,
        agent_id: str,
        run_id: str,
        *,
        cleanup_timeout: float | None = None,
    ) -> bool:
        """Cancel the selected in-process run after its cancellation was persisted.

        Returns ``False`` when cleanup remains in progress after the configured
        timeout, matching the distinction made by the original tool response.
        """

        live = self.owned_runs.get(self.owned_run_key(store, run_id))
        if live is None or live.agent_id != agent_id:
            return False
        live.parent_canceled = True
        owned_task = live.runner_task or live.setup_task or live.cleanup_task
        if owned_task is None:
            return await self.safe_complete_cancel(live)
        if not owned_task.done():
            owned_task.cancel()
        cleanup = asyncio.gather(owned_task, return_exceptions=True)
        try:
            await asyncio.wait_for(
                asyncio.shield(cleanup),
                timeout=(
                    CANCEL_CLEANUP_TIMEOUT_SECONDS if cleanup_timeout is None else cleanup_timeout
                ),
            )
        except TimeoutError:
            return False
        return self.owned_run_key(store, run_id) not in self.owned_runs

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
    "CHILD_CANCEL_RETRY_INITIAL_SECONDS",
    "CHILD_CANCEL_RETRY_MAX_SECONDS",
    "DATABASE_FILENAME",
    "HEARTBEAT_INTERVAL_SECONDS",
    "HEARTBEAT_RETRY_MAX_SECONDS",
    "MIN_HEARTBEAT_INTERVAL_SECONDS",
    "RECURSION_GUARD_ENV",
    "STEERING_POLL_SECONDS",
    "STEERING_RETRY_SECONDS",
    "WORKER_UPDATE_RETRY_INITIAL_SECONDS",
    "WORKER_UPDATE_RETRY_MAX_SECONDS",
    "LiveRun",
    "RuntimeState",
]
