from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import time
import unittest
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest import mock

from kodelet_sdk import ChildClient, TaskProgress, TaskProgressContext

import kodelet_subagent.extension as extension
import kodelet_subagent.runtime as runtime_module
from kodelet_subagent.persistence import (
    DATABASE_FILENAME,
    LEASE_DURATION_SECONDS,
    AgentConflictError,
    AgentLimitError,
    AgentStore,
    Claim,
    LeaseLostError,
)
from kodelet_subagent.runtime import RuntimeState
from kodelet_subagent.ui import WIDGET_ID, forward_child_progress

RECURSION_GUARD_ENV = "KODELET_SUBAGENT_EXTENSION_CHILD"


class MutableClock:
    def __init__(self, value: float = 1_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class FakeUI:
    def __init__(self) -> None:
        self.widgets: dict[str, tuple[list[Any], dict[str, str] | None] | None] = {}
        self.updates: list[tuple[str, list[Any] | None]] = []

    async def set_widget(
        self,
        widget_id: str,
        lines: list[Any] | None,
        options: dict[str, str] | None = None,
    ) -> None:
        self.widgets[widget_id] = None if lines is None else (lines, options)
        self.updates.append((widget_id, lines))

    def text(self, widget_id: str) -> str:
        widget = self.widgets.get(widget_id)
        if widget is None:
            return ""
        lines, _options = widget
        rendered: list[str] = []
        for line in lines:
            if isinstance(line, str):
                rendered.append(line)
                continue
            rendered.append("".join(str(span.get("text", "")) for span in line["spans"]))
        return "\n".join(rendered)


class FakeBackgroundTaskLease:
    def __init__(
        self,
        description: str | None,
        failures_remaining: int = 0,
        *,
        block_close: bool = False,
    ) -> None:
        self.description = description
        self.id = "lease-test"
        self.failures_remaining = failures_remaining
        self.block_close = block_close
        self.close_calls = 0
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()

    async def close(self) -> None:
        self.close_calls += 1
        self.close_started.set()
        if self.block_close:
            await self.close_release.wait()
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise RuntimeError("temporary background lease release failure")


class FakeContext:
    def __init__(
        self,
        conversation_id: str,
        cwd: Path,
        data_dir: Path,
        *,
        invoked_by: str | None = None,
        block_fork: bool = False,
        block_background_acquire: bool = False,
        block_background_release: bool = False,
    ) -> None:
        self.conversation_id = conversation_id
        self.cwd = str(cwd)
        self.invoked_by = invoked_by
        self.storage = SimpleNamespace(data_dir=str(data_dir))
        self.ui = FakeUI()
        self.log = SimpleNamespace(warn=lambda _message, _fields=None: None)
        self.block_fork = block_fork
        self.block_background_acquire = block_background_acquire
        self.block_background_release = block_background_release
        self.fork_names: list[str | None] = []
        self.fork_started = asyncio.Event()
        self.fork_release = asyncio.Event()
        self.background_release_failures = 0
        self.background_acquire_started = asyncio.Event()
        self.background_acquire_release = asyncio.Event()
        self.background_leases: list[FakeBackgroundTaskLease] = []
        self.tool_updates: list[tuple[str, dict[str, Any] | None]] = []
        self.children = FakeChildren(self)

    async def update(
        self,
        content: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.tool_updates.append((content, data))

    async def acquire_background_task(
        self,
        description: str | None = None,
    ) -> FakeBackgroundTaskLease:
        self.background_acquire_started.set()
        if self.block_background_acquire:
            await self.background_acquire_release.wait()
        lease = FakeBackgroundTaskLease(
            description,
            self.background_release_failures,
            block_close=self.block_background_release,
        )
        lease.id = f"lease-{len(self.background_leases) + 1}"
        self.background_leases.append(lease)
        return lease

    async def fork_conversation(self, name: str | None = None) -> str:
        self.fork_names.append(name)
        self.fork_started.set()
        if self.block_fork:
            await self.fork_release.wait()
        return f"child-{self.conversation_id}-{len(self.fork_names)}"


class FakeSession:
    def __init__(self, client: FakeClient, session_id: str) -> None:
        self.client = client
        self.id = session_id
        self.conversation_id = session_id
        self.run_id = f"run-{session_id}-{len(FakeClient.instances)}"
        self.done = False
        self.cancelled = False
        self.on_event: Callable[[dict[str, Any]], Any] | None = None
        self.run_calls: list[str] = []
        self.steer_calls: list[str] = []
        self.steer_request_ids: list[str] = []
        self.close_calls = 0
        self.run_started = asyncio.Event()
        self.steer_received = asyncio.Event()
        self.listeners: dict[str, list[Callable[[Any], Any]]] = {}

    def on(self, event_name: str, listener: Callable[[Any], Any]) -> None:
        self.listeners.setdefault(event_name, []).append(listener)

    def off(self, event_name: str, listener: Callable[[Any], Any]) -> None:
        listeners = self.listeners.get(event_name)
        if listeners is not None and listener in listeners:
            listeners.remove(listener)

    def emit(self, event_name: str, event: Any) -> None:
        for listener in list(self.listeners.get(event_name, [])):
            listener(event)

    async def wait(self, *, on_event: Callable[[dict[str, Any]], Any]) -> dict[str, Any]:
        task = str(self.client.create_session_calls[0]["message"])
        self.on_event = on_event
        self.run_calls.append(task)
        self.run_started.set()
        type(self.client).run_started.set()
        if type(self.client).block_runs:
            await type(self.client).run_release.wait()
        failure = type(self.client).run_failure
        if failure is not None:
            raise failure
        self.done = True
        self.client.closed = True
        return {"output": f"result for {task}", "done": True}

    async def steer(self, message: str, *, request_id: str) -> dict[str, str]:
        self.steer_calls.append(message)
        self.steer_request_ids.append(request_id)
        self.steer_received.set()
        outcome = (
            type(self.client).steer_outcomes.pop(0)
            if type(self.client).steer_outcomes
            else "injected"
            if type(self.client).accept_steering
            else "failed"
        )
        result = {"outcome": outcome}
        if outcome == "promptRequired":
            result["reason"] = "noRunningTurn"
        return result

    async def close(self) -> None:
        self.close_calls += 1

    async def cancel(self) -> None:
        await self.client.close()
        self.cancelled = True
        self.done = True

    async def read(self) -> dict[str, Any]:
        return {"done": self.done}


class FakeChildren:
    """Scoped child boundary; existing lifecycle fixtures control each admission."""

    def __init__(self, context: FakeContext) -> None:
        self.context = context
        self.start_calls: list[dict[str, Any]] = []

    async def start(self, **options: Any) -> FakeSession:
        self.start_calls.append(options)
        if options.get("context_mode") == "fork":
            name = str(options["lease"].description).split()[1]
            conversation_id = await self.context.fork_conversation(name)
        else:
            conversation_id = options.get("resume")
        client = FakeClient(command="unused", cwd=self.context.cwd, env={})
        child = await client.create_session(**options)
        if conversation_id is not None:
            child.id = child.conversation_id = conversation_id
        return child


class FakeClient:
    instances: ClassVar[list[FakeClient]] = []
    block_runs: ClassVar[bool] = False
    block_startup: ClassVar[bool] = False
    block_close: ClassVar[bool] = False
    close_failures_remaining: ClassVar[int] = 0
    accept_steering: ClassVar[bool] = True
    steer_outcomes: ClassVar[list[str]] = []
    run_failure: ClassVar[Exception | None] = None
    run_release: ClassVar[asyncio.Event]
    run_started: ClassVar[asyncio.Event]
    startup_release: ClassVar[asyncio.Event]
    startup_started: ClassVar[asyncio.Event]
    close_release: ClassVar[asyncio.Event]
    close_started: ClassVar[asyncio.Event]
    fresh_session_count: ClassVar[int] = 0

    @classmethod
    def reset(cls) -> None:
        cls.instances = []
        cls.block_runs = False
        cls.block_startup = False
        cls.block_close = False
        cls.close_failures_remaining = 0
        cls.accept_steering = True
        cls.steer_outcomes = []
        cls.run_failure = None
        cls.run_release = asyncio.Event()
        cls.run_started = asyncio.Event()
        cls.startup_release = asyncio.Event()
        cls.startup_started = asyncio.Event()
        cls.close_release = asyncio.Event()
        cls.close_started = asyncio.Event()
        cls.fresh_session_count = 0

    def __init__(
        self,
        *,
        command: str,
        cwd: str,
        env: Mapping[str, str],
    ) -> None:
        self.command = command
        self.cwd = cwd
        self.env = dict(env)
        self.create_session_calls: list[dict[str, object]] = []
        self.sessions: list[FakeSession] = []
        self.close_calls = 0
        self.closed = False
        type(self).instances.append(self)

    async def create_session(self, **kwargs: object) -> FakeSession:
        self.create_session_calls.append(kwargs)
        type(self).startup_started.set()
        if type(self).block_startup:
            await type(self).startup_release.wait()
        resume = kwargs.get("resume")
        if isinstance(resume, str):
            session_id = resume
        else:
            type(self).fresh_session_count += 1
            session_id = f"fresh-{type(self).fresh_session_count}"
        session = FakeSession(self, session_id)
        self.sessions.append(session)
        return session

    async def close(self) -> None:
        self.close_calls += 1
        type(self).close_started.set()
        if type(self).close_failures_remaining > 0:
            type(self).close_failures_remaining -= 1
            raise RuntimeError("temporary client close failure")
        if type(self).block_close:
            await type(self).close_release.wait()
        for session in self.sessions:
            await session.close()
        self.closed = True


async def wait_until(
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout_seconds: float = 2.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not await predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("condition was not satisfied")
        await asyncio.sleep(0.01)


class ScopedChildHost:
    """Exercise the real SDK while controlling the daemon's exact child state."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], bool]] = []
        self.executions: dict[str, dict[str, Any]] = {}
        self.reject_start: str | None = None
        self.block_start = False
        self.start_entered = asyncio.Event()
        self.start_release = asyncio.Event()
        self.steer_outcome = "injected"
        self.cancel_done = True
        self.reject_reads = False

    async def request(self, method: str, params: Any = None) -> Any:
        return await self.call(method, params, False)

    async def request_persistent(self, method: str, params: Any = None) -> Any:
        return await self.call(method, params, True)

    async def call(self, method: str, params: dict[str, Any], persistent: bool) -> Any:
        self.calls.append((method, dict(params), persistent))
        if method == "kodelet.child.start":
            self.start_entered.set()
            if self.reject_start:
                raise RuntimeError(self.reject_start)
            run_id = f"host-run-{len(self.executions) + 1}"
            result = {
                "conversationId": params.get("resume") or f"conversation-{run_id}",
                "runId": run_id,
                "done": False,
                "output": f"result for {params['message']}",
            }
            self.executions[run_id] = result
            if self.block_start:
                await self.start_release.wait()
            return dict(result)
        result = self.executions[params["childRunId"]]
        assert result["conversationId"] == params["childId"]
        if method == "kodelet.child.cancel":
            result["done"] = self.cancel_done
            return None
        if method == "kodelet.child.steer":
            return (
                {"outcome": "injected"}
                if self.steer_outcome == "injected"
                else {"outcome": "promptRequired", "reason": "noRunningTurn"}
            )
        assert method == "kodelet.child.read"
        if self.reject_reads:
            raise RuntimeError("transport unavailable")
        return dict(result)


class SubagentExtensionTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.cwd = self.root / "workspace"
        self.cwd.mkdir()
        self.data_dir = self.root / "extension-data"
        self.data_dir.mkdir()

        self.environment_patch = mock.patch.dict(
            os.environ,
            {extension.RECURSION_GUARD_ENV: "0"},
        )
        self.environment_patch.start()
        FakeClient.reset()
        self.runtime = RuntimeState()
        self.app = extension.SubagentApplication(self.runtime)
        self.ext = self.app.extension

    async def asyncTearDown(self) -> None:
        FakeClient.run_release.set()
        FakeClient.startup_release.set()
        FakeClient.close_release.set()
        for live in list(self.runtime.owned_runs.values()):
            background_lease = live.background_lease
            if isinstance(background_lease, FakeBackgroundTaskLease):
                background_lease.close_release.set()
            owned_task = live.runner_task or live.setup_task
            if owned_task is not None and not owned_task.done():
                owned_task.cancel()
        await asyncio.gather(
            *(
                owned_task
                for live in list(self.runtime.owned_runs.values())
                if (owned_task := live.runner_task or live.setup_task) is not None
            ),
            return_exceptions=True,
        )
        await asyncio.gather(*self.runtime.cleanup_tasks, return_exceptions=True)
        self.environment_patch.stop()
        self._temp.cleanup()

    def context(
        self,
        owner: str = "owner-1",
        *,
        data_dir: Path | None = None,
        invoked_by: str | None = None,
        block_fork: bool = False,
        block_background_acquire: bool = False,
        block_background_release: bool = False,
    ) -> Any:
        return FakeContext(
            owner,
            self.cwd,
            data_dir or self.data_dir,
            invoked_by=invoked_by,
            block_fork=block_fork,
            block_background_acquire=block_background_acquire,
            block_background_release=block_background_release,
        )

    async def store(
        self,
        name: str,
        runtime_id: str,
        *,
        clock: Callable[[], float] = time.time,
    ) -> Any:
        path = self.root / name / DATABASE_FILENAME
        store = AgentStore(path, runtime_id, clock=clock)
        await store.initialize()
        return store

    async def wait_for_status(
        self,
        store: Any,
        owner: str,
        agent_id: str,
        *statuses: str,
    ) -> Any:
        record: Any = None

        async def has_status() -> bool:
            nonlocal record
            record = await store.get(owner, agent_id)
            return record.run.status in statuses

        await wait_until(has_status)
        return record

    def steering_rows(self, path: Path) -> list[tuple[str, int, str]]:
        with sqlite3.connect(path) as connection:
            rows = connection.execute(
                "SELECT run_id, generation, message FROM steering_messages ORDER BY id"
            ).fetchall()
        return [(str(row[0]), int(row[1]), str(row[2])) for row in rows]

    async def test_application_instances_own_isolated_runtime_state(self) -> None:
        other_runtime = RuntimeState()
        other_application = extension.SubagentApplication(other_runtime)

        self.assertIs(self.app.runtime, self.runtime)
        self.assertIs(other_application.runtime, other_runtime)
        self.assertIsNot(self.runtime.stores, other_runtime.stores)
        self.assertIsNot(self.runtime.live_runs, other_runtime.live_runs)
        self.assertIsNot(self.runtime.owned_runs, other_runtime.owned_runs)
        self.assertIsNot(self.runtime.cleanup_tasks, other_runtime.cleanup_tasks)
        for removed_alias in (
            "_stores",
            "_live_runs",
            "_reservation_completions",
            "_widget_locks",
        ):
            self.assertFalse(hasattr(extension, removed_alias))

    async def test_capability_gate_and_main_only_recursion_guards(self) -> None:
        params = {
            "extension": {
                "id": "subagent",
                "cwd": str(self.cwd),
                "dataDir": str(self.data_dir),
            },
            "capabilities": {"runtime": {"backgroundTasks": True}},
        }
        enabled = self.ext.initialize(params)
        self.assertEqual(
            {tool["name"] for tool in enabled["tools"]},
            set(extension.AGENT_TOOL_NAMES),
        )
        wait_tool = next(tool for tool in enabled["tools"] if tool["name"] == "wait_agent")
        self.assertEqual(
            set(wait_tool["inputSchema"]["properties"]),
            {"agent_id", "timeout_ms"},
        )
        spawn_tool = next(tool for tool in enabled["tools"] if tool["name"] == "spawn_agent")
        self.assertEqual(
            set(spawn_tool["inputSchema"]["required"]),
            {"name", "task"},
        )

        for capabilities in (
            {"runtime": {"backgroundTasks": False}},
            {"runtime": {}},
            {},
        ):
            unavailable = self.ext.initialize({**params, "capabilities": capabilities})
            self.assertEqual(unavailable["tools"], [])

        with mock.patch.dict(os.environ, {RECURSION_GUARD_ENV: "1"}):
            child_application = extension.SubagentApplication(RuntimeState())
        child_initialized = child_application.extension.initialize(params)
        self.assertEqual(child_initialized["tools"], [])

        child_context = self.context(invoked_by="spawn_agent")
        self.assertTrue(extension.is_agent_child(child_context))
        self.assertFalse(extension.is_agent_child(self.context(invoked_by="main")))
        disabled = await self.app.disable_recursive_agents(cast(Any, {}), child_context)
        self.assertEqual(
            disabled,
            {"tools": {"disable": list(extension.AGENT_TOOL_NAMES)}},
        )
        nested = await self.app.spawn_agent(
            extension.SpawnAgentInput(name="nested-worker", task="nested work"),
            child_context,
        )
        self.assertIn("only available to the main agent", nested["error"])
        self.assertEqual(
            nested["data"]["presentation"],
            {"summary": "Spawn agent"},
        )
        with mock.patch.dict(os.environ, {RECURSION_GUARD_ENV: "1"}):
            self.assertTrue(extension.is_agent_child(self.context()))

    async def test_schema_wal_and_records_survive_store_reconstruction(self) -> None:
        context = self.context()
        store = await self.runtime.store_for_context(context)
        claim = await store.create(
            context.conversation_id,
            "persistence-worker",
            "persist this task",
            context.cwd,
            "fresh",
        )
        expected_path = self.data_dir / "subagents.sqlite"
        self.assertEqual(store.path, expected_path)
        self.assertTrue(expected_path.exists())

        with sqlite3.connect(expected_path) as connection:
            journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()[0]
            tables = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
        self.assertEqual(journal_mode, "wal")
        self.assertEqual(revision, "0002_canceling_state")
        self.assertTrue({"agents", "runs", "steering_messages"} <= tables)

        self.runtime.stores.clear()
        self.runtime.live_runs.clear()
        reconstructed = AgentStore(expected_path, "runtime-reconstructed")
        await reconstructed.initialize()
        persisted = await reconstructed.get(
            context.conversation_id,
            claim.agent.id,
            claim.lease.run_id,
        )
        self.assertEqual(persisted.run.task, "persist this task")
        self.assertEqual(persisted.name, "persistence-worker")
        self.assertEqual(persisted.status, "starting")

    async def test_spawn_fork_and_fresh_wait_and_list_ownership(self) -> None:
        context = self.context()
        FakeClient.block_runs = True
        forked = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="authentication-inspector",
                task="inspect authentication",
            ),
            context,
        )
        forked_data = forked["data"]
        self.assertEqual(forked_data["name"], "authentication-inspector")
        self.assertIn("authentication-inspector", forked["content"])
        self.assertEqual(
            forked_data["presentation"],
            {
                "summary": "Spawn authentication-inspector",
                "body": "inspect authentication",
                "format": "markdown",
            },
        )
        self.assertEqual(len(context.background_leases), 1)
        forked_lease = context.background_leases[0]
        self.assertIn(forked_data["agent_id"], forked_lease.description or "")
        self.assertEqual(forked_lease.close_calls, 0)
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)
        running = await self.wait_for_status(
            store,
            context.conversation_id,
            forked_data["agent_id"],
            "running",
        )
        self.assertEqual(running.conversation_id, "child-owner-1-1")
        self.assertEqual(running.context_mode, "fork")
        self.assertEqual(context.fork_names, ["authentication-inspector"])
        self.assertIn(running.conversation_id, forked["content"])
        self.assertEqual(
            context.children.start_calls[0]["context_mode"],
            "fork",
        )
        self.assertEqual(context.children.start_calls[0]["request_id"], forked_data["run_id"])
        self.assertIs(context.children.start_calls[0]["lease"], forked_lease)
        self.assertEqual(context.children.start_calls[0]["profile"], "subagent")
        self.assertIn("1 active", context.ui.text(WIDGET_ID))
        self.assertIn("authentication-inspector", context.ui.text(WIDGET_ID))

        FakeClient.run_release.set()
        completed = await self.app.wait_agent(
            extension.WaitAgentInput(
                agent_id=forked_data["agent_id"],
                timeout_ms=1_000,
            ),
            context,
        )
        self.assertEqual(completed["content"], "result for inspect authentication")
        self.assertEqual(completed["data"]["status"], "completed")
        self.assertEqual(
            completed["data"]["presentation"],
            {
                "summary": "Wait for authentication-inspector",
            },
        )
        self.assertIn("1 completed", context.ui.text(WIDGET_ID))

        async def forked_lease_closed() -> bool:
            return forked_lease.close_calls == 1

        await wait_until(forked_lease_closed)
        self.assertEqual(forked_lease.close_calls, 1)

        FakeClient.block_runs = False
        fresh = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="independent-reviewer",
                task="independent review",
                context_mode="fresh",
            ),
            context,
        )
        fresh_data = fresh["data"]
        fresh_result = await self.app.wait_agent(
            extension.WaitAgentInput(
                agent_id=fresh_data["agent_id"],
                timeout_ms=1_000,
            ),
            context,
        )
        self.assertEqual(fresh_result["content"], "result for independent review")
        fresh_client = FakeClient.instances[1]
        self.assertNotIn("resume", fresh_client.create_session_calls[0])
        self.assertEqual(fresh_result["data"]["context_mode"], "fresh")
        self.assertEqual(len(context.background_leases), 2)
        fresh_lease = context.background_leases[1]

        async def fresh_lease_closed() -> bool:
            return fresh_lease.close_calls == 1

        await wait_until(fresh_lease_closed)
        self.assertEqual(fresh_lease.close_calls, 1)

        listed = await self.app.list_agents(extension.ListAgentsInput(), context)
        self.assertEqual(len(listed["data"]["agents"]), 2)
        self.assertIn("authentication-inspector", listed["content"])
        self.assertIn("independent-reviewer", listed["content"])
        self.assertIn(running.conversation_id, listed["content"])
        self.assertIn(fresh_result["data"]["conversation_id"], listed["content"])
        list_presentation = listed["data"]["presentation"]
        self.assertEqual(list_presentation["summary"], "List agents")
        self.assertEqual(list_presentation["format"], "markdown")
        self.assertIn("**authentication-inspector** — completed", list_presentation["body"])
        self.assertIn("inspect authentication", list_presentation["body"])
        self.assertIn("**independent-reviewer** — completed", list_presentation["body"])
        self.assertIn("independent review", list_presentation["body"])
        self.assertNotIn(forked_data["agent_id"], list_presentation["body"])
        self.assertNotIn(running.conversation_id, list_presentation["body"])
        other_context = self.context("owner-2")
        other_list = await self.app.list_agents(
            extension.ListAgentsInput(),
            other_context,
        )
        self.assertEqual(other_list["data"]["agents"], [])
        self.assertEqual(
            other_list["data"]["presentation"],
            {
                "summary": "List agents",
                "body": "No background agents.",
                "format": "markdown",
            },
        )
        hidden = await self.app.wait_agent(
            extension.WaitAgentInput(
                agent_id=forked_data["agent_id"],
                timeout_ms=0,
            ),
            other_context,
        )
        self.assertIn("agent not found", hidden["error"])
        self.assertEqual(
            hidden["data"]["presentation"],
            {"summary": "Wait for agent"},
        )

        restored_context = self.context()
        await self.app.restore_agent_widget(cast(Any, {}), restored_context)
        restored_text = restored_context.ui.text(WIDGET_ID)
        self.assertIn("2 completed", restored_text)
        self.assertIn("independent-reviewer", restored_text)

    async def test_agent_names_are_canonical_and_unique_per_owner(self) -> None:
        store = await self.store("agent-names", "runtime-names")
        first = await store.create(
            "owner",
            "research-agent",
            "first task",
            str(self.cwd),
            "fresh",
        )
        self.assertEqual(first.agent.name, "research-agent")

        with self.assertRaisesRegex(
            AgentConflictError,
            "agent name already exists",
        ):
            await store.create(
                "owner",
                "research-agent",
                "duplicate task",
                str(self.cwd),
                "fresh",
            )

        other_owner = await store.create(
            "other-owner",
            "research-agent",
            "other task",
            str(self.cwd),
            "fresh",
        )
        self.assertEqual(other_owner.agent.name, "research-agent")

        for invalid_name in (
            "Research-agent",
            "research agent",
            "research_agent",
            "one-two-three-four",
            "-reviewer",
            "reviewer-",
            "reviewer--one",
            "1-reviewer",
        ):
            with (
                self.subTest(name=invalid_name),
                self.assertRaisesRegex(ValueError, "name must contain"),
            ):
                await store.create(
                    "owner",
                    invalid_name,
                    "invalid task",
                    str(self.cwd),
                    "fresh",
                )

    async def test_wait_current_run_and_followup_resumes_child(self) -> None:
        context = self.context()
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(name="iterative-reviewer", task="first pass"),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        first = await self.app.wait_agent(
            extension.WaitAgentInput(
                agent_id=agent_id,
                timeout_ms=1_000,
            ),
            context,
        )
        child_id = first["data"]["conversation_id"]

        FakeClient.block_runs = True
        FakeClient.run_release = asyncio.Event()
        FakeClient.run_started = asyncio.Event()
        followup = await self.app.followup_agent(
            extension.FollowupAgentInput(
                agent_id=agent_id,
                task="second pass",
            ),
            context,
        )
        self.assertIn("Follow-up task sent:\nsecond pass", followup["content"])
        self.assertEqual(followup["data"]["task"], "second pass")
        self.assertEqual(
            followup["data"]["presentation"],
            {
                "summary": "Follow up iterative-reviewer",
                "body": "second pass",
                "format": "markdown",
            },
        )
        second_run_id = followup["data"]["run_id"]
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        self.assertEqual(
            FakeClient.instances[1].create_session_calls[0]["resume"],
            child_id,
        )

        current = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=0),
            context,
        )
        self.assertEqual(current["data"]["run_id"], second_run_id)
        self.assertEqual(current["data"]["status"], "running")
        self.assertEqual(
            current["data"]["presentation"],
            {"summary": "Wait for iterative-reviewer"},
        )
        FakeClient.run_release.set()
        second = await self.app.wait_agent(
            extension.WaitAgentInput(
                agent_id=agent_id,
                timeout_ms=1_000,
            ),
            context,
        )
        self.assertEqual(second["content"], "result for second pass")

    async def test_wait_keeps_the_run_captured_when_followup_starts(self) -> None:
        context = self.context()
        FakeClient.block_runs = True
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="run-capture",
                task="first pass",
                context_mode="fresh",
            ),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        first_run_id = spawned["data"]["run_id"]
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)

        with mock.patch.object(extension, "WAIT_POLL_SECONDS", 0.2):
            waiting = asyncio.create_task(
                self.app.wait_agent(
                    extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000),
                    context,
                )
            )

            async def wait_started() -> bool:
                return bool(context.tool_updates)

            await wait_until(wait_started)
            first_session = FakeClient.instances[0].sessions[0]
            assert first_session.on_event is not None
            first_session.on_event(
                {
                    "sequence": 1,
                    "kind": "tool-result",
                    "toolCallId": "first-call",
                    "toolName": "bash",
                    "success": False,
                    "error": "first pass failed check",
                }
            )
            FakeClient.run_release.set()
            await self.wait_for_status(
                store,
                context.conversation_id,
                agent_id,
                "completed",
            )

            FakeClient.run_release = asyncio.Event()
            FakeClient.run_started = asyncio.Event()
            followup = await self.app.followup_agent(
                extension.FollowupAgentInput(
                    agent_id=agent_id,
                    task="second pass",
                ),
                context,
            )
            await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
            second_session = FakeClient.instances[-1].sessions[0]
            assert second_session.on_event is not None
            second_session.on_event(
                {
                    "sequence": 1,
                    "kind": "tool-result",
                    "toolCallId": "second-call",
                    "toolName": "file_read",
                    "success": True,
                }
            )
            first = await waiting

        self.assertEqual(first["content"], "result for first pass")
        self.assertEqual(first["data"]["run_id"], first_run_id)
        self.assertNotEqual(followup["data"]["run_id"], first_run_id)
        self.assertEqual(
            [activity["id"] for activity in first["data"]["taskRun"]["activities"]],
            ["first-call"],
        )
        self.assertEqual(first["data"]["taskRun"]["counts"]["failed"], 1)

        FakeClient.run_release.set()
        second = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000),
            context,
        )
        self.assertEqual(second["content"], "result for second pass")

    async def test_wait_streams_live_subagent_task_progress(self) -> None:
        context = self.context()
        FakeClient.block_runs = True
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="progress-inspector",
                task="inspect progress",
                context_mode="fresh",
            ),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        session = FakeClient.instances[0].sessions[0]

        waiting = asyncio.create_task(
            self.app.wait_agent(
                extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000),
                context,
            )
        )

        assert session.on_event is not None
        for event in [
            {
                "sequence": 1,
                "kind": "tool-use",
                "toolCallId": "bash-call",
                "toolName": "bash",
                "input": json.dumps({"command": "uv run pytest", "description": "Run tests"}),
            },
            {
                "sequence": 2,
                "kind": "tool-use",
                "toolCallId": "search-call",
                "toolName": "grep_tool",
                "input": json.dumps({"pattern": "progress", "path": f"{context.cwd}/src"}),
            },
            {
                "sequence": 3,
                "kind": "tool-update",
                "toolCallId": "bash-call",
                "toolOutput": "collecting tests\n3 tests collected",
            },
        ]:
            session.on_event(event)

        async def tool_activity_published() -> bool:
            for _content, data in context.tool_updates:
                task_run = (data or {}).get("taskRun")
                if not isinstance(task_run, dict):
                    continue
                activities = {activity["id"]: activity for activity in task_run["activities"]}
                if (
                    task_run["counts"]["running"] == 2
                    and activities.get("bash-call", {}).get("preview") == "3 tests collected"
                ):
                    return True
            return False

        await wait_until(tool_activity_published)
        for event in [
            {
                "sequence": 4,
                "kind": "tool-result",
                "toolCallId": "bash-call",
                "toolName": "bash",
                "success": False,
                "toolOutput": "test output",
                "error": "test assertion failed",
            },
            {
                "sequence": 5,
                "kind": "tool-result",
                "toolCallId": "search-call",
                "toolName": "grep_tool",
                "success": True,
                "toolOutput": "src/ui.py:18:progress",
            },
            {"sequence": 6, "kind": "text-delta", "text": "Here is the summary"},
        ]:
            session.on_event(event)
        # An active wait must see outcomes even when a burst evicts them from
        # replay history before the next database poll.
        for sequence in range(7, 7 + runtime_module.CHILD_EVENT_HISTORY_LIMIT):
            session.on_event({"sequence": sequence, "kind": "text-delta", "text": "summary"})

        async def responding_published() -> bool:
            return any(
                (data or {}).get("taskRun", {}).get("phase") == "responding"
                for _content, data in context.tool_updates
            )

        await wait_until(responding_published)
        FakeClient.run_release.set()
        completed = await waiting

        self.assertEqual(completed["content"], "result for inspect progress")
        task_run = completed["data"]["taskRun"]
        self.assertEqual(task_run["kind"], "subagent")
        self.assertEqual(task_run["title"], "Wait for progress-inspector")
        self.assertEqual(task_run["status"], "completed")
        activities = {activity["id"]: activity for activity in task_run["activities"]}
        self.assertEqual(activities["bash-call"]["kind"], "bash")
        self.assertEqual(activities["bash-call"]["label"], "Bash: Run tests")
        self.assertEqual(activities["bash-call"]["detail"], "run tests")
        self.assertEqual(activities["bash-call"]["status"], "failed")
        self.assertEqual(activities["bash-call"]["preview"], "test assertion failed")
        self.assertEqual(activities["search-call"]["kind"], "grep_tool")
        self.assertEqual(activities["search-call"]["label"], 'Search "progress" in src')
        self.assertEqual(activities["search-call"]["detail"], "searching src")
        self.assertEqual(activities["search-call"]["status"], "succeeded")
        self.assertNotIn("preview", activities["search-call"])
        self.assertEqual(task_run["counts"], {"succeeded": 1, "failed": 1, "running": 0})

    async def test_child_progress_handles_bounded_previews_and_missing_starts(self) -> None:
        progress = TaskProgress(
            cast(TaskProgressContext, self.context()),
            kind="subagent",
            task="check child events",
            cwd=str(self.cwd),
            running_title="Working",
            completed_title="Done",
            failed_title="Failed",
            responding_detail="agent is responding",
        )
        active_calls: set[str] = set()
        await progress.start()
        try:
            for index, tool_input in enumerate(['{"command": "truncated', '"string"', "null"]):
                with self.subTest(tool_input=tool_input):
                    call_id = f"call-{index}"
                    forward_child_progress(
                        progress,
                        {
                            "kind": "tool-use",
                            "toolCallId": call_id,
                            "toolName": "bash",
                            "input": tool_input,
                        },
                        str(self.cwd),
                        active_calls,
                    )
                    activity = progress.snapshot()["activities"][-1]
                    self.assertEqual(activity["label"], "bash")
                    self.assertEqual(activity["detail"], "running bash")
                    self.assertIn(call_id, active_calls)
                    forward_child_progress(
                        progress,
                        {"kind": "tool-result", "toolCallId": call_id, "success": True},
                        str(self.cwd),
                        active_calls,
                    )
                    self.assertNotIn(call_id, active_calls)

            forward_child_progress(
                progress,
                {
                    "kind": "tool-result",
                    "toolCallId": "missing-start",
                    "toolName": "file_read",
                    "success": False,
                    "toolOutput": "file not found",
                },
                str(self.cwd),
                active_calls,
            )
            activity = progress.snapshot()["activities"][-1]
            self.assertEqual(activity["label"], "file_read")
            self.assertEqual(activity["status"], "failed")
            self.assertEqual(activity["preview"], "file not found")
            self.assertEqual(active_calls, set())
            before = progress.snapshot()
            for event in [
                {"kind": "tool-use", "input": "{}"},
                {"kind": "tool-update", "toolCallId": "missing-update", "toolOutput": "ignored"},
                {"kind": "tool-result", "success": True},
                {"kind": "thinking"},
                {"kind": "result", "text": "done"},
            ]:
                forward_child_progress(progress, event, str(self.cwd), active_calls)
            self.assertEqual(progress.snapshot()["activities"], before["activities"])
            self.assertEqual(progress.snapshot()["phase"], "working")
            forward_child_progress(progress, {"kind": "text"}, str(self.cwd), active_calls)
            self.assertEqual(progress.snapshot()["phase"], "responding")
            self.assertEqual(progress.snapshot()["detail"], "agent is responding")
        finally:
            await progress.finish(success=True)

    async def test_wait_timeout_returns_running_progress_and_detaches(self) -> None:
        context = self.context()
        FakeClient.block_runs = True
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="long-runner",
                task="keep working",
                context_mode="fresh",
            ),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        session = FakeClient.instances[0].sessions[0]

        assert session.on_event is not None
        session.on_event(
            {
                "sequence": 1,
                "kind": "tool-use",
                "toolCallId": "pending-call",
                "toolName": "bash",
                "input": '{"description": "Run slow tests"}',
            }
        )
        result = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=20),
            context,
        )

        self.assertEqual(result["data"]["status"], "running")
        self.assertEqual(result["data"]["taskRun"]["title"], "Wait for long-runner")
        self.assertEqual(result["data"]["taskRun"]["status"], "running")
        self.assertEqual(result["data"]["taskRun"]["counts"]["running"], 1)
        self.assertEqual(result["data"]["taskRun"]["counts"]["succeeded"], 0)
        self.assertTrue(context.tool_updates)
        self.assertFalse(session.cancelled)
        store = await self.runtime.store_for_context(context)
        live = self.runtime.get_live_run(store, agent_id)
        assert live is not None
        self.assertEqual(live.event_listeners, set())

        session.on_event(
            {
                "sequence": 2,
                "kind": "tool-result",
                "toolCallId": "pending-call",
                "toolName": "bash",
                "success": False,
                "error": "slow tests failed",
            }
        )
        replayed = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=20),
            context,
        )
        self.assertEqual(replayed["data"]["taskRun"]["counts"]["failed"], 1)
        [activity] = replayed["data"]["taskRun"]["activities"]
        self.assertEqual(activity["label"], "Bash: Run slow tests")
        self.assertEqual(activity["preview"], "slow tests failed")

        FakeClient.run_release.set()
        completed = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000),
            context,
        )
        self.assertEqual(completed["content"], "result for keep working")

    async def test_waiters_independently_replay_bounded_child_history(self) -> None:
        context = self.context()
        host = ScopedChildHost()
        context.children = ChildClient(host)
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(name="history-agent", task="many calls"), context
        )
        agent_id = spawned["data"]["agent_id"]
        limit = runtime_module.CHILD_EVENT_HISTORY_LIMIT
        host.executions["host-run-1"]["events"] = [
            {
                "sequence": sequence,
                "kind": "tool-result",
                "toolCallId": f"call-{sequence}",
                "toolName": "file_read",
                "success": True,
            }
            for sequence in range(1, limit + 3)
        ]
        store = await self.runtime.store_for_context(context)
        live = self.runtime.get_live_run(store, agent_id)
        assert live is not None

        async def events_received() -> bool:
            return bool(live.events and live.events[-1]["sequence"] == limit + 2)

        await wait_until(events_received)
        self.assertEqual(len(live.events), limit)
        self.assertEqual(live.events[0]["sequence"], 3)
        results = await asyncio.gather(
            *(
                self.app.wait_agent(
                    extension.WaitAgentInput(agent_id=agent_id, timeout_ms=150),
                    self.context(),
                )
                for _ in range(2)
            )
        )
        for result in results:
            self.assertEqual(result["data"]["taskRun"]["status"], "running")
            self.assertEqual(
                result["data"]["taskRun"]["counts"],
                {"succeeded": limit, "failed": 0, "running": 0},
            )
            self.assertEqual(result["data"]["taskRun"]["omittedSucceeded"], limit - 3)
        self.assertEqual(len(live.events), limit)
        self.assertEqual(live.event_listeners, set())

        waiting = asyncio.create_task(
            self.app.wait_agent(extension.WaitAgentInput(agent_id=agent_id), self.context())
        )

        async def wait_subscribed() -> bool:
            return bool(live.event_listeners)

        await wait_until(wait_subscribed)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        self.assertEqual(live.event_listeners, set())
        self.assertFalse(any(method == "kodelet.child.cancel" for method, _, _ in host.calls))

    async def test_followup_reforks_when_interrupted_before_attachment(self) -> None:
        context = self.context()
        store = await self.runtime.store_for_context(context)
        initial = await store.create(
            context.conversation_id,
            "setup-agent",
            "initial setup",
            context.cwd,
            "fork",
        )
        interrupted = await store.terminal(
            initial.lease,
            "interrupted",
            error="setup interrupted",
        )
        self.assertIsNone(interrupted.conversation_id)

        followup = await self.app.followup_agent(
            extension.FollowupAgentInput(
                agent_id=initial.agent.id,
                task="retry setup",
            ),
            context,
        )
        self.assertEqual(context.fork_names, ["setup-agent"])
        self.assertEqual(
            followup["data"]["conversation_id"],
            "child-owner-1-1",
        )
        completed = await self.app.wait_agent(
            extension.WaitAgentInput(
                agent_id=initial.agent.id,
                timeout_ms=1_000,
            ),
            context,
        )
        self.assertEqual(completed["content"], "result for retry setup")

    async def test_store_limits_are_atomic_per_owner_and_globally(self) -> None:
        owner_path = self.root / "owner-limit" / DATABASE_FILENAME
        owner_stores = [AgentStore(owner_path, f"runtime-owner-{index}") for index in range(4)]
        await asyncio.gather(*(store.initialize() for store in owner_stores))
        owner_results = await asyncio.gather(
            *(
                store.create(
                    "one-owner",
                    f"agent-{index}",
                    f"task-{index}",
                    str(self.cwd),
                    "fresh",
                )
                for index, store in enumerate(owner_stores)
            ),
            return_exceptions=True,
        )
        self.assertEqual(
            sum(isinstance(result, Claim) for result in owner_results),
            3,
        )
        owner_errors = [result for result in owner_results if isinstance(result, BaseException)]
        self.assertEqual(len(owner_errors), 1)
        self.assertIsInstance(owner_errors[0], AgentLimitError)

        total_path = self.root / "total-limit" / DATABASE_FILENAME
        total_stores = [AgentStore(total_path, f"runtime-total-{index}") for index in range(9)]
        await asyncio.gather(*(store.initialize() for store in total_stores))
        total_results = await asyncio.gather(
            *(
                store.create(
                    f"owner-{index // 3}",
                    f"agent-{index}",
                    f"task-{index}",
                    str(self.cwd),
                    "fresh",
                )
                for index, store in enumerate(total_stores)
            ),
            return_exceptions=True,
        )
        self.assertEqual(
            sum(isinstance(result, Claim) for result in total_results),
            8,
        )
        total_errors = [result for result in total_results if isinstance(result, BaseException)]
        self.assertEqual(len(total_errors), 1)
        self.assertIsInstance(total_errors[0], AgentLimitError)
        self.assertIn("extension", str(total_errors[0]))

    async def test_expiry_reconciliation_and_generation_runtime_fencing(self) -> None:
        clock = MutableClock()
        store = await self.store("leases", "runtime-a", clock=clock)
        claim = await store.create(
            "owner",
            "lease-worker",
            "work",
            str(self.cwd),
            "fresh",
        )
        await store.mark_running(claim.lease, "child-lease")
        clock.advance(LEASE_DURATION_SECONDS + 0.1)
        self.assertEqual(await store.reconcile_expired(), 1)
        expired = await store.get("owner", claim.agent.id)
        self.assertEqual(expired.status, "interrupted")
        self.assertEqual(expired.run.status, "interrupted")
        self.assertIn("lease expired", expired.run.error)

        resumed = await store.claim("owner", claim.agent.id, "retry")
        with self.assertRaises(LeaseLostError):
            await store.heartbeat(claim.lease)

        second_runtime = AgentStore(
            store.path,
            "runtime-b",
            clock=clock,
        )
        await second_runtime.initialize()
        forged_runtime = replace(resumed.lease, runtime_id="runtime-b")
        with self.assertRaises(LeaseLostError):
            await second_runtime.heartbeat(forged_runtime)

    async def test_canceling_lease_expiry_recovers_to_resumable_canceled(self) -> None:
        clock = MutableClock()
        owner = await self.store("cancel-expiry", "runtime-owner", clock=clock)
        claim = await owner.create(
            "owner",
            "cancel-expiry-worker",
            "work",
            str(self.cwd),
            "fresh",
        )
        await owner.mark_running(claim.lease, "child-cancel-expiry")
        other = AgentStore(owner.path, "runtime-other", clock=clock)
        await other.initialize()

        canceling = await other.cancel("owner", claim.agent.id)
        self.assertEqual(canceling.status, "canceling")
        self.assertEqual(canceling.run.status, "canceled")
        with self.assertRaisesRegex(AgentConflictError, "agent is canceling"):
            await other.claim("owner", claim.agent.id, "too early")

        clock.advance(LEASE_DURATION_SECONDS - 10)
        renewed_expiry = await owner.heartbeat_canceling(claim.lease)
        self.assertEqual(renewed_expiry, clock.value + LEASE_DURATION_SECONDS)
        clock.advance(11)
        self.assertEqual(await other.reconcile_expired(), 0)
        still_canceling = await other.get("owner", claim.agent.id)
        self.assertEqual(still_canceling.status, "canceling")

        clock.advance(LEASE_DURATION_SECONDS - 10 + 0.1)
        self.assertEqual(await other.reconcile_expired(), 1)
        canceled = await other.get("owner", claim.agent.id)
        self.assertEqual(canceled.status, "canceled")
        resumed = await other.claim("owner", claim.agent.id, "resume after recovery")
        self.assertEqual(resumed.agent.generation, 2)
        self.assertFalse(await owner.complete_cancel(claim.lease))
        with self.assertRaises(LeaseLostError):
            await owner.heartbeat(claim.lease)

    async def test_session_end_interrupts_only_current_runtime_rows(self) -> None:
        path = self.root / "session-end" / DATABASE_FILENAME
        current = AgentStore(path, self.runtime.runtime_id)
        other = AgentStore(path, "other-runtime")
        await current.initialize()
        await other.initialize()
        current_claim = await current.create(
            "current-owner",
            "current-worker",
            "current task",
            str(self.cwd),
            "fresh",
        )
        other_claim = await other.create(
            "other-owner",
            "other-worker",
            "other task",
            str(self.cwd),
            "fresh",
        )
        self.runtime.stores[path] = current

        await self.app.interrupt_live_agents(cast(Any, {}), self.context())

        current_record = await current.get("current-owner", current_claim.agent.id)
        other_record = await current.get("other-owner", other_claim.agent.id)
        self.assertEqual(current_record.status, "interrupted")
        self.assertEqual(current_record.run.status, "interrupted")
        self.assertEqual(other_record.status, "starting")
        self.assertEqual(other_record.run.status, "starting")

    async def test_steering_is_run_scoped_delivered_and_acknowledged(
        self,
    ) -> None:
        store = await self.store("steering", "runtime-steering")
        claim = await store.create(
            "owner",
            "parser-worker",
            "long task",
            str(self.cwd),
            "fresh",
        )
        await store.mark_running(claim.lease, "child-steering")
        first = await store.enqueue_steering(
            "owner",
            claim.agent.id,
            "focus on the parser",
        )
        self.assertEqual(first, {"accepted": True, "alreadyPending": False})
        self.assertEqual(
            self.steering_rows(store.path),
            [(claim.lease.run_id, claim.lease.generation, "focus on the parser")],
        )

        client = FakeClient(command="kodelet", cwd=str(self.cwd), env={})
        session = FakeSession(client, "child-steering")
        live = self.runtime.live_run_from_claim(claim, "long task", store)
        live.conversation_id = session.id
        FakeClient.steer_outcomes = ["failed", "injected"]
        with (
            mock.patch.object(runtime_module, "STEERING_POLL_SECONDS", 0.01),
            mock.patch.object(runtime_module, "STEERING_RETRY_SECONDS", 0.01),
        ):
            pump = asyncio.create_task(self.runtime.steering_pump(live, cast(Any, session)))
            try:
                await asyncio.wait_for(session.steer_received.wait(), timeout=1)

                async def acknowledged() -> bool:
                    return not self.steering_rows(store.path)

                await wait_until(acknowledged)
            finally:
                pump.cancel()
                await asyncio.gather(pump, return_exceptions=True)
        self.assertEqual(
            session.steer_calls,
            ["focus on the parser", "focus on the parser"],
        )
        self.assertEqual(len(set(session.steer_request_ids)), 1)
        self.assertIsNone(await store.next_steering(claim.lease))

        await store.enqueue_steering(
            "owner",
            claim.agent.id,
            "message carried into the follow-up run",
        )
        terminal = await store.terminal(
            claim.lease,
            "idle",
            conversation_id=session.id,
            result="done",
        )
        self.assertEqual(terminal.run.status, "completed")
        self.assertEqual(
            self.steering_rows(store.path),
            [
                (
                    claim.lease.run_id,
                    claim.lease.generation,
                    "message carried into the follow-up run",
                )
            ],
        )

        followup = await store.claim("owner", claim.agent.id, "follow-up task")
        self.assertEqual(
            self.steering_rows(store.path),
            [
                (
                    followup.lease.run_id,
                    followup.lease.generation,
                    "message carried into the follow-up run",
                )
            ],
        )
        await store.mark_running(followup.lease, session.id)
        carried = await store.next_steering(followup.lease)
        self.assertIsNotNone(carried)
        assert carried is not None
        self.assertEqual(carried.message, "message carried into the follow-up run")
        self.assertTrue(await store.acknowledge_steering(followup.lease, carried.id))
        self.assertEqual(self.steering_rows(store.path), [])

    async def test_steer_result_exposes_the_normalized_queued_message(self) -> None:
        context = self.context()
        store = await self.runtime.store_for_context(context)
        claim = await store.create(
            context.conversation_id,
            "steerable-worker",
            "long task",
            context.cwd,
            "fresh",
        )
        await store.mark_running(claim.lease, "child-steerable")

        result = await self.app.steer_agent(
            extension.SteerAgentInput(
                agent_id=claim.agent.id,
                message="  focus on the parser  ",
            ),
            context,
        )

        self.assertIn(
            "Steering message queued for delivery:\nfocus on the parser",
            result["content"],
        )
        self.assertIn("steerable-worker", result["content"])
        self.assertEqual(result["data"]["name"], "steerable-worker")
        self.assertEqual(result["data"]["message"], "focus on the parser")
        self.assertEqual(
            result["data"]["presentation"],
            {
                "summary": "Steer steerable-worker",
                "body": "focus on the parser",
                "format": "markdown",
            },
        )
        self.assertTrue(result["data"]["accepted"])
        self.assertEqual(
            self.steering_rows(store.path),
            [(claim.lease.run_id, claim.lease.generation, "focus on the parser")],
        )

    async def test_cancel_persists_cleanup(self) -> None:
        context = self.context()
        FakeClient.block_runs = True
        FakeClient.block_close = True
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="cancelable-worker",
                task="cancel me",
                context_mode="fresh",
            ),
            context,
        )
        self.assertEqual(len(context.background_leases), 1)
        background_lease = context.background_leases[0]
        self.assertEqual(background_lease.close_calls, 0)
        agent_id = spawned["data"]["agent_id"]
        first_run_id = spawned["data"]["run_id"]
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)
        running = await store.get(context.conversation_id, agent_id)
        child_id = running.conversation_id
        assert child_id is not None
        await store.enqueue_steering(context.conversation_id, agent_id, "pending")

        with mock.patch.object(runtime_module, "CANCEL_CLEANUP_TIMEOUT_SECONDS", 0.01):
            canceled = await self.app.cancel_agent(
                extension.CancelAgentInput(agent_id=agent_id),
                context,
            )
            blocked_followup = await self.app.followup_agent(
                extension.FollowupAgentInput(
                    agent_id=agent_id,
                    task="conclude after cancellation",
                ),
                context,
            )
        self.assertIn("cleanup is still finishing", canceled["content"])
        self.assertEqual(
            canceled["data"]["presentation"],
            {
                "summary": "Cancel cancelable-worker",
                "body": (
                    "Cancellation saved; worker cleanup is still finishing. "
                    "Retry followup_agent after cleanup completes."
                ),
                "format": "markdown",
            },
        )
        self.assertIn("cancellation is still in progress", blocked_followup["error"])
        self.assertEqual(len(FakeClient.instances), 1)
        self.assertNotIn(agent_id, canceled["data"]["presentation"]["body"])
        persisted = await store.get(context.conversation_id, agent_id)
        self.assertEqual(persisted.status, "canceling")
        self.assertEqual(persisted.run.status, "canceled")
        self.assertEqual(persisted.generation, 1)
        self.assertEqual(self.steering_rows(store.path), [])
        self.assertIn("1 canceling", context.ui.text(WIDGET_ID))

        waited = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=0),
            context,
        )
        self.assertIn("still canceling", waited["content"])

        client = FakeClient.instances[0]
        session = client.sessions[0]
        self.assertFalse(client.closed)
        await asyncio.wait_for(FakeClient.close_started.wait(), timeout=1)
        FakeClient.close_release.set()

        key = self.runtime.live_run_key(store, agent_id)

        async def cleaned_up() -> bool:
            return key not in self.runtime.live_runs

        await wait_until(cleaned_up)
        persisted = await store.get(context.conversation_id, agent_id)
        self.assertEqual(persisted.status, "canceled")
        self.assertTrue(client.closed)
        self.assertEqual(client.close_calls, 1)
        self.assertEqual(session.close_calls, 1)
        self.assertEqual(background_lease.close_calls, 1)
        self.assertEqual(self.runtime.owned_runs, {})

        canceled_again = await self.app.cancel_agent(
            extension.CancelAgentInput(agent_id=agent_id),
            context,
        )
        self.assertEqual(
            canceled_again["data"]["presentation"],
            {
                "summary": "Cancel cancelable-worker",
                "body": "Canceled. Use followup_agent to resume this agent later.",
                "format": "markdown",
            },
        )
        self.assertNotIn(agent_id, canceled_again["content"])

        FakeClient.run_release = asyncio.Event()
        FakeClient.run_started = asyncio.Event()
        followup = await self.app.followup_agent(
            extension.FollowupAgentInput(
                agent_id=agent_id,
                task="conclude after cancellation",
            ),
            context,
        )
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        self.assertEqual(followup["data"]["generation"], 2)
        self.assertNotEqual(followup["data"]["run_id"], first_run_id)
        self.assertEqual(FakeClient.instances[1].create_session_calls[0]["resume"], child_id)
        historical = await store.get(
            context.conversation_id,
            agent_id,
            first_run_id,
        )
        self.assertEqual(historical.run.status, "canceled")

        FakeClient.run_release.set()
        completed = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000),
            context,
        )
        self.assertEqual(completed["content"], "result for conclude after cancellation")

    async def test_cancel_becomes_resumable_before_background_lease_release(self) -> None:
        context = self.context(block_background_release=True)
        FakeClient.block_runs = True
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="cancel-lease-worker",
                task="cancel before lease release",
                context_mode="fresh",
            ),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)
        background_lease = context.background_leases[0]

        with mock.patch.object(runtime_module, "CANCEL_CLEANUP_TIMEOUT_SECONDS", 0.01):
            canceled = await self.app.cancel_agent(
                extension.CancelAgentInput(agent_id=agent_id),
                context,
            )
        await asyncio.wait_for(background_lease.close_started.wait(), timeout=1)
        self.assertIn("it can now be resumed", canceled["content"])
        persisted = await store.get(context.conversation_id, agent_id)
        self.assertEqual(persisted.status, "canceled")
        self.assertTrue(self.runtime.owned_runs)

        context.block_background_release = False
        FakeClient.run_started = asyncio.Event()
        followup = await self.app.followup_agent(
            extension.FollowupAgentInput(
                agent_id=agent_id,
                task="resume while old lease closes",
            ),
            context,
        )
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        self.assertEqual(followup["data"]["generation"], 2)
        background_lease.close_release.set()
        FakeClient.run_release.set()
        completed = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000),
            context,
        )
        self.assertEqual(completed["content"], "result for resume while old lease closes")

    async def test_cancel_retries_client_close_before_becoming_resumable(self) -> None:
        context = self.context()
        FakeClient.block_runs = True
        FakeClient.close_failures_remaining = 100
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="cancel-close-worker",
                task="retry close",
                context_mode="fresh",
            ),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)

        with (
            mock.patch.object(runtime_module, "CANCEL_CLEANUP_TIMEOUT_SECONDS", 0.01),
            mock.patch.object(runtime_module, "CHILD_CANCEL_RETRY_INITIAL_SECONDS", 0.01),
            mock.patch.object(runtime_module, "CHILD_CANCEL_RETRY_MAX_SECONDS", 0.01),
        ):
            canceled = await self.app.cancel_agent(
                extension.CancelAgentInput(agent_id=agent_id),
                context,
            )
            self.assertIn("worker cleanup is still finishing", canceled["content"])
            persisted = await store.get(context.conversation_id, agent_id)
            self.assertEqual(persisted.status, "canceling")
            self.assertTrue(self.runtime.owned_runs)
            blocked = await self.app.followup_agent(
                extension.FollowupAgentInput(
                    agent_id=agent_id,
                    task="too early",
                ),
                context,
            )
            self.assertIn("cancellation is still in progress", blocked["error"])

            FakeClient.close_failures_remaining = 0

            async def cancellation_finished() -> bool:
                record = await store.get(context.conversation_id, agent_id)
                return record.status == "canceled"

            await wait_until(cancellation_finished)

        client = FakeClient.instances[0]
        self.assertTrue(client.closed)
        self.assertGreater(client.close_calls, 1)

    async def test_canceling_blocks_cross_runtime_followup_until_owner_cleanup(self) -> None:
        context = self.context()
        FakeClient.block_runs = True
        FakeClient.block_close = True
        with (
            mock.patch.object(runtime_module, "HEARTBEAT_INTERVAL_SECONDS", 0.01),
            mock.patch.object(runtime_module, "MIN_HEARTBEAT_INTERVAL_SECONDS", 0.001),
        ):
            spawned = await self.app.spawn_agent(
                extension.SpawnAgentInput(
                    name="shared-runtime-worker",
                    task="first generation",
                    context_mode="fresh",
                ),
                context,
            )
            agent_id = spawned["data"]["agent_id"]
            first_run_id = spawned["data"]["run_id"]
            await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
            owner_store = await self.runtime.store_for_context(context)

            other_runtime = RuntimeState(runtime_id="other-runtime")
            other_app = extension.SubagentApplication(other_runtime)
            canceled = await other_app.cancel_agent(
                extension.CancelAgentInput(agent_id=agent_id),
                context,
            )
            self.assertIn("worker cleanup is still finishing", canceled["content"])
            blocked = await other_app.followup_agent(
                extension.FollowupAgentInput(
                    agent_id=agent_id,
                    task="second generation",
                ),
                context,
            )
            self.assertIn("agent cancellation is still in progress", blocked["error"])
            self.assertEqual(len(FakeClient.instances), 1)

            await asyncio.wait_for(FakeClient.close_started.wait(), timeout=1)
            canceling = await owner_store.get(context.conversation_id, agent_id)
            self.assertEqual(canceling.status, "canceling")
            self.assertEqual(canceling.run.status, "canceled")
            FakeClient.close_release.set()

            async def cancellation_finished() -> bool:
                record = await owner_store.get(context.conversation_id, agent_id)
                return record.status == "canceled"

            await wait_until(cancellation_finished)
            historical = await owner_store.get(
                context.conversation_id,
                agent_id,
                first_run_id,
            )
            self.assertEqual(historical.run.status, "canceled")

            FakeClient.run_started = asyncio.Event()
            followup = await other_app.followup_agent(
                extension.FollowupAgentInput(
                    agent_id=agent_id,
                    task="second generation",
                ),
                context,
            )
            await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
            self.assertEqual(followup["data"]["generation"], 2)
            self.assertNotEqual(followup["data"]["run_id"], first_run_id)

            FakeClient.run_release.set()
            completed = await other_app.wait_agent(
                extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000),
                context,
            )
            self.assertEqual(completed["content"], "result for second generation")

            await other_runtime.shutdown()

    async def test_cancel_owns_prelaunch_fork_setup_before_resuming(self) -> None:
        context = self.context(block_fork=True)
        spawning = asyncio.create_task(
            self.app.spawn_agent(
                extension.SpawnAgentInput(
                    name="cancel-setup-worker",
                    task="first attempt",
                ),
                context,
            )
        )
        await asyncio.wait_for(context.fork_started.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)
        [reserved] = await store.list(context.conversation_id)
        live = self.runtime.get_live_run(store, reserved.id)
        assert live is not None
        self.assertIs(live.setup_task, spawning)
        self.assertIsNone(live.runner_task)

        canceled = await self.app.cancel_agent(
            extension.CancelAgentInput(agent_id=reserved.id),
            context,
        )
        self.assertIn("Use followup_agent to resume it later", canceled["content"])
        with self.assertRaises(asyncio.CancelledError):
            await spawning
        persisted = await store.get(context.conversation_id, reserved.id)
        self.assertEqual(persisted.status, "canceled")
        self.assertEqual(persisted.run.status, "canceled")
        self.assertIsNone(persisted.conversation_id)
        self.assertEqual(FakeClient.instances, [])
        self.assertEqual(self.runtime.live_runs, {})
        self.assertEqual(self.runtime.owned_runs, {})

        context.block_fork = False
        followup = await self.app.followup_agent(
            extension.FollowupAgentInput(
                agent_id=reserved.id,
                task="retry after setup cancellation",
            ),
            context,
        )
        self.assertEqual(followup["data"]["conversation_id"], "child-owner-1-2")
        completed = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=reserved.id, timeout_ms=1_000),
            context,
        )
        self.assertEqual(completed["content"], "result for retry after setup cancellation")

    async def test_cancel_preserves_fork_created_before_attachment(self) -> None:
        context = self.context()
        store = await self.runtime.store_for_context(context)
        attach_started = asyncio.Event()
        attach_release = asyncio.Event()
        cancel_attach_started = asyncio.Event()
        cancel_attach_release = asyncio.Event()
        original_attach = store.attach_conversation
        original_cancel_attach = store.attach_canceling_conversation

        async def delayed_attach(*args: Any, **kwargs: Any) -> Any:
            attach_started.set()
            await attach_release.wait()
            return await original_attach(*args, **kwargs)

        async def delayed_cancel_attach(*args: Any, **kwargs: Any) -> Any:
            cancel_attach_started.set()
            await cancel_attach_release.wait()
            return await original_cancel_attach(*args, **kwargs)

        with (
            mock.patch.object(store, "attach_conversation", new=delayed_attach),
            mock.patch.object(
                store,
                "attach_canceling_conversation",
                new=delayed_cancel_attach,
            ),
            mock.patch.object(runtime_module, "CANCEL_CLEANUP_TIMEOUT_SECONDS", 0.01),
        ):
            spawning = asyncio.create_task(
                self.app.spawn_agent(
                    extension.SpawnAgentInput(
                        name="cancel-fork-worker",
                        task="cancel after fork",
                    ),
                    context,
                )
            )
            await asyncio.wait_for(attach_started.wait(), timeout=1)
            [reserved] = await store.list(context.conversation_id)
            canceled = await self.app.cancel_agent(
                extension.CancelAgentInput(agent_id=reserved.id),
                context,
            )
            await asyncio.wait_for(cancel_attach_started.wait(), timeout=1)
            self.assertIn("worker cleanup is still finishing", canceled["content"])
            blocked = await self.app.followup_agent(
                extension.FollowupAgentInput(
                    agent_id=reserved.id,
                    task="too early",
                ),
                context,
            )
            self.assertIn("cancellation is still in progress", blocked["error"])
            cancel_attach_release.set()
            with self.assertRaises(asyncio.CancelledError):
                await spawning

        persisted = await store.get(context.conversation_id, reserved.id)
        self.assertEqual(persisted.status, "canceled")
        self.assertEqual(persisted.conversation_id, "child-owner-1-1")
        self.assertEqual(context.fork_names, ["cancel-fork-worker"])

        followup = await self.app.followup_agent(
            extension.FollowupAgentInput(
                agent_id=reserved.id,
                task="resume preserved fork",
            ),
            context,
        )
        self.assertEqual(followup["data"]["conversation_id"], "child-owner-1-1")
        self.assertEqual(context.fork_names, ["cancel-fork-worker"])
        completed = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=reserved.id, timeout_ms=1_000),
            context,
        )
        self.assertEqual(completed["content"], "result for resume preserved fork")

    async def test_terminal_cancel_response_reloads_concurrent_generation(self) -> None:
        context = self.context()
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="cancel-response-worker",
                task="initial task",
                context_mode="fresh",
            ),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000),
            context,
        )
        store = await self.runtime.store_for_context(context)
        original_cancel = store.cancel
        raced_claim: Claim | None = None

        async def cancel_then_claim(owner_id: str, selected_agent_id: str) -> Any:
            nonlocal raced_claim
            canceled = await original_cancel(owner_id, selected_agent_id)
            raced_claim = await store.claim(owner_id, selected_agent_id, "concurrent follow-up")
            return canceled

        with mock.patch.object(store, "cancel", new=cancel_then_claim):
            canceled = await self.app.cancel_agent(
                extension.CancelAgentInput(agent_id=agent_id),
                context,
            )

        assert raced_claim is not None
        self.assertIn("agent has since changed generation", canceled["content"])
        self.assertEqual(canceled["data"]["run_id"], raced_claim.lease.run_id)
        await store.terminal(
            raced_claim.lease,
            "interrupted",
            error="test cleanup",
        )

    async def test_cancel_owns_blocked_background_acquisition(self) -> None:
        context = self.context(block_background_acquire=True)
        spawning = asyncio.create_task(
            self.app.spawn_agent(
                extension.SpawnAgentInput(
                    name="cancel-acquire-worker",
                    task="wait for background ownership",
                    context_mode="fresh",
                ),
                context,
            )
        )
        await asyncio.wait_for(context.background_acquire_started.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)
        [reserved] = await store.list(context.conversation_id)

        canceled = await self.app.cancel_agent(
            extension.CancelAgentInput(agent_id=reserved.id),
            context,
        )
        self.assertIn("Use followup_agent to resume it later", canceled["content"])
        with self.assertRaises(asyncio.CancelledError):
            await spawning
        persisted = await store.get(context.conversation_id, reserved.id)
        self.assertEqual(persisted.status, "canceled")
        self.assertEqual(persisted.run.status, "canceled")
        self.assertEqual(context.background_leases, [])
        self.assertEqual(FakeClient.instances, [])
        self.assertEqual(self.runtime.live_runs, {})
        self.assertEqual(self.runtime.owned_runs, {})

    async def test_completed_run_remains_owned_until_background_lease_release(
        self,
    ) -> None:
        context = self.context(block_background_release=True)
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="lease-cleanup-worker",
                task="finish before lease release",
                context_mode="fresh",
            ),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        store = await self.runtime.store_for_context(context)
        background_lease = context.background_leases[0]

        await asyncio.wait_for(background_lease.close_started.wait(), timeout=1)
        key = self.runtime.live_run_key(store, agent_id)
        live = self.runtime.live_runs[key]
        self.assertTrue(FakeClient.instances[0].closed)
        self.assertIsNotNone(live.cleanup_task)
        self.assertIn(live.cleanup_task, self.runtime.cleanup_tasks)

        shutdown = asyncio.create_task(self.runtime.shutdown())
        await asyncio.sleep(0)
        self.assertFalse(shutdown.done())
        self.assertIn(key, self.runtime.live_runs)

        background_lease.close_release.set()
        await asyncio.wait_for(shutdown, timeout=1)

        self.assertEqual(background_lease.close_calls, 1)
        self.assertNotIn(key, self.runtime.live_runs)
        self.assertEqual(self.runtime.owned_runs, {})
        self.assertEqual(self.runtime.cleanup_tasks, set())

    async def test_shutdown_during_child_cancel_still_releases_background_lease(
        self,
    ) -> None:
        context = self.context()
        FakeClient.block_close = True
        FakeClient.run_failure = RuntimeError("child read failed")
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="client-cleanup-worker",
                task="finish before client close",
                context_mode="fresh",
            ),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        store = await self.runtime.store_for_context(context)
        background_lease = context.background_leases[0]

        await asyncio.wait_for(FakeClient.close_started.wait(), timeout=1)
        key = self.runtime.live_run_key(store, agent_id)
        self.assertIn(key, self.runtime.live_runs)
        self.assertEqual(background_lease.close_calls, 0)

        shutdown = asyncio.create_task(self.runtime.shutdown())
        await asyncio.sleep(0)
        self.assertFalse(shutdown.done())
        self.assertIn(key, self.runtime.live_runs)

        FakeClient.close_release.set()
        await asyncio.wait_for(shutdown, timeout=1)

        client = FakeClient.instances[0]
        self.assertTrue(client.closed)
        self.assertEqual(client.close_calls, 1)
        self.assertEqual(background_lease.close_calls, 1)
        self.assertNotIn(key, self.runtime.live_runs)
        self.assertEqual(self.runtime.owned_runs, {})
        self.assertEqual(self.runtime.cleanup_tasks, set())

    async def test_shutdown_owns_prior_run_during_immediate_followup(self) -> None:
        context = self.context()
        FakeClient.block_runs = True
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(
                name="handoff-worker",
                task="first generation",
                context_mode="fresh",
            ),
            context,
        )
        agent_id = spawned["data"]["agent_id"]
        first_run_id = spawned["data"]["run_id"]
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)
        first_live = self.runtime.get_live_run(store, agent_id)
        assert first_live is not None
        first_runner = first_live.runner_task
        assert first_runner is not None
        first_client = FakeClient.instances[0]
        first_background_lease = context.background_leases[0]

        terminal_committed = asyncio.Event()
        terminal_release = asyncio.Event()
        original_terminal = store.terminal

        async def delayed_terminal(*args: Any, **kwargs: Any) -> Any:
            record = await original_terminal(*args, **kwargs)
            lease = args[0]
            status = args[1]
            if lease.run_id == first_run_id and status == "idle":
                terminal_committed.set()
                await terminal_release.wait()
            return record

        shutdown: asyncio.Task[None] | None = None
        with mock.patch.object(store, "terminal", new=delayed_terminal):
            FakeClient.run_release.set()
            await asyncio.wait_for(terminal_committed.wait(), timeout=1)
            persisted = await store.get(context.conversation_id, agent_id)
            self.assertEqual(persisted.run.status, "completed")
            self.assertIsNone(first_live.cleanup_task)

            FakeClient.run_release = asyncio.Event()
            FakeClient.run_started = asyncio.Event()
            followup = await self.app.followup_agent(
                extension.FollowupAgentInput(
                    agent_id=agent_id,
                    task="second generation",
                ),
                context,
            )
            await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
            second_live = self.runtime.get_live_run(store, agent_id)
            assert second_live is not None
            self.assertEqual(second_live.run_id, followup["data"]["run_id"])
            self.assertIsNot(second_live, first_live)
            self.assertIn(
                self.runtime.owned_run_key(store, first_run_id),
                self.runtime.owned_runs,
            )
            self.assertIn(
                self.runtime.owned_run_key(store, second_live.run_id),
                self.runtime.owned_runs,
            )

            shutdown = asyncio.create_task(self.runtime.shutdown())
            try:
                await asyncio.wait_for(asyncio.shield(shutdown), timeout=1)
                self.assertTrue(first_runner.done())
                self.assertTrue(first_client.closed)
                self.assertEqual(first_background_lease.close_calls, 1)
                self.assertEqual(context.background_leases[1].close_calls, 1)
                self.assertEqual(self.runtime.owned_runs, {})
            finally:
                terminal_release.set()
                if not shutdown.done():
                    await asyncio.wait_for(shutdown, timeout=1)

    async def test_startup_timeout_releases_lease_and_removes_unattached_reservation(self) -> None:
        context = self.context()
        FakeClient.block_startup = True
        with mock.patch.object(runtime_module, "AGENT_START_TIMEOUT_SECONDS", 0.02):
            spawned = await self.app.spawn_agent(
                extension.SpawnAgentInput(
                    name="slow-starter",
                    task="never starts",
                    context_mode="fresh",
                ),
                context,
            )
            await asyncio.wait_for(FakeClient.startup_started.wait(), timeout=1)
            store = await self.runtime.store_for_context(context)
            self.assertEqual(await store.list(context.conversation_id), [])
        self.assertIn("error", spawned)
        self.assertEqual(self.runtime.owned_runs, {})

        async def background_lease_closed() -> bool:
            return bool(context.background_leases and context.background_leases[0].close_calls == 1)

        await wait_until(background_lease_closed)

    async def test_background_lease_release_retries_transient_failures(self) -> None:
        context = self.context()
        context.background_release_failures = 1
        with mock.patch.object(
            runtime_module,
            "BACKGROUND_LEASE_RELEASE_RETRY_INITIAL_SECONDS",
            0.001,
        ):
            spawned = await self.app.spawn_agent(
                extension.SpawnAgentInput(
                    name="lease-releaser",
                    task="retry lease release",
                    context_mode="fresh",
                ),
                context,
            )
            await self.app.wait_agent(
                extension.WaitAgentInput(
                    agent_id=spawned["data"]["agent_id"],
                    timeout_ms=1_000,
                ),
                context,
            )

            async def released() -> bool:
                return bool(
                    context.background_leases and context.background_leases[0].close_calls == 2
                )

            await wait_until(released)

    async def test_terminal_retry_and_idempotency(self) -> None:
        clock = MutableClock()
        store = await self.store("terminal-retry", "runtime-retry", clock=clock)
        claim = await store.create(
            "owner",
            "retry-worker",
            "retry update",
            str(self.cwd),
            "fresh",
        )
        await store.mark_running(claim.lease, "child-retry")
        live = self.runtime.live_run_from_claim(claim, "retry update", store)
        live.conversation_id = "child-retry"
        original_terminal = store.terminal
        attempts = 0

        async def flaky_terminal(*args: Any, **kwargs: Any) -> Any:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise sqlite3.OperationalError("database is busy")
            return await original_terminal(*args, **kwargs)

        with (
            mock.patch.object(store, "terminal", new=flaky_terminal),
            mock.patch.object(
                runtime_module,
                "WORKER_UPDATE_RETRY_INITIAL_SECONDS",
                0.001,
            ),
            mock.patch.object(runtime_module, "WORKER_UPDATE_RETRY_MAX_SECONDS", 0.002),
        ):
            committed = await self.runtime.safe_worker_terminal(
                live,
                "idle",
                result="final answer",
            )
        self.assertTrue(committed)
        self.assertEqual(attempts, 3)

        repeated = await store.terminal(
            claim.lease,
            "idle",
            conversation_id="child-retry",
            result="final answer",
        )
        self.assertEqual(repeated.run.status, "completed")
        self.assertEqual(repeated.run.result, "final answer")
        with self.assertRaises(LeaseLostError):
            await store.terminal(
                claim.lease,
                "idle",
                conversation_id="child-retry",
                result="different answer",
            )

    async def test_terminal_update_does_not_retry_non_transient_store_errors(self) -> None:
        store = await self.store("terminal-programming-error", "runtime-error")
        claim = await store.create(
            "owner",
            "error-worker",
            "fail once",
            str(self.cwd),
            "fresh",
        )
        await store.mark_running(claim.lease, "child-error")
        live = self.runtime.live_run_from_claim(claim, "fail once", store)
        live.conversation_id = "child-error"
        attempts = 0

        async def broken_terminal(*_args: Any, **_kwargs: Any) -> Any:
            nonlocal attempts
            attempts += 1
            raise sqlite3.OperationalError("no such table: agents")

        with mock.patch.object(store, "terminal", new=broken_terminal):
            committed = await self.runtime.safe_worker_terminal(
                live,
                "idle",
                result="unreachable",
            )

        self.assertFalse(committed)
        self.assertEqual(attempts, 1)

    async def test_canceled_reservation_is_compensated_after_commit(self) -> None:
        store = await self.store("canceled-reservation", "runtime-cancel")
        started = asyncio.Event()
        release = asyncio.Event()

        async def delayed_create() -> Any:
            started.set()
            await release.wait()
            return await store.create(
                "owner",
                "delayed-worker",
                "delayed",
                str(self.cwd),
                "fresh",
            )

        reservation = asyncio.create_task(
            self.runtime.reserve_claim(
                delayed_create(),
                "delayed",
                store,
                initial=True,
            )
        )
        await started.wait()
        reservation.cancel()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await reservation
        self.assertEqual(await store.list("owner"), [])

    async def test_shutdown_cancels_and_awaits_inflight_agent_setup(self) -> None:
        context = self.context(
            block_fork=True,
            block_background_release=True,
        )
        spawning = asyncio.create_task(
            self.app.spawn_agent(
                extension.SpawnAgentInput(
                    name="setup-worker",
                    task="block during setup",
                ),
                context,
            )
        )
        await asyncio.wait_for(context.fork_started.wait(), timeout=1)
        self.assertIn(spawning, self.runtime.setup_tasks)
        self.assertEqual(len(context.background_leases), 1)
        background_lease = context.background_leases[0]

        shutdown = asyncio.create_task(self.runtime.shutdown())
        await asyncio.wait_for(background_lease.close_started.wait(), timeout=1)
        await asyncio.sleep(0)
        self.assertFalse(shutdown.done())
        self.assertIn(spawning, self.runtime.setup_tasks)

        background_lease.close_release.set()
        await asyncio.wait_for(shutdown, timeout=1)
        with self.assertRaises(asyncio.CancelledError):
            await spawning

        self.assertEqual(self.runtime.setup_tasks, set())
        self.assertEqual(self.runtime.live_runs, {})
        self.assertEqual(self.runtime.owned_runs, {})
        store = next(iter(self.runtime.stores.values()))
        self.assertEqual(await store.list(context.conversation_id), [])
        self.assertEqual(background_lease.close_calls, 1)

    async def test_shutdown_barrier_compensates_concurrent_reservation(self) -> None:
        context = self.context()
        store = await self.runtime.store_for_context(context)
        started = asyncio.Event()
        release = asyncio.Event()

        async def delayed_create() -> Any:
            started.set()
            await release.wait()
            return await store.create(
                context.conversation_id,
                "shutdown-worker",
                "during shutdown",
                context.cwd,
                "fresh",
            )

        reservation = asyncio.create_task(
            self.runtime.reserve_claim(
                delayed_create(),
                "during shutdown",
                store,
                initial=True,
            )
        )
        await started.wait()
        shutdown = asyncio.create_task(self.app.interrupt_live_agents(cast(Any, {}), context))
        await asyncio.sleep(0)
        self.assertFalse(shutdown.done())
        release.set()
        await shutdown
        with self.assertRaisesRegex(RuntimeError, "shutting down"):
            await reservation
        self.assertEqual(await store.list(context.conversation_id), [])

    async def test_real_sdk_fork_resume_exact_steer_and_cancel(self) -> None:
        context = self.context()
        host = ScopedChildHost()
        context.children = ChildClient(host)
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(name="sdk-agent", task="first task"), context
        )
        agent_id = spawned["data"]["agent_id"]
        first_id = spawned["data"]["conversation_id"]
        first_method, first_params, persistent = host.calls[0]
        self.assertEqual(first_method, "kodelet.child.start")
        self.assertFalse(persistent)
        self.assertEqual(
            first_params,
            {
                "profile": "subagent",
                "message": "first task",
                "contextMode": "fork",
                "requestId": spawned["data"]["run_id"],
                "cwd": context.cwd,
                "leaseId": "lease-1",
            },
        )
        self.assertEqual(context.fork_names, [])  # Fork and admission are one daemon operation.
        host.executions["host-run-1"]["done"] = True
        completed = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000), context
        )
        self.assertEqual(completed["content"], "result for first task")

        followed = await self.app.followup_agent(
            extension.FollowupAgentInput(agent_id=agent_id, task="second task"), context
        )
        self.assertEqual(followed["data"]["conversation_id"], first_id)
        starts = [call for call in host.calls if call[0] == "kodelet.child.start"]
        self.assertEqual(len(starts), 2)
        _, resumed_params, persistent = starts[1]
        self.assertFalse(persistent)  # The new lease is authorized by this follow-up tool.
        self.assertNotIn("contextMode", resumed_params)
        self.assertEqual(resumed_params["resume"], first_id)
        self.assertEqual(resumed_params["leaseId"], "lease-2")
        self.assertNotEqual(resumed_params["requestId"], first_params["requestId"])
        await self.app.steer_agent(
            extension.SteerAgentInput(agent_id=agent_id, message="From parent: inspect tests"),
            context,
        )

        async def was_steered() -> bool:
            return any(method == "kodelet.child.steer" for method, _, _ in host.calls)

        await wait_until(was_steered)
        _, params, persistent = next(
            call for call in host.calls if call[0] == "kodelet.child.steer"
        )
        self.assertTrue(persistent)
        self.assertEqual(params["childRunId"], "host-run-2")
        self.assertEqual(params["childId"], first_id)
        self.assertEqual(params["message"], "From parent: inspect tests")
        self.assertTrue(params["requestId"].startswith(f"{agent_id}:steering:"))
        canceled = await self.app.cancel_agent(
            extension.CancelAgentInput(agent_id=agent_id), context
        )
        self.assertEqual(canceled["data"]["agent_status"], "canceled")
        cancellations = [call for call in host.calls if call[0] == "kodelet.child.cancel"]
        self.assertTrue(cancellations)
        self.assertTrue(all(call[1]["childRunId"] == "host-run-2" for call in cancellations))
        self.assertEqual(self.runtime.owned_runs, {})

    async def test_steering_prompt_required_preserves_queue_for_followup(self) -> None:
        context = self.context()
        host = ScopedChildHost()
        host.steer_outcome = "promptRequired"
        context.children = ChildClient(host)
        spawned = await self.app.spawn_agent(
            extension.SpawnAgentInput(name="queued-agent", task="first"), context
        )
        agent_id = spawned["data"]["agent_id"]
        await self.app.steer_agent(
            extension.SteerAgentInput(agent_id=agent_id, message="retain this guidance"), context
        )
        store = await self.runtime.store_for_context(context)

        async def was_steered() -> bool:
            return any(method == "kodelet.child.steer" for method, _, _ in host.calls)

        await wait_until(was_steered)
        self.assertEqual(len(self.steering_rows(store.path)), 1)
        host.executions["host-run-1"]["done"] = True
        await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000), context
        )
        host.steer_outcome = "injected"
        await self.app.followup_agent(
            extension.FollowupAgentInput(agent_id=agent_id, task="continue"), context
        )

        async def acknowledged() -> bool:
            return not self.steering_rows(store.path)

        await wait_until(acknowledged)
        messages = [params for method, params, _ in host.calls if method == "kodelet.child.steer"]
        self.assertEqual(
            [message["childRunId"] for message in messages], ["host-run-1", "host-run-2"]
        )
        self.assertEqual(messages[0]["requestId"], messages[1]["requestId"])
        await self.app.cancel_agent(extension.CancelAgentInput(agent_id=agent_id), context)

    async def test_uncertain_start_cancellation_waits_for_lease_revocation_ack(self) -> None:
        context = self.context(block_background_release=True)
        host = ScopedChildHost()
        host.block_start = True
        context.children = ChildClient(host)
        spawning = asyncio.create_task(
            self.app.spawn_agent(
                extension.SpawnAgentInput(name="uncertain-agent", task="admitted before response"),
                context,
            )
        )
        await asyncio.wait_for(host.start_entered.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)
        [reserved] = await store.list(context.conversation_id)
        self.assertFalse(spawning.done())
        live = self.runtime.get_live_run(store, reserved.id)
        assert live is not None
        self.assertIsNone(live.runner_task)
        with mock.patch.object(runtime_module, "CANCEL_CLEANUP_TIMEOUT_SECONDS", 0.01):
            canceled = await self.app.cancel_agent(
                extension.CancelAgentInput(agent_id=reserved.id), context
            )
        self.assertEqual(canceled["data"]["agent_status"], "canceling")
        self.assertFalse(spawning.done())
        self.assertTrue(self.runtime.owned_runs)
        self.assertEqual(len(host.executions), 1)
        blocked = await self.app.followup_agent(
            extension.FollowupAgentInput(agent_id=reserved.id, task="not yet"), context
        )
        self.assertIn("cancellation is still in progress", blocked["error"])
        # The host ACK only succeeds once the unknown child has actually stopped.
        host.executions["host-run-1"]["done"] = True
        context.background_leases[0].close_release.set()
        with self.assertRaises(asyncio.CancelledError):
            await spawning
        self.assertEqual((await store.get(context.conversation_id, reserved.id)).status, "canceled")
        self.assertEqual(self.runtime.owned_runs, {})

    async def test_legacy_resume_rejection_preserves_identity_and_records_failure(self) -> None:
        context = self.context()
        store = await self.runtime.store_for_context(context)
        claim = await store.create(
            context.conversation_id, "legacy-agent", "old", context.cwd, "fork"
        )
        await store.mark_running(claim.lease, "legacy-conversation")
        await store.terminal(claim.lease, "idle", result="old result")
        host = ScopedChildHost()
        host.reject_start = "child conversation lacks delegation ownership; start a new agent"
        context.children = ChildClient(host)
        result = await self.app.followup_agent(
            extension.FollowupAgentInput(agent_id=claim.agent.id, task="resume"), context
        )
        self.assertIn("start a new agent", result["error"])
        agent = await store.get(context.conversation_id, claim.agent.id)
        self.assertEqual(agent.conversation_id, "legacy-conversation")
        self.assertEqual(agent.status, "failed")
        self.assertEqual(agent.run.generation, 2)
        self.assertEqual(context.background_leases[0].close_calls, 1)
        self.assertEqual(host.executions, {})

    async def test_lost_child_result_uses_acknowledged_lease_release_before_cancel_complete(
        self,
    ) -> None:
        context = self.context(block_background_release=True)
        context.background_release_failures = 1
        host = ScopedChildHost()
        host.reject_reads = True
        host.cancel_done = False
        context.children = ChildClient(host)
        with (
            mock.patch.object(runtime_module, "CANCEL_CLEANUP_TIMEOUT_SECONDS", 0.01),
            mock.patch.object(runtime_module, "CHILD_CANCEL_TIMEOUT_SECONDS", 0.02),
            mock.patch.object(runtime_module, "HEARTBEAT_INTERVAL_SECONDS", 0.01),
            mock.patch.object(runtime_module, "MIN_HEARTBEAT_INTERVAL_SECONDS", 0.001),
            mock.patch.object(
                runtime_module, "BACKGROUND_LEASE_RELEASE_RETRY_INITIAL_SECONDS", 0.01
            ),
        ):
            spawned = await self.app.spawn_agent(
                extension.SpawnAgentInput(name="release-agent", task="must actually stop"), context
            )
            agent_id = spawned["data"]["agent_id"]
            lease = context.background_leases[0]
            # A cancel can arrive after transport failure has begun draining.
            await asyncio.wait_for(lease.close_started.wait(), timeout=1)
            result = await self.app.cancel_agent(
                extension.CancelAgentInput(agent_id=agent_id), context
            )
            self.assertEqual(result["data"]["agent_status"], "canceling")
            store = await self.runtime.store_for_context(context)
            self.assertEqual(
                (await store.get(context.conversation_id, agent_id)).status, "canceling"
            )
            self.assertTrue(self.runtime.owned_runs)
            self.assertFalse(host.executions["host-run-1"]["done"])
            live = self.runtime.get_live_run(store, agent_id)
            assert live is not None
            expires_at = live.lease.expires_at

            async def cancellation_renewed() -> bool:
                return live.lease.expires_at > expires_at

            await wait_until(cancellation_renewed)
            host.executions["host-run-1"]["done"] = True
            lease.close_release.set()
            await self.wait_for_status(store, context.conversation_id, agent_id, "canceled")

            async def cleanup_finished() -> bool:
                return not self.runtime.owned_runs

            await wait_until(cleanup_finished)
        self.assertEqual(lease.close_calls, 2)
        self.assertEqual((await store.get(context.conversation_id, agent_id)).status, "canceled")

    async def test_child_read_failure_retains_active_claim_until_lease_release_ack(self) -> None:
        context = self.context(block_background_release=True)
        host = ScopedChildHost()
        host.reject_reads = True
        host.cancel_done = False
        context.children = ChildClient(host)
        with mock.patch.object(runtime_module, "CHILD_CANCEL_TIMEOUT_SECONDS", 0.02):
            spawned = await self.app.spawn_agent(
                extension.SpawnAgentInput(name="read-failure-agent", task="still running"),
                context,
            )
            agent_id = spawned["data"]["agent_id"]
            lease = context.background_leases[0]
            await asyncio.wait_for(lease.close_started.wait(), timeout=1)
            store = await self.runtime.store_for_context(context)
            self.assertEqual((await store.get(context.conversation_id, agent_id)).status, "running")
            self.assertFalse(host.executions["host-run-1"]["done"])
            self.assertTrue(self.runtime.owned_runs)
            followup = await self.app.followup_agent(
                extension.FollowupAgentInput(agent_id=agent_id, task="not yet"), context
            )
            self.assertIn("error", followup)
            self.assertEqual(len(host.executions), 1)
            self.assertEqual((await store.get(context.conversation_id, agent_id)).run.generation, 1)
            host.executions["host-run-1"]["done"] = True
            lease.close_release.set()
            failed = await self.wait_for_status(store, context.conversation_id, agent_id, "failed")
            self.assertIn("transport unavailable", failed.run.error)

            async def cleanup_finished() -> bool:
                return not self.runtime.owned_runs

            await wait_until(cleanup_finished)

    async def test_cwd_rejection_precedes_database_and_child_effects(self) -> None:
        context = self.context()
        outside = self.root / "outside"
        outside.mkdir()
        (self.cwd / "escape").symlink_to(outside, target_is_directory=True)
        for cwd in (str(outside), "../outside", "escape"):
            with self.subTest(cwd=cwd):
                result = await self.app.spawn_agent(
                    extension.SpawnAgentInput(name="outside-agent", task="do not run", cwd=cwd),
                    context,
                )
                self.assertIn("workspace or a descendant", result["error"])
        self.assertEqual(context.children.start_calls, [])
        self.assertEqual(context.background_leases, [])
        self.assertEqual(self.runtime.stores, {})
        self.assertFalse((self.data_dir / DATABASE_FILENAME).exists())

    async def test_followup_rejects_legacy_outside_cwd_without_claiming_generation(self) -> None:
        context = self.context()
        store = await self.runtime.store_for_context(context)
        outside = self.root / "legacy-workspace"
        outside.mkdir()
        claim = await store.create(
            context.conversation_id, "legacy-outside", "old task", str(outside), "fresh"
        )
        await store.mark_running(claim.lease, "old-conversation")
        await store.terminal(claim.lease, "idle", result="old output")
        result = await self.app.followup_agent(
            extension.FollowupAgentInput(agent_id=claim.agent.id, task="do not run"), context
        )
        self.assertIn("workspace or a descendant", result["error"])
        stored = await store.get(context.conversation_id, claim.agent.id)
        self.assertEqual(stored.run.generation, 1)
        self.assertEqual(stored.status, "idle")
        self.assertEqual(context.children.start_calls, [])
        self.assertEqual(context.background_leases, [])


if __name__ == "__main__":
    unittest.main()
