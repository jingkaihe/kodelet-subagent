from __future__ import annotations

import asyncio
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
from kodelet_subagent.runtime import RuntimeState, SessionSteerResult
from kodelet_subagent.ui import WIDGET_ID

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
        block_background_release: bool = False,
    ) -> None:
        self.conversation_id = conversation_id
        self.cwd = str(cwd)
        self.invoked_by = invoked_by
        self.storage = SimpleNamespace(data_dir=str(data_dir))
        self.ui = FakeUI()
        self.log = SimpleNamespace(warn=lambda _message, _fields=None: None)
        self.block_fork = block_fork
        self.block_background_release = block_background_release
        self.fork_names: list[str | None] = []
        self.fork_started = asyncio.Event()
        self.fork_release = asyncio.Event()
        self.background_release_failures = 0
        self.background_leases: list[FakeBackgroundTaskLease] = []
        self.tool_updates: list[tuple[str, dict[str, Any] | None]] = []

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
        lease = FakeBackgroundTaskLease(
            description,
            self.background_release_failures,
            block_close=self.block_background_release,
        )
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
        self.run_calls: list[str] = []
        self.steer_calls: list[str] = []
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

    async def run_and_wait(self, task: str) -> dict[str, str]:
        self.run_calls.append(task)
        self.run_started.set()
        type(self.client).run_started.set()
        if type(self.client).block_runs:
            await type(self.client).run_release.wait()
        failure = type(self.client).run_failure
        if failure is not None:
            raise failure
        return {"content": f"result for {task}"}

    async def steer(self, message: str) -> SessionSteerResult:
        self.steer_calls.append(message)
        self.steer_received.set()
        outcome = (
            type(self.client).steer_outcomes.pop(0)
            if type(self.client).steer_outcomes
            else "injected"
            if type(self.client).accept_steering
            else "failed"
        )
        result: SessionSteerResult = {"outcome": cast(Any, outcome)}
        if outcome == "promptRequired":
            result["reason"] = "noRunningTurn"
        return result

    async def close(self) -> None:
        self.close_calls += 1


class FakeClient:
    instances: ClassVar[list[FakeClient]] = []
    block_runs: ClassVar[bool] = False
    block_startup: ClassVar[bool] = False
    block_close: ClassVar[bool] = False
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
        self.runtime = RuntimeState(client_factory=FakeClient)
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
            if live.runner_task is not None and not live.runner_task.done():
                live.runner_task.cancel()
        await asyncio.gather(
            *(
                live.runner_task
                for live in list(self.runtime.owned_runs.values())
                if live.runner_task is not None
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
        block_background_release: bool = False,
    ) -> Any:
        return FakeContext(
            owner,
            self.cwd,
            data_dir or self.data_dir,
            invoked_by=invoked_by,
            block_fork=block_fork,
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
        other_runtime = RuntimeState(client_factory=FakeClient)
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
            child_application = extension.SubagentApplication(
                RuntimeState(client_factory=FakeClient)
            )
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
        self.assertEqual(revision, "0001_initial")
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
            FakeClient.instances[0].create_session_calls[0]["resume"],
            running.conversation_id,
        )
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
        self.assertIn("1 completed", context.ui.text(WIDGET_ID))
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
        self.assertEqual(context.background_leases[1].close_calls, 1)

        listed = await self.app.list_agents(extension.ListAgentsInput(), context)
        self.assertEqual(len(listed["data"]["agents"]), 2)
        self.assertIn("authentication-inspector", listed["content"])
        self.assertIn("independent-reviewer", listed["content"])
        self.assertIn(running.conversation_id, listed["content"])
        self.assertIn(fresh_result["data"]["conversation_id"], listed["content"])
        other_context = self.context("owner-2")
        other_list = await self.app.list_agents(
            extension.ListAgentsInput(),
            other_context,
        )
        self.assertEqual(other_list["data"]["agents"], [])
        hidden = await self.app.wait_agent(
            extension.WaitAgentInput(
                agent_id=forked_data["agent_id"],
                timeout_ms=0,
            ),
            other_context,
        )
        self.assertIn("agent not found", hidden["error"])

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
            first = await waiting

        self.assertEqual(first["content"], "result for first pass")
        self.assertEqual(first["data"]["run_id"], first_run_id)
        self.assertNotEqual(followup["data"]["run_id"], first_run_id)

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

        async def progress_attached() -> bool:
            return bool(session.listeners.get("tool.call"))

        await wait_until(progress_attached)
        session.emit(
            "tool.call",
            {
                "data": {
                    "toolCallId": "tool-1",
                    "toolName": "bash",
                    "input": {"command": "go test ./..."},
                }
            },
        )
        session.emit(
            "tool.update",
            {"data": {"toolCallId": "tool-1", "result": "tests running"}},
        )
        session.emit(
            "tool.result",
            {
                "data": {
                    "toolCallId": "tool-1",
                    "status": "completed",
                    "result": "ok",
                }
            },
        )

        async def tool_activity_published() -> bool:
            for _content, data in context.tool_updates:
                task_run = (data or {}).get("taskRun")
                if not isinstance(task_run, dict):
                    continue
                if any(
                    activity.get("id") == "tool-1" for activity in task_run.get("activities", [])
                ):
                    return True
            return False

        await wait_until(tool_activity_published)
        FakeClient.run_release.set()
        completed = await waiting

        self.assertEqual(completed["content"], "result for inspect progress")
        task_run = completed["data"]["taskRun"]
        self.assertEqual(task_run["kind"], "subagent")
        self.assertEqual(task_run["status"], "completed")
        tool_activity = next(
            activity for activity in task_run["activities"] if activity["id"] == "tool-1"
        )
        self.assertEqual(tool_activity["kind"], "bash")
        self.assertEqual(tool_activity["status"], "succeeded")
        self.assertEqual(session.listeners.get("tool.call"), [])

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

        result = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=20),
            context,
        )

        self.assertEqual(result["data"]["status"], "running")
        self.assertEqual(result["data"]["taskRun"]["status"], "running")
        self.assertTrue(context.tool_updates)
        self.assertEqual(session.listeners.get("tool.call"), [])

        FakeClient.run_release.set()
        completed = await self.app.wait_agent(
            extension.WaitAgentInput(agent_id=agent_id, timeout_ms=1_000),
            context,
        )
        self.assertEqual(completed["content"], "result for keep working")

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
        FakeClient.steer_outcomes = ["promptRequired", "injected"]
        with (
            mock.patch.object(runtime_module, "STEERING_POLL_SECONDS", 0.01),
            mock.patch.object(runtime_module, "STEERING_RETRY_SECONDS", 0.01),
        ):
            pump = asyncio.create_task(self.runtime.steering_pump(live, session))
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
        self.assertEqual(result["data"]["message"], "focus on the parser")
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
        await asyncio.wait_for(FakeClient.run_started.wait(), timeout=1)
        store = await self.runtime.store_for_context(context)
        await store.enqueue_steering(context.conversation_id, agent_id, "pending")

        with mock.patch.object(runtime_module, "CANCEL_CLEANUP_TIMEOUT_SECONDS", 0.01):
            canceled = await self.app.cancel_agent(
                extension.CancelAgentInput(agent_id=agent_id),
                context,
            )
        self.assertIn("cleanup is still finishing", canceled["content"])
        persisted = await store.get(context.conversation_id, agent_id)
        self.assertEqual(persisted.status, "canceled")
        self.assertEqual(persisted.run.status, "canceled")
        self.assertEqual(self.steering_rows(store.path), [])

        client = FakeClient.instances[0]
        session = client.sessions[0]
        self.assertFalse(client.closed)
        await asyncio.wait_for(FakeClient.close_started.wait(), timeout=1)
        FakeClient.close_release.set()

        key = self.runtime.live_run_key(store, agent_id)

        async def cleaned_up() -> bool:
            return key not in self.runtime.live_runs

        await wait_until(cleaned_up)
        self.assertTrue(client.closed)
        self.assertEqual(client.close_calls, 1)
        self.assertEqual(session.close_calls, 1)
        self.assertEqual(background_lease.close_calls, 1)
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

    async def test_shutdown_during_client_close_still_releases_background_lease(
        self,
    ) -> None:
        context = self.context()
        FakeClient.block_close = True
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

    async def test_startup_timeout_is_persisted_as_failure(self) -> None:
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
            failed = await self.wait_for_status(
                store,
                context.conversation_id,
                spawned["data"]["agent_id"],
                "failed",
            )
        self.assertEqual(failed.run.status, "failed")
        self.assertIn("timed out while starting", failed.run.error)

        async def background_lease_closed() -> bool:
            return bool(context.background_leases and context.background_leases[0].close_calls == 1)

        await wait_until(background_lease_closed)
        self.assertTrue(FakeClient.instances[0].closed)

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


if __name__ == "__main__":
    unittest.main()
