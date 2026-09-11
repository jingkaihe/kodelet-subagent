"""Provider-free integration tests using the production SDK Client and transport."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from kodelet_sdk import Client, TaskProgress, ToolContext
from kodelet_sdk.agent import transport
from kodelet_sdk.task_progress import TaskProgressSession

from kodelet_subagent import extension
from kodelet_subagent.persistence import AgentStore
from kodelet_subagent.runtime import AgentClient, RuntimeState, default_client_factory

FAKE_ACP = r"""
import json
import os
import signal
import sys
import threading
import time

mode = os.environ.get("ACP_TEST_MODE", "large")
message_bytes = int(os.environ.get("ACP_TEST_BYTES", str(256 * 1024)))
session_id = "saved-child"
pending_prompt = None
output_lock = threading.Lock()
if mode == "ignore-term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(os.environ["ACP_TEST_PID"], "w") as handle:
    handle.write(str(os.getpid()))

def emit(message):
    with output_lock:
        print(json.dumps(message), flush=True)

def update(value):
    emit({"jsonrpc": "2.0", "method": "session/update", "params": {
        "sessionId": session_id, "update": value
    }})

def text(value):
    update({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": value}})

def finish_when_released():
    while not os.path.exists(os.environ["ACP_TEST_FINISH"]):
        time.sleep(0.01)
    text("complete")
    emit({"jsonrpc": "2.0", "id": pending_prompt, "result": {"stopReason": "end_turn"}})

for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    if os.environ.get("ACP_TEST_REQUESTS"):
        with open(os.environ["ACP_TEST_REQUESTS"], "a") as handle:
            handle.write(json.dumps(request) + "\n")
    if mode == method + ":stdout":
        sys.stdout.write("x" * message_bytes)
        sys.stdout.flush()
        time.sleep(60)
    if mode == method + ":stderr":
        sys.stderr.write("x" * message_bytes)
        sys.stderr.flush()
        time.sleep(60)
    if mode == "eof" and method == "initialize":
        os.close(1)
        time.sleep(60)
    if mode in ("hang", "ignore-term") and method == "session/load":
        time.sleep(60)
    if mode == "exit" and method == "initialize":
        sys.exit(7)
    if method == "initialize":
        result = {"protocolVersion": 1, "_meta": {"steering": {"supported": True}}}
        if mode != "no-hierarchy":
            result["_meta"]["conversationHierarchy"] = {"version": 1}
        if mode != "no-extensions":
            result["_meta"]["sessionExtensions"] = {
                "version": 2 if mode == "unsupported-extensions" else 1
            }
        if mode == "large":
            result["padding"] = "i" * (256 * 1024)
            sys.stderr.write("diagnostic " + "d" * (256 * 1024) + "\n")
            sys.stderr.flush()
    elif method in ("session/new", "session/load"):
        session_id = request["params"].get("sessionId", session_id)
        if mode == "large":
            text("replayed " + "r" * message_bytes)
        result = {"sessionId": session_id}
    elif method == "session/prompt":
        if mode == "interactive":
            pending_prompt = request["id"]
            continue
        text("p" * message_bytes)
        result = {"stopReason": "cancelled" if mode == "cancelled" else "end_turn"}
    elif method == "_session/steering":
        message = request["params"]["prompt"][0]["text"]
        if message == "finish":
            threading.Thread(target=finish_when_released, daemon=True).start()
        else:
            update({"sessionUpdate": "tool_call", "toolCallId": "read-1", "toolName": "file_read",
                    "rawInput": {"path": "src/main.py"}, "status": "in_progress"})
            update({"sessionUpdate": "tool_call_update", "toolCallId": "read-1",
                    "status": "in_progress", "content": [
                        {"type": "content", "content": {"type": "text", "text": "reading source"}}
                    ]})
            update({"sessionUpdate": "tool_call_update", "toolCallId": "read-1",
                    "status": "completed", "content": [
                        {"type": "content", "content": {"type": "text", "text": "source code"}}
                    ]})
            text("Inspecting: ")
        result = {"outcome": "injected"}
    else:
        continue
    emit({"jsonrpc": "2.0", "id": request["id"], "result": result})
"""


@pytest.fixture
def fake_acp(tmp_path: Path) -> Path:
    executable = tmp_path / "fake-kodelet"
    executable.write_text(f"#!{sys.executable}\n{FAKE_ACP}")
    executable.chmod(0o700)
    return executable


def assert_child_reaped(pid_file: Path) -> None:
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


async def wait_for_file(path: Path) -> None:
    async with asyncio.timeout(3):
        while not await asyncio.to_thread(path.exists):  # noqa: ASYNC110 - process marker
            await asyncio.sleep(0.01)


def make_client(fake_acp: Path, mode: str = "large", **env: str) -> AgentClient:
    client = default_client_factory(
        command=str(fake_acp),
        cwd=str(fake_acp.parent),
        env={"ACP_TEST_PID": str(fake_acp.parent / "pid"), "ACP_TEST_MODE": mode, **env},
    )
    assert type(client) is Client  # No extension-owned transport subclass or spawn shim.
    return client


@pytest.mark.parametrize("resume", [False, True])
@pytest.mark.parametrize("message_bytes", [256 * 1024, 17 * 1024 * 1024])
async def test_large_initialize_replay_and_live_updates(
    fake_acp: Path, resume: bool, message_bytes: int
) -> None:
    client = make_client(fake_acp, ACP_TEST_BYTES=str(message_bytes))
    try:
        options = {"resume": "saved-child"} if resume else {}
        session = await asyncio.wait_for(client.create_session(**options), timeout=10)
        assert session.id == "saved-child"
        response = await asyncio.wait_for(session.run_and_wait("continue"), timeout=10)
        assert response["content"] == "p" * message_bytes
    finally:
        await asyncio.wait_for(client.close(), timeout=3)
    assert_child_reaped(fake_acp.parent / "pid")


async def test_default_limit_rejects_message_above_64_mib(fake_acp: Path) -> None:
    client = make_client(fake_acp, "initialize:stdout", ACP_TEST_BYTES=str(64 * 1024 * 1024 + 1))
    try:
        with pytest.raises(RuntimeError, match="ACP stdout read failed"):
            await asyncio.wait_for(client.create_session(), timeout=10)
    finally:
        await asyncio.wait_for(client.close(), timeout=3)
    assert_child_reaped(fake_acp.parent / "pid")


@pytest.mark.parametrize("stage", ["initialize", "session/load", "session/prompt"])
@pytest.mark.parametrize("stream", ["stdout", "stderr"])
async def test_reader_failure_reaps_child_and_rejects_pending_rpc(
    fake_acp: Path, stage: str, stream: str
) -> None:
    client = make_client(fake_acp, f"{stage}:{stream}")
    try:
        with mock.patch.object(transport, "ACP_MESSAGE_LIMIT", 128 * 1024):
            with pytest.raises(RuntimeError, match=f"ACP {stream} read failed"):
                session = await asyncio.wait_for(
                    client.create_session(resume="saved-child"), timeout=3
                )
                await asyncio.wait_for(session.run_and_wait("continue"), timeout=3)
    finally:
        await asyncio.wait_for(client.close(), timeout=3)
    assert_child_reaped(fake_acp.parent / "pid")


@pytest.mark.parametrize("mode", ["hang", "ignore-term"])
async def test_canceled_startup_reaps_child_before_returning(fake_acp: Path, mode: str) -> None:
    client = make_client(fake_acp, mode)
    startup = asyncio.create_task(client.create_session(resume="saved-child"))
    try:
        await wait_for_file(fake_acp.parent / "pid")
        startup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(startup, timeout=3)
        assert_child_reaped(fake_acp.parent / "pid")
    finally:
        startup.cancel()
        await asyncio.gather(startup, return_exceptions=True)
        await asyncio.wait_for(client.close(), timeout=3)


async def test_repeated_startup_cancellation_keeps_process_owned(fake_acp: Path) -> None:
    client = make_client(fake_acp, "ignore-term")
    terminating = asyncio.Event()
    terminate = transport._ACPProcess.terminate

    def signal_terminate(process: transport._ACPProcess) -> None:
        terminate(process)
        terminating.set()

    startup = asyncio.create_task(client.create_session(resume="saved-child"))
    try:
        with mock.patch.object(transport._ACPProcess, "terminate", signal_terminate):
            await wait_for_file(fake_acp.parent / "pid")
            startup.cancel()
            await asyncio.wait_for(terminating.wait(), timeout=3)
            startup.cancel()
            with pytest.raises(asyncio.CancelledError):
                await startup
            await asyncio.wait_for(client.close(), timeout=3)
            assert_child_reaped(fake_acp.parent / "pid")
    finally:
        startup.cancel()
        await asyncio.gather(startup, return_exceptions=True)
        await asyncio.wait_for(client.close(), timeout=3)


@pytest.mark.parametrize("mode", ["eof", "exit"])
async def test_unexpected_stdout_eof_or_exit_fails_startup(fake_acp: Path, mode: str) -> None:
    client = make_client(fake_acp, mode)
    try:
        with pytest.raises(RuntimeError, match=r"ACP stdout ended|kodelet acp exited"):
            await asyncio.wait_for(client.create_session(resume="saved-child"), timeout=3)
    finally:
        await asyncio.wait_for(client.close(), timeout=3)
    assert_child_reaped(fake_acp.parent / "pid")


@pytest.mark.parametrize("mode", ["large", "session/load:stdout", "ignore-term"])
async def test_runtime_releases_ownership_and_lease_only_after_process_exit(
    fake_acp: Path, mode: str
) -> None:
    pid_file = fake_acp.parent / "pid"

    def factory(*, command: str, cwd: str, env: Mapping[str, str]) -> AgentClient:
        return make_client(fake_acp, mode, **env)

    runtime = RuntimeState(client_factory=factory)
    store = AgentStore(fake_acp.parent / "agents.sqlite", runtime.runtime_id)
    await store.initialize()
    claim = await store.create("parent", "worker", "continue", str(fake_acp.parent), "fresh")
    await store.attach_conversation(claim.lease, "saved-child")
    live = runtime.live_run_from_claim(claim, "continue", store)
    live.conversation_id = "saved-child"
    lease = mock.Mock()
    lease.close = mock.AsyncMock(side_effect=lambda: assert_child_reaped(pid_file))
    live.background_lease = lease
    limit = 128 * 1024 if mode == "session/load:stdout" else transport.ACP_MESSAGE_LIMIT
    try:
        with mock.patch.object(transport, "ACP_MESSAGE_LIMIT", limit):
            runtime.launch_live_run(live)
            if mode == "ignore-term":
                await wait_for_file(pid_file)
                await store.cancel("parent", live.agent_id)
                assert await runtime.cancel_live_run(
                    store, live.agent_id, live.run_id, cleanup_timeout=3
                )
            else:
                assert live.runner_task is not None
                await asyncio.wait_for(live.runner_task, timeout=3)
    finally:
        await asyncio.wait_for(runtime.shutdown(), timeout=3)

    record = await store.get("parent", live.agent_id)
    expected = {"large": "idle", "session/load:stdout": "failed", "ignore-term": "canceled"}
    assert record.status == expected[mode]
    if mode == "session/load:stdout":
        assert "ACP stdout read failed" in (record.run.error or "")
    assert runtime.live_runs == {}
    assert runtime.owned_runs == {}
    assert runtime.cleanup_tasks == set()
    lease.close.assert_awaited_once()
    assert_child_reaped(pid_file)


@pytest.mark.parametrize("mode", ["no-extensions", "unsupported-extensions"])
@pytest.mark.parametrize("resume", [False, True])
async def test_fresh_recursion_guard_fails_closed_without_supported_acp_extensions(
    fake_acp: Path, mode: str, resume: bool
) -> None:
    requests_file = fake_acp.parent / "requests"

    def factory(*, command: str, cwd: str, env: Mapping[str, str]) -> AgentClient:
        return make_client(fake_acp, mode, ACP_TEST_REQUESTS=str(requests_file), **env)

    runtime = RuntimeState(client_factory=factory)
    store = AgentStore(fake_acp.parent / "agents.sqlite", runtime.runtime_id)
    await store.initialize()
    claim = await store.create("parent", "worker", "continue", str(fake_acp.parent), "fresh")
    live = runtime.live_run_from_claim(claim, "continue", store)
    if resume:
        await store.attach_conversation(claim.lease, "saved-child")
        live.conversation_id = "saved-child"
    lease = mock.Mock()
    lease.close = mock.AsyncMock(side_effect=lambda: assert_child_reaped(fake_acp.parent / "pid"))
    live.background_lease = lease
    try:
        runtime.launch_live_run(live)
        assert live.runner_task is not None
        await asyncio.wait_for(live.runner_task, timeout=3)
        record = await store.get("parent", live.agent_id)
        assert record.run.status == "failed"
        assert "sessionExtensions version 1 support" in (record.run.error or "")
        requests = [json.loads(line) for line in requests_file.read_text().splitlines()]
        assert [request["method"] for request in requests] == ["initialize"]
        assert runtime.owned_runs == {}
        assert runtime.cleanup_tasks == set()
        lease.close.assert_awaited_once()
        assert_child_reaped(fake_acp.parent / "pid")
    finally:
        await asyncio.wait_for(runtime.shutdown(), timeout=3)


@pytest.mark.parametrize("resume", [False, True])
@pytest.mark.parametrize("mode", ["normal", "no-hierarchy"])
async def test_fresh_parent_relationship_is_sent_only_on_creation(
    fake_acp: Path, mode: str, resume: bool
) -> None:
    requests_file = fake_acp.parent / "requests"

    def factory(*, command: str, cwd: str, env: Mapping[str, str]) -> AgentClient:
        return make_client(fake_acp, mode, ACP_TEST_REQUESTS=str(requests_file), **env)

    runtime = RuntimeState(client_factory=factory)
    store = AgentStore(fake_acp.parent / "agents.sqlite", runtime.runtime_id)
    await store.initialize()
    claim = await store.create("parent", "worker", "continue", str(fake_acp.parent), "fresh")
    live = runtime.live_run_from_claim(claim, "continue", store)
    if resume:
        await store.attach_conversation(claim.lease, "saved-child")
        live.conversation_id = "saved-child"
    try:
        runtime.launch_live_run(live)
        assert live.runner_task is not None
        await asyncio.wait_for(live.runner_task, timeout=3)
        record = await store.get("parent", live.agent_id)
        requests = [json.loads(line) for line in requests_file.read_text().splitlines()]
        if mode == "no-hierarchy" and not resume:
            assert record.run.status == "failed"
            assert "conversationHierarchy version 1" in (record.run.error or "")
            assert [request["method"] for request in requests] == ["initialize"]
        else:
            assert record.run.status == "completed"
            session_request = requests[1]
            assert session_request["method"] == ("session/load" if resume else "session/new")
            metadata = session_request["params"]["_meta"]
            assert metadata["sessionExtensions"] == {"version": 1, "extensionIds": ["inline-1"]}
            if resume:
                assert "conversationHierarchy" not in metadata
            else:
                assert metadata["conversationHierarchy"] == {
                    "version": 1,
                    "parentConversationId": "parent",
                }
        assert runtime.owned_runs == {}
        assert_child_reaped(fake_acp.parent / "pid")
    finally:
        await asyncio.wait_for(runtime.shutdown(), timeout=3)


@pytest.mark.parametrize("message_bytes", [0, 31])
async def test_runtime_preserves_real_sdk_cancelled_stop_reason_and_partial_output(
    fake_acp: Path, message_bytes: int
) -> None:
    def factory(*, command: str, cwd: str, env: Mapping[str, str]) -> AgentClient:
        return make_client(fake_acp, "cancelled", ACP_TEST_BYTES=str(message_bytes), **env)

    runtime = RuntimeState(client_factory=factory)
    store = AgentStore(fake_acp.parent / "agents.sqlite", runtime.runtime_id)
    await store.initialize()
    claim = await store.create("parent", "worker", "continue", str(fake_acp.parent), "fresh")
    live = runtime.live_run_from_claim(claim, "continue", store)
    try:
        runtime.launch_live_run(live)
        assert live.runner_task is not None
        await asyncio.wait_for(live.runner_task, timeout=3)
        record = await store.get("parent", live.agent_id)
        assert record.run.status == "interrupted"
        assert record.run.error == "agent canceled by kodelet"
        assert record.run.result == ("p" * message_bytes or None)
        assert live.progress.snapshot()["status"] == "failed"
        assert runtime.owned_runs == {}
        assert_child_reaped(fake_acp.parent / "pid")
    finally:
        await asyncio.wait_for(runtime.shutdown(), timeout=3)


class ACPContextHost:
    """Host half of the real ToolContext named-fork/progress/lifetime protocol."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.fork_names: list[str] = []
        self.updates: list[dict[str, Any]] = []
        self.acquired = 0
        self.released: list[str] = []

    async def request(self, method: str, params: Any = None) -> Any:
        if method == "kodelet.conversation.fork":
            assert params["asChild"] is True
            self.fork_names.append(params["name"])
            return {"conversationId": "named-child"}
        if method == "kodelet.runtime.background.acquire":
            self.acquired += 1
            return {"leaseId": str(self.acquired)}
        if method == "kodelet.tool.update":
            self.updates.append(params["data"])
            return None
        raise AssertionError(f"Unexpected host request: {method}")

    async def request_persistent(self, method: str, params: Any = None) -> None:
        assert method == "kodelet.runtime.background.release"
        assert_child_reaped(self.directory / f"pid-{params['leaseId']}")
        self.released.append(params["leaseId"])


async def test_named_fork_streaming_steering_resume_and_cancel_use_real_sdk(fake_acp: Path) -> None:
    directory = fake_acp.parent
    workspace = directory / "workspace"
    workspace.mkdir()
    host = ACPContextHost(directory)
    clients: list[AgentClient] = []
    requests_file = directory / "requests"
    finish_file = directory / "finish"

    def factory(*, command: str, cwd: str, env: Mapping[str, str]) -> AgentClient:
        assert command == "kodelet"
        assert cwd == str(workspace)
        assert env[extension.RECURSION_GUARD_ENV] == "1"
        client = default_client_factory(
            command=str(fake_acp),
            cwd=cwd,
            env={
                **env,
                "ACP_TEST_MODE": "interactive",
                "ACP_TEST_REQUESTS": str(requests_file),
                "ACP_TEST_PID": str(directory / f"pid-{len(clients) + 1}"),
                "ACP_TEST_FINISH": str(finish_file),
            },
        )
        assert type(client) is Client
        clients.append(client)
        return client

    with mock.patch("kodelet_sdk.context._current_host_rpc_client", return_value=host):
        context = ToolContext(
            {
                "extension": {"dataDir": str(directory / "data")},
                "capabilities": {
                    "runtime": {"backgroundTasks": True},
                    "conversations": {"fork": True, "hierarchy": True},
                    "toolUpdates": True,
                },
            },
            {"conversationId": "parent", "cwd": str(workspace)},
        )
    runtime = RuntimeState(client_factory=factory)
    app = extension.SubagentApplication(runtime)
    attached = asyncio.Event()
    attach = TaskProgress.attach

    def attach_session(progress: TaskProgress, session: TaskProgressSession) -> None:
        attach(progress, session)
        attached.set()

    waiting: asyncio.Task[Any] | None = None
    try:
        with mock.patch.object(TaskProgress, "attach", attach_session):
            spawned = await app.spawn_agent(
                extension.SpawnAgentInput(name="named-reviewer", task="inspect"),
                context,
            )
            agent_id = spawned["data"]["agent_id"]
            assert spawned["data"]["conversation_id"] == "named-child"
            assert host.fork_names == ["named-reviewer"]
            waiting = asyncio.create_task(
                app.wait_agent(
                    extension.WaitAgentInput(agent_id=agent_id, timeout_ms=5_000), context
                )
            )
            await asyncio.wait_for(attached.wait(), timeout=3)
            # Progress attaches before mark_running; steering requires that
            # persisted transition, not merely an attached ACP session.
            store = await runtime.store_for_context(context)
            async with asyncio.timeout(3):
                while (await store.get("parent", agent_id)).status != "running":  # noqa: ASYNC110
                    await asyncio.sleep(0.01)
            steered = await app.steer_agent(
                extension.SteerAgentInput(agent_id=agent_id, message="inspect source"), context
            )
            assert "error" not in steered
            async with asyncio.timeout(3):
                while not any(  # noqa: ASYNC110 - asynchronously published SDK progress
                    update.get("taskRun", {}).get("phase") == "responding"
                    for update in host.updates
                ):
                    await asyncio.sleep(0.01)
            await app.steer_agent(
                extension.SteerAgentInput(agent_id=agent_id, message="finish"), context
            )
            # Complete only after durable steering acknowledgement, not while
            # the worker is still committing it and might carry it to follow-up.
            live = runtime.get_live_run(store, agent_id)
            assert live is not None
            async with asyncio.timeout(3):
                while await store.next_steering(live.lease) is not None:  # noqa: ASYNC110
                    await asyncio.sleep(0.01)
            await asyncio.to_thread(finish_file.touch)
            completed = await asyncio.wait_for(waiting, timeout=3)
        assert completed["content"] == "Inspecting: complete"
        assert completed["data"]["taskRun"]["counts"] == {"succeeded": 1, "failed": 0, "running": 0}
        [activity] = completed["data"]["taskRun"]["activities"]
        assert activity["kind"] == "file_read"
        assert activity["status"] == "succeeded"

        followed = await app.followup_agent(
            extension.FollowupAgentInput(agent_id=agent_id, task="second pass"), context
        )
        assert followed["data"]["conversation_id"] == "named-child"
        await wait_for_file(directory / "pid-2")
        # Wait for ACP session/prompt so cancellation exercises an active session.
        async with asyncio.timeout(3):
            while (  # noqa: ASYNC110 - external ACP wire transcript
                sum(
                    json.loads(line)["method"] == "session/prompt"
                    for line in (await asyncio.to_thread(requests_file.read_text)).splitlines()
                )
                < 2
            ):
                await asyncio.sleep(0.01)
        canceled = await app.cancel_agent(extension.CancelAgentInput(agent_id=agent_id), context)
        assert canceled["data"]["agent_status"] == "canceled"
    finally:
        if waiting is not None:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
        await asyncio.wait_for(runtime.shutdown(), timeout=3)

    assert host.fork_names == ["named-reviewer"]
    assert sorted(host.released) == ["1", "2"]
    assert len(clients) == 2
    assert runtime.owned_runs == {}
    requests = [json.loads(line) for line in requests_file.read_text().splitlines()]
    loads = [request["params"] for request in requests if request["method"] == "session/load"]
    assert loads == [{"sessionId": "named-child", "cwd": str(workspace)}] * 2
    steering = [
        request["params"] for request in requests if request["method"] == "_session/steering"
    ]
    assert [request["prompt"][0]["text"] for request in steering] == ["inspect source", "finish"]
    assert all(
        request["_meta"]["steering"]["idleBehavior"] == "promptRequired" for request in steering
    )
    assert not any(request["method"] == "session/new" for request in requests)
