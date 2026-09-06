from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from unittest import mock

import pytest

from kodelet_subagent import acp
from kodelet_subagent.persistence import AgentStore
from kodelet_subagent.runtime import AgentClient, RuntimeState, default_client_factory

FAKE_ACP = r"""
import json
import os
import signal
import sys
import time

mode = os.environ.get("ACP_TEST_MODE", "large")
message_bytes = int(os.environ.get("ACP_TEST_BYTES", str(256 * 1024)))
if mode == "ignore-term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(os.environ["ACP_TEST_PID"], "w") as handle:
    handle.write(str(os.getpid()))

def emit(message):
    print(json.dumps(message), flush=True)

def update(text):
    emit({"jsonrpc": "2.0", "method": "session/update", "params": {
        "sessionId": "saved-child", "update": {
            "sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": text}
        }
    }})

for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    if mode == method + ":stdout":
        sys.stdout.write("x" * message_bytes)
        sys.stdout.flush()
        time.sleep(60)
    if mode == method + ":stderr":
        sys.stderr.write("x" * (256 * 1024))
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
        result = {"protocolVersion": 1}
        if mode == "large":
            result["padding"] = "i" * (256 * 1024)
            sys.stderr.write("diagnostic " + "d" * (256 * 1024) + "\n")
            sys.stderr.flush()
    elif method in ("session/new", "session/load"):
        if mode == "large":
            update("replayed " + "r" * message_bytes)
        result = {"sessionId": "saved-child"}
    elif method == "session/prompt":
        update("p" * message_bytes)
        result = {"stopReason": "end_turn"}
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
    pid = int(pid_file.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.parametrize("resume", [False, True])
@pytest.mark.parametrize("message_bytes", [256 * 1024, 17 * 1024 * 1024])
async def test_large_initialize_replay_and_live_updates(
    fake_acp: Path, resume: bool, message_bytes: int
) -> None:
    pid_file = fake_acp.parent / "pid"
    client = default_client_factory(
        command=str(fake_acp),
        cwd=str(fake_acp.parent),
        env={"ACP_TEST_PID": str(pid_file), "ACP_TEST_BYTES": str(message_bytes)},
    )
    try:
        options = {"resume": "saved-child"} if resume else {}
        session = await asyncio.wait_for(client.create_session(**options), timeout=10)
        assert session.id == "saved-child"
        response = await asyncio.wait_for(session.run_and_wait("continue"), timeout=10)
        assert response["content"] == "p" * message_bytes
    finally:
        await asyncio.wait_for(client.close(), timeout=3)
    assert_child_reaped(pid_file)


async def test_default_limit_rejects_message_above_64_mib(fake_acp: Path) -> None:
    pid_file = fake_acp.parent / "pid"
    client = default_client_factory(
        command=str(fake_acp),
        cwd=str(fake_acp.parent),
        env={
            "ACP_TEST_PID": str(pid_file),
            "ACP_TEST_MODE": "initialize:stdout",
            "ACP_TEST_BYTES": str(64 * 1024 * 1024 + 1),
        },
    )
    try:
        with pytest.raises(RuntimeError, match="ACP stdout read failed"):
            await asyncio.wait_for(client.create_session(), timeout=10)
    finally:
        await asyncio.wait_for(client.close(), timeout=3)
    assert_child_reaped(pid_file)


@pytest.mark.parametrize("stage", ["initialize", "session/load", "session/prompt"])
@pytest.mark.parametrize("stream", ["stdout", "stderr"])
async def test_reader_limit_fails_pending_rpc_and_reaps_child(
    fake_acp: Path, stage: str, stream: str
) -> None:
    pid_file = fake_acp.parent / "pid"
    client = default_client_factory(
        command=str(fake_acp),
        cwd=str(fake_acp.parent),
        env={"ACP_TEST_PID": str(pid_file), "ACP_TEST_MODE": f"{stage}:{stream}"},
    )
    try:
        with mock.patch.object(acp, "ACP_MESSAGE_LIMIT", 128 * 1024):
            with pytest.raises(RuntimeError, match=f"ACP {stream} read failed"):
                session = await asyncio.wait_for(
                    client.create_session(resume="saved-child"), timeout=3
                )
                await asyncio.wait_for(session.run_and_wait("continue"), timeout=3)
    finally:
        await asyncio.wait_for(client.close(), timeout=3)
    assert_child_reaped(pid_file)


@pytest.mark.parametrize("mode", ["hang", "ignore-term"])
async def test_canceled_startup_closes_child_before_returning(fake_acp: Path, mode: str) -> None:
    pid_file = fake_acp.parent / "pid"
    client = default_client_factory(
        command=str(fake_acp),
        cwd=str(fake_acp.parent),
        env={"ACP_TEST_PID": str(pid_file), "ACP_TEST_MODE": mode},
    )
    startup = asyncio.create_task(client.create_session(resume="saved-child"))
    try:
        async with asyncio.timeout(3):
            while not pid_file.exists():  # noqa: ASYNC110 - external child process marker
                await asyncio.sleep(0.01)
        startup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(startup, timeout=3)
    finally:
        await asyncio.wait_for(client.close(), timeout=3)
    assert_child_reaped(pid_file)


async def test_repeated_startup_cancellation_keeps_child_owned_until_close(fake_acp: Path) -> None:
    pid_file = fake_acp.parent / "pid"
    client = default_client_factory(
        command=str(fake_acp),
        cwd=str(fake_acp.parent),
        env={"ACP_TEST_PID": str(pid_file), "ACP_TEST_MODE": "ignore-term"},
    )
    terminating = asyncio.Event()
    terminate = acp._ACPProcess.terminate

    def signal_terminate(process: acp._ACPProcess) -> None:
        terminate(process)
        terminating.set()

    startup = asyncio.create_task(client.create_session(resume="saved-child"))
    with mock.patch.object(acp._ACPProcess, "terminate", signal_terminate):
        async with asyncio.timeout(3):
            while not pid_file.exists():  # noqa: ASYNC110 - external child process marker
                await asyncio.sleep(0.01)
        startup.cancel()
        await asyncio.wait_for(terminating.wait(), timeout=3)
        startup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await startup
        try:
            await asyncio.wait_for(client.close(), timeout=3)
            assert_child_reaped(pid_file)
        finally:
            # Also reap the fixture if this ownership regression returns early.
            try:
                os.kill(int(pid_file.read_text()), 9)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize("mode", ["eof", "exit"])
async def test_unexpected_stdout_eof_or_exit_fails_startup(fake_acp: Path, mode: str) -> None:
    pid_file = fake_acp.parent / "pid"
    client = default_client_factory(
        command=str(fake_acp),
        cwd=str(fake_acp.parent),
        env={"ACP_TEST_PID": str(pid_file), "ACP_TEST_MODE": mode},
    )
    try:
        with pytest.raises(RuntimeError, match=r"ACP stdout closed|kodelet acp exited"):
            await asyncio.wait_for(client.create_session(resume="saved-child"), timeout=3)
    finally:
        await asyncio.wait_for(client.close(), timeout=3)
    assert_child_reaped(pid_file)


@pytest.mark.parametrize("mode", ["large", "session/load:stdout", "ignore-term"])
async def test_runtime_releases_ownership_and_lease_only_after_child_exit(
    fake_acp: Path, mode: str
) -> None:
    pid_file = fake_acp.parent / "pid"

    def factory(*, command: str, cwd: str, env: Mapping[str, str]) -> AgentClient:
        return default_client_factory(
            command=str(fake_acp),
            cwd=cwd,
            env={**env, "ACP_TEST_PID": str(pid_file), "ACP_TEST_MODE": mode},
        )

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
    limit = 128 * 1024 if mode == "session/load:stdout" else acp.ACP_MESSAGE_LIMIT
    with mock.patch.object(acp, "ACP_MESSAGE_LIMIT", limit):
        runtime.launch_live_run(live)
        if mode == "ignore-term":
            async with asyncio.timeout(3):
                while not pid_file.exists():  # noqa: ASYNC110 - external child process marker
                    await asyncio.sleep(0.01)
            await store.cancel("parent", live.agent_id)
            assert await runtime.cancel_live_run(
                store, live.agent_id, live.run_id, cleanup_timeout=3
            )
        else:
            assert live.runner_task is not None
            await asyncio.wait_for(live.runner_task, timeout=3)

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
