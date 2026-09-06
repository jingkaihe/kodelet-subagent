"""Subprocess transport for large ACP updates and fail-closed reader errors."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Mapping, Sequence
from typing import cast

from kodelet_sdk import Client
from kodelet_sdk.agent.types import SpawnedProcess, SpawnOptions

# Replayed tool results routinely exceed asyncio's default 64 KiB line limit.
# Keep a finite bound; exceeding it must fail the agent, not strand its lease.
ACP_MESSAGE_LIMIT = 64 * 1024 * 1024


class _LineReader:
    def __init__(self, process: _ACPProcess, reader: asyncio.StreamReader, name: str) -> None:
        self.process = process
        self.reader = reader
        self.name = name
        self.finished = asyncio.Event()
        self.error_reported = False

    async def readline(self) -> bytes:
        if self.process.read_error is None:
            try:
                line = await self.reader.readline()
            except (ValueError, OSError) as exc:
                self.process.abort(
                    f"ACP {self.name} read failed (limit {ACP_MESSAGE_LIMIT}): {exc}"
                )
            else:
                if line and self.process.read_error is None:
                    return line
                if self.name == "stderr" and self.process.read_error is None:
                    # stderr may close before stdout fails; don't lose the
                    # transport diagnostic just because those EOFs race.
                    await self.process.process.wait()
                if self.name == "stdout" and not self.process.stopping:
                    self.process.abort("ACP stdout closed before the client closed the session")

        # The SDK's process waiter uses stderr to report pending RPC failures.
        # Deliver our diagnostic there instead of letting a reader task die.
        if self.name == "stderr" and self.process.read_error and not self.error_reported:
            self.error_reported = True
            return (self.process.read_error + "\n").encode()
        self.finished.set()
        return b""


class _ACPProcess:
    def __init__(
        self,
        process: asyncio.subprocess.Process,
        transport: asyncio.SubprocessTransport,
    ) -> None:
        self.process = process
        self.transport = transport
        self.stdin = process.stdin
        assert process.stdout is not None
        assert process.stderr is not None
        self.stdout = _LineReader(self, process.stdout, "stdout")
        self.stderr = _LineReader(self, process.stderr, "stderr")
        self.read_error: str | None = None
        self.stopping = False

    def terminate(self) -> None:
        self.stopping = True
        with contextlib.suppress(ProcessLookupError):
            self.transport.terminate()

    def kill(self) -> None:
        self.stopping = True
        with contextlib.suppress(ProcessLookupError):
            self.transport.kill()
        # asyncio's Process.wait() also waits for pipe disconnection. A failed
        # reader can leave a full, paused pipe even after the child is dead.
        # Close the pipes on forced shutdown so reaping cannot wait on readers.
        for fd in (0, 1, 2):
            pipe = self.transport.get_pipe_transport(fd)
            if pipe is not None:
                pipe.close()

    def abort(self, error: str) -> None:
        if self.read_error is None:
            self.read_error = error
        self.kill()

    async def wait(self) -> int:
        code = await self.process.wait()
        if self.read_error is not None:
            await self.stderr.finished.wait()
        return code

    async def close(self) -> None:
        self.terminate()
        try:
            await asyncio.wait_for(self.process.wait(), timeout=1)
        except TimeoutError:
            self.kill()
            await asyncio.wait_for(self.process.wait(), timeout=1)


class ACPClient(Client):
    """Retain children even when SDK session creation is canceled repeatedly."""

    def __init__(self, *, command: str, cwd: str, env: Mapping[str, str]) -> None:
        self.children: list[_ACPProcess] = []
        super().__init__(command=command, cwd=cwd, env=env, spawn=self.spawn_owned)

    async def spawn_owned(
        self, command: str, args: Sequence[str], options: SpawnOptions
    ) -> SpawnedProcess:
        process = await spawn_acp(command, args, options)
        self.children.append(cast(_ACPProcess, process))
        return process

    async def close(self) -> None:
        try:
            await super().close()
        finally:
            # The SDK only tracks fully initialized sessions. A second cancel
            # can interrupt its startup cleanup before it registers a session.
            # Retain that child ourselves until its process is actually reaped.
            await asyncio.gather(*(child.close() for child in self.children))
            self.children.clear()


async def spawn_acp(
    command: str,
    args: Sequence[str],
    options: SpawnOptions,
) -> SpawnedProcess:
    """Use the SDK's public spawn hook without patching its RPC internals."""

    loop = asyncio.get_running_loop()
    # Retain the subprocess transport so forced cleanup can close paused pipes
    # through get_pipe_transport(), rather than accessing SDK/reader internals.
    transport, protocol = await loop.subprocess_exec(
        lambda: asyncio.subprocess.SubprocessStreamProtocol(limit=ACP_MESSAGE_LIMIT, loop=loop),
        command,
        *args,
        cwd=options.get("cwd"),
        env=dict(options.get("env") or {}),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    process = asyncio.subprocess.Process(transport, protocol, loop)
    return cast(SpawnedProcess, _ACPProcess(process, transport))
