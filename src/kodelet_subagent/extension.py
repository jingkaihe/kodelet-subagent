"""Kodelet extension registration and public subagent tool handlers."""

from __future__ import annotations

import asyncio
import contextlib
import os
import textwrap
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from kodelet_sdk import (
    AgentInitEvent,
    AgentStartEvent,
    BaseModel,
    EventContext,
    EventResult,
    Extension,
    Field,
    SessionEndEvent,
    SessionStartEvent,
    TaskProgress,
    TaskProgressContext,
    TaskRunSnapshot,
    ToolContext,
    ToolExecutionResult,
)

from . import __version__
from .persistence import (
    ACTIVE_RUN_STATUSES,
    AGENT_NAME_MAX_LENGTH,
    AGENT_NAME_PATTERN,
    MAX_STEERING_MESSAGE_LENGTH,
    AgentNotFoundError,
    AgentRecord,
    AgentStore,
    SpawnContextMode,
    validate_agent_name,
)
from .runtime import RECURSION_GUARD_ENV, RuntimeState
from .ui import public_snapshot

SPAWN_AGENT_DESCRIPTION = textwrap.dedent(
    """
    Start a named independent Kodelet agent in the background and return
    immediately. The name must contain one to three lowercase kebab-case words and
    be unique among agents owned by this conversation.
    By default, the child inherits the caller's live conversation context in an
    isolated fork. Set context_mode to fresh to start with no parent conversation
    memory. Agent identity, run history, and recovery state are persisted by this
    extension. Use followup_agent to wake an idle, failed, or interrupted agent.
    """
).strip()

WAIT_AGENT_DESCRIPTION = textwrap.dedent(
    """
    Wait for the current run of a persisted background agent owned by this
    conversation. Returns the final response when the run finishes, or its current
    status when the timeout expires. Use a zero timeout to check the current status
    without waiting.
    """
).strip()

LIST_AGENTS_DESCRIPTION = textwrap.dedent(
    """
    List persisted background agents owned by this conversation, including their
    current agent state, latest run state, and child conversation IDs.
    """
).strip()

FOLLOWUP_AGENT_DESCRIPTION = textwrap.dedent(
    """
    Wake an idle, failed, or interrupted background agent with a new task. The new
    run resumes its persisted child conversation, or recreates the requested
    fork/fresh context if interruption happened before attachment, and returns
    immediately. Only the main agent that owns the original agent may call it.
    """
).strip()

STEER_AGENT_DESCRIPTION = textwrap.dedent(
    """
    Queue guidance for an owned background agent that is currently running. The
    extension persists the message until ACP reports that it was injected into
    the live child session. If the turn closes first, the pending message is
    carried into the next follow-up run. Injected but unconsumed guidance remains
    on the child conversation and may also be applied during a later follow-up.
    """
).strip()

CANCEL_AGENT_DESCRIPTION = textwrap.dedent(
    """
    Permanently cancel an owned background agent and fence its active worker. A
    canceled agent cannot be resumed with followup_agent.
    """
).strip()

SPAWN_TOOL_TIMEOUT_SECONDS = 60
FOLLOWUP_TOOL_TIMEOUT_SECONDS = 60
WAIT_TOOL_TIMEOUT_SECONDS = 5 * 60 + 10
WAIT_POLL_SECONDS = 0.1
STEER_TOOL_TIMEOUT_SECONDS = 15
CANCEL_TOOL_TIMEOUT_SECONDS = 15
MAX_WAIT_MILLISECONDS = 5 * 60 * 1000
AGENT_TOOL_NAMES = (
    "spawn_agent",
    "wait_agent",
    "list_agents",
    "followup_agent",
    "steer_agent",
    "cancel_agent",
)


class SpawnAgentInput(BaseModel):
    name: str = Field(
        min_length=1,
        max_length=AGENT_NAME_MAX_LENGTH,
        pattern=AGENT_NAME_PATTERN.pattern,
        description=(
            "A unique canonical name for the agent within this conversation. Use "
            "one to three lowercase words separated by hyphens, such as "
            "'reviewer' or 'code-reviewer'."
        ),
    )
    task: str = Field(
        min_length=1,
        description=(
            "A complete, self-contained task for the agent. Include the relevant "
            "context, constraints, desired outcome, and expected response format."
        ),
    )
    cwd: str | None = Field(
        default=None,
        description=(
            "Optional directory for the agent. Defaults to the current workspace "
            "directory. Relative paths are resolved against the current workspace."
        ),
    )
    context_mode: SpawnContextMode = Field(
        default="fork",
        description=(
            "How to initialize the child conversation. 'fork' copies the main "
            "agent's live conversation into an isolated child; 'fresh' starts "
            "with no parent conversation memory. Defaults to 'fork'."
        ),
    )


class WaitAgentInput(BaseModel):
    agent_id: str = Field(
        min_length=1,
        description="The agent ID returned by spawn_agent or followup_agent.",
    )
    timeout_ms: int = Field(
        default=30_000,
        ge=0,
        le=MAX_WAIT_MILLISECONDS,
        description=(
            "Maximum time to wait in milliseconds. Use 0 to return the current "
            "status immediately. Defaults to 30000 and is capped at 300000."
        ),
    )


class ListAgentsInput(BaseModel):
    pass


class FollowupAgentInput(BaseModel):
    agent_id: str = Field(
        min_length=1,
        description="The persisted agent ID to wake or resume.",
    )
    task: str = Field(
        min_length=1,
        description="The new task to run in the agent's persisted child conversation.",
    )


class SteerAgentInput(BaseModel):
    agent_id: str = Field(
        min_length=1,
        description="The running agent ID to steer.",
    )
    message: str = Field(
        min_length=1,
        max_length=MAX_STEERING_MESSAGE_LENGTH,
        description="The instruction to inject before the child agent's next model request.",
    )


class CancelAgentInput(BaseModel):
    agent_id: str = Field(
        min_length=1,
        description="The agent ID returned by spawn_agent or followup_agent.",
    )


class SubagentExtension(Extension):
    """Extension host with capability gating tied to one runtime state."""

    def __init__(self, runtime: RuntimeState) -> None:
        super().__init__(name="subagent", version=__version__)
        self.runtime = runtime

    def initialize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        self.runtime.reset_for_initialize()
        result = super().initialize(params)
        capabilities = params.get("capabilities")
        runtime_capabilities = (
            capabilities.get("runtime") if isinstance(capabilities, Mapping) else None
        )
        if (
            not isinstance(runtime_capabilities, Mapping)
            or runtime_capabilities.get("backgroundTasks") is not True
        ):
            result["tools"] = [
                tool for tool in result["tools"] if tool.get("name") not in AGENT_TOOL_NAMES
            ]
        return result


def is_agent_child(ctx: ToolContext | EventContext) -> bool:
    if os.environ.get(RECURSION_GUARD_ENV) == "1":
        return True
    invoked_by = (getattr(ctx, "invoked_by", None) or "").strip()
    return bool(invoked_by and invoked_by != "main")


def resolve_agent_cwd(raw_cwd: str | None, workspace_cwd: str) -> Path:
    if raw_cwd is None:
        return Path(workspace_cwd).resolve()
    if not raw_cwd.strip():
        raise ValueError("cwd must be a non-empty string when provided")
    candidate = Path(raw_cwd).expanduser()
    if not candidate.is_absolute():
        candidate = Path(workspace_cwd) / candidate
    resolved = candidate.resolve()
    if not resolved.exists():
        raise ValueError(f"cwd does not exist: {resolved}")
    if not resolved.is_dir():
        raise ValueError(f"cwd is not a directory: {resolved}")
    return resolved


def owner_conversation_id(ctx: ToolContext) -> str:
    conversation_id = (ctx.conversation_id or "").strip()
    if not conversation_id:
        raise ValueError("async agents require an active conversation ID")
    return conversation_id


def child_tool_error() -> ToolExecutionResult:
    message = "async agent tools are only available to the main agent"
    return {"content": message, "error": message}


def tool_error(message: str) -> ToolExecutionResult:
    return {"content": message, "error": message}


def agent_not_found(agent_id: str) -> ToolExecutionResult:
    return tool_error(f"agent not found: {agent_id.strip()}")


class SubagentApplication:
    """Bind extension protocol handlers to an isolated runtime state."""

    def __init__(self, runtime: RuntimeState | None = None) -> None:
        self.runtime = runtime or RuntimeState()
        self.extension = SubagentExtension(self.runtime)
        self._register_handlers()

    def _register_handlers(self) -> None:
        self.extension.on("agent.init")(self.disable_recursive_agents)
        self.extension.on("session.start")(self.restore_agent_widget)
        self.extension.on("agent.start")(self.restore_agent_widget)
        self.extension.on("session.end")(self.interrupt_live_agents)
        if os.environ.get(RECURSION_GUARD_ENV) == "1":
            return
        self.extension.tool(
            "spawn_agent",
            description=SPAWN_AGENT_DESCRIPTION,
            input_schema=SpawnAgentInput,
            timeout_in_sec=SPAWN_TOOL_TIMEOUT_SECONDS,
        )(self.spawn_agent)
        self.extension.tool(
            "wait_agent",
            description=WAIT_AGENT_DESCRIPTION,
            input_schema=WaitAgentInput,
            timeout_in_sec=WAIT_TOOL_TIMEOUT_SECONDS,
        )(self.wait_agent)
        self.extension.tool(
            "list_agents",
            description=LIST_AGENTS_DESCRIPTION,
            input_schema=ListAgentsInput,
            timeout_in_sec=10,
        )(self.list_agents)
        self.extension.tool(
            "followup_agent",
            description=FOLLOWUP_AGENT_DESCRIPTION,
            input_schema=FollowupAgentInput,
            timeout_in_sec=FOLLOWUP_TOOL_TIMEOUT_SECONDS,
        )(self.followup_agent)
        self.extension.tool(
            "steer_agent",
            description=STEER_AGENT_DESCRIPTION,
            input_schema=SteerAgentInput,
            timeout_in_sec=STEER_TOOL_TIMEOUT_SECONDS,
        )(self.steer_agent)
        self.extension.tool(
            "cancel_agent",
            description=CANCEL_AGENT_DESCRIPTION,
            input_schema=CancelAgentInput,
            timeout_in_sec=CANCEL_TOOL_TIMEOUT_SECONDS,
        )(self.cancel_agent)

    async def disable_recursive_agents(
        self,
        _event: AgentInitEvent,
        ctx: EventContext,
    ) -> EventResult | None:
        if is_agent_child(ctx):
            return {"tools": {"disable": list(AGENT_TOOL_NAMES)}}
        return None

    async def restore_agent_widget(
        self,
        _event: SessionStartEvent | AgentStartEvent,
        ctx: EventContext,
    ) -> None:
        if is_agent_child(ctx):
            return
        owner_id = (ctx.conversation_id or "").strip()
        if not owner_id:
            return
        try:
            store = await self.runtime.store_for_context(ctx)
            await self.runtime.sync_agent_widget(ctx.ui, store, owner_id)
        except Exception as exc:
            ctx.log.warn(f"failed to restore background-agent widget: {exc}")

    async def interrupt_live_agents(
        self,
        _event: SessionEndEvent,
        _ctx: EventContext,
    ) -> None:
        await self.runtime.shutdown()

    async def spawn_agent(
        self,
        input: SpawnAgentInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        if is_agent_child(ctx):
            return child_tool_error()
        try:
            name = validate_agent_name(input.name)
        except ValueError as exc:
            return tool_error(str(exc))
        task = input.task.strip()
        if not task:
            return tool_error("task is required and must be a non-empty string")
        try:
            owner_id = owner_conversation_id(ctx)
            agent_cwd = resolve_agent_cwd(input.cwd, ctx.cwd)
            store = await self.runtime.store_for_context(ctx)
            claim = await self.runtime.reserve_claim(
                store.create(
                    owner_id,
                    name,
                    task,
                    str(agent_cwd),
                    input.context_mode,
                ),
                task,
                store,
                initial=True,
            )
            claim, live = await self.runtime.prepare_claim(
                claim,
                task,
                store,
                ctx,
                initial=True,
            )
            await self.runtime.safe_sync_agent_widget(ctx.ui, store, owner_id)
        except Exception as exc:
            return tool_error(f"spawn_agent failed: {exc}")

        conversation_detail = (
            f" in conversation {live.conversation_id}"
            if live.conversation_id is not None
            else " with fresh context"
        )
        return {
            "content": (
                f"Spawned agent {claim.agent.name!r} ({live.agent_id})"
                f"{conversation_detail}. Continue with independent work and call "
                "wait_agent before relying on its result."
            ),
            "data": public_snapshot(claim.agent),
        }

    async def wait_agent(
        self,
        input: WaitAgentInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        if is_agent_child(ctx):
            return child_tool_error()
        agent_id = input.agent_id.strip()
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            agent = await store.get(owner_id, agent_id)
            progress: TaskProgress | None = None
            if agent.run.status in ACTIVE_RUN_STATUSES and input.timeout_ms > 0:
                agent, progress = await self._wait_for_active_run(
                    ctx,
                    store,
                    owner_id,
                    agent,
                    input.timeout_ms,
                )
        except AgentNotFoundError:
            return agent_not_found(agent_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return tool_error(f"wait_agent failed: {exc}")

        await self.runtime.safe_sync_agent_widget(ctx.ui, store, owner_id)
        snapshot = public_snapshot(
            agent,
            include_result=agent.run.status == "completed",
        )
        if progress is not None:
            snapshot["taskRun"] = await self._finish_wait_progress(progress, agent)
        return self._wait_result(agent_id, agent, snapshot)

    async def _wait_for_active_run(
        self,
        ctx: ToolContext,
        store: AgentStore,
        owner_id: str,
        agent: AgentRecord,
        timeout_ms: int,
    ) -> tuple[AgentRecord, TaskProgress]:
        progress = TaskProgress(
            cast(TaskProgressContext, ctx),
            kind="subagent",
            task=agent.run.task,
            cwd=agent.cwd,
            running_title="Waiting for background agent",
            completed_title="Finished waiting for background agent",
            failed_title="Background agent failed",
            responding_detail="agent is responding",
        )
        await progress.start()
        try:
            agent = await self._poll_agent_run(
                progress,
                store,
                owner_id,
                agent,
                timeout_ms,
            )
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await progress.finish(success=False, error="wait canceled")
            raise
        except Exception as exc:
            with contextlib.suppress(Exception):
                await progress.finish(success=False, error=str(exc))
            raise
        return agent, progress

    async def _poll_agent_run(
        self,
        progress: TaskProgress,
        store: AgentStore,
        owner_id: str,
        agent: AgentRecord,
        timeout_ms: int,
    ) -> AgentRecord:
        selected_run_id = agent.run.id
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_ms / 1000
        attached = False
        while agent.run.status in ACTIVE_RUN_STATUSES:
            if not attached:
                live = self.runtime.get_live_run(store, agent.id)
                if live is not None and live.run_id == selected_run_id and live.session is not None:
                    progress.attach(live.session)
                    attached = True
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(WAIT_POLL_SECONDS, remaining))
            agent = await store.get(owner_id, agent.id, selected_run_id)
        return agent

    @staticmethod
    async def _finish_wait_progress(
        progress: TaskProgress,
        agent: AgentRecord,
    ) -> TaskRunSnapshot:
        if agent.run.status in ACTIVE_RUN_STATUSES:
            await progress.flush()
            task_run = progress.snapshot()
            await progress.finish(success=True)
            return task_run
        success = agent.run.status == "completed"
        error = None if success else agent.run.error or f"agent {agent.run.status}"
        return await progress.finish(success=success, error=error)

    @staticmethod
    def _wait_result(
        agent_id: str,
        agent: AgentRecord,
        snapshot: dict[str, object],
    ) -> ToolExecutionResult:
        if agent.run.status in ACTIVE_RUN_STATUSES:
            return {
                "content": f"Agent {agent_id} is still {agent.run.status}.",
                "data": snapshot,
            }
        if agent.run.status == "completed":
            return {"content": agent.run.result or "", "data": snapshot}
        if agent.run.status == "failed":
            message = f"Agent {agent_id} failed: {agent.run.error or 'unknown error'}"
            return {"content": message, "error": message, "data": snapshot}
        if agent.run.status == "interrupted":
            message = (
                f"Agent {agent_id} was interrupted: "
                f"{agent.run.error or 'worker stopped'}. Use followup_agent to resume it."
            )
            return {"content": message, "error": message, "data": snapshot}
        return {
            "content": f"Agent {agent_id} was canceled.",
            "data": snapshot,
        }

    async def list_agents(
        self,
        _input: ListAgentsInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        if is_agent_child(ctx):
            return child_tool_error()
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            agents = await store.list(owner_id)
            await self.runtime.safe_sync_agent_widget(ctx.ui, store, owner_id)
        except Exception as exc:
            return tool_error(f"list_agents failed: {exc}")
        if not agents:
            return {
                "content": "No background agents have been spawned by this conversation.",
                "data": {"agents": []},
            }
        lines = [
            "Background agents:",
            "Name\tAgent ID\tConversation ID\tAgent state\tRun state\tTask",
        ]
        for agent in agents:
            task = " ".join(agent.run.task.split())
            if len(task) > 100:
                task = f"{task[:97]}..."
            conversation_id = agent.conversation_id or "pending"
            lines.append(
                f"{agent.name}\t{agent.id}\t{conversation_id}\t{agent.status}\t"
                f"{agent.run.status}\t{task}"
            )
        return {
            "content": "\n".join(lines),
            "data": {"agents": [public_snapshot(agent) for agent in agents]},
        }

    async def followup_agent(
        self,
        input: FollowupAgentInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        if is_agent_child(ctx):
            return child_tool_error()
        agent_id = input.agent_id.strip()
        task = input.task.strip()
        if not task:
            return tool_error("task is required and must be a non-empty string")
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            claim = await self.runtime.reserve_claim(
                store.claim(owner_id, agent_id, task),
                task,
                store,
                initial=False,
            )
            claim, live = await self.runtime.prepare_claim(
                claim,
                task,
                store,
                ctx,
                initial=False,
            )
            await self.runtime.safe_sync_agent_widget(ctx.ui, store, owner_id)
        except AgentNotFoundError:
            return agent_not_found(agent_id)
        except Exception as exc:
            return tool_error(f"followup_agent failed: {exc}")
        conversation_detail = (
            f" in conversation {live.conversation_id}."
            if live.conversation_id is not None
            else " with fresh context."
        )
        return {
            "content": (
                f"Started follow-up run {live.run_id} for agent "
                f"{claim.agent.name!r} ({live.agent_id}){conversation_detail}\n\n"
                f"Follow-up task sent:\n{task}\n\n"
                "Call wait_agent before relying on its result."
            ),
            "data": public_snapshot(claim.agent),
        }

    async def steer_agent(
        self,
        input: SteerAgentInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        if is_agent_child(ctx):
            return child_tool_error()
        agent_id = input.agent_id.strip()
        message = input.message.strip()
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            result = await store.enqueue_steering(
                owner_id,
                agent_id,
                message,
            )
        except AgentNotFoundError:
            return agent_not_found(agent_id)
        except Exception as exc:
            return tool_error(f"steer_agent failed: {exc}")
        queued = " behind pending steering" if result["alreadyPending"] else ""
        return {
            "content": (
                f"Queued steering for agent {agent_id}{queued}.\n\n"
                f"Steering message queued for delivery:\n{message}"
            ),
            "data": {"agent_id": agent_id, "message": message, **result},
        }

    async def cancel_agent(
        self,
        input: CancelAgentInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        if is_agent_child(ctx):
            return child_tool_error()
        agent_id = input.agent_id.strip()
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            agent = await store.cancel(owner_id, agent_id)
            await self.runtime.safe_sync_agent_widget(ctx.ui, store, owner_id)
        except AgentNotFoundError:
            return agent_not_found(agent_id)
        except Exception as exc:
            return tool_error(f"cancel_agent failed: {exc}")

        cleanup_complete = await self.runtime.cancel_live_run(store, agent_id)
        if not cleanup_complete:
            return {
                "content": (
                    f"Cancellation persisted for agent {agent_id}; "
                    "local cleanup is still finishing."
                ),
                "data": public_snapshot(agent),
            }
        return {
            "content": f"Canceled agent {agent_id}.",
            "data": public_snapshot(agent),
        }


application = SubagentApplication()
runtime = application.runtime
ext = application.extension

# Stable handler names support direct library use as well as the executable entrypoint.
disable_recursive_agents = application.disable_recursive_agents
restore_agent_widget = application.restore_agent_widget
interrupt_live_agents = application.interrupt_live_agents
spawn_agent = application.spawn_agent
wait_agent = application.wait_agent
list_agents = application.list_agents
followup_agent = application.followup_agent
steer_agent = application.steer_agent
cancel_agent = application.cancel_agent

__all__ = [
    "AGENT_TOOL_NAMES",
    "RECURSION_GUARD_ENV",
    "CancelAgentInput",
    "FollowupAgentInput",
    "ListAgentsInput",
    "RuntimeState",
    "SpawnAgentInput",
    "SteerAgentInput",
    "SubagentApplication",
    "SubagentExtension",
    "WaitAgentInput",
    "cancel_agent",
    "disable_recursive_agents",
    "ext",
    "followup_agent",
    "interrupt_live_agents",
    "is_agent_child",
    "list_agents",
    "restore_agent_widget",
    "spawn_agent",
    "steer_agent",
    "wait_agent",
]
