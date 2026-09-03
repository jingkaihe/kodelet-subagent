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
    ToolPresentation,
)

from . import __version__
from .persistence import (
    ACTIVE_RUN_STATUSES,
    AGENT_NAME_MAX_LENGTH,
    AGENT_NAME_PATTERN,
    MAX_STEERING_MESSAGE_LENGTH,
    AgentConflictError,
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
    Start a named Kodelet agent in the background and return immediately. Names
    must contain one to three lowercase kebab-case words and be unique. By default,
    the agent inherits the current conversation; set context_mode to fresh to start
    without it. Use followup_agent to wake an idle, failed, interrupted, or canceled
    agent.
    """
).strip()

WAIT_AGENT_DESCRIPTION = textwrap.dedent(
    """
    Wait for a background agent's current run. Returns its final response when the
    run finishes or its current status when the timeout expires. Set timeout_ms to
    0 to check without waiting.
    """
).strip()

LIST_AGENTS_DESCRIPTION = textwrap.dedent(
    """
    List background agents with their current status and latest run.
    """
).strip()

FOLLOWUP_AGENT_DESCRIPTION = textwrap.dedent(
    """
    Give an idle, failed, interrupted, or canceled background agent a new task and
    return immediately. The agent continues its existing conversation when
    available.
    """
).strip()

STEER_AGENT_DESCRIPTION = textwrap.dedent(
    """
    Send guidance to a background agent that is currently running. The guidance is
    delivered before its next model response, or kept for a later follow-up if the
    current run ends first.
    """
).strip()

CANCEL_AGENT_DESCRIPTION = textwrap.dedent(
    """
    Cancel a background agent. An active agent remains in the canceling state until
    it fully stops. Once canceled, it can be resumed with followup_agent.
    """
).strip()

SPAWN_TOOL_TIMEOUT_SECONDS = 60
FOLLOWUP_TOOL_TIMEOUT_SECONDS = 60
WAIT_TOOL_TIMEOUT_SECONDS = 5 * 60 + 10
WAIT_POLL_SECONDS = 0.1
STEER_TOOL_TIMEOUT_SECONDS = 15
CANCEL_TOOL_TIMEOUT_SECONDS = 15
MAX_WAIT_MILLISECONDS = 5 * 60 * 1000
PRESENTATION_TASK_PREVIEW_LENGTH = 160
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
            "How to initialize the agent's conversation. 'fork' copies the current "
            "conversation into an isolated child; 'fresh' starts without parent "
            "conversation context. Defaults to 'fork'."
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
        description="The ID of an idle, failed, interrupted, or canceled agent to resume.",
    )
    task: str = Field(
        min_length=1,
        description="The new task to run in the agent's existing conversation.",
    )


class SteerAgentInput(BaseModel):
    agent_id: str = Field(
        min_length=1,
        description="The ID of the currently running agent to guide.",
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


def child_tool_error(summary: str) -> ToolExecutionResult:
    message = "async agent tools are only available to the main agent"
    return tool_error(message, summary=summary)


def tool_error(message: str, *, summary: str) -> ToolExecutionResult:
    return {
        "content": message,
        "error": message,
        "data": {"presentation": tool_presentation(summary)},
    }


def tool_presentation(summary: str, *, body: str | None = None) -> ToolPresentation:
    presentation: ToolPresentation = {"summary": summary}
    if body is not None:
        presentation["body"] = body
        presentation["format"] = "markdown"
    return presentation


def agent_not_found(*, summary: str) -> ToolExecutionResult:
    return tool_error("agent not found", summary=summary)


def agent_list_presentation(agents: list[AgentRecord]) -> ToolPresentation:
    if not agents:
        return tool_presentation("List agents", body="No background agents.")

    lines: list[str] = []
    for agent in agents:
        task = " ".join(agent.run.task.split())
        if len(task) > PRESENTATION_TASK_PREVIEW_LENGTH:
            task = f"{task[: PRESENTATION_TASK_PREVIEW_LENGTH - 3]}..."
        status = "canceling" if agent.status == "canceling" else agent.run.status
        lines.append(f"- **{agent.name}** — {status}")
        if task:
            lines.append(f"  {task}")
    return tool_presentation("List agents", body="\n".join(lines))


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
        summary = "Spawn agent"
        if is_agent_child(ctx):
            return child_tool_error(summary)
        try:
            name = validate_agent_name(input.name)
        except ValueError as exc:
            return tool_error(str(exc), summary=summary)
        summary = f"Spawn {name}"
        task = input.task.strip()
        if not task:
            return tool_error(
                "task is required and must be a non-empty string",
                summary=summary,
            )
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
            summary = f"Spawn {claim.agent.name}"
            claim, live = await self.runtime.prepare_claim(
                claim,
                task,
                store,
                ctx,
                initial=True,
            )
            await self.runtime.safe_sync_agent_widget(ctx.ui, store, owner_id)
        except Exception as exc:
            return tool_error(f"spawn_agent failed: {exc}", summary=summary)

        conversation_detail = (
            f" in conversation {live.conversation_id}"
            if live.conversation_id is not None
            else " with fresh context"
        )
        snapshot = public_snapshot(claim.agent)
        snapshot["presentation"] = tool_presentation(summary, body=task)
        return {
            "content": (
                f"Spawned agent {claim.agent.name!r} ({live.agent_id})"
                f"{conversation_detail}. Continue with independent work and call "
                "wait_agent before relying on its result."
            ),
            "data": snapshot,
        }

    async def wait_agent(
        self,
        input: WaitAgentInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        summary = "Wait for agent"
        if is_agent_child(ctx):
            return child_tool_error(summary)
        agent_id = input.agent_id.strip()
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            agent = await store.get(owner_id, agent_id)
            summary = f"Wait for {agent.name}"
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
            return agent_not_found(summary=summary)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return tool_error(f"wait_agent failed: {exc}", summary=summary)

        await self.runtime.safe_sync_agent_widget(ctx.ui, store, owner_id)
        snapshot = public_snapshot(
            agent,
            include_result=agent.run.status == "completed",
        )
        snapshot["presentation"] = tool_presentation(summary)
        if progress is not None:
            snapshot["taskRun"] = await self._finish_wait_progress(progress, agent)
        return self._wait_result(agent, snapshot)

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
            running_title=f"Wait for {agent.name}",
            completed_title=f"Wait for {agent.name}",
            failed_title=f"Wait for {agent.name}",
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
        agent: AgentRecord,
        snapshot: dict[str, object],
    ) -> ToolExecutionResult:
        if agent.run.status in ACTIVE_RUN_STATUSES:
            return {
                "content": f"Agent {agent.name!r} is still {agent.run.status}.",
                "data": snapshot,
            }
        if agent.run.status == "completed":
            return {"content": agent.run.result or "", "data": snapshot}
        if agent.run.status == "failed":
            message = f"Agent {agent.name!r} failed: {agent.run.error or 'unknown error'}"
            return {"content": message, "error": message, "data": snapshot}
        if agent.run.status == "interrupted":
            message = (
                f"Agent {agent.name!r} was interrupted: "
                f"{agent.run.error or 'worker stopped'}. Use followup_agent to resume it."
            )
            return {"content": message, "error": message, "data": snapshot}
        if agent.status == "canceling":
            return {
                "content": (
                    f"Agent {agent.name!r} is still canceling. "
                    "Retry followup_agent after cancellation finishes."
                ),
                "data": snapshot,
            }
        return {
            "content": (f"Agent {agent.name!r} was canceled. Use followup_agent to resume it."),
            "data": snapshot,
        }

    async def list_agents(
        self,
        _input: ListAgentsInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        summary = "List agents"
        if is_agent_child(ctx):
            return child_tool_error(summary)
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            agents = await store.list(owner_id)
            await self.runtime.safe_sync_agent_widget(ctx.ui, store, owner_id)
        except Exception as exc:
            return tool_error(f"list_agents failed: {exc}", summary=summary)
        if not agents:
            return {
                "content": "No background agents have been spawned by this conversation.",
                "data": {
                    "agents": [],
                    "presentation": agent_list_presentation(agents),
                },
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
            "data": {
                "agents": [public_snapshot(agent) for agent in agents],
                "presentation": agent_list_presentation(agents),
            },
        }

    async def followup_agent(
        self,
        input: FollowupAgentInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        summary = "Follow up agent"
        if is_agent_child(ctx):
            return child_tool_error(summary)
        agent_id = input.agent_id.strip()
        task = input.task.strip()
        if not task:
            return tool_error(
                "task is required and must be a non-empty string",
                summary=summary,
            )
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            agent = await store.get(owner_id, agent_id)
            summary = f"Follow up {agent.name}"
            if agent.status == "canceling":
                raise AgentConflictError(
                    "agent cancellation is still in progress; retry followup_agent shortly"
                )
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
            return agent_not_found(summary=summary)
        except Exception as exc:
            return tool_error(f"followup_agent failed: {exc}", summary=summary)
        conversation_detail = (
            f" in conversation {live.conversation_id}."
            if live.conversation_id is not None
            else " with fresh context."
        )
        snapshot = public_snapshot(claim.agent)
        snapshot["presentation"] = tool_presentation(
            summary,
            body=task,
        )
        return {
            "content": (
                f"Started follow-up run {live.run_id} for agent "
                f"{claim.agent.name!r} ({live.agent_id}){conversation_detail}\n\n"
                f"Follow-up task sent:\n{task}\n\n"
                "Call wait_agent before relying on its result."
            ),
            "data": snapshot,
        }

    async def steer_agent(
        self,
        input: SteerAgentInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        summary = "Steer agent"
        if is_agent_child(ctx):
            return child_tool_error(summary)
        agent_id = input.agent_id.strip()
        message = input.message.strip()
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            agent = await store.get(owner_id, agent_id)
            summary = f"Steer {agent.name}"
            result = await store.enqueue_steering(
                owner_id,
                agent_id,
                message,
            )
        except AgentNotFoundError:
            return agent_not_found(summary=summary)
        except Exception as exc:
            return tool_error(f"steer_agent failed: {exc}", summary=summary)
        queued = " behind pending steering" if result["alreadyPending"] else ""
        return {
            "content": (
                f"Queued steering for agent {agent.name!r} ({agent.id}){queued}.\n\n"
                f"Steering message queued for delivery:\n{message}"
            ),
            "data": {
                "agent_id": agent.id,
                "name": agent.name,
                "message": message,
                "presentation": tool_presentation(
                    summary,
                    body=message,
                ),
                **result,
            },
        }

    async def cancel_agent(
        self,
        input: CancelAgentInput,
        ctx: ToolContext,
    ) -> ToolExecutionResult:
        summary = "Cancel agent"
        if is_agent_child(ctx):
            return child_tool_error(summary)
        agent_id = input.agent_id.strip()
        try:
            owner_id = owner_conversation_id(ctx)
            store = await self.runtime.store_for_context(ctx)
            agent = await store.cancel(owner_id, agent_id)
            summary = f"Cancel {agent.name}"
            await self.runtime.safe_sync_agent_widget(ctx.ui, store, owner_id)
        except AgentNotFoundError:
            return agent_not_found(summary=summary)
        except Exception as exc:
            return tool_error(f"cancel_agent failed: {exc}", summary=summary)

        canceled_run_id = agent.run.id
        cleanup_complete = True
        if agent.status == "canceling":
            cleanup_complete = await self.runtime.cancel_live_run(
                store,
                agent.id,
                canceled_run_id,
            )
        agent = await store.get(owner_id, agent.id)
        snapshot = public_snapshot(agent)
        if agent.run.id != canceled_run_id:
            snapshot["presentation"] = tool_presentation(
                summary,
                body="Cancellation was recorded; the agent has since changed generation.",
            )
            return {
                "content": (
                    f"Recorded cancellation for agent {agent.name!r}; "
                    "the agent has since changed generation."
                ),
                "data": snapshot,
            }
        if agent.status == "canceling":
            snapshot["presentation"] = tool_presentation(
                summary,
                body=(
                    "Cancellation saved; worker cleanup is still finishing. "
                    "Retry followup_agent after cleanup completes."
                ),
            )
            return {
                "content": (
                    f"Cancellation persisted for agent {agent.name!r}; "
                    "worker cleanup is still finishing. Retry followup_agent after "
                    "cleanup completes."
                ),
                "data": snapshot,
            }
        if not cleanup_complete:
            snapshot["presentation"] = tool_presentation(
                summary,
                body=(
                    "Canceled and resumable. Remaining local resource cleanup is still finishing."
                ),
            )
            return {
                "content": (
                    f"Canceled agent {agent.name!r}; it can now be resumed with "
                    "followup_agent while remaining local resource cleanup finishes."
                ),
                "data": snapshot,
            }
        snapshot["presentation"] = tool_presentation(
            summary,
            body="Canceled. Use followup_agent to resume this agent later.",
        )
        return {
            "content": (f"Canceled agent {agent.name!r}. Use followup_agent to resume it later."),
            "data": snapshot,
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
