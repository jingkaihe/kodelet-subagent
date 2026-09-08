"""Presentation helpers for persisted background agents."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from kodelet_sdk import (
    LogContext,
    TaskProgress,
    TaskProgressContext,
    TaskProgressLogger,
    TaskRunSnapshot,
    UIFrameLine,
    UIStyle,
    UIStyledSpan,
)

from .persistence import ACTIVE_RUN_STATUSES, AgentRecord

WIDGET_ID = "background-agents"
WIDGET_AGENT_LIMIT = 8


class _SnapshotProgressContext:
    """Accumulate SDK progress without publishing to an expired spawn/wait call."""

    log: TaskProgressLogger = LogContext("kodelet-subagent")

    async def update(self, content: str, data: Mapping[str, Any] | None = None) -> None:
        pass


def agent_task_progress(agent: AgentRecord) -> TaskProgress:
    """Keep bounded activity history for a run, independently of its waiters."""

    return TaskProgress(
        _SnapshotProgressContext(),
        kind="subagent",
        task=agent.run.task,
        cwd=agent.cwd,
        running_title=f"Wait for {agent.name}",
        completed_title=f"Wait for {agent.name}",
        failed_title=f"Wait for {agent.name}",
        responding_detail="agent is responding",
    )


async def publish_task_progress(ctx: TaskProgressContext, snapshot: TaskRunSnapshot) -> None:
    """Forward a snapshot only while its wait call is alive; do not spawn a task."""

    content = snapshot["title"]
    if snapshot["detail"]:
        content += f" - {snapshot['detail']}"
    try:
        await ctx.update(content, {"taskRun": snapshot})
    except Exception as exc:
        ctx.log.warn("failed to publish tool update", {"error": str(exc)})


def timestamp(value: float | None) -> str | None:
    """Render an epoch timestamp as the extension's public UTC representation."""

    if value is None:
        return None
    return datetime.fromtimestamp(value, UTC).isoformat().replace("+00:00", "Z")


def public_snapshot(
    agent: AgentRecord,
    *,
    include_result: bool = False,
) -> dict[str, object]:
    """Return the stable assistant-facing representation of an agent run."""

    snapshot: dict[str, object] = {
        "agent_id": agent.id,
        "name": agent.name,
        "run_id": agent.run.id,
        "generation": agent.run.generation,
        "conversation_id": agent.conversation_id,
        "status": agent.run.status,
        "agent_status": agent.status,
        "task": agent.run.task,
        "cwd": agent.cwd,
        "context_mode": agent.context_mode,
        "created_at": timestamp(agent.run.created_at),
        "started_at": timestamp(agent.run.started_at),
        "completed_at": timestamp(agent.run.completed_at),
        "updated_at": timestamp(agent.run.updated_at),
    }
    if agent.run.error:
        snapshot["error"] = agent.run.error
    if include_result and agent.run.result is not None:
        result_key = "result" if agent.run.status == "completed" else "partial_result"
        snapshot[result_key] = agent.run.result
    return snapshot


def agent_widget_line(agent: AgentRecord) -> UIFrameLine:
    """Render one persisted agent as a styled widget line."""

    icon_by_status = {
        "starting": "◌",
        "running": "●",
        "canceling": "×",
        "completed": "✓",
        "failed": "!",
        "interrupted": "!",
        "canceled": "×",
    }
    name = agent.name
    if len(name) > 88:
        name = f"{name[:85]}..."
    status = "canceling" if agent.status == "canceling" else agent.run.status
    icon_style: UIStyle = {"bold": True} if status in ACTIVE_RUN_STATUSES else {"dim": True}
    status_style: UIStyle = {"bold": True} if status in {"failed", "interrupted"} else {"dim": True}
    spans: list[UIStyledSpan] = [
        {"text": f"{icon_by_status[status]} ", "style": icon_style},
        {"text": name},
        {"text": f"  {status}", "style": status_style},
        {"text": f"  {agent.id[-8:]}", "style": {"dim": True}},
    ]
    return {"spans": spans}


def agent_widget_lines(agents: list[AgentRecord]) -> list[UIFrameLine]:
    """Render the complete persistent background-agent widget."""

    active = sum(agent.run.status in ACTIVE_RUN_STATUSES for agent in agents)
    canceling = sum(agent.status == "canceling" for agent in agents)
    completed = sum(agent.run.status == "completed" for agent in agents)
    attention = sum(agent.run.status in {"failed", "interrupted"} for agent in agents)
    canceled = sum(
        agent.run.status == "canceled" and agent.status != "canceling" for agent in agents
    )
    summary_parts = [f"{active} active", f"{completed} completed"]
    if canceling:
        summary_parts.append(f"{canceling} canceling")
    if attention:
        summary_parts.append(f"{attention} need attention")
    if canceled:
        summary_parts.append(f"{canceled} canceled")
    header: list[UIStyledSpan] = [
        {"text": "Background agents", "style": {"bold": True}},
        {"text": f"  {' · '.join(summary_parts)}", "style": {"dim": True}},
    ]
    lines: list[UIFrameLine] = [{"spans": header}]
    lines.extend(agent_widget_line(agent) for agent in agents[:WIDGET_AGENT_LIMIT])
    if len(agents) > WIDGET_AGENT_LIMIT:
        lines.append(
            {
                "spans": [
                    {
                        "text": f"… and {len(agents) - WIDGET_AGENT_LIMIT} more",
                        "style": {"dim": True},
                    }
                ]
            }
        )
    return lines


__all__ = [
    "ACTIVE_RUN_STATUSES",
    "WIDGET_AGENT_LIMIT",
    "WIDGET_ID",
    "agent_widget_line",
    "agent_widget_lines",
    "public_snapshot",
    "timestamp",
]
