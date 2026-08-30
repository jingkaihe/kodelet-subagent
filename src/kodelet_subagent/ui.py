"""Presentation helpers for persisted background agents."""

from __future__ import annotations

from datetime import UTC, datetime

from kodelet_sdk import UIFrameLine, UIStyle, UIStyledSpan

from .persistence import ACTIVE_RUN_STATUSES, AgentRecord

WIDGET_ID = "background-agents"
WIDGET_AGENT_LIMIT = 8


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
        snapshot["result"] = agent.run.result
    return snapshot


def agent_widget_line(agent: AgentRecord) -> UIFrameLine:
    """Render one persisted agent as a styled widget line."""

    icon_by_status = {
        "starting": "◌",
        "running": "●",
        "completed": "✓",
        "failed": "!",
        "interrupted": "!",
        "canceled": "×",
    }
    name = agent.name
    if len(name) > 88:
        name = f"{name[:85]}..."
    status = agent.run.status
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
    completed = sum(agent.run.status == "completed" for agent in agents)
    attention = sum(agent.run.status in {"failed", "interrupted"} for agent in agents)
    canceled = sum(agent.run.status == "canceled" for agent in agents)
    summary_parts = [f"{active} active", f"{completed} completed"]
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
