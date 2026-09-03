"""Add the durable canceling agent state.

Revision ID: 0002_canceling_state
Revises: 0001_initial
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002_canceling_state"
down_revision = "0001_initial"
branch_labels = None
depends_on = None

AGENT_STATUSES = "'starting', 'running', 'canceling', 'idle', 'failed', 'interrupted', 'canceled'"
PREVIOUS_AGENT_STATUSES = "'starting', 'running', 'idle', 'failed', 'interrupted', 'canceled'"


def _backup_dependents() -> None:
    op.execute("CREATE TEMP TABLE subagent_runs_backup AS SELECT * FROM runs")
    op.execute("CREATE TEMP TABLE subagent_steering_backup AS SELECT * FROM steering_messages")


def _restore_dependents() -> None:
    op.execute(
        """
        INSERT INTO runs (
            id, agent_id, generation, lease_token, task, status,
            result, error, created_at, started_at, completed_at, updated_at
        )
        SELECT
            id, agent_id, generation, lease_token, task, status,
            result, error, created_at, started_at, completed_at, updated_at
        FROM subagent_runs_backup
        """
    )
    op.execute(
        """
        INSERT INTO steering_messages (
            id, agent_id, run_id, generation, message, created_at
        )
        SELECT id, agent_id, run_id, generation, message, created_at
        FROM subagent_steering_backup
        """
    )
    op.execute("DROP TABLE subagent_steering_backup")
    op.execute("DROP TABLE subagent_runs_backup")


def _replace_agent_constraints(*, statuses: str, leased_statuses: str) -> None:
    _backup_dependents()
    with op.batch_alter_table("agents", recreate="always") as batch_op:
        batch_op.drop_constraint("ck_agents_status", type_="check")
        batch_op.drop_constraint("ck_agents_active_lease", type_="check")
        batch_op.create_check_constraint(
            "ck_agents_status",
            f"status IN ({statuses})",
        )
        batch_op.create_check_constraint(
            "ck_agents_active_lease",
            sa.text(
                f"""
                (
                    status IN ({leased_statuses})
                    AND lease_runtime_id IS NOT NULL
                    AND lease_token IS NOT NULL
                    AND lease_expires_at IS NOT NULL
                ) OR (
                    status NOT IN ({leased_statuses})
                    AND lease_runtime_id IS NULL
                    AND lease_token IS NULL
                    AND lease_expires_at IS NULL
                )
                """
            ),
        )
    _restore_dependents()


def upgrade() -> None:
    _replace_agent_constraints(
        statuses=AGENT_STATUSES,
        leased_statuses="'starting', 'running', 'canceling'",
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE agents
        SET status = 'canceled', lease_runtime_id = NULL,
            lease_token = NULL, lease_expires_at = NULL
        WHERE status = 'canceling'
        """
    )
    _replace_agent_constraints(
        statuses=PREVIOUS_AGENT_STATUSES,
        leased_statuses="'starting', 'running'",
    )
