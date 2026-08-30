"""Create the initial subagent schema.

Revision ID: 0001_initial
Revises:
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agents",
        sa.Column("id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("owner_conversation_id", sa.Text(), nullable=False),
        sa.Column("child_conversation_id", sa.Text(), nullable=True),
        sa.Column("context_mode", sa.Text(), nullable=False),
        sa.Column("cwd", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("active_run_id", sa.Text(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("lease_runtime_id", sa.Text(), nullable=True),
        sa.Column("lease_token", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.Float(), nullable=False),
        sa.CheckConstraint(
            "context_mode IN ('fork', 'fresh')",
            name="ck_agents_context_mode",
        ),
        sa.CheckConstraint(
            "status IN ('starting', 'running', 'idle', 'failed', 'interrupted', 'canceled')",
            name="ck_agents_status",
        ),
        sa.CheckConstraint("generation >= 1", name="ck_agents_generation"),
        sa.CheckConstraint(
            "status != 'running' OR child_conversation_id IS NOT NULL",
            name="ck_agents_running_conversation",
        ),
        sa.CheckConstraint(
            """
            (
                status IN ('starting', 'running')
                AND lease_runtime_id IS NOT NULL
                AND lease_token IS NOT NULL
                AND lease_expires_at IS NOT NULL
            ) OR (
                status NOT IN ('starting', 'running')
                AND lease_runtime_id IS NULL
                AND lease_token IS NULL
                AND lease_expires_at IS NULL
            )
            """,
            name="ck_agents_active_lease",
        ),
        sa.UniqueConstraint(
            "child_conversation_id",
            name="uq_agents_child_conversation_id",
        ),
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("agent_id", sa.Text(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("lease_token", sa.Text(), nullable=False),
        sa.Column("task", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("result", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("started_at", sa.Float(), nullable=True),
        sa.Column("completed_at", sa.Float(), nullable=True),
        sa.Column("updated_at", sa.Float(), nullable=False),
        sa.CheckConstraint("generation >= 1", name="ck_runs_generation"),
        sa.CheckConstraint(
            "status IN ('starting', 'running', 'completed', 'failed', 'interrupted', 'canceled')",
            name="ck_runs_status",
        ),
        sa.ForeignKeyConstraint(
            ["agent_id"],
            ["agents.id"],
            name="fk_runs_agent_id_agents",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("agent_id", "generation", name="uq_runs_agent_generation"),
    )
    op.create_table(
        "steering_messages",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("agent_id", sa.Text(), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.CheckConstraint("generation >= 1", name="ck_steering_messages_generation"),
        sa.ForeignKeyConstraint(
            ["agent_id"],
            ["agents.id"],
            name="fk_steering_messages_agent_id_agents",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["runs.id"],
            name="fk_steering_messages_run_id_runs",
            ondelete="CASCADE",
        ),
    )

    op.create_index(
        "idx_agents_owner_updated",
        "agents",
        ["owner_conversation_id", sa.text("updated_at DESC")],
    )
    op.create_index(
        "idx_agents_owner_name",
        "agents",
        ["owner_conversation_id", "name"],
        unique=True,
    )
    op.create_index(
        "idx_agents_status_lease",
        "agents",
        ["status", "lease_expires_at"],
    )
    op.create_index(
        "idx_agents_runtime_status",
        "agents",
        ["lease_runtime_id", "status"],
    )
    op.create_index(
        "idx_runs_agent_created",
        "runs",
        ["agent_id", sa.text("created_at DESC")],
    )
    op.create_index(
        "idx_steering_run",
        "steering_messages",
        ["agent_id", "run_id", "generation", "id"],
    )


def downgrade() -> None:
    op.drop_index("idx_steering_run", table_name="steering_messages")
    op.drop_table("steering_messages")
    op.drop_index("idx_runs_agent_created", table_name="runs")
    op.drop_table("runs")
    op.drop_index("idx_agents_runtime_status", table_name="agents")
    op.drop_index("idx_agents_status_lease", table_name="agents")
    op.drop_index("idx_agents_owner_name", table_name="agents")
    op.drop_index("idx_agents_owner_updated", table_name="agents")
    op.drop_table("agents")
