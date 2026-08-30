"""Baseline the legacy version 1 subagent schema.

Revision ID: 0001_legacy_v1
Revises:
"""

from __future__ import annotations

from alembic import op

from kodelet_subagent.persistence._schema import (
    LEGACY_SCHEMA_STATEMENTS,
    LEGACY_USER_VERSION,
)

revision = "0001_legacy_v1"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    for statement in LEGACY_SCHEMA_STATEMENTS:
        connection.exec_driver_sql(statement)
    connection.exec_driver_sql(f"PRAGMA user_version = {LEGACY_USER_VERSION}")


def downgrade() -> None:
    connection = op.get_bind()
    connection.exec_driver_sql("DROP TABLE IF EXISTS steering_messages")
    connection.exec_driver_sql("DROP TABLE IF EXISTS runs")
    connection.exec_driver_sql("DROP TABLE IF EXISTS agents")
    connection.exec_driver_sql("PRAGMA user_version = 0")
