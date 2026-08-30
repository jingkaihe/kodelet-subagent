from __future__ import annotations

from alembic import context

config = context.config
target_metadata = None


def _run_with_connection(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        transaction_per_migration=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connection = config.attributes.get("connection")
    if connection is None:
        raise RuntimeError(
            "subagent migrations require a connection supplied by migrate_database()"
        )
    _run_with_connection(connection)


run_migrations_online()
