from __future__ import annotations

BASELINE_REVISION = "0001_legacy_v1"
LEGACY_USER_VERSION = 1

LEGACY_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS agents (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        owner_conversation_id TEXT NOT NULL,
        child_conversation_id TEXT UNIQUE,
        context_mode TEXT NOT NULL CHECK(context_mode IN ('fork', 'fresh')),
        cwd TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN (
            'starting', 'running', 'idle', 'failed', 'interrupted', 'canceled'
        )),
        active_run_id TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK(generation >= 1),
        lease_runtime_id TEXT,
        lease_token TEXT,
        lease_expires_at REAL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        CHECK(status != 'running' OR child_conversation_id IS NOT NULL),
        CHECK(
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
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
        id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK(generation >= 1),
        lease_token TEXT NOT NULL,
        task TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN (
            'starting', 'running', 'completed', 'failed', 'interrupted', 'canceled'
        )),
        result TEXT,
        error TEXT,
        created_at REAL NOT NULL,
        started_at REAL,
        completed_at REAL,
        updated_at REAL NOT NULL,
        UNIQUE(agent_id, generation),
        FOREIGN KEY(agent_id) REFERENCES agents(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS steering_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK(generation >= 1),
        message TEXT NOT NULL,
        created_at REAL NOT NULL,
        FOREIGN KEY(agent_id) REFERENCES agents(id) ON DELETE CASCADE,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agents_owner_updated
    ON agents(owner_conversation_id, updated_at DESC)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_owner_name
    ON agents(owner_conversation_id, name)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agents_status_lease
    ON agents(status, lease_expires_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agents_runtime_status
    ON agents(lease_runtime_id, status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_runs_agent_created
    ON runs(agent_id, created_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_steering_run
    ON steering_messages(agent_id, run_id, generation, id)
    """,
)

LEGACY_SCHEMA_SQL = (
    ";\n\n".join(statement.strip() for statement in LEGACY_SCHEMA_STATEMENTS)
    + f";\n\nPRAGMA user_version = {LEGACY_USER_VERSION};\n"
)
