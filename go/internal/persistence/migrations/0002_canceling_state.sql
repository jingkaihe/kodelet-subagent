-- Revision ID: 0002_canceling_state
-- Revises: 0001_initial
-- Add the durable canceling agent state.
--
-- Rebuilds agents the way Alembic batch mode did. Dropping agents cascades to
-- its dependents while foreign keys are enforced, so they are restored after.

CREATE TEMP TABLE subagent_runs_backup AS SELECT * FROM runs;

CREATE TEMP TABLE subagent_steering_backup AS SELECT * FROM steering_messages;

CREATE TABLE _alembic_tmp_agents (
	id TEXT NOT NULL, 
	name TEXT NOT NULL, 
	owner_conversation_id TEXT NOT NULL, 
	child_conversation_id TEXT, 
	context_mode TEXT NOT NULL, 
	cwd TEXT NOT NULL, 
	status TEXT NOT NULL, 
	active_run_id TEXT NOT NULL, 
	generation INTEGER NOT NULL, 
	lease_runtime_id TEXT, 
	lease_token TEXT, 
	lease_expires_at FLOAT, 
	created_at FLOAT NOT NULL, 
	updated_at FLOAT NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_agents_context_mode CHECK (context_mode IN ('fork', 'fresh')), 
	CONSTRAINT ck_agents_running_conversation CHECK (status != 'running' OR child_conversation_id IS NOT NULL), 
	CONSTRAINT uq_agents_child_conversation_id UNIQUE (child_conversation_id), 
	CONSTRAINT ck_agents_generation CHECK (generation >= 1), 
	CONSTRAINT ck_agents_status CHECK (status IN ('starting', 'running', 'canceling', 'idle', 'failed', 'interrupted', 'canceled')), 
	CONSTRAINT ck_agents_active_lease CHECK (
                (
                    status IN ('starting', 'running', 'canceling')
                    AND lease_runtime_id IS NOT NULL
                    AND lease_token IS NOT NULL
                    AND lease_expires_at IS NOT NULL
                ) OR (
                    status NOT IN ('starting', 'running', 'canceling')
                    AND lease_runtime_id IS NULL
                    AND lease_token IS NULL
                    AND lease_expires_at IS NULL
                )
                )
);

INSERT INTO _alembic_tmp_agents (id, name, owner_conversation_id, child_conversation_id, context_mode, cwd, status, active_run_id, generation, lease_runtime_id, lease_token, lease_expires_at, created_at, updated_at) SELECT id, name, owner_conversation_id, child_conversation_id, context_mode, cwd, status, active_run_id, generation, lease_runtime_id, lease_token, lease_expires_at, created_at, updated_at FROM agents;

DROP TABLE agents;

ALTER TABLE _alembic_tmp_agents RENAME TO agents;

CREATE INDEX idx_agents_owner_updated ON agents (owner_conversation_id, updated_at);

CREATE UNIQUE INDEX idx_agents_owner_name ON agents (owner_conversation_id, name);

CREATE INDEX idx_agents_runtime_status ON agents (lease_runtime_id, status);

CREATE INDEX idx_agents_status_lease ON agents (status, lease_expires_at);

INSERT INTO runs (
    id, agent_id, generation, lease_token, task, status,
    result, error, created_at, started_at, completed_at, updated_at
)
SELECT
    id, agent_id, generation, lease_token, task, status,
    result, error, created_at, started_at, completed_at, updated_at
FROM subagent_runs_backup;

INSERT INTO steering_messages (
    id, agent_id, run_id, generation, message, created_at
)
SELECT id, agent_id, run_id, generation, message, created_at
FROM subagent_steering_backup;

DROP TABLE subagent_steering_backup;

DROP TABLE subagent_runs_backup;
