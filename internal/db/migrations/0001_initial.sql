-- Revision ID: 0001_initial
-- Create the initial subagent schema.

CREATE TABLE agents (
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
	CONSTRAINT ck_agents_status CHECK (status IN ('starting', 'running', 'idle', 'failed', 'interrupted', 'canceled')),
	CONSTRAINT ck_agents_generation CHECK (generation >= 1),
	CONSTRAINT ck_agents_running_conversation CHECK (status != 'running' OR child_conversation_id IS NOT NULL),
	CONSTRAINT ck_agents_active_lease CHECK (
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
            ),
	CONSTRAINT uq_agents_child_conversation_id UNIQUE (child_conversation_id)
);

CREATE TABLE runs (
	id TEXT NOT NULL,
	agent_id TEXT NOT NULL,
	generation INTEGER NOT NULL,
	lease_token TEXT NOT NULL,
	task TEXT NOT NULL,
	status TEXT NOT NULL,
	result TEXT,
	error TEXT,
	created_at FLOAT NOT NULL,
	started_at FLOAT,
	completed_at FLOAT,
	updated_at FLOAT NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_runs_generation CHECK (generation >= 1),
	CONSTRAINT ck_runs_status CHECK (status IN ('starting', 'running', 'completed', 'failed', 'interrupted', 'canceled')),
	CONSTRAINT fk_runs_agent_id_agents FOREIGN KEY(agent_id) REFERENCES agents (id) ON DELETE CASCADE,
	CONSTRAINT uq_runs_agent_generation UNIQUE (agent_id, generation)
);

CREATE TABLE steering_messages (
	id INTEGER NOT NULL,
	agent_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	generation INTEGER NOT NULL,
	message TEXT NOT NULL,
	created_at FLOAT NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_steering_messages_generation CHECK (generation >= 1),
	CONSTRAINT fk_steering_messages_agent_id_agents FOREIGN KEY(agent_id) REFERENCES agents (id) ON DELETE CASCADE,
	CONSTRAINT fk_steering_messages_run_id_runs FOREIGN KEY(run_id) REFERENCES runs (id) ON DELETE CASCADE
);

CREATE INDEX idx_agents_owner_updated ON agents (owner_conversation_id, updated_at DESC);

CREATE UNIQUE INDEX idx_agents_owner_name ON agents (owner_conversation_id, name);

CREATE INDEX idx_agents_status_lease ON agents (status, lease_expires_at);

CREATE INDEX idx_agents_runtime_status ON agents (lease_runtime_id, status);

CREATE INDEX idx_runs_agent_created ON runs (agent_id, created_at DESC);

CREATE INDEX idx_steering_run ON steering_messages (agent_id, run_id, generation, id);

