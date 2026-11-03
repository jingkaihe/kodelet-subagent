-- Revision ID: 0003_mailbox
-- Rooms opt conversations in without replaying historical lifecycle events.
CREATE TABLE mailbox_rooms (
	owner_conversation_id TEXT PRIMARY KEY NOT NULL,
	created_at FLOAT NOT NULL
);

CREATE TABLE mailbox_events (
	id TEXT PRIMARY KEY NOT NULL,
	owner_conversation_id TEXT NOT NULL REFERENCES mailbox_rooms(owner_conversation_id),
	created_at FLOAT NOT NULL
);

CREATE TABLE mails (
	id INTEGER PRIMARY KEY,
	owner_conversation_id TEXT NOT NULL REFERENCES mailbox_rooms(owner_conversation_id),
	event_id TEXT REFERENCES mailbox_events(id),
	sender_id TEXT NOT NULL,
	sender_name TEXT NOT NULL,
	sender_kind TEXT NOT NULL CHECK (sender_kind IN ('main', 'agent', 'system')),
	recipient_id TEXT NOT NULL,
	subject TEXT NOT NULL,
	body TEXT NOT NULL,
	kind TEXT NOT NULL,
	agent_id TEXT NOT NULL DEFAULT '',
	run_id TEXT NOT NULL DEFAULT '',
	created_at FLOAT NOT NULL,
	read_at FLOAT,
	UNIQUE (event_id, recipient_id)
);

CREATE INDEX idx_mails_inbox ON mails (owner_conversation_id, recipient_id, id DESC);
CREATE INDEX idx_mails_unread ON mails (owner_conversation_id, recipient_id, id DESC) WHERE read_at IS NULL;
