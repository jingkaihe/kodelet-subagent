-- Revision ID: 0004_mail_ids
-- Notification revisions must not reuse IDs after mail is pruned.
CREATE TABLE mails_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
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

INSERT INTO mails_new (
	id, owner_conversation_id, event_id, sender_id, sender_name, sender_kind,
	recipient_id, subject, body, kind, agent_id, run_id, created_at, read_at
) SELECT
	id, owner_conversation_id, event_id, sender_id, sender_name, sender_kind,
	recipient_id, subject, body, kind, agent_id, run_id, created_at, read_at
FROM mails;

DROP TABLE mails;
ALTER TABLE mails_new RENAME TO mails;

CREATE INDEX idx_mails_inbox ON mails (owner_conversation_id, recipient_id, id DESC);
CREATE INDEX idx_mails_unread ON mails (owner_conversation_id, recipient_id, id DESC) WHERE read_at IS NULL;
