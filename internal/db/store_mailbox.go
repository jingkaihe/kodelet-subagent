package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// Mailbox limits are byte-based so Unicode and serialized tool output stay bounded.
const (
	MaxMailSubjectBytes = 256
	MaxMailBodyBytes    = 16_000
	MaxReadMails        = 10
	MaxMailReadBytes    = 48_000
	MaxMailListLimit    = 50
)

// MailboxEnabled opts this store into publishing lifecycle mail in enabled rooms.
// Mailbox methods themselves are explicit opt-in operations; the extension gates
// their availability. Disabling publication never deletes existing mail.
func MailboxEnabled(enabled bool) StoreOption {
	return func(s *Store) { s.mailboxEnabled = enabled }
}

// ResolveMailbox resolves host identity and opts its root conversation into mail.
// Child lookup comes first, even when the caller could otherwise be a root.
func (s *Store) ResolveMailbox(ctx context.Context, conversationID string, allowRoot bool) (types.MailboxIdentity, error) {
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return types.MailboxIdentity{}, errors.New("mailbox requires an active conversation ID")
	}
	var identity types.MailboxIdentity
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		// Reconcile before creating a room, so enabling mail does not announce
		// claims that had already expired while the room was disabled.
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		var row struct {
			ID    string `db:"id"`
			Owner string `db:"owner_conversation_id"`
		}
		err := tx.Get(&row, `SELECT id, owner_conversation_id FROM agents WHERE child_conversation_id = ?`, conversationID)
		switch {
		case err == nil:
			identity = types.MailboxIdentity{OwnerConversationID: row.Owner, ContactID: row.ID}
		case !errors.Is(err, sql.ErrNoRows):
			return err
		case !allowRoot:
			return newError(kindNotFound, "conversation is not a registered mailbox participant")
		default:
			identity = types.MailboxIdentity{OwnerConversationID: conversationID, ContactID: "main"}
		}
		_, err = tx.Exec(`INSERT INTO mailbox_rooms (owner_conversation_id, created_at) VALUES (?, ?)
			ON CONFLICT (owner_conversation_id) DO NOTHING`, identity.OwnerConversationID, now)
		return err
	})
	if err != nil {
		return types.MailboxIdentity{}, err
	}
	return identity, nil
}

const contactColumns = `agents.id, agents.name, 'agent' AS kind, agents.status, runs.task`

func mailboxContactTx(tx *sqlx.Tx, ownerID, contactID string) (types.Contact, error) {
	if contactID == "main" {
		return types.Contact{ID: "main", Name: "main", Kind: "main"}, nil
	}
	var contact types.Contact
	err := tx.Get(&contact, `SELECT `+contactColumns+` FROM agents JOIN runs ON runs.id = agents.active_run_id
		WHERE agents.owner_conversation_id = ? AND agents.id = ?`, ownerID, contactID)
	if errors.Is(err, sql.ErrNoRows) {
		return types.Contact{}, newError(kindNotFound, "mailbox contact not found")
	}
	return contact, err
}

func validateMailboxTx(tx *sqlx.Tx, identity types.MailboxIdentity) (types.Contact, error) {
	var enabled bool
	if err := tx.Get(&enabled, `SELECT EXISTS (SELECT 1 FROM mailbox_rooms WHERE owner_conversation_id = ?)`, identity.OwnerConversationID); err != nil {
		return types.Contact{}, err
	}
	if !enabled || identity.OwnerConversationID == "" || identity.ContactID == "system" {
		return types.Contact{}, newError(kindNotFound, "mailbox participant not found")
	}
	return mailboxContactTx(tx, identity.OwnerConversationID, identity.ContactID)
}

// ListContacts returns main and every worker in the caller's room, including idle workers.
func (s *Store) ListContacts(ctx context.Context, identity types.MailboxIdentity) ([]types.Contact, error) {
	var contacts []types.Contact
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		if _, err := validateMailboxTx(tx, identity); err != nil {
			return err
		}
		var workers []types.Contact
		if err := tx.Select(&workers, `SELECT `+contactColumns+` FROM agents JOIN runs ON runs.id = agents.active_run_id
			WHERE agents.owner_conversation_id = ? ORDER BY agents.name, agents.id`, identity.OwnerConversationID); err != nil {
			return err
		}
		contacts = append([]types.Contact{{ID: "main", Name: "main", Kind: "main"}}, workers...)
		return nil
	})
	return contacts, err
}

// SendMail stores an addressed message without waking the recipient. Authorship
// is derived from the resolved identity; callers cannot create system mail.
func (s *Store) SendMail(ctx context.Context, identity types.MailboxIdentity, recipientID, subject, body string) (types.Mail, error) {
	recipientID, subject, body = strings.TrimSpace(recipientID), strings.TrimSpace(subject), strings.TrimSpace(body)
	switch {
	case subject == "" || body == "":
		return types.Mail{}, errors.New("mail subject and body must not be empty")
	case !utf8.ValidString(subject) || !utf8.ValidString(body):
		return types.Mail{}, errors.New("mail subject and body must be valid UTF-8")
	case len(subject) > MaxMailSubjectBytes || len(body) > MaxMailBodyBytes:
		return types.Mail{}, fmt.Errorf("mail subject must fit %d bytes and body %d bytes", MaxMailSubjectBytes, MaxMailBodyBytes)
	}
	var mail types.Mail
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		sender, err := validateMailboxTx(tx, identity)
		if err != nil {
			return err
		}
		if _, err := mailboxContactTx(tx, identity.OwnerConversationID, recipientID); err != nil {
			return err
		}
		mail = types.Mail{
			Sender:      types.Contact{ID: sender.ID, Name: sender.Name, Kind: sender.Kind},
			RecipientID: recipientID, Subject: subject, Body: body, Kind: "message", CreatedAt: now,
		}
		mail.ID, err = insertMailTx(tx, identity.OwnerConversationID, nil, mail)
		return err
	})
	if err != nil {
		return types.Mail{}, err
	}
	return mail, nil
}

func insertMailTx(tx *sqlx.Tx, ownerID string, eventID *string, mail types.Mail) (int64, error) {
	result, err := tx.Exec(`INSERT INTO mails (owner_conversation_id, event_id, sender_id, sender_name, sender_kind,
		recipient_id, subject, body, kind, agent_id, run_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ownerID, eventID, mail.Sender.ID, mail.Sender.Name, mail.Sender.Kind, mail.RecipientID,
		mail.Subject, mail.Body, mail.Kind, mail.AgentID, mail.RunID, mail.CreatedAt)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}
