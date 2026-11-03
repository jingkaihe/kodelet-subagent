package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

type mailRow struct {
	ID          int64    `db:"id"`
	SenderID    string   `db:"sender_id"`
	SenderName  string   `db:"sender_name"`
	SenderKind  string   `db:"sender_kind"`
	RecipientID string   `db:"recipient_id"`
	Subject     string   `db:"subject"`
	Body        string   `db:"body"`
	Kind        string   `db:"kind"`
	AgentID     string   `db:"agent_id"`
	RunID       string   `db:"run_id"`
	CreatedAt   float64  `db:"created_at"`
	ReadAt      *float64 `db:"read_at"`
}

const mailColumns = `id, sender_id, sender_name, sender_kind, recipient_id, subject, kind, agent_id, run_id, created_at, read_at`

func (r mailRow) mail() types.Mail {
	return types.Mail{
		ID: r.ID, Sender: types.Contact{ID: r.SenderID, Name: r.SenderName, Kind: r.SenderKind},
		RecipientID: r.RecipientID, Subject: r.Subject, Body: r.Body, Kind: r.Kind,
		AgentID: r.AgentID, RunID: r.RunID, CreatedAt: r.CreatedAt, ReadAt: r.ReadAt,
	}
}

// ListMails returns metadata newest first without marking messages read. Total
// counts the selected status; UnreadCount counts the entire caller's inbox.
func (s *Store) ListMails(ctx context.Context, identity types.MailboxIdentity, status string, limit, offset int) (types.MailPage, error) {
	if limit < 1 || limit > MaxMailListLimit || offset < 0 {
		return types.MailPage{}, fmt.Errorf("mail limit must be between 1 and %d and offset nonnegative", MaxMailListLimit)
	}
	filter := ""
	switch status {
	case "unread":
		filter = " AND read_at IS NULL"
	case "read":
		filter = " AND read_at IS NOT NULL"
	case "all":
	default:
		return types.MailPage{}, errors.New("mail status must be unread, read, or all")
	}
	page := types.MailPage{Mails: []types.Mail{}}
	err := s.transaction(ctx, func(tx *sqlx.Tx, _ float64) error {
		if _, err := validateMailboxTx(tx, identity); err != nil {
			return err
		}
		const scope = ` FROM mails WHERE owner_conversation_id = ? AND recipient_id = ?`
		if err := tx.Get(&page.Total, `SELECT COUNT(*)`+scope+filter, identity.OwnerConversationID, identity.ContactID); err != nil {
			return err
		}
		if err := tx.Get(&page.UnreadCount, `SELECT COUNT(*)`+scope+` AND read_at IS NULL`, identity.OwnerConversationID, identity.ContactID); err != nil {
			return err
		}
		var rows []mailRow
		if err := tx.Select(&rows, `SELECT `+mailColumns+scope+filter+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			identity.OwnerConversationID, identity.ContactID, limit, offset); err != nil {
			return err
		}
		for _, row := range rows {
			page.Mails = append(page.Mails, row.mail())
		}
		page.HasMore = offset < page.Total && len(rows) < page.Total-offset
		return nil
	})
	if err != nil {
		return types.MailPage{}, err
	}
	return page, nil
}

// ReadMails atomically retrieves the selected messages and marks them read.
// Reads are replayable and keep their first read timestamp. The combined
// subject/body byte budget is checked before any read-state changes.
func (s *Store) ReadMails(ctx context.Context, identity types.MailboxIdentity, ids []int64) ([]types.Mail, error) {
	if len(ids) == 0 || len(ids) > MaxReadMails {
		return nil, fmt.Errorf("read between 1 and %d mail IDs at a time", MaxReadMails)
	}
	seen := make(map[int64]bool, len(ids))
	for _, id := range ids {
		if id <= 0 || seen[id] {
			return nil, errors.New("mail IDs must be positive and unique")
		}
		seen[id] = true
	}
	mails := make([]types.Mail, 0, len(ids))
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := validateMailboxTx(tx, identity); err != nil {
			return err
		}
		totalBytes := 0
		for _, id := range ids {
			var row mailRow
			err := tx.Get(&row, `SELECT `+mailColumns+`, body FROM mails
				WHERE owner_conversation_id = ? AND recipient_id = ? AND id = ?`, identity.OwnerConversationID, identity.ContactID, id)
			if errors.Is(err, sql.ErrNoRows) {
				return newError(kindNotFound, "mail not found in your inbox")
			}
			if err != nil {
				return err
			}
			totalBytes += len(row.Subject) + len(row.Body)
			if totalBytes > MaxMailReadBytes {
				return fmt.Errorf("selected mail exceeds %d bytes; read fewer IDs", MaxMailReadBytes)
			}
			mails = append(mails, row.mail())
		}
		for i := range mails {
			mail := &mails[i]
			if _, err := tx.Exec(`UPDATE mails SET read_at = COALESCE(read_at, ?)
				WHERE owner_conversation_id = ? AND recipient_id = ? AND id = ?`, now, identity.OwnerConversationID, identity.ContactID, mail.ID); err != nil {
				return err
			}
			if mail.ReadAt == nil {
				mail.ReadAt = new(now)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return mails, nil
}
