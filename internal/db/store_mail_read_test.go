package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func TestMailboxSnapshotsDoNotWaitForWriter(t *testing.T) {
	store, _ := mailboxStore(t, false)
	identity := mailboxIdentity(t, store, "owner", true)
	claim := mailAgent(t, store, "owner", "researcher")
	first, err := store.SendMail(t.Context(), types.MailboxIdentity{OwnerConversationID: "owner", ContactID: claim.Agent.ID}, "main", "Committed", "body")
	require.NoError(t, err)
	database, err := store.database()
	require.NoError(t, err)
	writer, err := database.BeginTxx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = writer.Rollback() }()
	_, err = insertMailTx(writer, identity.OwnerConversationID, nil, types.Mail{
		Sender:      types.Contact{ID: "main", Name: "main", Kind: "main"},
		RecipientID: "main", Subject: "Uncommitted", Body: "body", Kind: "message",
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	resolved, err := store.LookupMailbox(ctx, "owner", true)
	require.NoError(t, err)
	require.Equal(t, identity, resolved)
	// Active resolves of an existing room must not reconcile or acquire a writer.
	resolved, err = store.ResolveMailbox(ctx, "owner", true)
	require.NoError(t, err)
	require.Equal(t, identity, resolved)
	page, err := store.ListMails(ctx, identity, "unread", 3, 0)
	require.NoError(t, err)
	require.Less(t, time.Since(started), 200*time.Millisecond)
	require.Equal(t, 1, page.UnreadCount)
	require.Equal(t, 1, page.Total)
	require.Len(t, page.Mails, 1)
	require.Equal(t, first.ID, page.Mails[0].ID)
	require.NoError(t, writer.Commit())
	page, err = store.ListMails(t.Context(), identity, "unread", 3, 0)
	require.NoError(t, err)
	require.Equal(t, 2, page.UnreadCount)
	require.Len(t, page.Mails, 2)
}

func TestMailboxSnapshotDeadlineWhileReaderPoolBusy(t *testing.T) {
	store, _ := mailboxStore(t, false)
	identity := mailboxIdentity(t, store, "owner", true)
	database, err := store.readDatabase()
	require.NoError(t, err)
	database.SetMaxOpenConns(1)
	held, err := database.Connx(t.Context())
	require.NoError(t, err)
	defer held.Close()
	for _, operation := range []string{"lookup", "snapshot"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			started := time.Now()
			if operation == "lookup" {
				_, err = store.LookupMailbox(ctx, "owner", true)
			} else {
				_, err = store.ListMails(ctx, identity, "unread", 3, 0)
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Less(t, time.Since(started), 250*time.Millisecond)
		})
	}
}
