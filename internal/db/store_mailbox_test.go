package db

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func mailboxStore(t *testing.T, enabled bool) (*Store, *mutableClock) {
	t.Helper()
	clock := newMutableClock()
	store := NewStore(filepath.Join(t.TempDir(), Filename), "mail-runtime", clock.now, MailboxEnabled(enabled))
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Initialize(t.Context()))
	return store, clock
}

func mailboxIdentity(t *testing.T, store *Store, conversation string, allowRoot bool) types.MailboxIdentity {
	t.Helper()
	identity, err := store.ResolveMailbox(t.Context(), conversation, allowRoot)
	require.NoError(t, err)
	return identity
}

func mailAgent(t *testing.T, store *Store, owner, name string) types.Claim {
	t.Helper()
	claim, err := store.Create(t.Context(), owner, name, name+" task", t.TempDir(), types.ContextFresh)
	require.NoError(t, err)
	return claim
}

func startMailAgent(t *testing.T, store *Store, claim types.Claim) types.MailboxIdentity {
	t.Helper()
	conversation := "child-" + claim.Agent.ID
	_, err := store.MarkRunning(t.Context(), claim.Lease, conversation)
	require.NoError(t, err)
	return mailboxIdentity(t, store, conversation, false)
}

func TestMailboxResolvesChildFirstAndScopesContacts(t *testing.T) {
	store, _ := mailboxStore(t, false)
	main := mailboxIdentity(t, store, "owner", true)
	require.Equal(t, types.MailboxIdentity{OwnerConversationID: "owner", ContactID: "main"}, main)
	first := mailAgent(t, store, "owner", "researcher")
	child := startMailAgent(t, store, first)
	second := mailAgent(t, store, "owner", "reviewer")
	other := mailAgent(t, store, "other-owner", "outsider")
	// Even an allowRoot caller and an existing owner row cannot hide child membership.
	mailAgent(t, store, "child-"+first.Agent.ID, "nested-record")
	require.Equal(t, child, mailboxIdentity(t, store, "child-"+first.Agent.ID, true))
	_, err := store.ResolveMailbox(t.Context(), "unknown-child", false)
	require.ErrorIs(t, err, ErrAgentNotFound)
	_, err = store.ResolveMailbox(t.Context(), " 	", true)
	require.Error(t, err)
	contacts, err := store.ListContacts(t.Context(), child)
	require.NoError(t, err)
	require.Equal(t, []types.Contact{
		{ID: "main", Name: "main", Kind: "main"},
		{ID: first.Agent.ID, Name: "researcher", Kind: "agent", Status: "running", Task: "researcher task"},
		{ID: second.Agent.ID, Name: "reviewer", Kind: "agent", Status: "starting", Task: "reviewer task"},
	}, contacts)
	for _, invalid := range []types.MailboxIdentity{
		{},
		{OwnerConversationID: "owner", ContactID: "system"},
		{OwnerConversationID: "owner", ContactID: other.Agent.ID},
		{OwnerConversationID: "unregistered-owner", ContactID: "main"},
	} {
		_, err := store.ListContacts(t.Context(), invalid)
		require.ErrorIs(t, err, ErrAgentNotFound)
	}
}

func TestMailboxMessagingReadStateAndReplay(t *testing.T) {
	store, clock := mailboxStore(t, false)
	main := mailboxIdentity(t, store, "owner", true)
	first := startMailAgent(t, store, mailAgent(t, store, "owner", "researcher"))
	second := startMailAgent(t, store, mailAgent(t, store, "owner", "reviewer"))
	other := startMailAgent(t, store, mailAgent(t, store, "other-owner", "outsider"))
	send := func(from types.MailboxIdentity, recipient, subject string) types.Mail {
		t.Helper()
		mail, err := store.SendMail(t.Context(), from, recipient, subject, subject+" body")
		require.NoError(t, err)
		require.Equal(t, "message", mail.Kind)
		require.Equal(t, from.ContactID, mail.Sender.ID)
		require.Empty(t, mail.AgentID)
		require.Empty(t, mail.RunID)
		return mail
	}
	toWorker := send(main, first.ContactID, "assignment")
	toMain := send(first, "main", "question")
	toPeer := send(second, first.ContactID, "findings")
	toOther := send(other, "main", "other room")
	require.Equal(t, "agent", toMain.Sender.Kind)
	require.Equal(t, "researcher", toMain.Sender.Name)
	require.Equal(t, "main", toWorker.Sender.Kind)

	for _, target := range []string{"system", "missing", other.ContactID} {
		_, err := store.SendMail(t.Context(), first, target, "hello", "body")
		require.ErrorIs(t, err, ErrAgentNotFound)
	}
	_, err := store.SendMail(t.Context(), types.MailboxIdentity{OwnerConversationID: "owner", ContactID: "system"}, "main", "fake", "notice")
	require.ErrorIs(t, err, ErrAgentNotFound)

	page, err := store.ListMails(t.Context(), first, "unread", 1, 0)
	require.NoError(t, err)
	require.Equal(t, 2, page.Total)
	require.Equal(t, 2, page.UnreadCount)
	require.True(t, page.HasMore)
	require.Len(t, page.Mails, 1)
	require.Equal(t, toPeer.ID, page.Mails[0].ID)
	require.Empty(t, page.Mails[0].Body)
	require.Nil(t, page.Mails[0].ReadAt)
	page, err = store.ListMails(t.Context(), first, "unread", 1, 1)
	require.NoError(t, err)
	require.False(t, page.HasMore)
	require.Equal(t, toWorker.ID, page.Mails[0].ID)

	// A valid ID followed by an inaccessible or missing ID changes nothing.
	for _, foreign := range []int64{toMain.ID, toOther.ID, 99999} {
		mails, err := store.ReadMails(t.Context(), first, []int64{toWorker.ID, foreign})
		require.ErrorIs(t, err, ErrAgentNotFound)
		require.Nil(t, mails)
	}
	page, err = store.ListMails(t.Context(), first, "unread", 20, 0)
	require.NoError(t, err)
	require.Equal(t, 2, page.UnreadCount)
	mails, err := store.ReadMails(t.Context(), first, []int64{toPeer.ID, toWorker.ID})
	require.NoError(t, err)
	require.Equal(t, "findings body", mails[0].Body)
	require.Equal(t, toWorker.ID, mails[1].ID)
	require.Equal(t, clock.now(), *mails[0].ReadAt)
	clock.advance(1)
	replayed, err := store.ReadMails(t.Context(), first, []int64{toPeer.ID, toWorker.ID})
	require.NoError(t, err)
	require.Equal(t, mails, replayed)

	require.NoError(t, store.Close())
	reopened := NewStore(store.Path(), "new-runtime", clock.now)
	t.Cleanup(func() { _ = reopened.Close() })
	for _, status := range []string{"all", "read"} {
		page, err = reopened.ListMails(t.Context(), first, status, 20, 0)
		require.NoError(t, err)
		require.Equal(t, 2, page.Total)
		require.Zero(t, page.UnreadCount)
	}
	page, err = reopened.ListMails(t.Context(), first, "unread", 20, 0)
	require.NoError(t, err)
	require.Equal(t, []types.Mail{}, page.Mails)
	page, err = reopened.ListMails(t.Context(), main, "unread", 20, 0)
	require.NoError(t, err)
	require.Equal(t, 1, page.UnreadCount)
	require.Equal(t, toMain.ID, page.Mails[0].ID)
}

func TestMailboxLimitsAndAtomicReads(t *testing.T) {
	store, _ := mailboxStore(t, false)
	main := mailboxIdentity(t, store, "owner", true)
	for _, pair := range [][2]string{
		{" 	", "body"},
		{"subject", ""},
		{strings.Repeat("界", MaxMailSubjectBytes/3+1), "body"},
		{"subject", strings.Repeat("界", MaxMailBodyBytes/3+1)},
		{"invalid\xff", "body"},
		{"subject", "invalid\xff"},
	} {
		_, err := store.SendMail(t.Context(), main, "main", pair[0], pair[1])
		require.Error(t, err)
	}
	var ids []int64
	for range 4 {
		mail, err := store.SendMail(t.Context(), main, "main", strings.Repeat("s", MaxMailSubjectBytes), strings.Repeat("b", MaxMailBodyBytes))
		require.NoError(t, err)
		ids = append(ids, mail.ID)
	}
	_, err := store.ReadMails(t.Context(), main, ids)
	require.ErrorContains(t, err, "read fewer IDs")
	page, err := store.ListMails(t.Context(), main, "unread", 20, 0)
	require.NoError(t, err)
	require.Equal(t, 4, page.UnreadCount)
	for _, invalid := range [][]int64{nil, {0}, {-1}, {ids[0], ids[0]}, make([]int64, MaxReadMails+1)} {
		_, err := store.ReadMails(t.Context(), main, invalid)
		require.Error(t, err)
	}
	for _, input := range []struct {
		status string
		limit  int
		offset int
	}{{"invalid", 20, 0}, {"all", 0, 0}, {"all", MaxMailListLimit + 1, 0}, {"all", 20, -1}} {
		_, err := store.ListMails(t.Context(), main, input.status, input.limit, input.offset)
		require.Error(t, err)
	}
	// Even a failure during the second UPDATE rolls back the first read mark.
	database := rawDB(t, store.Path())
	database.MustExec(`CREATE TRIGGER fail_read BEFORE UPDATE OF read_at ON mails
		WHEN OLD.id = 2 BEGIN SELECT RAISE(ABORT, 'read failed'); END`)
	_, err = store.ReadMails(t.Context(), main, ids[:2])
	require.ErrorContains(t, err, "read failed")
	page, err = store.ListMails(t.Context(), main, "unread", 20, 0)
	require.NoError(t, err)
	require.Equal(t, 4, page.UnreadCount)
	database.MustExec(`DROP TRIGGER fail_read`)
	mails, err := store.ReadMails(t.Context(), main, ids[:2])
	require.NoError(t, err)
	require.Len(t, mails, 2)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = store.ReadMails(ctx, main, ids[2:])
	require.ErrorIs(t, err, context.Canceled)
	page, err = store.ListMails(t.Context(), main, "unread", 20, 100)
	require.NoError(t, err)
	require.Equal(t, 2, page.UnreadCount)
	require.Empty(t, page.Mails)
	require.False(t, page.HasMore)
}
