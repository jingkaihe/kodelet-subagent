package subagent

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

type mailboxCountingStore struct {
	Store
	initializes, resolves, lookups, snapshots int
}

func (s *mailboxCountingStore) Initialize(ctx context.Context) error {
	s.initializes++
	return s.Store.Initialize(ctx)
}

func (s *mailboxCountingStore) ResolveMailbox(ctx context.Context, conversation string, allowRoot bool) (types.MailboxIdentity, error) {
	s.resolves++
	return s.Store.ResolveMailbox(ctx, conversation, allowRoot)
}

func (s *mailboxCountingStore) LookupMailbox(ctx context.Context, conversation string, allowRoot bool) (types.MailboxIdentity, error) {
	s.lookups++
	return s.Store.LookupMailbox(ctx, conversation, allowRoot)
}

func (s *mailboxCountingStore) ListMails(ctx context.Context, identity types.MailboxIdentity, status string, limit, offset int) (types.MailPage, error) {
	s.snapshots++
	return s.Store.ListMails(ctx, identity, status, limit, offset)
}

func TestMailboxHooksCacheMembershipAndReuseSnapshot(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store := &mailboxCountingStore{Store: f.runtime.store(call)}
	f.runtime.stores[store.Path()] = store
	h := f.harness(t)
	for range 3 {
		_, err := h.HandleEvent(f.ctx, "agent.init", map[string]any{
			"allowedTools": []string{"list_agent_messages", "read_agent_messages"},
		}, owner("owner-1"))
		require.NoError(t, err)
	}
	require.Zero(t, store.initializes)
	require.Zero(t, store.resolves)
	require.Zero(t, store.lookups)
	require.Zero(t, store.snapshots)
	_, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	claim, err := store.Create(f.ctx, identity.OwnerConversationID, "researcher", "task", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.SendMail(f.ctx, types.MailboxIdentity{OwnerConversationID: identity.OwnerConversationID, ContactID: claim.Agent.ID}, "main", "Notice", "body")
	require.NoError(t, err)
	for range 3 {
		_, same, err := f.app.mailboxForCall(f.ctx, call)
		require.NoError(t, err)
		require.Equal(t, identity, same)
		require.Len(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps, 1)
	}
	require.Equal(t, 1, store.resolves)
	require.Zero(t, store.initializes)
	require.Zero(t, store.lookups)
	require.Equal(t, 3, store.snapshots, "one inbox snapshot per turn.end")
	// An unchanged inbox must not call the UI again.
	call.ui.block, call.ui.blockStarted = newEvent(), newEvent()
	defer call.ui.block.set()
	require.Len(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps, 1)
	select {
	case <-call.ui.blockStarted.ch:
		t.Fatal("unchanged inbox was rendered again")
	default:
	}

	// The same conversation in another database must not reuse this membership.
	other := f.call("owner-1")
	other.dataDir = filepath.Join(t.TempDir(), "other")
	_, _, err = f.app.resolveMailboxForCall(f.ctx, other, false)
	require.Error(t, err)
	require.NotContains(t, f.runtime.mailboxIdentities, mailboxNoticeKey(other))
	_, _, err = f.app.resolveMailboxForCall(f.ctx, f.call("owner-2"), false)
	require.ErrorIs(t, err, db.ErrAgentNotFound)
}

func TestMailboxHooksRetryUnattachedChildMembership(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	parent := f.call("owner-1")
	store, _, err := f.app.mailboxForCall(f.ctx, parent)
	require.NoError(t, err)
	child := f.call("child-conversation", invokedBy("subagent"))
	_, _, err = f.app.resolveMailboxForCall(f.ctx, child, false)
	require.ErrorIs(t, err, db.ErrAgentNotFound)
	claim, err := store.Create(f.ctx, "owner-1", "researcher", "task", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.AttachConversation(f.ctx, claim.Lease, "child-conversation")
	require.NoError(t, err)
	_, identity, err := f.app.resolveMailboxForCall(f.ctx, child, false)
	require.NoError(t, err)
	require.Equal(t, claim.Agent.ID, identity.ContactID)
	require.Equal(t, "owner-1", identity.OwnerConversationID)
}

func TestMailboxWidgetFailureKeepsReminderAndIsRetryable(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	claim, err := store.Create(f.ctx, identity.OwnerConversationID, "researcher", "task", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.SendMail(f.ctx, types.MailboxIdentity{OwnerConversationID: identity.OwnerConversationID, ContactID: claim.Agent.ID}, "main", "Notice", "body")
	require.NoError(t, err)
	call.ui.block, call.ui.blockStarted = newEvent(), newEvent()
	defer call.ui.block.set()
	page, err := f.runtime.syncMailboxWidget(f.ctx, call.ui, store, identity)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, page.Mails, 1)
	require.NotContains(t, f.runtime.mailboxWidgets, mailboxWidgetKey(store, identity))
	f.app.noteMailboxTools(extension.AgentInitEvent{AllowedTools: []string{"list_agent_messages", "read_agent_messages"}}, call)
	require.Len(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps, 1)
	call.ui.block.set()
	require.Len(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps, 1)
	require.Contains(t, call.ui.text(mailboxWidgetID), "1 unread")
}

func TestMailboxNoticesPreserveSystemPromptAndStructuredResults(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	sender, _ := mailboxSender(t, f, store, "owner-1")
	mail, err := store.SendMail(f.ctx, sender, "main", "Private subject", "Private body")
	require.NoError(t, err)
	h := f.harness(t)
	init, err := h.HandleEvent(f.ctx, "agent.init", map[string]any{
		"systemPrompt": "Keep this cached", "allowedTools": []string{"list_agent_messages", "read_agent_messages"},
	}, owner("owner-1"))
	require.NoError(t, err)
	require.Nil(t, init.SystemPrompt)
	require.Empty(t, init.FollowUpMessages)
	require.Empty(t, init.FollowUps)

	result := execute(t, h, "list_agents", map[string]any{})
	require.NotContains(t, result.Content, "[Subagent mailbox]")
	require.NotContains(t, result.Content, "Private body")
	require.Len(t, result.StructuredContent.(map[string]any)["agents"], 1)
	end, err := h.HandleEvent(f.ctx, "agent.end", map[string]any{}, owner("owner-1"))
	require.NoError(t, err)
	require.Empty(t, end.FollowUpMessages)
	require.Empty(t, end.FollowUps, "mailbox reminders belong to turn.end, not agent.end")
	for _, text := range []string{"", "   ", "Working on the task"} {
		end, err = h.HandleEvent(f.ctx, "turn.end", map[string]any{"response": text, "turnNumber": 1}, owner("owner-1"))
		require.NoError(t, err)
		require.Empty(t, end.FollowUpMessages)
		require.Len(t, end.FollowUps, 1)
		notice := end.FollowUps[0]
		require.Equal(t, "hidden", notice.Visibility)
		require.Equal(t, "mailbox:owner-1:main", notice.Key)
		require.Equal(t, mail.ID, notice.Revision)
		require.Contains(t, notice.Text, "1 unread messages")
		require.Contains(t, notice.Text, "list_agent_messages")
		require.Contains(t, notice.Text, "read_agent_messages")
		require.NotContains(t, notice.Text, "Private subject")
		require.NotContains(t, notice.Text, "Private body")
	}

	_, err = store.ReadMails(f.ctx, identity, []int64{mail.ID})
	require.NoError(t, err)
	f.app.restoreMailboxWidget(f.ctx, call)
	require.Empty(t, call.ui.text(mailboxWidgetID))
	end = f.app.mailboxFollowUp(f.ctx, call)
	require.Empty(t, end.FollowUps)
}

func TestMailboxFollowUpsUseReplayableInboxRevisions(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	sender, _ := mailboxSender(t, f, store, "owner-1")
	first, err := store.SendMail(f.ctx, sender, "main", "Notice", "Read me")
	require.NoError(t, err)
	init := extension.AgentInitEvent{AllowedTools: []string{"list_agent_messages", "read_agent_messages"}}
	for _, allowed := range [][]string{nil, {"list_agent_messages"}, {"read_agent_messages"}} {
		f.app.noteMailboxTools(extension.AgentInitEvent{AllowedTools: allowed}, call)
		require.Empty(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps)
	}
	f.app.noteMailboxTools(init, call)
	initial := f.app.mailboxFollowUp(f.ctx, call)
	require.Len(t, initial.FollowUps, 1)
	require.Equal(t, first.ID, initial.FollowUps[0].Revision)
	require.Equal(t, initial, f.app.mailboxFollowUp(f.ctx, call), "only host acceptance advances the notification watermark")
	second, err := store.SendMail(f.ctx, sender, "main", "New notice", "Read me too")
	require.NoError(t, err)
	latest, err := store.SendMail(f.ctx, sender, "main", "Third notice", "One combined reminder")
	require.NoError(t, err)
	batch := f.app.mailboxFollowUp(f.ctx, call)
	require.Len(t, batch.FollowUps, 1)
	require.Equal(t, initial.FollowUps[0].Key, batch.FollowUps[0].Key)
	require.Equal(t, latest.ID, batch.FollowUps[0].Revision)
	require.Contains(t, batch.FollowUps[0].Text, "3 unread messages")
	_, err = store.ReadMails(f.ctx, identity, []int64{latest.ID, second.ID})
	require.NoError(t, err)
	require.Equal(t, initial, f.app.mailboxFollowUp(f.ctx, call), "older revisions let the host suppress read-count-only changes")
	f.app.resetMailboxNotices(call)
	require.Empty(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps)
	f.app.noteMailboxTools(init, call)
	require.Equal(t, initial, f.app.mailboxFollowUp(f.ctx, call), "new runs retain the same deduplication key")
	_, err = store.ReadMails(f.ctx, identity, []int64{first.ID})
	require.NoError(t, err)
	require.Empty(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps)
	other := f.call("owner-2")
	f.app.noteMailboxTools(init, other)
	require.Empty(t, f.app.mailboxFollowUp(f.ctx, other).FollowUps)
	otherStore, _, err := f.app.mailboxForCall(f.ctx, other)
	require.NoError(t, err)
	otherSender, _ := mailboxSender(t, f, otherStore, "owner-2")
	_, err = otherStore.SendMail(f.ctx, otherSender, "main", "Other room", "Independent watermark")
	require.NoError(t, err)
	otherNotice := f.app.mailboxFollowUp(f.ctx, other)
	require.Len(t, otherNotice.FollowUps, 1)
	require.NotEqual(t, initial.FollowUps[0].Key, otherNotice.FollowUps[0].Key)
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	require.Empty(t, f.app.mailboxFollowUp(ctx, other).FollowUps)
}

func TestMailboxWidgetShowsAttributedHeadersNotBodies(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store, _, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	sender, _ := mailboxSender(t, f, store, "owner-1")
	for range mailboxWidgetLimit + 1 {
		_, err = store.SendMail(f.ctx, sender, "main", "A useful subject", "Do not put bodies in the widget")
		require.NoError(t, err)
	}
	f.app.restoreMailboxWidget(f.ctx, call)
	text := call.ui.text(mailboxWidgetID)
	require.Contains(t, text, "Inbox  4 unread")
	require.Contains(t, text, "mail-sender")
	require.Contains(t, text, "A useful subject")
	require.Contains(t, text, "… and 1 more")
	require.NotContains(t, text, "Do not put bodies")
	require.Empty(t, call.ui.text(WidgetID))
}

func TestMailboxLifecycleRefreshesMainWidget(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "researcher", Task: "research", ContextMode: types.ContextFresh}))
	require.Equal(t, "result for research", f.wait(call, id, 1000).Content)
	waitUntil(t, func() bool { return f.ownedCount() == 0 })
	require.Contains(t, call.ui.text(mailboxWidgetID), "1 unread")
	require.Contains(t, call.ui.text(mailboxWidgetID), "researcher")
	require.Contains(t, call.ui.text(WidgetID), "completed")
}

func TestMailboxPassiveHooksDoNotEnrollInactiveConversation(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	h := f.harness(t)
	for _, event := range []string{"session.start", "agent.start", "agent.init", "turn.end"} {
		result, err := h.HandleEvent(f.ctx, event, map[string]any{
			"allowedTools": []string{"list_agent_messages", "read_agent_messages"},
		}, owner("owner-1"))
		require.NoError(t, err)
		require.Empty(t, result.FollowUps)
	}
	f.app.restoreMailboxWidget(f.ctx, call)
	require.Empty(t, call.ui.text(mailboxWidgetID))
	require.Empty(t, f.runtime.mailboxWidgets)
	require.Empty(t, f.runtime.mailboxIdentities)
	database, err := db.Open(databasePath(call))
	require.NoError(t, err)
	defer database.Close()
	var rooms int
	require.NoError(t, database.Get(&rooms, "SELECT COUNT(*) FROM mailbox_rooms"))
	require.Zero(t, rooms, "passive hooks must not enroll conversations")

	// Explicit mailbox use can still opt in, but an empty inbox stays hidden.
	store, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	f.app.refreshMailboxWidget(f.ctx, call, store, identity)
	require.Empty(t, call.ui.text(mailboxWidgetID))
	require.NoError(t, database.Get(&rooms, "SELECT COUNT(*) FROM mailbox_rooms"))
	require.Equal(t, 1, rooms)
}

func TestMailboxReadingLastMailRemovesWidget(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	claim, err := store.Create(f.ctx, identity.OwnerConversationID, "researcher", "task", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	mail, err := store.SendMail(f.ctx, types.MailboxIdentity{OwnerConversationID: identity.OwnerConversationID, ContactID: claim.Agent.ID}, "main", "Notice", "body")
	require.NoError(t, err)
	f.app.noteMailboxTools(extension.AgentInitEvent{AllowedTools: []string{"list_agent_messages", "read_agent_messages"}}, call)
	require.Len(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps, 1)
	require.Contains(t, call.ui.text(mailboxWidgetID), "1 unread")
	result := f.app.ReadMails(f.ctx, ReadMailsInput{IDs: []int64{mail.ID}}, call)
	require.Empty(t, result.Error)
	require.Empty(t, call.ui.text(mailboxWidgetID))
	require.Empty(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps)
	require.Empty(t, call.ui.text(mailboxWidgetID))
}
