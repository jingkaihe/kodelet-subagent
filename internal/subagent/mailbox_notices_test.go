package subagent

import (
	"context"
	"testing"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func TestMailboxNoticesPreserveSystemPromptAndStructuredResults(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	mail, err := store.SendMail(f.ctx, identity, "main", "Private subject", "Private body")
	require.NoError(t, err)
	h := f.harness(t)
	init, err := h.HandleEvent(f.ctx, "agent.init", map[string]any{
		"systemPrompt": "Keep this cached", "allowedTools": []string{"list_mails", "read_mails"},
	}, owner("owner-1"))
	require.NoError(t, err)
	require.Nil(t, init.SystemPrompt)
	require.Empty(t, init.FollowUpMessages)
	require.Empty(t, init.FollowUps)

	result := execute(t, h, "list_agents", map[string]any{})
	require.NotContains(t, result.Content, "[Subagent mailbox]")
	require.NotContains(t, result.Content, "Private body")
	require.Equal(t, map[string]any{"agents": []any{}}, result.StructuredContent)
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
		require.NotContains(t, notice.Text, "Private subject")
		require.NotContains(t, notice.Text, "Private body")
	}

	_, err = store.ReadMails(f.ctx, identity, []int64{mail.ID})
	require.NoError(t, err)
	f.app.restoreMailboxWidget(f.ctx, call)
	require.Contains(t, call.ui.text(mailboxWidgetID), "0 unread")
	end = f.app.mailboxFollowUp(f.ctx, call)
	require.Empty(t, end.FollowUps)
}

func TestMailboxFollowUpsUseReplayableInboxRevisions(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	first, err := store.SendMail(f.ctx, identity, "main", "Notice", "Read me")
	require.NoError(t, err)
	init := extension.AgentInitEvent{AllowedTools: []string{"list_mails", "read_mails"}}
	f.app.noteMailboxTools(extension.AgentInitEvent{}, call)
	require.Empty(t, f.app.mailboxFollowUp(f.ctx, call).FollowUps)
	f.app.noteMailboxTools(init, call)
	initial := f.app.mailboxFollowUp(f.ctx, call)
	require.Len(t, initial.FollowUps, 1)
	require.Equal(t, first.ID, initial.FollowUps[0].Revision)
	require.Equal(t, initial, f.app.mailboxFollowUp(f.ctx, call), "only host acceptance advances the notification watermark")
	second, err := store.SendMail(f.ctx, identity, "main", "New notice", "Read me too")
	require.NoError(t, err)
	latest, err := store.SendMail(f.ctx, identity, "main", "Third notice", "One combined reminder")
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
	otherStore, otherIdentity, err := f.app.mailboxForCall(f.ctx, other)
	require.NoError(t, err)
	_, err = otherStore.SendMail(f.ctx, otherIdentity, "main", "Other room", "Independent watermark")
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
	store, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	for range mailboxWidgetLimit + 1 {
		_, err = store.SendMail(f.ctx, identity, "main", "A useful subject", "Do not put bodies in the widget")
		require.NoError(t, err)
	}
	f.app.restoreMailboxWidget(f.ctx, call)
	text := call.ui.text(mailboxWidgetID)
	require.Contains(t, text, "Inbox  4 unread")
	require.Contains(t, text, "main")
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
