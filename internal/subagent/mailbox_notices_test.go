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

	result := execute(t, h, "list_agents", map[string]any{})
	require.Contains(t, result.Content, "[Subagent mailbox] You have 1 unread messages")
	require.NotContains(t, result.Content, "Private body")
	require.Equal(t, map[string]any{"agents": []any{}}, result.StructuredContent)
	end, err := h.HandleEvent(f.ctx, "agent.end", map[string]any{}, owner("owner-1"))
	require.NoError(t, err)
	require.Len(t, end.FollowUpMessages, 1)
	require.Contains(t, end.FollowUpMessages[0], "1 unread messages")
	require.NotContains(t, end.FollowUpMessages[0], "Private subject")
	end, err = h.HandleEvent(f.ctx, "agent.end", map[string]any{}, owner("owner-1"))
	require.NoError(t, err)
	require.Empty(t, end.FollowUpMessages, "the same unread batch must not keep a run alive")

	_, err = store.ReadMails(f.ctx, identity, []int64{mail.ID})
	require.NoError(t, err)
	f.app.restoreMailboxWidget(f.ctx, call)
	require.Contains(t, call.ui.text(mailboxWidgetID), "0 unread")
	end = f.app.mailboxFollowUp(f.ctx, call)
	require.Empty(t, end.FollowUpMessages)
}

func TestMailboxFollowUpsAreBoundedAndPermissionAware(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store, identity, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	_, err = store.SendMail(f.ctx, identity, "main", "Notice", "Read me")
	require.NoError(t, err)
	init := extension.AgentInitEvent{AllowedTools: []string{"list_mails", "read_mails"}}
	f.app.noteMailboxTools(extension.AgentInitEvent{}, call)
	require.Empty(t, f.app.mailboxFollowUp(f.ctx, call).FollowUpMessages)
	f.app.noteMailboxTools(init, call)
	for range maxMailboxFollowUps {
		_, err = store.SendMail(f.ctx, identity, "main", "New notice", "Read me too")
		require.NoError(t, err)
		require.Len(t, f.app.mailboxFollowUp(f.ctx, call).FollowUpMessages, 1)
	}
	_, err = store.SendMail(f.ctx, identity, "main", "One more", "No endless continuations")
	require.NoError(t, err)
	require.Empty(t, f.app.mailboxFollowUp(f.ctx, call).FollowUpMessages)
	f.app.resetMailboxNotices(call)
	f.app.noteMailboxTools(init, call)
	require.Len(t, f.app.mailboxFollowUp(f.ctx, call).FollowUpMessages, 1)
	other := f.call("owner-2")
	f.app.noteMailboxTools(init, other)
	require.Empty(t, f.app.mailboxFollowUp(f.ctx, other).FollowUpMessages)
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	require.Empty(t, f.app.mailboxFollowUp(ctx, call).FollowUpMessages)
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
	require.Contains(t, call.ui.text(mailboxWidgetID), "2 unread")
	require.Contains(t, call.ui.text(mailboxWidgetID), "researcher")
	require.Contains(t, call.ui.text(WidgetID), "completed")
}
