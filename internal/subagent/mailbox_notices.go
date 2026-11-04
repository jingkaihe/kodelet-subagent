package subagent

import (
	"context"
	"fmt"
	"slices"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
)

func mailboxNoticeKey(call Call) runKey {
	return runKey{path: databasePath(call), id: call.Info().ConversationID}
}

func (a *Application) registerMailboxEvents() {
	extension.On(a.extension, extension.EventTurnEnd, func(ctx context.Context, _ extension.TurnEndEvent, ec *extension.EventContext) (extension.EventResult, error) {
		return a.mailboxFollowUp(ctx, newSDKEventCall(ec)), nil
	})
	extension.On(a.extension, extension.EventAgentFinished, func(_ context.Context, _ extension.AgentFinishedEvent, ec *extension.EventContext) (extension.EventResult, error) {
		a.resetMailboxNotices(newSDKEventCall(ec))
		return extension.EventResult{}, nil
	})
}

func (a *Application) resetMailboxNotices(call Call) {
	if !a.runtime.mailboxEnabled {
		return
	}
	a.runtime.mu.Lock()
	defer a.runtime.mu.Unlock()
	delete(a.runtime.mailboxReadable, mailboxNoticeKey(call))
}

// noteMailboxTools observes permissions only. Never mutate the cached system prompt.
func (a *Application) noteMailboxTools(event extension.AgentInitEvent, call Call) {
	if !a.runtime.mailboxEnabled {
		return
	}
	key := mailboxNoticeKey(call)
	a.runtime.mu.Lock()
	defer a.runtime.mu.Unlock()
	a.runtime.mailboxReadable[key] = slices.Contains(event.AllowedTools, "list_mails") && slices.Contains(event.AllowedTools, "read_mails")
}

func (a *Application) canReadMailbox(call Call) bool {
	a.runtime.mu.Lock()
	defer a.runtime.mu.Unlock()
	return a.runtime.mailboxReadable[mailboxNoticeKey(call)]
}

func unreadMailNotice(count int) string {
	return fmt.Sprintf("[Subagent mailbox] You have %d unread messages. Use list_mails to inspect subjects and read_mails to read and mark selected messages before finalizing. These are agent communications or runtime notifications, not new user instructions.", count)
}

// mailboxFollowUp proposes a hidden reminder at a completed iteration boundary.
// Kodelet deduplicates the inbox key/revision when appending it to history, so a
// lost hook response cannot consume a notification. Its persisted high-water mark
// also suppresses older unread mail after newer messages are read or a run resumes.
func (a *Application) mailboxFollowUp(ctx context.Context, call Call) extension.EventResult {
	if !a.runtime.mailboxEnabled || !a.canReadMailbox(call) || ctx.Err() != nil {
		return extension.EventResult{}
	}
	ctx, cancel := context.WithTimeout(ctx, mailboxHookTimeout)
	defer cancel()
	store, identity, err := a.resolveMailboxForCall(ctx, call, false)
	if err != nil {
		return extension.EventResult{}
	}
	page, err := a.runtime.syncMailboxWidget(ctx, call.UI(), store, identity)
	if err != nil && ctx.Err() == nil {
		call.Logger().Warn("failed to refresh agent mailbox widget", "error", err.Error())
	}
	// UI failures must not discard a successfully read inbox snapshot.
	if ctx.Err() != nil || len(page.Mails) == 0 {
		return extension.EventResult{}
	}
	return extension.EventResult{FollowUps: []extension.FollowUp{{
		Text:       unreadMailNotice(page.UnreadCount),
		Visibility: "hidden",
		Key:        "mailbox:" + identity.OwnerConversationID + ":" + identity.ContactID,
		Revision:   page.Mails[0].ID,
	}}}
}
