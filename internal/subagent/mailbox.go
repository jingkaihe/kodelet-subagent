package subagent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

const (
	mailboxWidgetID      = "agent-mailbox"
	mailboxWidgetLimit   = 3
	mailboxHookTimeout   = 2 * time.Second
	mailboxWidgetTimeout = 200 * time.Millisecond
)

// mailboxForCall derives membership from the host's conversation identity, never tool input.
func (a *Application) mailboxForCall(ctx context.Context, call Call) (Store, types.MailboxIdentity, error) {
	if !a.runtime.mailboxEnabled {
		return nil, types.MailboxIdentity{}, errors.New("agent mailbox is disabled; set KODELET_AGENT_MAILBOX=enabled on the runner")
	}
	conversationID, err := ownerConversationID(call)
	if err != nil {
		return nil, types.MailboxIdentity{}, err
	}
	store, err := a.runtime.storeForCall(ctx, call)
	if err != nil {
		return nil, types.MailboxIdentity{}, err
	}
	identity, err := store.ResolveMailbox(ctx, conversationID, !isAgentChild(call))
	return store, identity, err
}

func (a *Application) restoreMailboxWidget(ctx context.Context, call Call) {
	if !a.runtime.mailboxEnabled || strings.TrimSpace(call.Info().ConversationID) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, mailboxHookTimeout)
	defer cancel()
	store, identity, err := a.mailboxForCall(ctx, call)
	if err != nil {
		// A child's session.start may precede AttachConversation. agent.init will retry.
		call.Logger().Debug("mailbox not yet available", "error", err.Error())
		return
	}
	a.refreshMailboxWidget(ctx, call, store, identity)
}

func (a *Application) refreshMailboxWidget(ctx context.Context, call Call, store Store, identity types.MailboxIdentity) {
	if err := a.runtime.syncMailboxWidget(ctx, call.UI(), store, identity); err != nil && ctx.Err() == nil {
		call.Logger().Warn("failed to refresh agent mailbox widget", "error", err.Error())
	}
}

func (r *Runtime) syncMailboxWidget(ctx context.Context, ui WidgetUI, store Store, identity types.MailboxIdentity) error {
	if !r.mailboxEnabled || ui == nil {
		return nil
	}
	timeout := min(mailboxWidgetTimeout, r.timing.HostRequestTimeout)
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < 2*timeout {
		// Keep response-delivery headroom when the database operation ran slowly.
		return nil
	}
	lock := r.widgetLock(store, "mailbox:"+identity.OwnerConversationID+":"+identity.ContactID)
	// Refreshes are best-effort, including after committed send/read operations.
	// Never queue behind another refresh or spend the tool's deadline on the UI.
	if !lock.TryLock() {
		return nil
	}
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	page, err := store.ListMails(ctx, identity, "unread", mailboxWidgetLimit, 0)
	if err != nil {
		return err
	}
	return ui.SetWidget(ctx, mailboxWidgetID, mailboxWidgetLines(page), extension.WidgetAboveComposer)
}

func mailboxWidgetLines(page types.MailPage) []extension.FrameLine {
	lines := []extension.FrameLine{extension.Styled(
		extension.StyledSpan{Text: "Inbox", Style: boldStyle},
		extension.StyledSpan{Text: fmt.Sprintf("  %d unread", page.UnreadCount), Style: dimStyle},
	)}
	for _, mail := range page.Mails {
		lines = append(lines, extension.Styled(
			extension.StyledSpan{Text: fmt.Sprintf("#%d  %s", mail.ID, collapseWhitespace(mail.Sender.Name))},
			extension.StyledSpan{Text: "  " + truncate(collapseWhitespace(mail.Subject), 100), Style: dimStyle},
		))
	}
	if page.UnreadCount > len(page.Mails) {
		lines = append(lines, extension.Styled(extension.StyledSpan{
			Text: fmt.Sprintf("… and %d more", page.UnreadCount-len(page.Mails)), Style: dimStyle,
		}))
	}
	return lines
}
